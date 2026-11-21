#!/usr/bin/env bash
# Check separate-mode shards on Ubuntu; run as root.
# Use botctl and the running botd from the same release (mutual authentication).
# Set CONTROL_SOCKET to the configured absolute Unix socket path when not using TCP.

update_reply() {
  python3 -c '
import json, re, sys
try:
    reply = json.load(sys.stdin)
except ValueError:
    sys.exit()
if not isinstance(reply, dict):
    sys.exit()
op = reply.get("operation")
op_id = reply.get("operation_id")
if not isinstance(op_id, str) or not re.fullmatch(r"[a-f0-9]{32}", op_id):
    op_id = ""
state = ""
reason = reply.get("error")
if (op_id and isinstance(op, dict)
        and op.get("operation_id") == op_id and op.get("command") == "gateway-update"):
    op_state = op.get("state")
    if op_state in ("running", "failed") or (op_state == "succeeded" and reply.get("ok") is True):
        state = op_state
        reason = op.get("waiting_reason") or reason
        if not reason and state == "failed" and isinstance(op.get("shards"), list):
            reason = next((row["error"] for row in op["shards"]
                           if isinstance(row, dict) and row.get("error")), None)
print(op_id)
print(state)
print("unknown" if reply.get("ok") is False and reply.get("outcome_unknown") is True else "")
print(reason if isinstance(reason, str) else "")
'
}

check_gateway_update() {
  local round=$1
  shift
  local start=$SECONDS initial response state reason
  local -a fields=()
  initial=$(ctl "$@") || :
  mapfile -t fields < <(update_reply <<< "$initial")
  id=${fields[0]:-}
  state=${fields[1]:-}
  reason=$(printf '%s\n' "${fields[@]:3}")

  if [ -z "$state" ] && { [ -z "$id" ] || [ "${fields[2]:-}" != unknown ]; }; then
    record FAIL "R$round update" "${reason:-no confirmed operation or queryable outcome}"
    return 1
  fi

  while [ $((SECONDS-start)) -lt 310 ] && [ "$state" != succeeded ] && [ "$state" != failed ]; do
    response=$(ctl operation "$id") || :
    mapfile -t fields < <(update_reply <<< "$response")
    if [ "${fields[0]:-}" = "$id" ] && [ -n "${fields[1]:-}" ]; then
      state=${fields[1]}
      reason=$(printf '%s\n' "${fields[@]:3}")
    fi
    case "$state" in succeeded|failed) break;; esac
    sleep 0.5
  done
  echo "Operation elapsed: $((SECONDS-start)) s"
  if [ "$state" = succeeded ]; then
    record PASS "R$round update" 'operation succeeded'
  else
    record FAIL "R$round update" "operation state: ${state:-unknown}${reason:+ ($reason)}"
  fi
  return 0
}

# Let the self-test exercise the update request and polling without live processes.
if [[ ${BASH_SOURCE[0]} != "$0" ]]; then return 0; fi

set -u
set -o pipefail
USER_NAME=discord-rs
# The runtime root can be overridden for a non-default layout: ROOT=/path update-check.sh BOT
ROOT=${ROOT:-/var/lib/discord-rs/runtime}
SECRET=/etc/discord-rs/botd.secret
BOTCTL=/opt/discord-rs/bin/botctl
PORT=${PORT:-47321}
control_endpoint=(--port "$PORT")
if [ -n "${CONTROL_SOCKET:-}" ]; then
  control_endpoint=(--socket "$CONTROL_SOCKET")
fi
bot=test
repeat=1
selected_shard=
shard_given=0
bot_set=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --repeat)
      [ "$#" -ge 2 ] || { echo 'Missing repeat count' >&2; exit 2; }
      repeat=$2; shift 2 ;;
    --shard)
      [ "$#" -ge 2 ] && [ "$shard_given" -eq 0 ] || { echo 'Missing or duplicate shard' >&2; exit 2; }
      [[ "$2" =~ ^[0-9]+$ ]] && [ "${#2}" -le 10 ] || { echo 'Shard must be a non-negative integer' >&2; exit 2; }
      selected_shard=$((10#$2)); shard_given=1; shift 2 ;;
    --*) echo 'Unknown option' >&2; exit 2 ;;
    *) [ "$bot_set" -eq 0 ] || { echo 'Too many bot arguments' >&2; exit 2; }
       bot=$1; bot_set=1; shift ;;
  esac
done
case "$repeat" in ''|*[!0-9]*) echo 'Repeat count must be a positive integer' >&2; exit 2;; esac
[[ "$repeat" =~ ^0*[1-9][0-9]*$ ]] || { echo 'Repeat count must be positive' >&2; exit 2; }
repeat=$((10#$repeat))
[ "$(id -u)" -eq 0 ] || { echo 'Run as root via sudo' >&2; exit 2; }

results=()
failed=0
record() {
  local result=$1 label=$2 reason=$3
  printf '%-4s | %-25s | %s\n' "$result" "$label" "$reason"
  results+=("$(printf '%-4s | %-25s | %s' "$result" "$label" "$reason")")
  [ "$result" = PASS ] || failed=1
}
ctl() {
  if [ "$1" = gateway-update ]; then
    timeout 310 sudo -u "$USER_NAME" "$BOTCTL" "$@" --secret-file "$SECRET" "${control_endpoint[@]}" --json 2>/dev/null
  else
    timeout 8 sudo -u "$USER_NAME" "$BOTCTL" "$@" --timeout 5s --secret-file "$SECRET" "${control_endpoint[@]}" --json 2>/dev/null
  fi
}
json_string() {
  local key=$1 input=$2
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$input" | jq -r --arg key "$key" '.. | objects | .[$key] // empty | select(type == "string")' 2>/dev/null | head -n 1
  else
    printf '%s\n' "$input" | tr '\n' ' ' | sed -nE "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"([^\"]*)\".*/\\1/p"
  fi
}
warnings() {
  local output
  output=$(journalctl -u botd -b -p warning --no-pager 2>/dev/null) || return 1
  printf '%s\n' "$output" | awk 'NF && $0 !~ /^-- / {n++} END {print n+0}'
}
snapshot() {
  # A Gateway that an update promoted still has --standby in its command line, so --standby
  # is not a role: every target process with --core-dir is a Gateway, one per shard.
  cores=(); gateways=(); daemons=(); gateway_by_shard=(); ipc_by_shard=(); core_by_socket=(); core_socket_count=()
  local pid arg prev cmd role ipc epoch rss shard process_bot socket key
  for pid in $(pgrep -u "$USER_NAME" -f 'bin/(botd|src)' 2>/dev/null | sort -n); do
    [ -r "/proc/$pid/cmdline" ] || continue
    args=(); mapfile -d '' -t args < "/proc/$pid/cmdline" || continue
    role=; ipc=; epoch=absent; shard=; process_bot=; socket=; prev=; cmd=${args[0]:-}
    for arg in "${args[@]}"; do
      [ "$prev" = --core-dir ] && ipc=$arg
      [ "$prev" = --core-epoch ] && epoch=$arg
      [ "$prev" = --shard-id ] && shard=$arg
      [ "$prev" = --bot-id ] && process_bot=$arg
      [ "$prev" = --core-listen ] && socket=$arg
      case "$arg" in --core-listen) role=core;; --core-dir) role=gateway;; esac
      prev=$arg
    done
    case "$role" in
      core) [ -n "$socket" ] || continue; core_by_socket["$socket"]=$pid; core_socket_count["$socket"]=$(( ${core_socket_count["$socket"]:-0} + 1 )); continue ;;
      gateway)
        [ "$process_bot" = "$target_id" ] || continue
        [[ "$shard" =~ ^(0|[1-9][0-9]*)$ ]] && [ "$shard" -lt "$shard_count" ] && [ -n "${local_shard_set[$shard]:-}" ] && [ -n "$ipc" ] || { snapshot_invalid=1; continue; }
        [ -z "${gateway_by_shard[$shard]:-}" ] || snapshot_invalid=1
        gateways+=("$pid"); gateway_by_shard[$shard]=$pid; ipc_by_shard[$shard]=$ipc ;;
      *) case "$cmd" in */botd) daemons+=("$pid");; esac ;;
    esac
    rss=$(awk '/^VmRSS:/ {print $2; found=1} END {if (!found) print "unknown"}' "/proc/$pid/status" 2>/dev/null)
    printf '  pid=%s role=%s rss_kB=%s' "$pid" "${role:-botd}" "${rss:-unknown}"
    case "$role" in gateway) printf ' core_epoch=%s' "$epoch";; esac
    printf '\n'
  done
  for s in "${local_shards[@]}"; do
    ipc=${ipc_by_shard[$s]:-}
    [ -n "$ipc" ] || continue
    for other in "${local_shards[@]}"; do
      [ "$other" -ge "$s" ] && break
      [ "$ipc" != "${ipc_by_shard[$other]:-}" ] || snapshot_invalid=1
    done
    for key in "${!core_by_socket[@]}"; do
      if [[ "$key" == "$ipc/"* ]] && [ "$key" != "$ipc/core-0.sock" ] && [ "$key" != "$ipc/core-1.sock" ]; then snapshot_invalid=1; fi
    done
    for slot in 0 1; do
      pid=${core_by_socket["$ipc/core-$slot.sock"]:-}
      if [ -n "$pid" ]; then
        cores+=("$pid")
        [ "${core_socket_count["$ipc/core-$slot.sock"]}" -eq 1 ] || snapshot_invalid=1
        rss=$(awk '/^VmRSS:/ {print $2; found=1} END {if (!found) print "unknown"}' "/proc/$pid/status" 2>/dev/null)
        printf '  pid=%s role=core rss_kB=%s\n' "$pid" "${rss:-unknown}"
      fi
    done
  done
}

declare -A gateway_by_shard ipc_by_shard core_by_socket core_socket_count baseline_rss
inspect=$(ctl inspect "$bot") || inspect=
target_id=$(json_string bot_id "$inspect")
shard_count=$(printf '%s' "$inspect" | python3 -c 'import json,sys; v=json.load(sys.stdin)["bot"]["total_shards"]; print(v if type(v) is int and 0 < v <= 4294967295 else "")' 2>/dev/null) || shard_count=
local_shard_list=$(printf '%s' "$inspect" | python3 -c '
import json, sys
bot = json.load(sys.stdin)["bot"]
total = bot["total_shards"]
processes = bot["processes"]
if type(total) is not int or not 0 < total <= 4294967295 or not isinstance(processes, list):
    raise ValueError("invalid inspection")
ids = [process["key"]["shard_id"] for process in processes]
if not ids or any(type(i) is not int or not 0 <= i < total for i in ids) or len(ids) != len(set(ids)):
    raise ValueError("invalid local shards")
print(" ".join(map(str, sorted(ids))))
' 2>/dev/null) || local_shard_list=
read -r -a local_shards <<< "$local_shard_list"
local_count=${#local_shards[@]}
declare -A local_shard_set=()
for s in "${local_shards[@]}"; do local_shard_set[$s]=1; done
if [ -z "$target_id" ] || [ -z "$shard_count" ] || [ "$local_count" -eq 0 ] || { [ "$shard_given" -eq 1 ] && { [ "$selected_shard" -ge "$shard_count" ] || [ -z "${local_shard_set[$selected_shard]:-}" ]; }; }; then
  record FAIL 'R1 preconditions' 'bot inspection failed or shard is out of range or not local; update skipped'
  echo '=== Summary ==='; printf '%s\n' "${results[@]}"; exit 1
fi
for ((round=1; round<=repeat; round++)); do
  echo "=== Round $round/$repeat PRE ==="
  snapshot_invalid=0
  snapshot
  before_cores="${cores[*]}"; old=${gateway_by_shard[${local_shards[0]}]:-}; declare -A old_gateways=() old_ipcs=()
  for s in "${local_shards[@]}"; do old_gateways[$s]=${gateway_by_shard[$s]:-}; old_ipcs[$s]=${ipc_by_shard[$s]:-}; done
  if [ "$(systemctl is-active botd 2>/dev/null)" = active ] && [ "${#daemons[@]}" -eq 1 ] && [ "${#cores[@]}" -eq $((2*local_count)) ] && [ "${#gateways[@]}" -eq "$local_count" ] && [ "$snapshot_invalid" -eq 0 ]; then
    if [ "$local_count" -eq 1 ]; then record PASS "R$round preconditions" 'botd, two cores and one gateway found'; else record PASS "R$round preconditions" "botd, $((2*local_count)) cores and $local_count gateways found"; fi
  else
    if [ "$local_count" -eq 1 ]; then record FAIL "R$round preconditions" 'expected one botd, two cores and one gateway; update skipped'; else record FAIL "R$round preconditions" "expected one botd, $((2*local_count)) cores and $local_count gateways; update skipped"; fi
    break
  fi
  before_warnings=$(warnings) || before_warnings=unknown
  update_args=(gateway-update "$bot")
  [ "$shard_given" -eq 0 ] || update_args+=(--shard "$selected_shard")
  check_gateway_update "$round" "${update_args[@]}" || break
  # The old Gateway is asked to exit after the hand-over and is killed after 30 s: wait for it
  # to be gone before taking the "after" snapshot, so the new Gateway cannot be mistaken for it.
  deadline=$((SECONDS+40))
  for s in "${local_shards[@]}"; do
    [ "$shard_given" -eq 0 ] || [ "$s" -eq "$selected_shard" ] || continue
    while kill -0 "${old_gateways[$s]}" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do sleep 1; done
  done
  sleep 2
  echo "=== Round $round/$repeat POST ==="
  snapshot_invalid=0; snapshot
  if [ "$snapshot_invalid" -eq 0 ] && [ "${#cores[@]}" -eq $((2*local_count)) ] && [ "${cores[*]}" = "$before_cores" ]; then record PASS "R$round cores" "unchanged: $before_cores"; else record FAIL "R$round cores" 'core count or PIDs changed'; fi
  new=${gateway_by_shard[${local_shards[0]}]:-}; gateway_ok=1
  for s in "${local_shards[@]}"; do
    current=${gateway_by_shard[$s]:-}
    if [ -z "$current" ] || [ "${ipc_by_shard[$s]:-}" != "${old_ipcs[$s]}" ]; then gateway_ok=0
    elif [ "$shard_given" -eq 0 ] || [ "$s" -eq "$selected_shard" ]; then [ "$current" != "${old_gateways[$s]}" ] || gateway_ok=0
    else [ "$current" = "${old_gateways[$s]}" ] || gateway_ok=0; fi
  done
  if [ "$gateway_ok" -eq 1 ] && [ "$snapshot_invalid" -eq 0 ] && [ "${#gateways[@]}" -eq "$local_count" ]; then
    if [ "$local_count" -eq 1 ]; then record PASS "R$round gateway PID" "$old -> $new"; else record PASS "R$round gateway PID" 'target gateways replaced; other shard PIDs unchanged'; fi
  else record FAIL "R$round gateway PID" 'replacement not uniquely identified'; fi
  if [ "${#gateways[@]}" -eq "$local_count" ] && [ "$snapshot_invalid" -eq 0 ]; then
    if [ "$local_count" -eq 1 ]; then record PASS "R$round gateway count" 'exactly one Gateway process (no standby or retiring one left)'; else record PASS "R$round gateway count" "exactly $local_count Gateway processes"; fi
  else record FAIL "R$round gateway count" "gateways=${#gateways[@]}"; fi
  deadline=$((SECONDS+35))
  old_ok=1; ipc_ok=1; sockets=0; secrets=0
  for s in "${local_shards[@]}"; do
    if [ "$shard_given" -eq 0 ] || [ "$s" -eq "$selected_shard" ]; then
      old_pid=${old_gateways[$s]}
      while kill -0 "$old_pid" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do sleep 1; done
      kill -0 "$old_pid" 2>/dev/null && old_ok=0
    fi
    ipc=${old_ipcs[$s]}
    sockets=$(find "$ipc" -maxdepth 1 -type s -name '*.sock' -printf '.' 2>/dev/null | wc -c)
    secrets=$(find "$ipc" -maxdepth 1 -type f -name core.secret -printf '.' 2>/dev/null | wc -c)
    [ "$sockets" -eq 2 ] && [ "$secrets" -eq 1 ] && [ -S "$ipc/core-0.sock" ] && [ -S "$ipc/core-1.sock" ] || ipc_ok=0
  done
  if [ "$old_ok" -eq 1 ]; then if [ "$local_count" -eq 1 ]; then record PASS "R$round old gateway" "PID $old is gone"; else record PASS "R$round old gateway" 'all replaced Gateway PIDs are gone'; fi; else if [ "$local_count" -eq 1 ]; then record FAIL "R$round old gateway" "PID $old still exists"; else record FAIL "R$round old gateway" 'a replaced Gateway PID still exists'; fi; fi
  if [ "$ipc_ok" -eq 1 ]; then if [ "$local_count" -eq 1 ]; then record PASS "R$round IPC" 'two expected sockets and core.secret'; else record PASS "R$round IPC" 'two expected sockets and core.secret per shard'; fi; else if [ "$local_count" -eq 1 ]; then record FAIL "R$round IPC" "sockets=$sockets core.secret=$secrets"; else record FAIL "R$round IPC" 'socket or core.secret count incorrect for a shard'; fi; fi
  markers=$(find "$ROOT" \( -name standby-ready -o -name standby-active \) -print 2>/dev/null) || markers=unavailable
  if [ -z "$markers" ] && [ -d "$ROOT" ]; then record PASS "R$round markers" 'no standby markers'; else record FAIL "R$round markers" 'markers found or runtime root unreadable'; fi
  events=
  if [[ "$id" =~ ^[a-fA-F0-9]{32}$ ]] && [ -r "$ROOT/gateway-update-audit.jsonl" ]; then
    events=$(grep -F -- "\"operation_id\":\"$id\"" "$ROOT/gateway-update-audit.jsonl" | tail -n 2 | sed -nE 's/.*"event"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/p' | tr '\n' ' ')
  fi
  if [ "$events" = 'requested succeeded ' ]; then record PASS "R$round audit" 'requested then succeeded'; else record FAIL "R$round audit" 'expected audit events missing or out of order'; fi
  after_warnings=$(warnings) || after_warnings=unknown
  if [ "$before_warnings" != unknown ] && [ "$after_warnings" != unknown ] && [ "$before_warnings" = "$after_warnings" ]; then record PASS "R$round warnings" "count unchanged: $after_warnings"; else record FAIL "R$round warnings" "count: $before_warnings -> $after_warnings"; fi
  if [ "$repeat" -gt 1 ]; then
    rss_ok=1; gateway_rss=
    for s in "${local_shards[@]}"; do
      for role in gateway core-0 core-1; do
        case "$role" in gateway) pid=${gateway_by_shard[$s]:-};; *) pid=${core_by_socket["${old_ipcs[$s]}/$role.sock"]:-};; esac
        rss=$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status" 2>/dev/null)
        echo "Shard $s $role RSS after round $round: ${rss:-unknown} kB"
        [ "$s:$role" != "${local_shards[0]}:gateway" ] || gateway_rss=$rss
        key="$s:$role"
        if [[ "$rss" =~ ^[0-9]+$ ]] && [ "$rss" -gt 0 ]; then
          [ "$round" -ne 1 ] || baseline_rss[$key]=$rss
          [ -n "${baseline_rss[$key]:-}" ] && awk -v current="$rss" -v base="${baseline_rss[$key]}" 'BEGIN {exit !(current <= base * 1.2)}' || rss_ok=0
        else rss_ok=0; fi
      done
    done
    if [ "$rss_ok" -eq 1 ]; then
      if [ "$local_count" -eq 1 ]; then record PASS "R$round RSS" "${gateway_rss} kB, baseline ${baseline_rss[${local_shards[0]}:gateway]} kB"; else record PASS "R$round RSS" 'all process RSS values within 20% of their baselines'; fi
    else record FAIL "R$round RSS" 'process RSS unavailable or exceeds baseline by over 20%'; fi
  fi
done
echo '=== Summary ==='
printf '%s\n' "${results[@]}"
echo 'Discord で !count を数回送り、数が続くこと・返信が1件ずつであることを確認してください'
exit "$failed"
