#!/usr/bin/env bash
# 長時間稼働（放置）試験のスナップショット。読み取り専用で、秘密を出力しない。
# サーバーで root（または sudo）で実行する。例:  sudo ./soak-report.sh | tee -a /home/ubuntu/soak.log
# 稼働中の botd と同じリリースの botctl を使用する。Unix ソケットの場合は CONTROL_SOCKET を設定する。
# 数時間おきに実行して、増え続ける値（RSS、fd、ログの大きさ、再起動回数）を比べる。
set -u
UNIT=${UNIT:-botd}
USER_NAME=${USER_NAME:-discord-rs}
ROOT=${ROOT:-/var/lib/discord-rs/runtime}
PORT=${PORT:-47321}
control_endpoint=(--port "$PORT")
if [ -n "${CONTROL_SOCKET:-}" ]; then
  control_endpoint=(--socket "$CONTROL_SOCKET")
fi
SECRET=${SECRET:-/etc/discord-rs/botd.secret}

echo "=== soak snapshot $(date -Is) host=$(hostname) arch=$(uname -m) ==="
echo "unit: active=$(systemctl is-active "$UNIT") enabled=$(systemctl is-enabled "$UNIT" 2>/dev/null)" \
     "NRestarts=$(systemctl show "$UNIT" -p NRestarts --value) since=$(systemctl show "$UNIT" -p ActiveEnterTimestamp --value)"

echo "--- processes (pid ppid etime rss_kB fds cmd) ---"
for pid in $(pgrep -u "$USER_NAME" -f 'bin/(botd|src)' | sort -n); do
  [ -r "/proc/$pid/status" ] || continue
  ppid=$(awk '/^PPid:/{print $2}' "/proc/$pid/status")
  rss=$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status")
  fds=$(ls "/proc/$pid/fd" 2>/dev/null | wc -l)
  et=$(ps -o etime= -p "$pid" | tr -d ' ')
  cmd=$(tr '\0' ' ' < "/proc/$pid/cmdline" | sed -E 's/--token-env [A-Za-z0-9_]+//; s#/var/lib/discord-rs/runtime/[^ ]*#<path>#g' | cut -c1-90)
  echo "$pid $ppid $et $rss $fds $cmd"
done

echo "--- files (bytes) ---"
find "$ROOT" -type f \( -name '*.log' -o -name '*.log.1' -o -name 'core-state.json' -o -name '*.jsonl' \) \
  -printf '%s %p\n' 2>/dev/null | sed "s#$ROOT/##" | sort -k2
echo "sockets/secret in ipc dirs: $(find "$ROOT" \( -name '*.sock' -o -name 'core.secret' \) 2>/dev/null | wc -l)"
echo "reservations: $(find "$ROOT" -name '*.botd-reservation' 2>/dev/null | wc -l)"

echo "--- gateway.log signals (counts) ---"
# Since 2026-11-16 a standby Gateway logs into the same gateway.log. Versions before that wrote
# the active Gateway's output to gateway-candidate.log after an update; a leftover file of that
# name is still counted together with gateway.log.
# Count each signal from the same pass through the log. The timestamp is not part of a match.
while IFS= read -r -d '' f; do
  d=$(dirname "$f")
  logs=("$f")
  if [ -f "$d/gateway-candidate.log" ]; then logs+=("$d/gateway-candidate.log"); fi
  printf '%s: ' "$(basename "$d")"
  awk '
    /gateway generation/ { generations++ }
    /ログイン成功/       { login++ }
    /Gateway 切断/       { closes++ }
    /(^|: )worker is unavailable$/ { worker_unavailable++ }
    /(^|: )Identify send grant unavailable$/ { grant_unavailable++ }
    /結果不明/           { unknown++ }
    /Resume 成功/        { resumed++ }
    END {
      printf "generations=%d login=%d closes=%d worker_unavailable=%d rest_unknown=%d resumed=%d identify_grant_unavailable=%d\n",
             generations, login, closes, worker_unavailable, unknown, resumed, grant_unavailable
    }' "${logs[@]}"
done < <(find "$ROOT" -name gateway.log -print0 2>/dev/null)
echo "core duplicates dropped (last): $(grep -h '重複したメッセージ' "$ROOT"/*/*/ipc/core-*.log 2>/dev/null | tail -n 1)"

echo "--- journal since boot: warnings/errors count ---"
journalctl -u "$UNIT" -b -p warning --no-pager 2>/dev/null | grep -vc "^-- "

if [ -r "$SECRET" ] && command -v botctl >/dev/null 2>&1 || [ -x /opt/discord-rs/bin/botctl ]; then
  echo "--- botctl status (state, restarts, cores) ---"
  sudo -u "$USER_NAME" /opt/discord-rs/bin/botctl status --secret-file "$SECRET" "${control_endpoint[@]}" --json 2>/dev/null \
    | grep -E '"(state|restarts|restart_exhausted|running|failures)"' | sort | uniq -c
fi
echo "=== end ==="
