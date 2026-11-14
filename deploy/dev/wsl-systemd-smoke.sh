#!/usr/bin/env bash
# 手元の Linux（WSL2 の Ubuntu など、systemd が動く環境）で、botd を実 systemd 上で確認する。
# GitHub Actions の systemd-smoke.yml と同じ内容を、CI の往復なしで繰り返せる。
#
# 確認すること（模擬 Discord。token も外部への接続も使わない）:
#   1. unit の起動（Type=notify、WatchdogSec）
#   2. Gateway を kill -9 → botd が再起動する
#   3. Core ワーカーを kill -9 → Gateway が補充する
#   4. botd を kill -9 → systemd が再起動し、古い起動予約が整理されて子が起動し直す
#   5. systemctl stop → 子も含めて残らず止まる
#
# 使い方（root で実行。専用ユーザー・/opt・/etc・unit を作り、終了時にすべて削除する）:
#   sudo ./wsl-systemd-smoke.sh /path/to/discord-rs        # リポジトリのルート。target/release に botd botctl src が必要
set -u
REPO="${1:?usage: $0 /path/to/discord-rs}"
BIN_SRC="$REPO/src/target/release"
UNIT_SRC="$REPO/deploy/systemd/botd.service"
for f in botd botctl src; do [ -x "$BIN_SRC/$f" ] || { echo "missing $BIN_SRC/$f (cargo build --release --bin botd --bin botctl --bin src)"; exit 2; }; done
[ -f "$UNIT_SRC" ] || { echo "missing $UNIT_SRC"; exit 2; }
[ "$(id -u)" = 0 ] || { echo "run as root"; exit 2; }
[ "$(ps -p 1 -o comm=)" = systemd ] || { echo "systemd is not PID 1 here"; exit 2; }

USER_NAME=discord-rs
PORT=47321
COLLECTOR_PORT=47400
fail=0

cleanup() {
  echo "--- cleanup ---"
  systemctl stop botd 2>/dev/null
  systemctl reset-failed botd 2>/dev/null
  rm -rf /etc/systemd/system/botd.service.d /etc/systemd/system/botd.service
  systemctl daemon-reload
  [ -n "${COLLECTOR_PID:-}" ] && kill "$COLLECTOR_PID" 2>/dev/null
  rm -rf /opt/discord-rs /etc/discord-rs /var/lib/discord-rs /run/discord-rs /tmp/wsl-smoke-collector.py
  id "$USER_NAME" >/dev/null 2>&1 && userdel "$USER_NAME" 2>/dev/null
  echo "cleanup done"
}
trap cleanup EXIT

echo "--- setup ---"
id "$USER_NAME" >/dev/null 2>&1 && { echo "user $USER_NAME already exists; refusing to touch an existing install"; trap - EXIT; exit 2; }
[ -e /etc/discord-rs ] && { echo "/etc/discord-rs already exists; refusing to touch an existing install"; trap - EXIT; exit 2; }
useradd --system --no-create-home --shell /usr/sbin/nologin "$USER_NAME" || exit 2
install -d -m 0755 /opt/discord-rs/bin
install -m 0755 "$BIN_SRC/botd" "$BIN_SRC/botctl" "$BIN_SRC/src" /opt/discord-rs/bin/
install -d -m 0750 -o root -g "$USER_NAME" /etc/discord-rs
cat > /tmp/wsl-smoke-bots.toml <<'TOML'
[cluster]
id = "wsl-smoke"
mode = "standalone"
[node]
id = "one"
[[bots]]
name = "alpha"
bot_id = "101"
application_id = "101"
token_env = "TOKEN_ALPHA"
total_shards = 1
[[bots.shard_placement]]
shard_id = 0
primary = "one"
TOML
install -m 0640 -o root -g "$USER_NAME" /tmp/wsl-smoke-bots.toml /etc/discord-rs/bots.toml
rm -f /tmp/wsl-smoke-bots.toml
/opt/discord-rs/bin/botctl secret generate --out /etc/discord-rs/botd.secret --json >/dev/null || exit 2
chown "$USER_NAME:$USER_NAME" /etc/discord-rs/botd.secret && chmod 0600 /etc/discord-rs/botd.secret
install -m 0640 -o root -g "$USER_NAME" "$REPO/deploy/botd.env.example" /etc/discord-rs/botd.env
install -m 0644 "$UNIT_SRC" /etc/systemd/system/botd.service
install -d /etc/systemd/system/botd.service.d
# 出荷する unit と同じ引数 + 模擬 Discord。Watchdog と Type=notify も有効にして sd_notify を確認する。
cat > /etc/systemd/system/botd.service.d/smoke.conf <<CONF
[Service]
Type=notify
NotifyAccess=main
WatchdogSec=10s
TimeoutStartSec=30s
ExecStart=
ExecStart=/opt/discord-rs/bin/botd --file /etc/discord-rs/bots.toml --runtime-root /var/lib/discord-rs/runtime --gateway-program /opt/discord-rs/bin/src --secret-file /etc/discord-rs/botd.secret --control-port $PORT --stop-children-on-exit --mock-discord 127.0.0.1:$COLLECTOR_PORT
CONF
systemctl daemon-reload

cat > /tmp/wsl-smoke-collector.py <<PY
import socket
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", $COLLECTOR_PORT))
s.listen(64)
keep = []
while True:
    conn, _ = s.accept()
    keep.append(conn)
PY
python3 /tmp/wsl-smoke-collector.py >/dev/null 2>&1 &
COLLECTOR_PID=$!
sleep 1

status_json() {
  runuser -u "$USER_NAME" -- /opt/discord-rs/bin/botctl status --secret-file /etc/discord-rs/botd.secret --port "$PORT" --json 2>/dev/null
}
all_running() {
  status_json | python3 -c 'import json,sys; d=json.load(sys.stdin); ps=[p for b in d.get("bots",[]) for p in b.get("processes",[])]; sys.exit(0 if ps and all(p.get("state",{}).get("state") == "Running" and p.get("restarts",1) >= 0 for p in ps) else 1)'
}
wait_running() { for _ in $(seq 1 "$1"); do all_running && return 0; sleep 1; done; return 1; }
gateway_pid() { pgrep -u "$USER_NAME" -f 'src.* --bot-id 101' | head -1; }
worker_pids() { pgrep -u "$USER_NAME" -f 'src --worker'; }

echo "--- 1. start (Type=notify, READY=1) ---"
if systemctl start botd; then echo "OK: started"; else echo "FAIL: botd did not start"; journalctl -u botd --no-pager | tail -20; fail=1; exit 1; fi
if wait_running 45; then echo "OK: Gateway and Core are Running"; else echo "FAIL: not Running"; status_json | head -c 1500; fail=1; fi
echo "workers: $(worker_pids | wc -l)"

echo "--- 2. kill -9 a Core worker ---"
w=$(worker_pids | head -1); echo "worker pid before: $w"
kill -9 "$w"
ok=0; for _ in $(seq 1 60); do n=$(worker_pids | wc -l); if [ "$n" = 2 ] && ! worker_pids | grep -qx "$w"; then ok=1; break; fi; sleep 1; done
if [ "$ok" = 1 ]; then echo "OK: worker replaced (workers=$(worker_pids | wc -l))"; else echo "FAIL: worker not replaced"; ps -u "$USER_NAME" -o pid,ppid,cmd | head; fail=1; fi
gw_same=$(gateway_pid); echo "gateway pid (should be unchanged): $gw_same"

echo "--- 3. kill -9 the Gateway ---"
old=$(gateway_pid); echo "gateway pid before: $old"
kill -9 "$old"
ok=0; for _ in $(seq 1 90); do new=$(gateway_pid); if [ -n "$new" ] && [ "$new" != "$old" ] && all_running; then ok=1; break; fi; sleep 1; done
if [ "$ok" = 1 ]; then echo "OK: Gateway restarted (pid $(gateway_pid))"; else echo "FAIL: Gateway not restarted"; status_json | head -c 2500; fail=1; fi

echo "--- 4. kill -9 botd ---"
main=$(systemctl show botd -p MainPID --value); echo "botd pid before: $main"
kill -9 "$main"
ok=0; for _ in $(seq 1 120); do now=$(systemctl show botd -p MainPID --value); if [ "$now" != 0 ] && [ "$now" != "$main" ] && systemctl is-active --quiet botd && all_running; then ok=1; break; fi; sleep 1; done
echo "botd pid after: $(systemctl show botd -p MainPID --value) NRestarts=$(systemctl show botd -p NRestarts --value)"
if [ "$ok" = 1 ]; then echo "OK: systemd restarted botd and the Gateway is back"; else echo "FAIL: botd did not come back"; systemctl status botd --no-pager | head -15; journalctl -u botd --no-pager | tail -20; fail=1; fi

echo "--- 5. stop ---"
systemctl stop botd
result=$(systemctl show botd -p Result --value)
left=$(ps -u "$USER_NAME" -o pid= | wc -l)
echo "Result=$result leftover processes=$left"
[ "$result" = success ] && [ "$left" = 0 ] && echo "OK: clean stop" || { echo "FAIL: unclean stop"; ps -u "$USER_NAME" -o pid,ppid,cmd | head; fail=1; }

echo "=== result: $([ "$fail" = 0 ] && echo PASS || echo FAIL) ==="
exit "$fail"
