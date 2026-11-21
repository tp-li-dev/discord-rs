#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
export ROOT=$tmp/runtime SECRET=$tmp/no-secret
shard=$ROOT/bot/0
mkdir -p -- "$shard"
log=$tmp/soak.log

systemctl() {
    case "$1" in
        is-active) echo active ;;
        is-enabled) echo enabled ;;
        show) [ "$4" != NRestarts ] || echo 0 ;;
    esac
}
pgrep() { return 1; }
journalctl() { echo '-- No entries --'; }
sudo() { return 1; }
date() {
    if [ "$1" = -Is ]; then printf '%s\n' "$snapshot_time"; else command date "$@"; fi
}
export -f systemctl pgrep journalctl sudo date

append_closes() {
    local i
    for ((i=0; i<$1; i++)); do
        echo 'Gateway 切断: close code 1001 (2026-11-21T02:00:00.000Z)'
    done >> "$shard/gateway.log"
}

expect() {
    if [[ $1 != *"$2"* ]]; then
        printf 'Expected %s, got:\n%s\n' "$2" "$1" >&2
        exit 1
    fi
}

cat > "$shard/gateway.log" <<'LOG'
gateway generation
Worker error: worker is unavailable
Gateway startup error: gateway metadata unavailable [metadata_unavailable]
Worker error: worker interaction state unavailable
LOG
append_closes 27
snapshot_time=2026-11-21T01:00:00+00:00 bash "$script_dir/soak-report.sh" > "$log"
expect "$(cat "$log")" 'generations=1 login=0 closes=27 worker_unavailable=1 rest_unknown=0 resumed=0 identify_grant_unavailable=0'

echo 'Identify send grant unavailable' >> "$shard/gateway.log"
echo '実行に失敗しました: Identify send grant unavailable' > "$shard/gateway-candidate.log"
append_closes 29
snapshot_time=2026-11-21T02:00:00+00:00 bash "$script_dir/soak-report.sh" >> "$log"
expect "$(cat "$log")" 'generations=1 login=0 closes=56 worker_unavailable=1 rest_unknown=0 resumed=0 identify_grant_unavailable=2'

output=$(bash "$script_dir/soak-diff.sh" "$log")
expect "$output" 'worker_unavailable (sum over gateway.log): 1 -> 1'
expect "$output" 'worker_unavailable: OK'
expect "$output" 'identify_grant_unavailable (sum over gateway.log): 0 -> 2'
expect "$output" 'identify_grant_unavailable: WATCH (0->2)'
expect "$output" 'closes: WATCH (27->56'

# New reports cannot compare the precise Worker counter with legacy snapshots.
sed '1,/^=== end ===/s/ identify_grant_unavailable=0//' "$log" > "$tmp/mixed.log"
output=$(bash "$script_dir/soak-diff.sh" "$tmp/mixed.log")
expect "$output" 'worker_unavailable (sum over gateway.log): n/a (counter definition changed)'
expect "$output" 'identify_grant_unavailable (sum over gateway.log): n/a -> 2'
printf 'soak self-test passed\n'
