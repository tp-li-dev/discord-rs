#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$script_dir/update-check.sh"
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
operation_id=0123456789abcdef0123456789abcdef
calls=$tmp/calls

operation_reply() {
    printf '{"ok":true,"operation_id":"%s","operation":{"operation_id":"%s","command":"gateway-update","state":"%s"}}\n' \
        "$operation_id" "$operation_id" "$1"
}

ctl() {
    printf '%s\n' "$*" >> "$calls"
    if [ "$1" = operation ]; then
        [ "$2" = "$operation_id" ] || return 2
        if [ "$scenario" = ambiguous ] && [ "$(wc -l < "$calls")" -eq 2 ]; then
            printf '{"ok":false,"error":"operation not found","operation_id":"%s"}\n' "$operation_id"
            return 1
        fi
        operation_reply succeeded
        return 0
    fi
    case "$scenario" in
        admitted) operation_reply running ;;
        ambiguous)
            printf '{"ok":false,"error":"invalid or oversized local control frame","outcome_unknown":true,"operation_id":"%s"}\n' "$operation_id"
            return 1 ;;
        *)
            printf '{"ok":false,"error":"%s","operation_id":"%s"}\n' "$expected" "$operation_id"
            return 1 ;;
    esac
}

record() { printf '%s | %s | %s\n' "$@"; }
sleep() {
    if [ "$(wc -l < "$calls")" -ge 3 ]; then SECONDS=$((SECONDS+310)); fi
}

check() {
    local scenario=$1 expected=$2 exit_code=$3 polls=$4 output status=0
    : > "$calls"
    output=$(check_gateway_update 1 gateway-update test) || status=$?
    if [ "$status" -ne "$exit_code" ] || [[ "$output" != *"$expected"* ]] ||
        [ "$(wc -l < "$calls")" -ne "$((polls + 1))" ]; then
        printf '%s failed (status %s)\n%s\n' "$scenario" "$status" "$output" >&2
        cat "$calls" >&2
        exit 1
    fi
}

check busy 'bot already has an operation in progress' 1 0
check fallback-protected 'gateway fallback recovery pending; retry after recovery' 1 0
check not-sent 'local daemon unavailable' 1 0
check admitted 'PASS | R1 update | operation succeeded' 0 1
check ambiguous 'PASS | R1 update | operation succeeded' 0 2
printf 'update-check self-test passed\n'
