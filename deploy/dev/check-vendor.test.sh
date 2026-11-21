#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
repo=$tmp/repo
vendor=$repo/src/vendor/twilight-gateway
package=twilight-gateway-0.18.0
archive=$tmp/$package.crate
mkdir -p -- "$repo/deploy/dev" "$vendor/src" "$tmp/$package/src"
cp -- "$script_dir/check-vendor.sh" "$repo/deploy/dev/"
check=$repo/deploy/dev/check-vendor.sh
cat > "$repo/src/Cargo.lock" <<'LOCK'
version = 4

[[package]]
name = "twilight-gateway"
version = "0.18.0"
LOCK

printf 'upstream\n' > "$tmp/$package/src/example.rs"
cp -- "$tmp/$package/src/example.rs" "$vendor/src/"
tar -czf "$archive" -C "$tmp" "$package"
checksum=$(sha256sum --binary < "$archive")
printf 'Crate SHA-256: %s\n' "${checksum%% *}" > "$vendor/UPSTREAM.md"
: > "$vendor/PATCHED_FILES"

expect_failure() {
    local output
    if output=$(bash "$check" --offline "$archive" 2>&1); then
        printf 'Expected failure containing %s, but check passed\n' "$1" >&2
        exit 1
    fi
    if [[ $output != *"$1"* ]]; then
        printf 'Expected failure containing %s, got:\n%s\n' "$1" "$output" >&2
        exit 1
    fi
}

bash "$check" --offline "$archive"
printf 'intentional patch\n' > "$vendor/src/example.rs"
expect_failure 'unlisted change: src/example.rs'
checksum=$(sha256sum --binary < "$vendor/src/example.rs")
printf '%s  src/example.rs\n' "${checksum%% *}" > "$vendor/PATCHED_FILES"

cache=$tmp/cargo/registry/cache/test-index
mkdir -p -- "$cache"
cp -- "$archive" "$cache/"
CARGO_HOME=$tmp/cargo bash "$check" --offline
printf 'changed patch\n' > "$vendor/src/example.rs"
expect_failure 'patched file hash or presence mismatch: src/example.rs'

printf 'bad checksum\n' >> "$archive"
expect_failure 'archive SHA-256 mismatch'
printf 'check-vendor self-test passed\n'
