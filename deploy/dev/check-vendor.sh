#!/usr/bin/env bash
set -euo pipefail

fail() {
    printf 'check-vendor: %s\n' "$*" >&2
    exit 1
}

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
crate=twilight-gateway
vendor=$repo/src/vendor/$crate
locked=$(awk -F '"' -v crate="$crate" '
    function finish() {
        if (section && name == crate) {
            if (checksum == "" && source != "") exit 1
            print version, (checksum == "" ? "-" : checksum)
            matches++
        }
    }
    /^\[\[/ { finish(); section = ($0 ~ /^\[\[package\]\]\r?$/); name = version = checksum = source = "" }
    section && /^name = /     { name = $2 }
    section && /^version = /  { version = $2 }
    section && /^source = /   { source = $2 }
    section && /^checksum = / { checksum = $2 }
    END { finish(); if (matches != 1) exit 1 }
' "$repo/src/Cargo.lock") || fail "expected one $crate entry in src/Cargo.lock"
read -r version checksum <<< "$locked"
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "invalid pinned version: $version"

case ${1:-} in
    --version) [[ $# == 1 ]] || fail 'usage: check-vendor.sh --version'; printf '%s\n' "$version"; exit 0 ;;
    --offline) [[ $# -le 2 ]] || fail 'usage: check-vendor.sh --offline [archive.crate]' ;;
    '') [[ $# == 0 ]] || fail 'usage: check-vendor.sh [--offline [archive.crate]]' ;;
    *) fail 'usage: check-vendor.sh [--offline [archive.crate]]' ;;
esac

[[ -d $vendor && ! -L $vendor ]] || fail "missing vendored tree: $vendor"
for metadata in UPSTREAM.md PATCHED_FILES; do
    [[ -f $vendor/$metadata && ! -L $vendor/$metadata ]] || fail "missing regular file: $metadata"
done
if [[ $checksum == - ]]; then
    checksum=$(awk '/^Crate SHA-256: / { sub(/\r$/, "", $3); print $3 }' "$vendor/UPSTREAM.md")
fi
[[ $checksum =~ ^[0-9a-f]{64}$ ]] || fail 'expected one upstream SHA-256 in Cargo.lock or UPSTREAM.md'

tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
package=$crate-$version
archive=$tmp/$package.crate
if [[ ${1:-} == --offline ]]; then
    cached=${2:-}
    if [[ $# == 1 ]]; then
        shopt -s nullglob
        candidates=("${CARGO_HOME:-$HOME/.cargo}"/registry/cache/*/"$package.crate")
        cached=${candidates[0]:-}
    fi
    [[ -f $cached ]] || fail "offline archive unavailable for $package; pass --offline /path/to/$package.crate"
    cp -- "$cached" "$archive"
else
    curl --fail --silent --show-error --location --max-time 60 --output "$archive" \
        "https://static.crates.io/crates/$crate/$package.crate"
fi
actual=$(sha256sum --binary < "$archive")
[[ ${actual%% *} == "$checksum" ]] || fail "archive SHA-256 mismatch for $package"

mkdir -- "$tmp/extracted"
tar --extract --gzip --file "$archive" --directory "$tmp/extracted" --no-same-owner --no-same-permissions
shopt -s dotglob nullglob
entries=("$tmp/extracted"/*)
[[ ${#entries[@]} == 1 && ${entries[0]} == "$tmp/extracted/$package" && -d ${entries[0]} && ! -L ${entries[0]} ]] \
    || fail "expected one archive root named $package"
mv -- "${entries[0]}" "$tmp/upstream"

# Compare the complete on-disk trees, including ignored files. Exclude only the two root control files.
cp -R -- "$vendor" "$tmp/vendor"
cd -- "$tmp"
special=$(find upstream vendor ! -type d ! -type f -print -quit)
[[ -z $special ]] || fail "expected regular files and directories: $special"
declare -A expected=() seen=()
while IFS= read -r line || [[ -n $line ]]; do
    line=${line%$'\r'}
    [[ $line =~ ^([0-9a-f]{64}|-)\ \ (.+)$ ]] || fail "invalid PATCHED_FILES line: $line"
    path=${BASH_REMATCH[2]}
    [[ ! ${expected[$path]+listed} ]] || fail "duplicate PATCHED_FILES entry: $path"
    expected[$path]=${BASH_REMATCH[1]}
done < vendor/PATCHED_FILES
rm -- vendor/UPSTREAM.md vendor/PATCHED_FILES

diff_trees() {
    git -c core.autocrlf=false --no-pager diff --no-index --no-renames --no-ext-diff --no-textconv --no-color "$@" -- upstream vendor \
        || [[ $? == 1 ]]
}

printf 'Changes from crates.io %s (A = added, D = omitted, M = modified):\n' "$package"
diff_trees --name-status -z > changes
diff_trees --shortstat
[[ -s changes ]] || printf '  0 files changed\n'
drift=0
while IFS= read -r -d '' status && IFS= read -r -d '' path; do
    path=${path#*/}
    seen[$path]=1
    printf '  %s %q\n' "$status" "$path"
    if [[ $status == D ]]; then
        actual=-
    else
        actual=$(sha256sum --binary < "vendor/$path")
        actual=${actual%% *}
    fi
    if [[ ! ${expected[$path]+listed} ]]; then
        printf 'check-vendor: unlisted change: %q\n' "$path" >&2
        drift=1
    elif [[ ${expected[$path]} != "$actual" ]]; then
        printf 'check-vendor: patched file hash or presence mismatch: %q\n' "$path" >&2
        drift=1
    fi
done < changes
for path in "${!expected[@]}"; do
    if [[ ! ${seen[$path]+changed} ]]; then
        printf 'check-vendor: listed path has no upstream difference: %q\n' "$path" >&2
        drift=1
    fi
done
(( drift == 0 )) || fail 'vendored tree drift detected'
