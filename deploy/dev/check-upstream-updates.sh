#!/usr/bin/env bash
set -uo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
printf 'Review the Twilight changelog and security fixes before updating. https://github.com/twilight-rs/twilight/releases\n'
if ! version=$(bash "$script_dir/check-vendor.sh" --version); then
    printf 'Upstream update check skipped: unable to read the pinned version.\n'
    exit 0
fi
if ! response=$(curl --fail --silent --location --max-time 15 --user-agent 'discord-rs vendor maintenance' \
    'https://crates.io/api/v1/crates/twilight-gateway'); then
    printf 'Upstream update check skipped: crates.io is unreachable (offline or API unavailable).\n'
    exit 0
fi

if ! python3 -c '
import json
import re
import sys

pinned = tuple(map(int, sys.argv[1].split(".")))
versions = []
for release in json.load(sys.stdin)["versions"]:
    number = release["num"]
    if not release["yanked"] and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", number):
        versions.append(tuple(map(int, number.split("."))))

def report(label, candidates):
    newest = max(candidates, default=None)
    if newest is None:
        print(f"{label}: none")
    else:
        number = ".".join(map(str, newest))
        print(f"{label}: {number}")

print(f"Pinned twilight-gateway: {sys.argv[1]}")
newer = [version for version in versions if version > pinned]
series = ".".join(map(str, pinned[:2]))
report(f"Newer {series}.x patch", (version for version in newer if version[:2] == pinned[:2]))
report("Newer release", newer)
' "$version" <<< "$response"; then
    printf 'Upstream update check skipped: could not read the crates.io response with Python 3.\n'
fi
exit 0
