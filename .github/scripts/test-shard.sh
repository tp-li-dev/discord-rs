#!/usr/bin/env bash
# Runs one shard of the workspace tests. Shards are balanced by measured runtime;
# any test target not named in a heavy group falls into "rest", so new tests are
# never skipped. Usage: test-shard.sh <reload|cluster|daemon|process|rest>
set -euo pipefail
cd "$(dirname "$0")/../../src"

RELOAD="reload_command"
CLUSTER="cluster_sim raft_cluster_runtime e2e_cluster_processes"
DAEMON="botctl_daemon fixture_build raft_quorum"
# Tests that spawn several real botd/Gateway/Worker processes each. Running all of
# their tests at once starves a 4-vCPU runner and trips the deadlines, so this
# shard limits concurrency.
PROCESS="e2e_gateway_process_switch botd_child_recovery gateway_admission_process hot_swap_scenarios raft_botd_processes botd_metrics child_recovery_processes worker_child_recovery e2e_shard_worker e2e_shard_runtime"

targets() {
  cargo metadata --locked --no-deps --format-version 1 \
    | jq -r '.packages[].targets[] | select(.kind | index("test")) | .name' | sort -u
}

test_args() {
  local t
  for t in "$@"; do printf -- '--test %s ' "$t"; done
}

run() {
  set -o pipefail
  # shellcheck disable=SC2086
  cargo test --locked --workspace --no-fail-fast "$@" 2>&1 | tee test-output.log
}

case "${1:?shard name required}" in
  reload)
    # The reload test builds a release binary; prebuild it outside its own timeout.
    cargo build --locked --release --bin src --target-dir target/reload-build
    # shellcheck disable=SC2086
    run $(test_args $RELOAD)
    ;;
  cluster)
    # shellcheck disable=SC2086
    run $(test_args $CLUSTER)
    ;;
  daemon)
    # shellcheck disable=SC2086
    run $(test_args $DAEMON)
    ;;
  process)
    # shellcheck disable=SC2086
    run $(test_args $PROCESS) -- --test-threads=2
    ;;
  rest)
    heavy=" $RELOAD $CLUSTER $DAEMON $PROCESS "
    args=""
    for t in $(targets); do
      case "$heavy" in *" $t "*) ;; *) args="$args --test $t" ;; esac
    done
    # Unit tests of lib/bin targets run here; doc tests cannot be combined with
    # other target selectors, so they run as a separate cargo invocation.
    # shellcheck disable=SC2086
    run --lib --bins $args
    cargo test --locked --workspace --doc 2>&1 | tee -a test-output.log
    ;;
  *)
    echo "unknown shard: $1" >&2
    exit 2
    ;;
esac
