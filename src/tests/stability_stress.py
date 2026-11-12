"""Repeat offline workspace tests in fresh targets, with measured CPU contention.

Run from any directory: python src/tests/stability_stress.py --runs 10 --busy 6
Logs and summary.json survive under src/target-codex-verify/stability-<nonce>.
Only successful targets created by this invocation are removed, except the last.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid


WORKSPACE = Path(__file__).resolve().parents[1]
NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


def busy_child(stop, lifetime):
    started = time.monotonic()
    value = 1
    while not stop.exists() and time.monotonic() - started < lifetime:
        for _ in range(100_000):
            value = (value * 1_664_525 + 1_013_904_223) & 0xFFFF_FFFF
    print(json.dumps({"cpu_seconds": time.process_time(), "checksum": value}))


def source_hash():
    digest = hashlib.sha256()
    suffixes = {".rs", ".toml", ".lock", ".py", ".stderr"}
    paths = [path for path in WORKSPACE.iterdir() if path.is_file() and path.suffix in suffixes]
    for directory in ("src", "tests", "examples", "benches", "framework", "framework-macros"):
        for current, directories, filenames in os.walk(WORKSPACE / directory):
            directories[:] = [name for name in directories if name != "target" and not name.startswith("target-")]
            paths.extend(Path(current) / name for name in filenames if Path(name).suffix in suffixes)
    for path in sorted(paths):
        digest.update(str(path.relative_to(WORKSPACE)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def remove_created_target(target, root):
    # Never follow a supplied or pre-existing path for recursive cleanup.
    resolved = target.resolve()
    if resolved.parent != root.resolve() or not re.fullmatch(r"target-\d+", resolved.name):
        raise RuntimeError("refusing cleanup outside this run's target directories")
    resolved.relative_to(WORKSPACE)
    deadline = time.monotonic() + 30
    while resolved.exists():
        try:
            shutil.rmtree(resolved)
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def stop_tree(process):
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=NO_WINDOW, check=False,
        )
    else:
        import signal
        os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=30)


def run_once(index, count, busy, deadline, root):
    target = root / f"target-{index:02d}"
    target.mkdir(exist_ok=False)
    stop = root / f"stop-{index:02d}"
    loads = []
    process = None
    started = time.monotonic()
    load_results = []
    environment = dict(os.environ)
    environment.pop("DISCORD_TOKEN", None)
    environment.update(CARGO_TARGET_DIR=str(target), CARGO_NET_OFFLINE="true", RUST_BACKTRACE="1")
    command = [shutil.which("cargo") or "cargo", "test", "--offline", "--workspace"]
    log = root / f"run-{index:02d}.log"
    try:
        for _ in range(busy):
            loads.append(subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--busy-child", str(stop), str(deadline + 60)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                creationflags=NO_WINDOW, env=environment,
            ))
        print(f"run {index}/{count}: fresh target; {busy} CPU workers; {log}", flush=True)
        with log.open("wb") as output:
            process = subprocess.Popen(
                command, cwd=WORKSPACE, env=environment, stdout=output,
                stderr=subprocess.STDOUT, creationflags=NO_WINDOW,
                start_new_session=os.name != "nt",
            )
            try:
                exit_code = process.wait(timeout=deadline)
            except subprocess.TimeoutExpired:
                exit_code = 124
                stop_tree(process)
    finally:
        try:
            if process is not None:
                stop_tree(process)
        finally:
            stop.touch()
            for child in loads:
                try:
                    stdout, _ = child.communicate(timeout=15)
                    load_results.append(json.loads(stdout))
                except (subprocess.TimeoutExpired, json.JSONDecodeError):
                    child.kill()
                    child.communicate()
                    load_results.append({"cpu_seconds": None})
    elapsed = time.monotonic() - started
    content = log.read_text(encoding="utf-8", errors="replace")
    results = re.findall(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", content)
    totals = [sum(int(row[column]) for row in results) for column in range(3)]
    record = {
        "run": index, "command": command, "target": str(target), "fresh_target": True,
        "busy_processes": busy, "load_results": load_results, "exit_code": exit_code,
        "elapsed_seconds": round(elapsed, 3), "passed": totals[0], "failed": totals[1],
        "ignored": totals[2], "log": str(log),
    }
    print(json.dumps(record), flush=True)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--busy", type=int, default=max(2, (os.cpu_count() or 4) // 2))
    parser.add_argument("--unloaded-runs", type=int, default=1)
    parser.add_argument("--deadline", type=int, default=900)
    args = parser.parse_args()
    if args.runs < 1 or args.busy < 0 or not 0 <= args.unloaded_runs <= args.runs or args.deadline < 1:
        parser.error("invalid run count, load count, or deadline")
    root = WORKSPACE / "target-codex-verify" / f"stability-{uuid.uuid4().hex[:12]}"
    root.mkdir(parents=True, exist_ok=False)
    initial_hash = source_hash()
    summary = {
        "source_sha256": initial_hash, "logical_cpus": os.cpu_count(),
        "platform": sys.platform, "runs": [],
    }
    for tool in ("cargo", "rustc"):
        summary[tool] = subprocess.check_output(
            [shutil.which(tool) or tool, "--version"], text=True, creationflags=NO_WINDOW,
            stderr=subprocess.DEVNULL,
        ).strip()
    print(f"artifacts: {root}", flush=True)
    for index in range(1, args.runs + 1):
        if source_hash() != initial_hash:
            raise RuntimeError("sources changed during the consecutive test series")
        busy = 0 if index <= args.unloaded_runs else args.busy
        record = run_once(index, args.runs, busy, args.deadline, root)
        summary["runs"].append(record)
        (root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        if record["exit_code"] != 0:
            return record["exit_code"]
        if source_hash() != initial_hash:
            raise RuntimeError("sources changed during the test run")
        if busy and any(result["cpu_seconds"] is None or result["cpu_seconds"] <= 0 for result in record["load_results"]):
            raise RuntimeError("CPU load was not verified")
        if index < args.runs:
            remove_created_target(Path(record["target"]), root)
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--busy-child":
        busy_child(Path(sys.argv[2]), int(sys.argv[3]))
    else:
        sys.exit(main())
