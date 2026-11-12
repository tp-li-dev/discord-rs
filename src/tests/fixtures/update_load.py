"""Bounded, open-loop process load probe. Standard library only; no credentials logged.

Invoked by update_load.rs, or directly with --botd/--gateway/--relay/--output.
The legacy Worker pool and the dedicated Shard Core are explicitly distinguished.
"""
import argparse
from collections import Counter
import ctypes
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import platform
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid

NO_WINDOW = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
CASES = ("core_series", "gateway_switch", "crash_old_core", "crash_new_core")
OPERATION_ERRORS = {"Gateway control acknowledgement identity mismatch", "Gateway acknowledgement state mismatch",
                    "local authority expired", "local lease expired", "Gateway or its control connection exited",
                    "Gateway process exited", "Gateway exited abnormally before stop", "Gateway recovery requires operator review",
                    "mock Gateway readiness rejected", "Shard is not ready", "Gateway drain did not exit successfully",
                    "Gateway control deadline exceeded", "invalid mock relay frame", "Gateway IPC read failed"}


class OperationFailed(Exception):
    pass


CONFIG = '''[cluster]
id = "update-load"
mode = "standalone"
[node]
id = "local"
[[bots]]
name = "load"
bot_id = "101"
application_id = "101"
token_env = "UNUSED_LOAD_TOKEN"
total_shards = 1
[[bots.shard_placement]]
shard_id = 0
primary = "local"
'''


def emit(value):
    print(json.dumps(value, separators=(",", ":")), flush=True)


def http_fixture():
    """One endpoint per serial Gateway; correlate PATCH with its preceding ACK.

    The mock Gateway serializes commands. Never use this correlation scheme for
    a production/concurrent HTTP client. Only IDs/timestamps are emitted.
    """
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        last_slash = None

        def log_message(self, *_args):
            pass

        def do_POST(self):
            self.respond()

        def do_PATCH(self):
            self.respond()

        def respond(self):
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            parts = self.path.split("/")
            if parts[1] == "interactions":
                event_id = int(parts[2])
                Handler.last_slash = event_id
                kind, status = "ack", 204
            elif parts[1] == "channels":
                event_id, kind, status = int(parts[2]), "effect", 200
            else:
                event_id, kind, status = Handler.last_slash, "effect", 200
            if kind == "effect" and body.get("content") != "Pong! 🦀":
                kind = "failed_effect"
            # Small fixed service cost makes backlog visible without CPU spinning.
            time.sleep(0.004)
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.flush()
            emit({"kind": kind, "id": event_id, "ns": time.monotonic_ns()})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    emit({"port": server.server_port})
    server.serve_forever(poll_interval=0.05)


def clean_env():
    # Explicit whitelist: inherited application secrets never enter any child.
    allowed = ("SystemRoot", "WINDIR", "PATH", "TEMP", "TMP", "TMPDIR")
    return {key: os.environ[key] for key in allowed if key in os.environ}


def spawn(args, **kwargs):
    return subprocess.Popen(args, env=clean_env(), creationflags=NO_WINDOW, **kwargs)


def read_json(stream):
    line = stream.readline(65537)
    if not line or len(line) > 65536 or not line.endswith(b"\n"):
        raise RuntimeError("bounded JSON channel ended or exceeded frame limit")
    return json.loads(line)


def announced(stream):
    """Bound setup pipe reads; daemon/HTTP children are owned by Run cleanup."""
    import queue
    result = queue.Queue()

    def read():
        try:
            result.put(read_json(stream))
        except Exception as error:
            result.put(error)

    threading.Thread(target=read, daemon=True).start()
    value = result.get(timeout=15)
    if isinstance(value, Exception):
        raise value
    return value


def eventually(predicate, seconds=15):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise TimeoutError("observed condition deadline exceeded")


def live(pid):
    if os.name == "nt":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            if ctypes.get_last_error() == 87:  # ERROR_INVALID_PARAMETER: no PID
                return False
            raise OSError(ctypes.get_last_error(), "process probe failed")
        try:
            code = ctypes.c_ulong()
            kernel.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
            if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)):
                raise OSError(ctypes.get_last_error(), "process exit-code probe failed")
            return code.value == 259
        finally:
            kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def kill_owned(pid):
    if os.name == "nt":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.restype = ctypes.c_void_p
        handle = kernel.OpenProcess(1, False, pid)
        if not handle:
            raise OSError("owned fault target unavailable")
        try:
            kernel.TerminateProcess.argtypes = [ctypes.c_void_p, ctypes.c_uint]
            if not kernel.TerminateProcess(handle, 86):
                raise OSError("owned fault injection failed")
        finally:
            kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            kernel.CloseHandle(handle)
    else:
        os.kill(pid, signal.SIGKILL)


def metrics(sent, observations, replies):
    effects = [row for row in observations if row["kind"] == "effect"]
    counts = Counter(row["id"] for row in observations if row["kind"] in ("effect", "failed_effect"))
    successful = Counter(row["id"] for row in effects)
    ack = {}
    ack_counts = Counter()
    for row in observations:
        if row["kind"] == "ack":
            ack_counts[row["id"]] += 1
            ack.setdefault(row["id"], row["ns"])
    # Only first side effects participate in order; duplicates are counted above.
    order, seen, highest, inversions = [], set(), -1, 0
    for row in sorted(effects, key=lambda value: value["ns"]):
        event_id = row["id"]
        if event_id in seen:
            continue
        seen.add(event_id)
        order.append(event_id)
        inversions += event_id < highest
        highest = max(highest, event_id)
    slash = {key: value for key, value in sent.items() if value["slash"]}
    latencies = [(ack[key] - row["ns"]) / 1e6 for key, row in slash.items() if key in ack]
    result_counts = Counter(row["id"] for row in replies if row.get("type") == "result")
    return {
        "sent": len(sent), "interactions": len(slash),
        "missing_effects": sum(successful[key] == 0 for key in sent),
        "duplicate_effects": sum(max(0, counts[key] - 1) for key in sent),
        "unexpected_effects": sum(count for key, count in counts.items() if key not in sent),
        "failed_effects": sum(row["kind"] == "failed_effect" for row in observations),
        "order_inversions": inversions,
        "interaction_missing_ack": sum(key not in ack for key in slash),
        "interaction_duplicate_ack": sum(max(0, ack_counts[key] - 1) for key in slash),
        "interaction_over_3s": sum(value > 3000 for value in latencies),
        "interaction_ack_max_ms": round(max(latencies, default=0), 3),
        "missing_results": sum(result_counts[key] == 0 for key in sent),
        "duplicate_results": sum(max(0, result_counts[key] - 1) for key in sent),
        "failed_results": sum(row.get("type") == "result" and row.get("outcome") != "completed" for row in replies),
    }


def violations(result):
    zero = ("missing_effects", "duplicate_effects", "unexpected_effects", "failed_effects", "order_inversions",
            "interaction_missing_ack", "interaction_duplicate_ack", "interaction_over_3s", "missing_results", "duplicate_results",
            "failed_results", "double_active_samples", "orphans", "harness_errors", "operation_failures")
    failures = [key for key in zero if result.get(key, 0) != 0]
    if result.get("sent", 0) < 20 or result.get("interactions", 0) < 10:
        failures.append("insufficient_load")
    if result.get("active_samples", 0) == 0:
        failures.append("missing_active_observation")
    return failures


class Gateway:
    def __init__(self, owner, stream):
        self.owner, self.stream = owner, stream
        self.lock = threading.Lock()
        self.file = stream.makefile("rb")
        self.ready = read_json(self.file)
        assert self.ready["type"] == "ready"
        assert not self.ready["credential_present"]
        self.done = False
        self.status = self.ready
        self.observe_workers(self.ready)
        self.http = spawn([sys.executable, "-u", __file__, "--http-fixture"],
                          stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        owner.processes.append(self.http)
        owner.pids.add(self.http.pid)
        self.addr = f"127.0.0.1:{announced(self.http.stdout)['port']}"
        self.threads = [threading.Thread(target=self.collect, daemon=True),
                        threading.Thread(target=self.collect_http, daemon=True)]
        for thread in self.threads:
            thread.start()

    def observe_workers(self, value):
        self.owner.pids.add(value["pid"])
        for worker in value["workers"]:
            assert worker["credential_free"]
            self.owner.pids.add(worker["pid"])

    def collect(self):
        try:
            while True:
                row = read_json(self.file)
                if row["type"] == "status":
                    self.status = row
                    self.observe_workers(row)
                    # Only routed slots are exposed by the legacy Worker pool.
                    self.owner.active_samples += 1
                    slots = [worker["index"] for worker in row["workers"]]
                    self.owner.double_active += len(slots) - len(set(slots))
                elif row["type"] == "result":
                    receipt = row.get("receipt")
                    if receipt:
                        self.owner.pids.add(receipt["pid"])
                    self.owner.replies.append(row)
                elif row["type"] == "stopped":
                    break
        except (OSError, RuntimeError):
            pass  # EOF is expected on retirement; missing work is scored separately.
        except Exception:
            self.owner.background_errors += 1
        finally:
            self.done = True

    def collect_http(self):
        try:
            while True:
                self.owner.observations.append(read_json(self.http.stdout))
        except (OSError, RuntimeError):
            pass
        except Exception:
            self.owner.background_errors += 1

    def send(self, value):
        with self.lock:
            self.stream.sendall(json.dumps(value, separators=(",", ":")).encode() + b"\n")


class Run:
    def __init__(self, options):
        self.options = options
        self.root = options.output.resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.secret = list(os.urandom(32))  # anonymous pipe/socket only; never saved
        self.sent, self.observations, self.replies = {}, [], []
        self.pids, self.processes, self.gateways = set(), [], []
        self.active_samples = self.double_active = self.send_errors = self.background_errors = 0
        self.stop = threading.Event()
        self.port, self.daemon = None, None
        self.changes, self.faults, self.errors = [], [], []
        self.operation_failures = []
        self.transitions = []
        self.probe_errors = []
        self.stage = "startup"
        self.rolling = options.case == "gateway_switch"
        self.collector = socket.create_server(("127.0.0.1", 0))
        self.collector.settimeout(15)

    def request(self, command, **kwargs):
        with socket.create_connection(("127.0.0.1", self.port), timeout=3) as sock:
            sock.sendall(json.dumps({"secret": self.secret, "command": command, **kwargs}).encode() + b"\n")
            return read_json(sock.makefile("rb"))

    def begin(self, command):
        operation = uuid.uuid4().hex
        response = self.request(command, bot="101/0" if self.rolling else "load", operation_id=operation)
        assert response["ok"], "management admission failed"
        return operation

    def finish_operation(self, operation):
        value = eventually(lambda: self.operation_done(operation))
        if value["state"] != "succeeded":
            self.operation_failures.append({"stage": self.stage, "state": value["state"],
                                            "code": value.get("error") if value.get("error") in OPERATION_ERRORS else "unclassified"})
            raise OperationFailed()

    def operation_done(self, operation):
        value = self.request("operation", operation_id=operation)["operation"]
        return value if value["state"] != "running" else None

    def observe_status(self, status):
        self.active_samples += 1
        # Only documented public transition metadata is persisted.
        self.transitions = [{key: row.get(key) for key in
                             ("event", "generation", "epoch", "pid", "active_count")}
                            for row in status["transitions"]]
        self.double_active += sum(event["active_count"] > 1 for event in self.transitions)
        for row in status["shards"]:
            self.double_active += sum(p["state"] == "active" for p in row["processes"]) > 1

    def accept(self):
        stream, _ = self.collector.accept()
        stream.settimeout(15)
        gateway = Gateway(self, stream)
        stream.settimeout(None)
        # Bound writes without changing the blocking reader's file flags.
        stream.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO,
                          1000 if os.name == "nt" else struct.pack("ll", 1, 0))
        self.gateways.append(gateway)
        return gateway

    def start(self):
        (self.root / "placement.toml").write_text(CONFIG, encoding="utf-8")
        args = [str(self.options.botd), "--file", str(self.root / "placement.toml"),
                "--runtime-root", str(self.root), "--gateway-program", str(self.options.gateway),
                "--bootstrap-stdin", "--mock-discord", f"127.0.0.1:{self.collector.getsockname()[1]}"]
        if self.rolling:
            budget = self.root / "identify.json"
            budget.write_text(json.dumps([{"bot_id": 101, "total": 10, "remaining": 10,
                                         "reset_after_ms": 60000, "max_concurrency": 1, "interval_ms": 50}]))
            args += ["--allow-test-gateway-switch", "--mock-identify-budget", str(budget)]
        self.daemon = spawn(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        self.processes.append(self.daemon)
        self.pids.add(self.daemon.pid)
        self.daemon.stdin.write(json.dumps({"secret": self.secret}).encode() + b"\n")
        self.daemon.stdin.close()
        announcement = announced(self.daemon.stdout)
        assert announcement["startup_ok"]
        self.port = announcement["port"]
        self.accept()
        if self.rolling:
            eventually(lambda: self.request("status")["shards"][0]["phase"] == "Stable")
        self.producer = threading.Thread(target=self.produce, daemon=True)
        self.producer.start()

    def produce(self):
        event_id, deadline = 0, time.monotonic()
        while not self.stop.is_set():
            event_id += 1
            self.sent[event_id] = {"slash": event_id % 2 == 0, "ns": time.monotonic_ns()}
            # Broadcast to all currently connected sessions; never choose Active
            # in the harness or retry a suppressed/failed event.
            for gateway in list(self.gateways):
                if gateway.done:
                    continue
                slash = event_id % 2 == 0
                value = {"type": "slash" if slash else "prefix", "id": event_id,
                         "key": event_id, "rest_addr": gateway.addr}
                if self.rolling:
                    # Stable semantic identity across both mock sessions for
                    # the local admission barrier; it does not select Core/Shard.
                    value["event_id"] = event_id
                value["name" if slash else "content"] = "ping" if slash else "!ping"
                try:
                    gateway.send(value)
                    if event_id % 5 == 0:
                        gateway.send({"type": "status", "id": 0})
                except OSError:
                    self.send_errors += 1
            deadline += 1 / self.options.rate
            self.stop.wait(max(0, deadline - time.monotonic()))

    def relay(self, name, mode):
        path = self.root / (name + self.options.relay.suffix)
        shutil.copyfile(self.options.relay, path)
        if os.name != "nt":
            path.chmod(0o700)
        Path(str(path) + ".real").write_text(str(self.options.gateway), encoding="utf-8")
        Path(str(path) + ".mode").write_text(mode)
        return path

    def publish(self, gateway, path):
        watch = Path(gateway.ready["state_path"]) / "worker-path.txt"
        temporary = watch.with_suffix(".new")
        temporary.write_text(str(path), encoding="utf-8")
        temporary.replace(watch)
        return Path(str(watch) + ".status")

    def discover_relays(self, final=False):
        for file in self.root.glob("*.started"):
            try:
                value = json.loads(file.read_text())
            except (ValueError, OSError):
                if final:
                    self.errors.append("unreadable_final_process_marker")
                continue  # writer may still own the newly created record
            self.pids.update((value["relay_pid"], value["worker_pid"]))

    def scenarios(self):
        gateway = self.gateways[0]
        eventually(lambda: len(self.replies) >= 20)
        if self.rolling:
            for _ in range(2):
                before = len(self.sent)
                started = time.monotonic_ns()
                self.stage = "gateway-prepare"
                operation = self.begin("gateway-prepare")
                self.accept()
                self.finish_operation(operation)
                self.stage = "gateway-update"
                operation = self.begin("gateway-update")
                while True:
                    status = self.request("status")
                    self.observe_status(status)
                    if self.operation_done(operation):
                        break
                    time.sleep(0.02)
                self.finish_operation(operation)
                self.changes.append({"kind": "gateway", "generation": self.gateways[-1].ready["gateway_generation"],
                                     "start_ns": started, "end_ns": time.monotonic_ns(), "events_during": len(self.sent) - before})
                time.sleep(0.4)
        elif self.options.case == "core_series":
            for index in range(3):
                before = len(self.sent)
                started = time.monotonic_ns()
                path = self.relay(f"core-{index}", "pass")
                status = self.publish(gateway, path)
                eventually(lambda: status.exists() and status.read_text() == str(path))
                self.discover_relays()
                self.changes.append({"kind": "legacy_worker_pool", "revision": index + 1,
                                     "start_ns": started, "end_ns": time.monotonic_ns(), "events_during": len(self.sent) - before})
        else:
            before = len(self.sent)
            started = time.monotonic_ns()
            old = gateway.status["workers"][0]["pid"]
            mode = "hold-restore" if self.options.case == "crash_old_core" else "crash-dispatch"
            path = self.relay("fault-core", mode)
            status = self.publish(gateway, path)
            restored = eventually(lambda: list(self.root.glob(path.name + ".*.restored")))
            self.discover_relays()
            if mode == "hold-restore":
                assert old in self.pids and live(old)
                kill_owned(old)
                self.faults.append({"target": "old_worker", "pid": old, "ns": time.monotonic_ns(),
                                    "stage": "candidate_restore_before_commit"})
                Path(str(path) + ".release").touch()
            else:
                crashed = eventually(lambda: list(self.root.glob(path.name + ".*.crashed")))
                marker = Path(str(crashed[0]).removesuffix(".crashed") + ".started")
                identity = json.loads(marker.read_text())
                self.faults.append({"target": "new_worker_relay", **identity, "ns": time.monotonic_ns(),
                                    "stage": "first_dispatch_after_restore", "restores": len(restored)})
            eventually(lambda: status.exists() and status.read_text() == str(path))
            self.changes.append({"kind": "legacy_worker_pool", "revision": 1,
                                 "start_ns": started, "end_ns": time.monotonic_ns(), "events_during": len(self.sent) - before})
        time.sleep(1)

    def shutdown(self):
        self.stop.set()
        if hasattr(self, "producer"):
            self.producer.join(timeout=3)
            if self.producer.is_alive():
                self.errors.append("producer_join_deadline")
                for gateway in self.gateways:
                    try:
                        gateway.stream.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                self.producer.join(timeout=3)
        # Allow backlog to complete; this wait is not subtracted from ACK latency.
        end = time.monotonic() + 5
        while time.monotonic() < end:
            results = {row["id"] for row in self.replies}
            if results.issuperset(self.sent):
                break
            time.sleep(0.05)
        try:
            if self.port:
                self.stage = "stop"
                self.finish_operation(self.begin("stop"))
        except OperationFailed:
            pass
        except Exception as error:
            self.errors.append(type(error).__name__ + ":stop")
        for gateway in self.gateways:
            try:
                gateway.stream.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            gateway.stream.close()
        self.discover_relays(final=True)
        # Fixture processes are explicitly owned and reaped; no tree-wide kill.
        for process in self.processes:
            try:
                if process.poll() is None:
                    process.terminate()
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                self.errors.append("owned_process_stop_deadline")
                try:
                    process.kill()
                    process.wait(timeout=3)
                except (OSError, subprocess.TimeoutExpired):
                    self.errors.append("owned_process_kill_deadline")
        for gateway in self.gateways:
            for thread in gateway.threads:
                thread.join(timeout=2)
        deadline = time.monotonic() + 5
        while True:
            remaining, unknown = [], []
            for pid in list(self.pids):
                try:
                    if live(pid):
                        remaining.append(pid)
                except OSError as error:
                    # A process can transiently reject a query while exiting.
                    # Retry within the observation window; never turn a failed
                    # probe into either "absent" or a fabricated live process.
                    unknown.append({"pid": pid, "code": error.errno})
            if not remaining and not unknown or time.monotonic() >= deadline:
                break
            time.sleep(0.05)
        self.probe_errors = unknown
        # Count before emergency cleanup: cleanup cannot turn an orphan into pass.
        for pid in remaining:
            try:
                kill_owned(pid)
            except OSError:
                self.errors.append("orphan_emergency_cleanup_failed")
        self.collector.close()
        return None if unknown else remaining

    def watchdog(self):
        # Last resort for a stuck startup/control/fixture. The normal path keeps
        # richer evidence. Never kill an unobserved PID or arbitrary process tree.
        self.stop.set()
        self.discover_relays()
        for process in self.processes:
            self.pids.add(process.pid)
        for pid in list(self.pids):
            try:
                if live(pid):
                    kill_owned(pid)
            except OSError:
                pass
        (self.root / "watchdog.json").write_text(json.dumps({"failure": "75s_harness_deadline"}))
        os._exit(2)

    def execute(self):
        started = time.monotonic()
        watchdog = threading.Timer(75, self.watchdog)
        watchdog.daemon = True
        watchdog.start()
        assert live(os.getpid()), "PID probe positive control"
        try:
            self.start()
            self.scenarios()
        except OperationFailed:
            pass
        except Exception as error:
            self.errors.append(self.stage + ":" + type(error).__name__)  # no raw external data
        try:
            remaining = self.shutdown()
        except Exception as error:
            self.errors.append(type(error).__name__ + ":cleanup")
            remaining = None
        result = metrics(self.sent, self.observations, self.replies)
        result.update(case=self.options.case, rate_hz=self.options.rate,
                      elapsed_s=round(time.monotonic() - started, 3), changes=self.changes,
                      faults=self.faults, active_samples=self.active_samples,
                      double_active_samples=self.double_active, orphans=None if remaining is None else len(remaining),
                      send_errors=self.send_errors, harness_errors=len(self.errors) + self.background_errors,
                      operation_failures=len(self.operation_failures), management_failures=self.operation_failures,
                      errors=self.errors, observed_pids=sorted(self.pids), orphan_pids=remaining,
                      orphan_probe_errors=self.probe_errors,
                      scope="dedicated_shard_core_gateway_switch" if self.rolling else "legacy_two_worker_pool",
                      binary_sha256={key: hashlib.sha256(getattr(self.options, key).read_bytes()).hexdigest()
                                     for key in ("botd", "gateway", "relay")})
        result["violations"] = violations(result)
        if self.rolling and len(self.changes) != 2:
            result["violations"].append("gateway_switch_count")
        elif not self.rolling and len(self.changes) != (3 if self.options.case == "core_series" else 1):
            result["violations"].append("worker_switch_count")
        if self.options.case.startswith("crash_") and not self.faults:
            result["violations"].append("fault_not_observed")
        if any(change["events_during"] == 0 for change in self.changes):
            result["violations"].append("no_load_during_change")
        result["environment"] = {"platform": platform.platform(), "python": platform.python_version()}
        result["active_observation_scope"] = "routed_slots_and_acknowledged_gateway_transitions" if self.rolling else "routed_worker_slots_only"
        result["transitions"] = self.transitions
        result["fixture_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        # Persist allowlisted evidence, not raw frames, management requests or env.
        (self.root / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
        evidence = {"sent": self.sent, "http": self.observations,
                    "results": [{key: row.get(key) for key in ("type", "id", "outcome")} for row in self.replies]}
        (self.root / "events.json").write_text(json.dumps(evidence), encoding="utf-8")
        emit(result)
        watchdog.cancel()
        return int(bool(result["violations"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--http-fixture", action="store_true")
    parser.add_argument("--case", choices=CASES)
    for name in ("botd", "gateway", "relay", "output"):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument("--rate", type=float, default=40)
    options = parser.parse_args()
    if options.http_fixture:
        http_fixture()
        return 0
    if not options.case or any(getattr(options, key) is None for key in ("botd", "gateway", "relay", "output")):
        parser.error("case, botd, gateway, relay and output are required")
    if not 1 <= options.rate <= 200:
        parser.error("rate must be between 1 and 200 Hz")
    for key in ("botd", "gateway", "relay"):
        setattr(options, key, getattr(options, key).resolve(strict=True))
    return Run(options).execute()


if __name__ == "__main__":
    sys.exit(main())
