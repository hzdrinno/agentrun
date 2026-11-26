"""Run trusted Python programs with buffered input and output through one TCP port."""

import argparse
import asyncio
import codecs
from collections import deque
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import ipaddress
import json
import logging
import os
from pathlib import Path
import platform
import secrets
import socket
import ssl
import subprocess
import sys
import time
import uuid

from runner_tls import TLS_NAME, cert_not_after, ensure_tls_files

MAX_FRAME = 1024 * 1024
MAX_SOURCE = 256 * 1024
MAX_EVENT = 64 * 1024
READ_BYTES = 512 * 1024
MAX_READ_LIMIT = 2000
PROGRAM_RETENTION = 256
REPO = Path(__file__).resolve().parent
WORKER = REPO / "runner_worker.py"
LOG = logging.getLogger("agentrun")


class RequestError(Exception):
    pass


def frame(value):
    return json.dumps(value, allow_nan=False, separators=(",", ":")).encode() + b"\n"


def integer(request, key, default, minimum, maximum):
    value = request.get(key, default)
    if type(value) is not int or not minimum <= value <= maximum:
        raise RequestError(f"{key} must be an integer between {minimum} and {maximum}")
    return value


def number(request, key, default, minimum, maximum):
    value = request.get(key, default)
    if type(value) not in (int, float) or not minimum <= value <= maximum:
        raise RequestError(f"{key} must be a number between {minimum} and {maximum}")
    return float(value)


def flag(request, key, default=False):
    value = request.get(key, default)
    if type(value) is not bool:
        raise RequestError(f"{key} must be a boolean")
    return value


def required_text(request, key):
    value = request.get(key)
    if not isinstance(value, str) or not value:
        raise RequestError(f"{key} must be a nonempty string")
    return value


def optional_name(request, key="name"):
    value = request.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not 0 < len(value) <= 128:
        raise RequestError(f"{key} must be a nonempty string of at most 128 characters")
    return value


def optional_channels(request):
    value = request.get("channels")
    if value is None:
        return None
    if (not isinstance(value, list) or not 1 <= len(value) <= 64
            or not all(isinstance(ch, str) and 0 < len(ch) <= 128 for ch in value)):
        raise RequestError("channels must be a list of 1 to 64 channel names")
    return set(value)


def git_rev_sync(repo):
    if not (repo / ".git").exists():
        return None
    try:
        res = subprocess.run(["git", "-C", str(repo), "rev-parse", "--short=12", "HEAD"],
                             capture_output=True, text=True, timeout=5, check=True)
        return res.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


async def run_subprocess(*cmd, cwd=None, timeout=30):
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
    except TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise RequestError(f"command timed out: {cmd[0]}") from exc
    return proc.returncode, stdout.decode("utf-8", "replace").strip(), stderr.decode("utf-8", "replace").strip()


class Run:
    def __init__(self, run_id, program, name, proc, buffer_records, buffer_bytes):
        self.id = run_id
        self.program = program
        self.name = name
        self.proc = proc
        self.buffer_records = buffer_records
        self.buffer_bytes = buffer_bytes
        self.events = deque()
        self.event_bytes = 0
        self.sequence = 0
        self.changed = asyncio.Event()
        self.inbox = asyncio.Queue(maxsize=32)
        self.state = "running"
        self.reason = None
        self.task = None
        self.input_task = None
        self.stop_lock = asyncio.Lock()
        self.started_utc = datetime.now(timezone.utc).isoformat()
        self.exited_utc = None
        self.exited_at = None
        self.last_event_utc = None

    def signal(self):
        previous = self.changed
        self.changed = asyncio.Event()
        previous.set()

    def emit(self, channel, value, timestamp=None):
        now_utc = datetime.now(timezone.utc).isoformat()
        event = {
            "seq": self.sequence + 1,
            "channel": channel,
            "value": value,
            "host_monotonic_ns": str(time.monotonic_ns()),
            "host_utc": now_utc,
        }
        if timestamp is not None:
            event["timestamp"] = timestamp
        size = len(frame(event))
        self.sequence += 1
        self.last_event_utc = now_utc
        self.events.append((size, event))
        self.event_bytes += size
        while len(self.events) > self.buffer_records or self.event_bytes > self.buffer_bytes:
            evicted_size, _ = self.events.popleft()
            self.event_bytes -= evicted_size
        self.signal()

    def status(self):
        return {
            "run_id": self.id,
            "program_id": self.program,
            "name": self.name,
            "pid": self.proc.pid,
            "state": self.state,
            "reason": self.reason,
            "returncode": self.proc.returncode,
            "latest_seq": self.sequence,
            "buffered_from_seq": self.events[0][1]["seq"] if self.events else None,
            "buffered_count": len(self.events),
            "buffered_bytes": self.event_bytes,
            "started_utc": self.started_utc,
            "exited_utc": self.exited_utc,
            "last_event_utc": self.last_event_utc,
        }

    async def read(self, after, limit, wait_ms, channels=None, tail=None):
        if tail is not None:
            selected = []
            size = 0
            for count, event in reversed(self.events):
                if channels is not None and event["channel"] not in channels:
                    continue
                if len(selected) == tail or size + count > READ_BYTES:
                    break
                size += count
                selected.append(event)
            selected.reverse()
            return {"records": selected, "cursor": self.sequence, "gap": None, **self.status()}

        deadline = time.monotonic() + wait_ms / 1000 if wait_ms else None
        while True:
            oldest = self.events[0][1]["seq"] if self.events else self.sequence + 1
            gap = {"from": after + 1, "through": oldest - 1} if after + 1 < oldest else None
            selected = []
            size = 0
            cursor = max(after, oldest - 1)
            for count, event in self.events:
                if event["seq"] <= after:
                    continue
                if channels is not None and event["channel"] not in channels:
                    cursor = event["seq"]
                    continue
                if len(selected) == limit or size + count > READ_BYTES:
                    break
                size += count
                selected.append(event)
                cursor = event["seq"]
            if selected or gap or self.state != "running" or deadline is None:
                return {"records": selected, "cursor": cursor, "gap": gap, **self.status()}
            after = cursor
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {"records": [], "cursor": cursor, "gap": None, **self.status()}
            try:
                await asyncio.wait_for(self.changed.wait(), remaining)
            except TimeoutError:
                return {"records": [], "cursor": cursor, "gap": None, **self.status()}

    async def _discard_line(self):
        while True:
            try:
                await self.proc.stdout.readuntil(b"\n")
                return
            except asyncio.LimitOverrunError as exc:
                await self.proc.stdout.readexactly(exc.consumed)
            except asyncio.IncompleteReadError:
                return

    async def pump_output(self):
        while True:
            try:
                line = await self.proc.stdout.readuntil(b"\n")
            except asyncio.IncompleteReadError as exc:
                if exc.partial:
                    self.emit("runner", "Truncated worker output")
                return
            except asyncio.LimitOverrunError as exc:
                await self.proc.stdout.readexactly(exc.consumed)
                await self._discard_line()
                self.emit("runner", "Discarded oversized worker output")
                continue
            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError()
                channel = required_text(event, "channel")
                if len(channel) > 128:
                    raise ValueError()
                self.emit(channel, event["value"], event.get("timestamp"))
            except (ValueError, RequestError, KeyError, RecursionError):
                self.emit("runner", "Discarded malformed worker output")

    async def pump_logs(self):
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        while chunk := await self.proc.stderr.read(4096):
            text = decoder.decode(chunk)
            if text:
                self.emit("log", text)
        tail = decoder.decode(b"", final=True)
        if tail:
            self.emit("log", tail)

    async def pump_input(self):
        try:
            while True:
                self.proc.stdin.write(await self.inbox.get())
                await self.proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    async def wait_for_exit(self, timeout):
        output_task = asyncio.create_task(self.pump_output())
        logs_task = asyncio.create_task(self.pump_logs())
        self.input_task = asyncio.create_task(self.pump_input())
        tasks = [output_task, logs_task, self.input_task]
        try:
            await asyncio.wait_for(self.proc.wait(), timeout)
        except TimeoutError:
            await self.stop("timeout")
        finally:
            if self.proc.stdin is not None:
                try:
                    self.proc.stdin.close()
                except OSError:
                    pass
            self.input_task.cancel()
            _, pending = await asyncio.wait(tasks, timeout=2)
            for task in pending:
                task.cancel()
            for result in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(result, Exception):
                    self.emit("runner", f"Worker I/O failed: {type(result).__name__}")
            self.state = "exited"
            self.exited_at = time.monotonic()
            self.exited_utc = datetime.now(timezone.utc).isoformat()
            LOG.info("exited %s (code=%s, reason=%s)", self.id, self.proc.returncode, self.reason)
            self.signal()

    async def stop(self, reason, grace_s=1.5):
        async with self.stop_lock:
            if self.proc.returncode is not None:
                return
            if self.reason is None:
                self.reason = reason
            if self.input_task is not None and not self.input_task.done():
                self.input_task.cancel()
            if self.proc.stdin is not None:
                try:
                    self.proc.stdin.close()
                except OSError:
                    pass
            if grace_s > 0:
                try:
                    await asyncio.wait_for(self.proc.wait(), grace_s)
                    return
                except TimeoutError:
                    pass
            try:
                self.proc.terminate()
                await asyncio.wait_for(self.proc.wait(), 2)
            except TimeoutError:
                if self.proc.returncode is None:
                    self.proc.kill()
                    await self.proc.wait()
            except ProcessLookupError:
                pass


class Runner:
    READ_OPS = {"ping", "list", "status", "read"}

    def __init__(self, args, token, read_token, tls_expires=None):
        self.args = args
        self.token = token
        self.read_token = read_token
        self.tls_expires = tls_expires
        self.programs = args.state_dir / "programs"
        self.programs.mkdir(parents=True, exist_ok=True)
        self.runs = {}
        self.clients = set()
        self.start_lock = asyncio.Lock()
        self.update_lock = asyncio.Lock()
        self.started_at = time.monotonic()
        self.rev = git_rev_sync(REPO)
        self.restart_requested = False
        self.shutdown_event = asyncio.Event()

    def authorize(self, token):
        if not isinstance(token, str):
            return None
        encoded = token.encode()
        if hmac.compare_digest(encoded, self.token):
            return "operator"
        if self.read_token is not None and hmac.compare_digest(encoded, self.read_token):
            return "reader"
        return None

    def _prune_programs(self):
        files = list(self.programs.glob("*.py"))
        if len(files) <= PROGRAM_RETENTION:
            return
        in_use = {run.program for run in self.runs.values()}
        candidates = sorted(
            (path for path in files if path.stem not in in_use),
            key=lambda p: p.stat().st_mtime,
        )
        excess = len(files) - PROGRAM_RETENTION
        for path in candidates[:excess]:
            path.unlink(missing_ok=True)

    def upload(self, req):
        source = required_text(req, "source").encode("utf-8")
        if len(source) > MAX_SOURCE:
            raise RequestError(f"source exceeds {MAX_SOURCE} bytes")
        try:
            compile(source, "<upload>", "exec")
        except SyntaxError as exc:
            raise RequestError(f"Python syntax error at line {exc.lineno}: {exc.msg}") from exc
        program = hashlib.sha256(source).hexdigest()
        destination = self.programs / (program + ".py")
        if not destination.exists():
            temporary = destination.with_suffix(".tmp")
            temporary.write_bytes(source)
            temporary.replace(destination)
        else:
            os.utime(destination, None)
        self._prune_programs()
        return {"program_id": program}

    async def start(self, req):
        program = required_text(req, "program_id")
        if len(program) != 64 or any(c not in "0123456789abcdef" for c in program):
            raise RequestError("invalid program_id")
        source = self.programs / (program + ".py")
        if not source.is_file():
            raise RequestError("unknown program_id")
        name = optional_name(req)
        timeout = integer(req, "timeout_s", 0, 0, 86400)
        buffer_records = integer(req, "buffer_records", self.args.buffer_records,
                                 1, self.args.buffer_records)
        buffer_bytes = integer(req, "buffer_bytes", self.args.buffer_bytes,
                               2 * MAX_EVENT, self.args.buffer_bytes)
        config = req.get("config", {})
        if not isinstance(config, dict):
            raise RequestError("config must be an object")
        initial = frame(config)
        if len(initial) > MAX_EVENT:
            raise RequestError("config is too large")
        async with self.start_lock:
            if sum(run.state == "running" for run in self.runs.values()) >= self.args.max_workers:
                raise RequestError("worker limit reached")
            if len(self.runs) >= self.args.max_runs:
                finished = min(
                    (run for run in self.runs.values() if run.state == "exited"),
                    key=lambda run: run.exited_at or 0.0,
                    default=None,
                )
                if finished is None:
                    raise RequestError("run retention limit reached")
                del self.runs[finished.id]
            run_id = uuid.uuid4().hex
            env = {
                **os.environ,
                "PYTHONIOENCODING": "utf-8",
                "AGENTRUN_RUN_ID": run_id,
                "AGENTRUN_PROGRAM_ID": program,
            }
            proc = await asyncio.create_subprocess_exec(
                self.args.worker_python, "-u", str(WORKER), str(source),
                cwd=self.args.work_dir, env=env, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                limit=MAX_EVENT * 2,
            )
            run = Run(run_id, program, name, proc, buffer_records, buffer_bytes)
            self.runs[run.id] = run
            run.inbox.put_nowait(initial)
            run.task = asyncio.create_task(run.wait_for_exit(timeout or None))
            LOG.info("started %s (%s%s)", run.id, program[:12], f" {name}" if name else "")
            return run.status()

    async def update(self, req):
        force = flag(req, "force", False)
        restart = flag(req, "restart", True)
        if not (REPO / ".git").exists():
            raise RequestError("runner is not running from a git checkout")
        async with self.update_lock:
            if restart and not force and any(run.state == "running" for run in self.runs.values()):
                raise RequestError("active workers are running; stop them or pass force=true")
            rc, old_rev, err = await run_subprocess("git", "-C", str(REPO), "rev-parse", "HEAD")
            if rc != 0 or not old_rev:
                raise RequestError(f"git rev-parse failed: {err}")
            rc, _, err = await run_subprocess("git", "-C", str(REPO), "pull", "--ff-only", timeout=60)
            if rc != 0:
                raise RequestError(f"git pull failed: {err}")
            rc, new_rev, err = await run_subprocess("git", "-C", str(REPO), "rev-parse", "HEAD")
            if rc != 0 or not new_rev:
                raise RequestError(f"git rev-parse failed: {err}")
            if new_rev == old_rev:
                return {"updated": False, "old_rev": old_rev, "new_rev": new_rev, "restarting": False}
            check = "import runner, runner_worker, runner_tls, runner_client"
            rc, _, err = await run_subprocess(sys.executable, "-c", check, cwd=REPO, timeout=15)
            if rc != 0:
                await run_subprocess("git", "-C", str(REPO), "reset", "--hard", old_rev)
                raise RequestError(f"update validation failed and was rolled back: {err}")
            self.rev = new_rev[:12]
            if not restart:
                return {"updated": True, "old_rev": old_rev, "new_rev": new_rev, "restarting": False}
            self.restart_requested = True
            LOG.info("updated %s -> %s; scheduling restart", old_rev[:12], new_rev[:12])
            return {"updated": True, "old_rev": old_rev, "new_rev": new_rev, "restarting": True}

    def ping_info(self):
        return {
            "version": 1,
            "rev": self.rev,
            "hostname": socket.gethostname(),
            "platform": platform.system(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "work_dir": str(self.args.work_dir),
            "active_workers": sum(run.state == "running" for run in self.runs.values()),
            "max_workers": self.args.max_workers,
            "runs": len(self.runs),
            "max_runs": self.args.max_runs,
            "uptime_s": round(time.monotonic() - self.started_at, 1),
            "tls_expires_utc": self.tls_expires.isoformat() if self.tls_expires else None,
        }

    async def dispatch(self, req, role):
        op = required_text(req, "op")
        if role == "reader" and op not in self.READ_OPS:
            raise RequestError("operation requires an operator token")
        if op == "ping":
            return self.ping_info()
        if op == "upload":
            return self.upload(req)
        if op == "start":
            return await self.start(req)
        if op == "list":
            return [run.status() for run in self.runs.values()]
        if op == "update":
            return await self.update(req)
        if op not in {"status", "read", "send", "stop"}:
            raise RequestError("unknown operation")
        run = self.runs.get(required_text(req, "run_id"))
        if run is None:
            raise RequestError("unknown or expired run_id")
        if op == "status":
            return run.status()
        if op == "read":
            tail = None if req.get("tail") is None else integer(req, "tail", 1, 1, MAX_READ_LIMIT)
            return await run.read(
                integer(req, "after", 0, 0, 2**63 - 1),
                integer(req, "limit", 100, 1, MAX_READ_LIMIT),
                integer(req, "wait_ms", 0, 0, 30000),
                channels=optional_channels(req),
                tail=tail,
            )
        if op == "stop":
            grace = number(req, "grace_s", 1.5, 0.0, 25.0)
            LOG.info("stopping %s (grace=%.1fs)", run.id, grace)
            await run.stop("stopped", grace_s=grace)
            if run.task is not None:
                await run.task
            return run.status()
        if run.state != "running" or run.reason is not None or run.proc.returncode is not None:
            raise RequestError("worker is stopping or has exited")
        if "value" not in req:
            raise RequestError("send requires a value")
        message = frame({"value": req["value"]})
        if len(message) > MAX_EVENT:
            raise RequestError("message is too large")
        try:
            run.inbox.put_nowait(message)
        except asyncio.QueueFull as exc:
            raise RequestError("worker inbox is full") from exc
        return {"queued": True}

    async def connection(self, reader, writer):
        task = asyncio.current_task()
        self.clients.add(task)
        peer = writer.get_extra_info("peername")
        role = None
        try:
            while True:
                try:
                    line = await asyncio.wait_for(reader.readuntil(b"\n"), 60 if role else 10)
                except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError):
                    return
                request_id = None
                try:
                    req = json.loads(line)
                    if not isinstance(req, dict):
                        raise RequestError("request must be an object")
                    request_id = req.get("id")
                    if request_id is not None and (not isinstance(request_id, str) or len(request_id) > 128):
                        request_id = None
                        raise RequestError("id must be a string of at most 128 characters")
                    if role is None:
                        role = self.authorize(req.get("token")) if req.get("op") == "auth" else None
                        if role is None:
                            LOG.warning("rejected auth from %s", peer)
                            writer.write(frame({"id": request_id, "error": "authentication required"}))
                            await asyncio.wait_for(writer.drain(), 5)
                            return
                        result = {"role": role, "version": 1, "max_limit": MAX_READ_LIMIT}
                    else:
                        result = await self.dispatch(req, role)
                    reply = {"id": request_id, "result": result}
                except (RequestError, ValueError, RecursionError) as exc:
                    reply = {"id": request_id, "error": str(exc)}
                except OSError as exc:
                    LOG.warning("operation failed: %s", exc)
                    reply = {"id": request_id, "error": f"local I/O error: {type(exc).__name__}"}
                writer.write(frame(reply))
                await asyncio.wait_for(writer.drain(), 5)
                if self.restart_requested:
                    self.shutdown_event.set()
                    return
        except (OSError, TimeoutError, asyncio.CancelledError):
            pass
        finally:
            self.clients.discard(task)
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 2)
            except (OSError, TimeoutError, asyncio.CancelledError):
                pass

    async def serve(self, tls):
        options = {"ssl": tls, "ssl_handshake_timeout": 10} if tls else {}
        server = await asyncio.start_server(self.connection, self.args.host, self.args.port,
                                            limit=MAX_FRAME, **options)
        async with server:
            LOG.info("listening on %s (TLS %s)", ", ".join(str(s.getsockname()) for s in server.sockets),
                     "enabled" if tls else "disabled")
            try:
                await self.shutdown_event.wait()
            finally:
                server.close()
                for task in list(self.clients):
                    task.cancel()
                if self.clients:
                    await asyncio.gather(*self.clients, return_exceptions=True)
                await asyncio.gather(*(run.stop("server shutdown") for run in self.runs.values()))
                tasks = [run.task for run in self.runs.values() if run.task is not None]
                if tasks:
                    _, pending = await asyncio.wait(tasks, timeout=4)
                    for task in pending:
                        task.cancel()


def operator_token(path):
    try:
        with path.open("rb") as file:
            return load_token(file.read())
    except FileNotFoundError:
        token = secrets.token_hex(32).encode()
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as file:
            file.write(token + b"\n")
        return token


def load_token(data):
    token = data.strip()
    if len(token) < 32 or not token.isascii():
        raise ValueError("tokens must be ASCII and at least 32 characters long")
    return token


def restart_process(initial_cwd, raw_argv, port, state_dir):
    argv = list(raw_argv)
    if "--port" not in argv and not any(arg.startswith("--port=") for arg in argv):
        argv = ["--port", str(port), *argv]
    cmd = [sys.executable, str(Path(__file__).resolve()), *argv]
    LOG.info("restarting runner: %s", cmd)
    if os.name == "nt":
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
        log_file = (state_dir / "runner.log").open("ab")
        subprocess.Popen(cmd, cwd=initial_cwd, stdin=subprocess.DEVNULL,
                         stdout=log_file, stderr=subprocess.STDOUT,
                         close_fds=True, creationflags=flags)
        log_file.close()
        return
    os.chdir(initial_cwd)
    os.execv(sys.executable, cmd)


def main():
    initial_cwd = os.getcwd()
    raw_argv = list(sys.argv[1:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, help="TCP port (prompted if omitted)")
    parser.add_argument("--host", default="127.0.0.1", help="numeric bind address")
    parser.add_argument("--cert", type=Path, help="PEM certificate chain for remote access")
    parser.add_argument("--key", type=Path, help="PEM private key for remote access")
    parser.add_argument("--tls-auto", action="store_true", help="create or reuse a TLS certificate in the state directory")
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".python-runner")
    parser.add_argument("--read-token-file", type=Path)
    parser.add_argument("--work-dir", type=Path, default=Path.cwd())
    parser.add_argument("--worker-python", default=sys.executable)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--max-runs", type=int, default=32)
    parser.add_argument("--buffer-records", type=int, default=2048)
    parser.add_argument("--buffer-bytes", type=int, default=2 * 1024 * 1024)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    try:
        if args.port is None:
            args.port = int(input("Listen on TCP port: ").strip())
        if not 1 <= args.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if not 1 <= args.max_workers <= args.max_runs <= 128:
            raise ValueError("require 1 <= max-workers <= max-runs <= 128")
        if not 1 <= args.buffer_records or args.buffer_bytes < MAX_EVENT * 2:
            raise ValueError("require at least one record and 131072 bytes of buffer")
        address = ipaddress.ip_address(args.host)
        if args.tls_auto and (args.cert or args.key):
            raise ValueError("use --tls-auto or supply --cert and --key")
        if bool(args.cert) != bool(args.key):
            raise ValueError("provide both --cert and --key")
        if not address.is_loopback and args.cert is None and not args.tls_auto:
            raise ValueError("a non-loopback listener requires --tls-auto or --cert and --key")
        args.state_dir = args.state_dir.resolve()
        args.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        if args.tls_auto:
            args.cert, args.key = ensure_tls_files(args.state_dir)
        tls = None
        tls_expires = None
        if args.cert:
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.minimum_version = ssl.TLSVersion.TLSv1_2
            tls.load_cert_chain(args.cert, args.key)
            tls_expires = cert_not_after(args.cert)
            if tls_expires is not None:
                now = datetime.now(timezone.utc)
                if tls_expires <= now:
                    LOG.warning("TLS certificate %s expired on %s", args.cert, tls_expires.isoformat())
                elif tls_expires - now < timedelta(days=30):
                    LOG.warning("TLS certificate %s expires soon (%s)", args.cert, tls_expires.isoformat())
            if args.tls_auto:
                LOG.info("public TLS certificate: %s (client --server-name %s)", args.cert, TLS_NAME)
        token_path = args.state_dir / "operator.token"
        token = operator_token(token_path)
        read_token = load_token(args.read_token_file.read_bytes()) if args.read_token_file else None
        if read_token == token:
            raise ValueError("reader and operator tokens must differ")
        LOG.info("operator token file: %s", token_path)
        runner = Runner(args, token, read_token, tls_expires=tls_expires)
        asyncio.run(runner.serve(tls))
        if runner.restart_requested:
            restart_process(initial_cwd, raw_argv, args.port, args.state_dir)
    except (OSError, ValueError, EOFError) as exc:
        parser.exit(2, f"runner: {exc}\n")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
