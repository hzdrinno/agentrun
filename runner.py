"""Run trusted Python programs with buffered input and output through one TCP port."""

import argparse
import asyncio
from collections import deque
from datetime import datetime, timezone
import hashlib
import hmac
import ipaddress
import json
import logging
import os
from pathlib import Path
import secrets
import ssl
import sys
import time
import uuid

MAX_FRAME = 1024 * 1024
MAX_SOURCE = 256 * 1024
MAX_EVENT = 64 * 1024
READ_BYTES = 512 * 1024
WORKER = Path(__file__).resolve().with_name("runner_worker.py")
LOG = logging.getLogger("python-runner")


class RequestError(Exception):
    pass


def frame(value):
    return json.dumps(value, allow_nan=False, separators=(",", ":")).encode() + b"\n"


def integer(request, key, default, minimum, maximum):
    value = request.get(key, default)
    if type(value) is not int or not minimum <= value <= maximum:
        raise RequestError(f"{key} must be an integer between {minimum} and {maximum}")
    return value


def required_text(request, key):
    value = request.get(key)
    if not isinstance(value, str) or not value:
        raise RequestError(f"{key} must be a nonempty string")
    return value


class Run:
    def __init__(self, program, proc, buffer_records, buffer_bytes):
        self.id = uuid.uuid4().hex
        self.program = program
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

    def signal(self):
        previous = self.changed
        self.changed = asyncio.Event()
        previous.set()

    def emit(self, channel, value, timestamp=None):
        event = {
            "seq": self.sequence + 1,
            "channel": channel,
            "value": value,
            "host_monotonic_ns": str(time.monotonic_ns()),
            "host_utc": datetime.now(timezone.utc).isoformat(),
        }
        if timestamp is not None:
            event["timestamp"] = timestamp
        size = len(frame(event))
        self.sequence += 1
        self.events.append((size, event))
        self.event_bytes += size
        while len(self.events) > self.buffer_records or self.event_bytes > self.buffer_bytes:
            size, _ = self.events.popleft()
            self.event_bytes -= size
        self.signal()

    def status(self):
        return {
            "run_id": self.id,
            "program_id": self.program,
            "state": self.state,
            "reason": self.reason,
            "returncode": self.proc.returncode,
            "latest_seq": self.sequence,
        }

    async def read(self, after, limit, wait_ms):
        if after >= self.sequence and self.state == "running" and wait_ms:
            try:
                await asyncio.wait_for(self.changed.wait(), wait_ms / 1000)
            except TimeoutError:
                pass
        oldest = self.events[0][1]["seq"] if self.events else self.sequence + 1
        gap = {"from": after + 1, "through": oldest - 1} if after + 1 < oldest else None
        selected = []
        size = 0
        for count, event in self.events:
            if event["seq"] <= after:
                continue
            if len(selected) == limit or size + count > READ_BYTES:
                break
            size += count
            selected.append(event)
        cursor = selected[-1]["seq"] if selected else max(after, oldest - 1)
        return {"records": selected, "cursor": cursor, "gap": gap, **self.status()}

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
        while chunk := await self.proc.stderr.read(4096):
            self.emit("log", chunk.decode("utf-8", "replace"))

    async def pump_input(self):
        try:
            while True:
                self.proc.stdin.write(await self.inbox.get())
                await self.proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass

    async def wait_for_exit(self, timeout):
        tasks = [asyncio.create_task(pump()) for pump in
                 (self.pump_output, self.pump_logs, self.pump_input)]
        try:
            await asyncio.wait_for(self.proc.wait(), timeout)
        except TimeoutError:
            await self.stop("timeout")
        finally:
            self.proc.stdin.close()
            tasks[-1].cancel()
            _, pending = await asyncio.wait(tasks, timeout=2)
            for task in pending:
                task.cancel()
            for result in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(result, Exception):
                    self.emit("runner", f"Worker I/O failed: {type(result).__name__}")
            self.state = "exited"
            self.signal()

    async def stop(self, reason):
        if self.proc.returncode is None:
            self.reason = reason
            try:
                self.proc.terminate()
                await asyncio.wait_for(self.proc.wait(), 2)
            except TimeoutError:
                if self.proc.returncode is None:
                    self.proc.kill()
            except ProcessLookupError:
                pass


class Runner:
    READ_OPS = {"ping", "list", "status", "read"}

    def __init__(self, args, token, read_token):
        self.args = args
        self.token = token
        self.read_token = read_token
        self.programs = args.state_dir / "programs"
        self.programs.mkdir(parents=True, exist_ok=True)
        self.runs = {}
        self.start_lock = asyncio.Lock()

    def authorize(self, token):
        if not isinstance(token, str):
            return None
        encoded = token.encode()
        if hmac.compare_digest(encoded, self.token):
            return "operator"
        if self.read_token is not None and hmac.compare_digest(encoded, self.read_token):
            return "reader"
        return None

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
        return {"program_id": program}

    async def start(self, req):
        program = required_text(req, "program_id")
        if len(program) != 64 or any(c not in "0123456789abcdef" for c in program):
            raise RequestError("invalid program_id")
        source = self.programs / (program + ".py")
        if not source.is_file():
            raise RequestError("unknown program_id")
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
                finished = next((key for key, run in self.runs.items() if run.state == "exited"), None)
                if finished is None:
                    raise RequestError("run retention limit reached")
                del self.runs[finished]
            env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
            proc = await asyncio.create_subprocess_exec(
                self.args.worker_python, "-u", str(WORKER), str(source),
                cwd=self.args.work_dir, env=env, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                limit=MAX_EVENT * 2,
            )
            run = Run(program, proc, buffer_records, buffer_bytes)
            self.runs[run.id] = run
            run.inbox.put_nowait(initial)
            run.task = asyncio.create_task(run.wait_for_exit(timeout or None))
            LOG.info("started %s (%s)", run.id, program)
            return run.status()

    async def dispatch(self, req, role):
        op = required_text(req, "op")
        if role == "reader" and op not in self.READ_OPS:
            raise RequestError("operation requires an operator token")
        if op == "ping":
            return {"version": 1}
        if op == "upload":
            return self.upload(req)
        if op == "start":
            return await self.start(req)
        if op == "list":
            return [run.status() for run in self.runs.values()]
        if op not in {"status", "read", "send", "stop"}:
            raise RequestError("unknown operation")
        run = self.runs.get(required_text(req, "run_id"))
        if run is None:
            raise RequestError("unknown or expired run_id")
        if op == "status":
            return run.status()
        if op == "read":
            return await run.read(integer(req, "after", 0, 0, 2**63 - 1),
                                  integer(req, "limit", 100, 1, 100),
                                  integer(req, "wait_ms", 0, 0, 30000))
        if op == "stop":
            await run.stop("stopped")
            return run.status()
        if run.state != "running" or run.proc.returncode is not None:
            raise RequestError("worker has exited")
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
                            writer.write(frame({"id": request_id, "error": "authentication required"}))
                            await asyncio.wait_for(writer.drain(), 5)
                            return
                        result = {"role": role, "version": 1}
                    else:
                        result = await self.dispatch(req, role)
                    reply = {"id": request_id, "result": result}
                except (RequestError, ValueError, RecursionError) as exc:
                    reply = {"id": request_id, "error": str(exc)}
                except OSError as exc:
                    LOG.info("operation failed: %s", exc)
                    reply = {"id": request_id, "error": "local I/O operation failed"}
                writer.write(frame(reply))
                await asyncio.wait_for(writer.drain(), 5)
        except (OSError, TimeoutError):
            pass
        finally:
            writer.close()

    async def serve(self, tls):
        options = {"ssl": tls, "ssl_handshake_timeout": 10} if tls else {}
        server = await asyncio.start_server(self.connection, self.args.host, self.args.port,
                                            limit=MAX_FRAME, **options)
        async with server:
            LOG.info("listening on %s (TLS %s)", ", ".join(str(s.getsockname()) for s in server.sockets),
                     "enabled" if tls else "disabled")
            try:
                await server.serve_forever()
            finally:
                await asyncio.gather(*(run.stop("server shutdown") for run in self.runs.values()))
                tasks = [run.task for run in self.runs.values()]
                if tasks:
                    _, pending = await asyncio.wait(tasks, timeout=3)
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, help="TCP port (prompted if omitted)")
    parser.add_argument("--host", default="127.0.0.1", help="numeric bind address")
    parser.add_argument("--cert", type=Path, help="PEM certificate chain for remote access")
    parser.add_argument("--key", type=Path, help="PEM private key for remote access")
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
        if bool(args.cert) != bool(args.key):
            raise ValueError("provide both --cert and --key")
        if not address.is_loopback and args.cert is None:
            raise ValueError("a non-loopback listener requires --cert and --key")
        tls = None
        if args.cert:
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.minimum_version = ssl.TLSVersion.TLSv1_2
            tls.load_cert_chain(args.cert, args.key)
        args.state_dir = args.state_dir.resolve()
        args.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        token_path = args.state_dir / "operator.token"
        token = operator_token(token_path)
        read_token = load_token(args.read_token_file.read_bytes()) if args.read_token_file else None
        if read_token == token:
            raise ValueError("reader and operator tokens must differ")
        LOG.info("operator token file: %s", token_path)
        asyncio.run(Runner(args, token, read_token).serve(tls))
    except (OSError, ValueError, EOFError) as exc:
        parser.exit(2, f"runner: {exc}\n")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
