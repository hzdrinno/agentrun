"""Small command-line client and Python API for AgentRun."""

import argparse
import base64
import hashlib
import itertools
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import uuid

MAX_FRAME = 1024 * 1024
IDLE_RECONNECT_S = 50.0
DEFAULT_TOKEN = Path.home() / ".python-runner" / "operator.token"
DEFAULT_CONFIG = Path.home() / ".agentrun.json"

_PULL_SCRIPT = """\
import base64, hashlib
from pathlib import Path

path = Path(ctx.config["path"])
chunk_size = ctx.config["chunk_size"]
window = ctx.config["window"]
digest = hashlib.sha256()
size = 0
sent = 0
with path.open("rb") as fh:
    while data := fh.read(chunk_size):
        digest.update(data)
        size += len(data)
        ctx.emit("chunk", base64.b64encode(data).decode("ascii"))
        sent += 1
        if sent % window == 0:
            ctx.recv(timeout=30)
ctx.emit("done", {"size": size, "sha256": digest.hexdigest()})
"""

_PUSH_SCRIPT = """\
import base64, hashlib
from pathlib import Path

dest = Path(ctx.config["path"])
dest.parent.mkdir(parents=True, exist_ok=True)
tmp = dest.with_name(dest.name + ".agentrun.tmp")
digest = hashlib.sha256()
size = 0
try:
    with tmp.open("wb") as fh:
        while True:
            msg = ctx.recv(timeout=30)
            if "chunk" in msg:
                raw = base64.b64decode(msg["chunk"])
                fh.write(raw)
                digest.update(raw)
                size += len(raw)
                if msg.get("ack"):
                    ctx.emit("ack", size)
            elif msg.get("done"):
                if digest.hexdigest() != msg["sha256"]:
                    raise ValueError("SHA-256 mismatch on uploaded file")
                break
    tmp.replace(dest)
except BaseException:
    tmp.unlink(missing_ok=True)
    raise
ctx.emit("done", {"path": str(dest), "size": size, "sha256": digest.hexdigest()})
"""

_HOST_ENV_SCRIPT = """\
import sys, sysconfig, tempfile
impl = {"cpython": "cp", "pypy": "pp"}.get(sys.implementation.name, sys.implementation.name)
try:
    from pip._vendor.packaging.tags import sys_tags
    platforms = [p for p in dict.fromkeys(t.platform for t in sys_tags()) if p != "any"]
except Exception:
    platforms = [sysconfig.get_platform().replace("-", "_").replace(".", "_")]
ctx.emit("env", {
    "python": f"{sys.version_info.major}.{sys.version_info.minor}",
    "impl": impl,
    "platforms": platforms,
    "temp_root": tempfile.gettempdir(),
})
"""

_PIP_INSTALL_SCRIPT = """\
import shutil, subprocess, sys, sysconfig, tempfile

def site_writable():
    if sys.prefix != sys.base_prefix:
        return True
    try:
        with tempfile.TemporaryFile(dir=sysconfig.get_path("purelib")):
            return True
    except OSError:
        return False

cfg = ctx.config
cmd = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check", "--no-input"]
if not cfg.get("allow_source"):
    cmd.append("--only-binary=:all:")
if cfg.get("upgrade"):
    cmd.append("--upgrade")
if not site_writable():
    cmd.append("--user")
if cfg.get("find_links"):
    cmd.extend(["--no-index", "--find-links", cfg["find_links"]])
cmd.extend(cfg.get("pip_args", []))
cmd.extend(cfg["packages"])
try:
    res = subprocess.run(cmd)
    ctx.emit("done", {"returncode": res.returncode, "python": sys.executable, "command": cmd})
finally:
    if cfg.get("cleanup_dir"):
        shutil.rmtree(cfg["cleanup_dir"], ignore_errors=True)
if res.returncode != 0:
    sys.exit(res.returncode)
"""


class RemoteError(Exception):
    pass


def load_profile(profile=None, config_path=None):
    path = Path(config_path or os.environ.get("AGENTRUN_CONFIG") or DEFAULT_CONFIG)
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"invalid config file: {path}")
    if "profiles" in data and isinstance(data["profiles"], dict):
        selected = profile or os.environ.get("AGENTRUN_PROFILE") or data.get("default")
        if selected is None:
            return {}
        if selected not in data["profiles"]:
            raise ValueError(f"unknown profile {selected!r} in {path}")
        return dict(data["profiles"][selected])
    return dict(data)


def resolve_connection(profile=None, **overrides):
    cfg = load_profile(profile=profile)
    env_map = {
        "host": "AGENTRUN_HOST",
        "port": "AGENTRUN_PORT",
        "ca": "AGENTRUN_CA",
        "server_name": "AGENTRUN_SERVER_NAME",
        "token_file": "AGENTRUN_TOKEN_FILE",
        "token": "AGENTRUN_TOKEN",
    }
    for key, env_key in env_map.items():
        if env_key in os.environ and os.environ[env_key]:
            cfg[key] = os.environ[env_key]
    for key, value in overrides.items():
        if value is not None:
            cfg[key] = value
    host = cfg.get("host", "127.0.0.1")
    port = cfg.get("port")
    port = int(port) if port is not None else None
    ca = Path(cfg["ca"]).expanduser() if cfg.get("ca") else None
    server_name = cfg.get("server_name")
    token = cfg.get("token")
    token_file = Path(cfg.get("token_file", DEFAULT_TOKEN)).expanduser()
    return {
        "host": host,
        "port": port,
        "ca": ca,
        "server_name": server_name,
        "token": token,
        "token_file": token_file,
    }


class Client:
    def __init__(self, host, port, token, *, ca=None, server_name=None, timeout=35):
        if ca is None and host not in {"127.0.0.1", "::1", "localhost"}:
            raise ValueError("provide a CA certificate for remote connections")
        self.host = host
        self.port = int(port)
        self.token = token
        self.tls = ssl.create_default_context(cafile=str(ca)) if ca else None
        self.server_name = server_name
        self.timeout = timeout
        self.socket = None
        self.incoming = None
        self.ids = itertools.count(1)
        self._last_ok = 0.0
        self._max_limit = 100
        self.last_status = None

    @classmethod
    def from_env(cls, profile=None, **overrides):
        cfg = resolve_connection(profile=profile, **overrides)
        if cfg["port"] is None:
            raise ValueError("port is required (set --port, AGENTRUN_PORT, or ~/.agentrun.json)")
        token = cfg["token"] or cfg["token_file"].read_text(encoding="ascii").strip()
        return cls(cfg["host"], cfg["port"], token, ca=cfg["ca"], server_name=cfg["server_name"])

    def connect(self):
        self.close()
        self.socket = socket.create_connection((self.host, self.port), timeout=10)
        try:
            if self.tls:
                self.socket = self.tls.wrap_socket(self.socket,
                                                   server_hostname=self.server_name or self.host)
            self.socket.settimeout(self.timeout)
            self.incoming = self.socket.makefile("rb")
            auth = self._send_and_recv("auth", token=self.token)
            self._max_limit = auth.get("max_limit", 100)
            return self
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_exc):
        self.close()

    def close(self):
        try:
            if self.incoming is not None:
                self.incoming.close()
        finally:
            self.incoming = None
            if self.socket is not None:
                try:
                    self.socket.close()
                finally:
                    self.socket = None

    def _send_and_recv(self, op, **data):
        request_id = str(next(self.ids))
        request = json.dumps({"id": request_id, "op": op, **data}, allow_nan=False).encode() + b"\n"
        if len(request) > MAX_FRAME:
            raise ValueError("request exceeds maximum frame size")
        self.socket.sendall(request)
        line = self.incoming.readline(MAX_FRAME + 1)
        if not line.endswith(b"\n") or len(line) > MAX_FRAME:
            raise RemoteError("connection closed or invalid response frame")
        reply = json.loads(line)
        if reply.get("id") != request_id:
            raise RemoteError("response ID did not match request")
        self._last_ok = time.monotonic()
        if "error" in reply:
            raise RemoteError(reply["error"])
        return reply["result"]

    def request(self, op, **data):
        if op != "auth" and (self.socket is None or time.monotonic() - self._last_ok > IDLE_RECONNECT_S):
            self.connect()
        return self._send_and_recv(op, **data)

    def ping(self):
        return self.request("ping")

    def upload_file(self, path):
        source = Path(path).read_text(encoding="utf-8")
        return self.request("upload", source=source)["program_id"]

    def iter_records(self, run_id, *, after=0, wait_ms=5000, limit=None,
                     channels=None, follow=True, on_gap=None):
        cursor = after
        page_limit = self._max_limit if limit is None else min(limit, self._max_limit)
        while True:
            params = {"run_id": run_id, "after": cursor, "limit": page_limit,
                      "wait_ms": wait_ms if follow else 0}
            if channels is not None:
                params["channels"] = list(channels)
            page = self.request("read", **params)
            self.last_status = page
            if page["gap"] and on_gap is not None:
                on_gap(page["gap"])
            for record in page["records"]:
                yield record
            cursor = page["cursor"]
            if page["state"] == "exited" and cursor >= page["latest_seq"]:
                return
            if not follow and not page["records"]:
                return

    def pull_file(self, remote_path, local_path, *, chunk_size=36 * 1024, window=16):
        program_id = self.request("upload", source=_PULL_SCRIPT)["program_id"]
        run_id = self.request("start", program_id=program_id,
                              config={"path": str(remote_path), "chunk_size": chunk_size, "window": window},
                              timeout_s=300)["run_id"]
        dest = Path(local_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".agentrun.tmp")
        digest = hashlib.sha256()
        received = 0
        chunks = 0
        done = None
        try:
            with tmp.open("wb") as fh:
                for record in self.iter_records(run_id, limit=100):
                    if record["channel"] == "chunk":
                        raw = base64.b64decode(record["value"])
                        fh.write(raw)
                        digest.update(raw)
                        received += len(raw)
                        chunks += 1
                        if chunks % window == 0:
                            self.request("send", run_id=run_id, value={"ack": chunks})
                    elif record["channel"] == "done":
                        done = record["value"]
                    elif record["channel"] == "log":
                        sys.stderr.write(record["value"])
            if done is None or self.last_status["returncode"] != 0:
                raise RemoteError(f"pull failed (returncode={self.last_status['returncode']})")
            if digest.hexdigest() != done["sha256"] or received != done["size"]:
                raise RemoteError("pull failed: SHA-256 or size mismatch")
            tmp.replace(dest)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return {"remote": str(remote_path), "local": str(dest), "size": received, "sha256": done["sha256"]}

    def push_file(self, local_path, remote_path, *, chunk_size=36 * 1024, window=16):
        src = Path(local_path)
        program_id = self.request("upload", source=_PUSH_SCRIPT)["program_id"]
        run_id = self.request("start", program_id=program_id,
                              config={"path": str(remote_path)}, timeout_s=300)["run_id"]
        digest = hashlib.sha256()
        chunks = 0
        cursor = 0
        with src.open("rb") as fh:
            while data := fh.read(chunk_size):
                digest.update(data)
                chunks += 1
                need_ack = chunks % window == 0
                self.request("send", run_id=run_id,
                             value={"chunk": base64.b64encode(data).decode("ascii"), "ack": need_ack})
                if need_ack:
                    while True:
                        page = self.request("read", run_id=run_id, after=cursor, wait_ms=10000)
                        cursor = page["cursor"]
                        if any(r["channel"] == "ack" for r in page["records"]):
                            break
                        if page["state"] == "exited":
                            logs = "".join(r["value"] for r in page["records"] if r["channel"] == "log")
                            raise RemoteError(f"push worker exited early: {logs.strip()}")
        self.request("send", run_id=run_id, value={"done": True, "sha256": digest.hexdigest()})
        done = None
        for record in self.iter_records(run_id, after=cursor, limit=100):
            if record["channel"] == "done":
                done = record["value"]
            elif record["channel"] == "log":
                sys.stderr.write(record["value"])
        if done is None or self.last_status["returncode"] != 0:
            raise RemoteError(f"push failed (returncode={self.last_status['returncode']})")
        return done

    def _wait_after_restart(self, wait_s, expected_rev=None):
        self.close()
        deadline = time.monotonic() + wait_s
        time.sleep(0.5)
        last_err = None
        while time.monotonic() < deadline:
            try:
                self.connect()
                info = self.ping()
                if expected_rev is None or info.get("rev") == expected_rev:
                    return info
                last_err = f"still on old revision ({info.get('rev')})"
            except (OSError, RemoteError) as exc:
                last_err = exc
            self.close()
            time.sleep(0.25)
        raise RemoteError(f"runner did not come back within {wait_s}s: {last_err}")

    def update(self, *, force=False, restart=True, wait_s=15.0):
        result = self.request("update", force=force, restart=restart)
        if result.get("restarting"):
            expected_rev = result.get("new_rev", "")[:12] or None
            result["live"] = self._wait_after_restart(wait_s, expected_rev=expected_rev)
        return result

    def install(self, packages, *, relay=False, allow_source=False, upgrade=False,
                timeout_s=300, pip_args=()):
        if not packages:
            raise ValueError("specify at least one package to install")
        if relay and allow_source:
            raise ValueError("--relay downloads prebuilt wheels for the target host and cannot be combined with --allow-source")
        packages = list(packages)
        extra_args = list(pip_args)
        find_links = None
        relayed = None
        if relay:
            env_prog = self.request("upload", source=_HOST_ENV_SCRIPT)["program_id"]
            env_run = self.request("start", program_id=env_prog, timeout_s=30)["run_id"]
            env = None
            for record in self.iter_records(env_run):
                if record["channel"] == "env":
                    env = record["value"]
            if env is None or self.last_status["returncode"] != 0:
                raise RemoteError("failed to inspect remote Python environment")
            with tempfile.TemporaryDirectory() as local_tmp:
                dl_cmd = [
                    sys.executable, "-m", "pip", "download",
                    "--disable-pip-version-check", "--no-input", "--dest", local_tmp,
                    "--python-version", env["python"], "--implementation", env["impl"],
                ]
                if not allow_source:
                    dl_cmd.append("--only-binary=:all:")
                for plat in env["platforms"]:
                    dl_cmd.extend(["--platform", plat])
                dl_cmd.extend(extra_args)
                dl_cmd.extend(packages)
                dl = subprocess.run(dl_cmd, capture_output=True, text=True, timeout=timeout_s)
                if dl.returncode != 0:
                    raise RemoteError(f"local pip download failed: {(dl.stderr or dl.stdout).strip()}")
                files = sorted(Path(local_tmp).iterdir())
                if not files:
                    raise RemoteError("pip download produced no files")
                find_links = f"{env['temp_root'].rstrip('/\\\\')}/agentrun-pip-{uuid.uuid4().hex[:12]}"
                for wheel in files:
                    self.push_file(wheel, f"{find_links}/{wheel.name}")
                relayed = [wheel.name for wheel in files]
                extra_args = []
        prog = self.request("upload", source=_PIP_INSTALL_SCRIPT)["program_id"]
        cfg = {
            "packages": packages,
            "allow_source": allow_source,
            "upgrade": upgrade,
            "find_links": find_links,
            "cleanup_dir": find_links,
            "pip_args": extra_args,
        }
        run_id = self.request("start", program_id=prog, config=cfg, timeout_s=timeout_s)["run_id"]
        done = None
        for record in self.iter_records(run_id):
            if record["channel"] == "done":
                done = record["value"]
            elif record["channel"] == "log":
                sys.stderr.write(record["value"])
                sys.stderr.flush()
        if done is None or done["returncode"] != 0 or self.last_status["returncode"] != 0:
            raise RemoteError(f"remote pip install failed (returncode={self.last_status['returncode']})")
        result = {"installed": packages, "python": done["python"]}
        if relayed is not None:
            result["relayed"] = relayed
        return result


def _load_config_arg(args):
    if getattr(args, "config_file", None) is not None:
        return json.loads(args.config_file.read_text(encoding="utf-8"))
    return json.loads(args.config)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", help="profile name from ~/.agentrun.json")
    parser.add_argument("--host", help="runner host (or AGENTRUN_HOST)")
    parser.add_argument("--port", type=int, help="runner TCP port (or AGENTRUN_PORT)")
    parser.add_argument("--token-file", type=Path, help="token file (or AGENTRUN_TOKEN_FILE)")
    parser.add_argument("--ca", type=Path, help="CA certificate (or AGENTRUN_CA)")
    parser.add_argument("--server-name", help="TLS server name (or AGENTRUN_SERVER_NAME)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("ping")
    sub.add_parser("list")
    upload = sub.add_parser("upload")
    upload.add_argument("file", type=Path)
    start = sub.add_parser("start")
    start.add_argument("program_id")
    start.add_argument("--name")
    start_cfg = start.add_mutually_exclusive_group()
    start_cfg.add_argument("--config", default="{}")
    start_cfg.add_argument("--config-file", type=Path)
    start.add_argument("--timeout", type=int, default=0)
    start.add_argument("--buffer-records", type=int)
    start.add_argument("--buffer-bytes", type=int)
    execute = sub.add_parser("exec", help="upload, start, and stream a program")
    source = execute.add_mutually_exclusive_group(required=True)
    source.add_argument("--code", "-c")
    source.add_argument("--file", "-f", help="Python file path, or '-' for stdin")
    execute.add_argument("--name")
    exec_cfg = execute.add_mutually_exclusive_group()
    exec_cfg.add_argument("--config", default="{}")
    exec_cfg.add_argument("--config-file", type=Path)
    execute.add_argument("--timeout", type=int, default=30)
    execute.add_argument("--buffer-records", type=int)
    execute.add_argument("--buffer-bytes", type=int)
    execute.add_argument("--detach", action="store_true", help="start and print run info without streaming")
    for command in ("status", "read", "send", "stop"):
        action = sub.add_parser(command)
        action.add_argument("run_id")
        if command == "read":
            action.add_argument("--after", type=int, default=0)
            action.add_argument("--limit", type=int, default=100)
            action.add_argument("--wait-ms", type=int, default=0)
            action.add_argument("--tail", type=int)
            action.add_argument("--channel", action="append", dest="channels")
            action.add_argument("--follow", "-f", action="store_true")
        elif command == "send":
            action.add_argument("value", help="JSON value")
        elif command == "stop":
            action.add_argument("--grace-s", type=float, default=1.5)
    pull = sub.add_parser("pull", help="download a file from the runner host")
    pull.add_argument("remote")
    pull.add_argument("local", nargs="?")
    push = sub.add_parser("push", help="upload a file to the runner host")
    push.add_argument("local", type=Path)
    push.add_argument("remote")
    upd = sub.add_parser("update", help="pull latest runner code from git and restart")
    upd.add_argument("--force", action="store_true")
    upd.add_argument("--no-restart", action="store_true")
    inst = sub.add_parser("install", help="install Python packages into the host Python")
    inst.add_argument("packages", nargs="+")
    inst.add_argument("--relay", action="store_true",
                      help="download wheels on the client and push them to an offline host")
    inst.add_argument("--allow-source", action="store_true",
                      help="allow source distributions instead of --only-binary=:all:")
    inst.add_argument("--upgrade", "-U", action="store_true")
    inst.add_argument("--timeout", type=int, default=300)

    args = parser.parse_args()
    try:
        cfg = resolve_connection(
            profile=args.profile, host=args.host, port=args.port,
            ca=args.ca, server_name=args.server_name, token_file=args.token_file,
        )
        if cfg["port"] is None:
            parser.error("provide --port (or set AGENTRUN_PORT / ~/.agentrun.json)")
        if cfg["ca"] is None and cfg["host"] not in {"127.0.0.1", "::1", "localhost"}:
            parser.error("provide --ca when connecting to a remote listener")
        token = cfg["token"] or cfg["token_file"].read_text(encoding="ascii").strip()
        with Client(cfg["host"], cfg["port"], token, ca=cfg["ca"], server_name=cfg["server_name"]) as client:
            if args.command == "exec":
                if args.code is not None:
                    code = args.code
                    auto_name = None
                elif args.file == "-":
                    code = sys.stdin.read()
                    auto_name = "<stdin>"
                else:
                    code = Path(args.file).read_text(encoding="utf-8")
                    auto_name = Path(args.file).name
                program = client.request("upload", source=code)["program_id"]
                opts = {k: getattr(args, k) for k in ("buffer_records", "buffer_bytes")
                        if getattr(args, k) is not None}
                name = args.name or auto_name
                if name is not None:
                    opts["name"] = name
                started = client.request("start", program_id=program,
                                         config=_load_config_arg(args), timeout_s=args.timeout, **opts)
                if args.detach:
                    print(json.dumps(started, indent=2))
                    return 0
                run_id = started["run_id"]
                print(f"run_id: {run_id}", file=sys.stderr, flush=True)
                try:
                    for record in client.iter_records(
                        run_id, wait_ms=5000,
                        on_gap=lambda g: print(json.dumps({"gap": g}), file=sys.stderr),
                    ):
                        print(json.dumps(record), flush=True)
                except KeyboardInterrupt:
                    try:
                        client.request("stop", run_id=run_id)
                    except (OSError, RemoteError):
                        pass
                    return 130
                page = client.last_status
                return 0 if page["returncode"] == 0 and page["reason"] is None else 1
            if args.command == "ping":
                result = client.ping()
            elif args.command == "upload":
                result = client.request("upload", source=args.file.read_text(encoding="utf-8"))
            elif args.command == "start":
                opts = {k: getattr(args, k) for k in ("buffer_records", "buffer_bytes", "name")
                        if getattr(args, k) is not None}
                result = client.request("start", program_id=args.program_id,
                                        config=_load_config_arg(args), timeout_s=args.timeout, **opts)
            elif args.command == "list":
                result = client.request("list")
            elif args.command == "send":
                result = client.request("send", run_id=args.run_id, value=json.loads(args.value))
            elif args.command == "stop":
                result = client.request("stop", run_id=args.run_id, grace_s=args.grace_s)
            elif args.command == "read":
                if args.follow:
                    after = args.after
                    if args.tail is not None:
                        initial = client.request("read", run_id=args.run_id, tail=args.tail,
                                                 **({"channels": args.channels} if args.channels else {}))
                        for record in initial["records"]:
                            print(json.dumps(record), flush=True)
                        after = initial["cursor"]
                        if initial["state"] == "exited" and after >= initial["latest_seq"]:
                            return 0
                    for record in client.iter_records(
                        args.run_id, after=after, wait_ms=args.wait_ms or 5000,
                        limit=args.limit, channels=args.channels, follow=True,
                        on_gap=lambda g: print(json.dumps({"gap": g}), file=sys.stderr),
                    ):
                        print(json.dumps(record), flush=True)
                    return 0
                params = {"run_id": args.run_id, "after": args.after,
                          "limit": args.limit, "wait_ms": args.wait_ms}
                if args.tail is not None:
                    params["tail"] = args.tail
                if args.channels:
                    params["channels"] = args.channels
                result = client.request("read", **params)
            elif args.command == "pull":
                local = args.local or Path(args.remote).name
                result = client.pull_file(args.remote, local)
            elif args.command == "push":
                result = client.push_file(args.local, args.remote)
            elif args.command == "update":
                result = client.update(force=args.force, restart=not args.no_restart)
            elif args.command == "install":
                result = client.install(args.packages, relay=args.relay,
                                        allow_source=args.allow_source,
                                        upgrade=args.upgrade, timeout_s=args.timeout)
            else:
                result = client.request(args.command, run_id=args.run_id)
            print(json.dumps(result, indent=2))
    except (OSError, ValueError, RemoteError) as exc:
        print(f"client: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
