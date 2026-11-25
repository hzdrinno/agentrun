"""Small command-line client and Python API for Python Runner."""

import argparse
import itertools
import json
from pathlib import Path
import socket
import ssl
import sys

MAX_FRAME = 1024 * 1024
DEFAULT_TOKEN = Path.home() / ".python-runner" / "operator.token"


class RemoteError(Exception):
    pass


class Client:
    def __init__(self, host, port, token, *, ca=None, server_name=None):
        if ca is None and host not in {"127.0.0.1", "::1", "localhost"}:
            raise ValueError("provide a CA certificate for remote connections")
        self.host = host
        self.port = port
        self.token = token
        self.tls = ssl.create_default_context(cafile=str(ca)) if ca else None
        self.server_name = server_name
        self.socket = None
        self.incoming = None
        self.ids = itertools.count(1)

    def __enter__(self):
        self.socket = socket.create_connection((self.host, self.port), timeout=10)
        try:
            if self.tls:
                self.socket = self.tls.wrap_socket(self.socket,
                                                   server_hostname=self.server_name or self.host)
            self.socket.settimeout(35)
            self.incoming = self.socket.makefile("rb")
            self.request("auth", token=self.token)
            return self
        except BaseException:
            self.close()
            raise

    def __exit__(self, *_exc):
        self.close()

    def close(self):
        if self.incoming is not None:
            self.incoming.close()
        if self.socket is not None:
            self.socket.close()

    def request(self, op, **data):
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
        if "error" in reply:
            raise RemoteError(reply["error"])
        return reply["result"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--token-file", type=Path, default=DEFAULT_TOKEN)
    parser.add_argument("--ca", type=Path, help="CA certificate that signs the runner certificate")
    parser.add_argument("--server-name", help="TLS name when connecting through a proxy")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("list")
    upload = sub.add_parser("upload")
    upload.add_argument("file", type=Path)
    start = sub.add_parser("start")
    start.add_argument("program_id")
    start.add_argument("--config", default="{}")
    start.add_argument("--timeout", type=int, default=0)
    start.add_argument("--buffer-records", type=int)
    start.add_argument("--buffer-bytes", type=int)
    execute = sub.add_parser("exec", help="upload, start, and stream a short program")
    source = execute.add_mutually_exclusive_group(required=True)
    source.add_argument("--code", "-c")
    source.add_argument("--file", type=Path)
    execute.add_argument("--config", default="{}")
    execute.add_argument("--timeout", type=int, default=30)
    for command in ("status", "read", "send", "stop"):
        action = sub.add_parser(command)
        action.add_argument("run_id")
        if command == "read":
            action.add_argument("--after", type=int, default=0)
            action.add_argument("--wait-ms", type=int, default=0)
        elif command == "send":
            action.add_argument("value", help="JSON value")

    args = parser.parse_args()
    if args.ca is None and args.host not in {"127.0.0.1", "::1", "localhost"}:
        parser.error("provide --ca when connecting to a remote listener")
    try:
        token = args.token_file.read_text(encoding="ascii").strip()
        with Client(args.host, args.port, token, ca=args.ca, server_name=args.server_name) as client:
            if args.command == "exec":
                source = args.code if args.code is not None else args.file.read_text(encoding="utf-8")
                program = client.request("upload", source=source)["program_id"]
                run_id = client.request("start", program_id=program,
                                        config=json.loads(args.config), timeout_s=args.timeout)["run_id"]
                cursor = 0
                while True:
                    page = client.request("read", run_id=run_id, after=cursor, wait_ms=5000)
                    if page["gap"]:
                        print(json.dumps({"gap": page["gap"]}), file=sys.stderr)
                    for record in page["records"]:
                        print(json.dumps(record), flush=True)
                    cursor = page["cursor"]
                    if page["state"] == "exited" and cursor >= page["latest_seq"]:
                        return 0 if page["returncode"] == 0 and page["reason"] is None else 1
            if args.command == "upload":
                result = client.request("upload", source=args.file.read_text(encoding="utf-8"))
            elif args.command == "start":
                retention = {key: getattr(args, key) for key in ("buffer_records", "buffer_bytes")
                             if getattr(args, key) is not None}
                result = client.request("start", program_id=args.program_id,
                                        config=json.loads(args.config), timeout_s=args.timeout,
                                        **retention)
            elif args.command == "list":
                result = client.request("list")
            elif args.command == "send":
                result = client.request("send", run_id=args.run_id, value=json.loads(args.value))
            elif args.command == "read":
                result = client.request("read", run_id=args.run_id, after=args.after, wait_ms=args.wait_ms)
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
