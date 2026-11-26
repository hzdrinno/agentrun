import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from runner_client import Client, RemoteError, resolve_connection
from runner_tls import TLS_NAME, cert_not_after

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_runner(proc, port, token_file, *, ca=None, server_name=None, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(proc.stderr.read().decode("utf-8", "replace"))
        try:
            token = token_file.read_text(encoding="ascii").strip()
            with Client("127.0.0.1", port, token, ca=ca, server_name=server_name) as client:
                client.ping()
            return token
        except (OSError, RemoteError):
            time.sleep(0.03)
    raise RuntimeError("runner failed to start")


def stop_proc(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()


class RunnerIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        state = Path(cls.directory.name)
        reader_token = state / "reader.token"
        cls.reader_token = "test-reader-token-" * 3
        reader_token.write_text(cls.reader_token)
        (state / "sample.txt").write_text("local file")
        cls.port = free_port()
        cls.server = subprocess.Popen(
            [sys.executable, str(ROOT / "runner.py"), "--state-dir", str(state),
             "--work-dir", str(state),
             "--read-token-file", str(reader_token)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        cls.addClassCleanup(lambda: stop_proc(cls.server))
        cls.server.stdin.write(f"{cls.port}\n".encode())
        cls.server.stdin.flush()
        cls.token = wait_for_runner(cls.server, cls.port, state / "operator.token")

    @classmethod
    def connect(cls, token=None):
        return Client("127.0.0.1", cls.port, token or cls.token)

    def test_cli_lists_files_through_runner(self):
        token_file = Path(self.directory.name) / "operator.token"
        result = subprocess.run(
            [sys.executable, str(ROOT / "runner_client.py"), "--port", str(self.port),
             "--token-file", str(token_file), "exec", "--file",
             str(ROOT / "examples" / "list_files.py")],
            check=True, capture_output=True, text=True, timeout=10,
        )
        records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([record["channel"] for record in records], ["files"])
        self.assertIn("sample.txt", records[0]["value"])
        self.assertIn("run_id:", result.stderr)

    def test_cli_exec_stdin_and_config_file(self):
        token_file = Path(self.directory.name) / "operator.token"
        cfg_file = Path(self.directory.name) / "cfg.json"
        cfg_file.write_text(json.dumps({"greeting": "hello"}))
        result = subprocess.run(
            [sys.executable, str(ROOT / "runner_client.py"), "--port", str(self.port),
             "--token-file", str(token_file), "exec", "--file", "-",
             "--config-file", str(cfg_file)],
            input='ctx.emit("out", [ctx.config["greeting"], bool(ctx.run_id), bool(ctx.program_id)])\n',
            check=True, capture_output=True, text=True, timeout=10,
        )
        records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(records[0]["value"], ["hello", True, True])

    def test_worker_continues_after_disconnect_and_cursors_report_loss(self):
        source = """\
ctx.emit("started", ctx.config["tag"])
value = ctx.recv(timeout=5)
for index in range(8):
    ctx.emit("data", [value, index], timestamp={"sample": index})
"""
        with self.connect() as client:
            program = client.request("upload", source=source)["program_id"]
            run = client.request("start", program_id=program, name="cursor-test",
                                 config={"tag": "test"}, buffer_records=5)["run_id"]
            first = client.request("read", run_id=run, wait_ms=3000)
            self.assertEqual(first["records"][0]["value"], "test")
            self.assertEqual(first["name"], "cursor-test")
            self.assertIsNotNone(first["pid"])
            client.request("send", run_id=run, value={"message": "hello"})
        time.sleep(0.2)
        with self.connect(self.reader_token) as client:
            after = first["cursor"]
            for _ in range(3):
                page = client.request("read", run_id=run, after=after, wait_ms=3000)
                if page["state"] == "exited":
                    break
                time.sleep(0.05)
            self.assertEqual(page["returncode"], 0)
            self.assertEqual(page["gap"], {"from": 2, "through": 4})
            self.assertEqual([rec["seq"] for rec in page["records"]], [5, 6, 7, 8, 9])
            self.assertEqual(page["records"][-1]["value"], [{"message": "hello"}, 7])
            self.assertEqual(page["records"][-1]["timestamp"], {"sample": 7})
            self.assertEqual(page["cursor"], 9)
            self.assertIsNotNone(page["exited_utc"])
            with self.assertRaisesRegex(RemoteError, "requires an operator"):
                client.request("send", run_id=run, value=None)

    def test_wrong_token_is_rejected(self):
        with self.assertRaisesRegex(RemoteError, "authentication required"):
            with self.connect("wrong token"):
                pass

    def test_timeout_and_stop(self):
        source = "import time\nctx.emit('started', True)\ntime.sleep(60)\n"
        with self.connect() as client:
            program = client.request("upload", source=source)["program_id"]
            for reason, timeout in [("stopped", 0), ("timeout", 1)]:
                run = client.request("start", program_id=program, timeout_s=timeout)["run_id"]
                first = client.request("read", run_id=run, wait_ms=3000)
                self.assertEqual(first["records"][0]["channel"], "started")
                if not timeout:
                    stopped = client.request("stop", run_id=run, grace_s=0.1)
                    # Stop must return final exited state, never state="running" with returncode set.
                    self.assertEqual(stopped["state"], "exited")
                    self.assertEqual(stopped["reason"], "stopped")
                    self.assertIsNotNone(stopped["returncode"])
                final = client.request("read", run_id=run, after=first["cursor"], wait_ms=5000)
                self.assertEqual(final["state"], "exited")
                self.assertEqual(final["reason"], reason)
                self.assertNotEqual(final["returncode"], 0)

    def test_graceful_stop_closes_inbox_and_runs_finally(self):
        source = """\
try:
    ctx.emit("ready", ctx.try_recv("none"))
    try:
        ctx.recv(timeout=10)
    except EOFError:
        ctx.emit("eof", ctx.stopping.is_set())
finally:
    ctx.emit("cleaned_up", True)
"""
        with self.connect() as client:
            program = client.request("upload", source=source)["program_id"]
            run = client.request("start", program_id=program)["run_id"]
            first = client.request("read", run_id=run, wait_ms=3000)
            self.assertEqual(first["records"][0]["value"], "none")
            stopped = client.request("stop", run_id=run, grace_s=3)
            self.assertEqual(stopped["state"], "exited")
            self.assertEqual(stopped["returncode"], 0)
            self.assertEqual(stopped["reason"], "stopped")
            rest = client.request("read", run_id=run, after=first["cursor"])
            self.assertEqual([(r["channel"], r["value"]) for r in rest["records"]],
                             [("eof", True), ("cleaned_up", True)])

    def test_stdin_is_detached_from_control_pipe(self):
        source = """\
import sys
ctx.emit("stdin", sys.stdin.read())
ctx.emit("msg", ctx.recv(timeout=5))
"""
        with self.connect() as client:
            program = client.request("upload", source=source)["program_id"]
            run = client.request("start", program_id=program)["run_id"]
            client.request("send", run_id=run, value={"ok": 42})
            records = list(client.iter_records(run))
            self.assertEqual([(r["channel"], r["value"]) for r in records],
                             [("stdin", ""), ("msg", {"ok": 42})])

    def test_utf8_split_in_logs_and_oversized_output_drain(self):
        source = """\
import sys
sys.stderr.write("a" * 4095 + "µV\\n")
sys.stderr.flush()
ctx._events.write(b"x" * 150000 + b"\\n")
ctx._events.flush()
ctx.emit("after", "ok")
"""
        with self.connect() as client:
            program = client.request("upload", source=source)["program_id"]
            run = client.request("start", program_id=program)["run_id"]
            records = list(client.iter_records(run))
            logs = "".join(r["value"] for r in records if r["channel"] == "log")
            self.assertNotIn("\ufffd", logs)
            self.assertTrue(logs.endswith("µV\n"))
            runner_msgs = [r["value"] for r in records if r["channel"] == "runner"]
            self.assertEqual(runner_msgs, ["Discarded oversized worker output"])
            after_msgs = [r["value"] for r in records if r["channel"] == "after"]
            self.assertEqual(after_msgs, ["ok"])

    def test_read_tail_channels_and_large_limit(self):
        source = """\
for i in range(150):
    ctx.emit("noise", i)
    if i in (10, 80, 149):
        ctx.emit("important", i)
"""
        with self.connect() as client:
            program = client.request("upload", source=source)["program_id"]
            run = client.request("start", program_id=program)["run_id"]
            all_records = list(client.iter_records(run))
            self.assertEqual(len(all_records), 153)
            # Single read with limit > 100
            page = client.request("read", run_id=run, after=0, limit=500)
            self.assertEqual(len(page["records"]), 153)
            # Channel filter
            filtered = client.request("read", run_id=run, after=0, channels=["important"])
            self.assertEqual([r["value"] for r in filtered["records"]], [10, 80, 149])
            self.assertEqual(filtered["cursor"], 153)
            # Tail
            tailed = client.request("read", run_id=run, tail=2, channels=["important"])
            self.assertEqual([r["value"] for r in tailed["records"]], [80, 149])
            self.assertEqual(tailed["cursor"], 153)

    def test_ping_metadata_and_file_push_pull_over_buffer_size(self):
        with self.connect() as client:
            info = client.ping()
            self.assertEqual(info["version"], 1)
            self.assertIn("hostname", info)
            self.assertIn("uptime_s", info)
            # Transfer a 2.5 MiB binary file (> default 2 MiB buffer_bytes) to verify flow control.
            payload = os.urandom(2_500_000)
            expected = hashlib.sha256(payload).hexdigest()
            src = Path(self.directory.name) / "upload.bin"
            dst = Path(self.directory.name) / "download.bin"
            src.write_bytes(payload)
            pushed = client.push_file(src, "nested/remote.bin")
            self.assertEqual(pushed["sha256"], expected)
            pulled = client.pull_file("nested/remote.bin", dst)
            self.assertEqual(pulled["sha256"], expected)
            self.assertEqual(dst.read_bytes(), payload)

    def test_profile_and_env_resolution(self):
        cfg_path = Path(self.directory.name) / "agentrun.json"
        cfg_path.write_text(json.dumps({
            "default": "remote",
            "profiles": {
                "remote": {"host": "192.0.2.10", "port": 7700, "server_name": "agentrun"},
            },
        }))
        old_env = dict(os.environ)
        try:
            os.environ["AGENTRUN_CONFIG"] = str(cfg_path)
            os.environ.pop("AGENTRUN_HOST", None)
            os.environ["AGENTRUN_PORT"] = "19999"
            resolved = resolve_connection(host="127.0.0.1")
            self.assertEqual(resolved["host"], "127.0.0.1")
            self.assertEqual(resolved["port"], 19999)
            self.assertEqual(resolved["server_name"], "agentrun")
        finally:
            os.environ.clear()
            os.environ.update(old_env)


class EvictionAndTlsTests(unittest.TestCase):
    def test_eviction_removes_oldest_exited_not_oldest_started(self):
        with tempfile.TemporaryDirectory() as td:
            state = Path(td)
            port = free_port()
            proc = subprocess.Popen(
                [sys.executable, str(ROOT / "runner.py"), "--port", str(port),
                 "--state-dir", str(state), "--work-dir", str(state),
                 "--max-workers", "3", "--max-runs", "3"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                tok = wait_for_runner(proc, port, state / "operator.token")
                with Client("127.0.0.1", port, tok) as client:
                    p_long = client.request("upload", source="ctx.emit('up', 1)\nctx.recv(timeout=10)\n")["program_id"]
                    p_short = client.request("upload", source="ctx.emit('ok', 1)\n")["program_id"]
                    r_long = client.request("start", program_id=p_long)["run_id"]
                    client.request("read", run_id=r_long, wait_ms=2000)
                    r_short1 = client.request("start", program_id=p_short)["run_id"]
                    list(client.iter_records(r_short1))
                    time.sleep(0.05)
                    r_short2 = client.request("start", program_id=p_short)["run_id"]
                    list(client.iter_records(r_short2))
                    time.sleep(0.05)
                    # Finish r_long AFTER r_short1 and r_short2.
                    client.request("send", run_id=r_long, value="go")
                    list(client.iter_records(r_long, after=1))
                    # Start r_short3 -> must evict r_short1 (oldest exited), keeping r_long and r_short2!
                    r_short3 = client.request("start", program_id=p_short)["run_id"]
                    ids = {r["run_id"] for r in client.request("list")}
                    self.assertIn(r_long, ids)
                    self.assertIn(r_short2, ids)
                    self.assertIn(r_short3, ids)
                    self.assertNotIn(r_short1, ids)
            finally:
                stop_proc(proc)

    def test_tls_handshake_and_expiry_reporting(self):
        with tempfile.TemporaryDirectory() as td:
            state = Path(td)
            port = free_port()
            cert = state / "runner.pem"
            key = state / "runner-key.pem"
            try:
                import cryptography  # noqa: F401
                has_crypto = True
                tls_args = ["--tls-auto"]
            except ImportError:
                has_crypto = False
                if shutil.which("openssl") is None:
                    self.skipTest("neither cryptography nor openssl available")
                subprocess.run(
                    ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                     "-keyout", str(key), "-out", str(cert), "-days", "30",
                     "-subj", f"/CN={TLS_NAME}",
                     "-addext", f"subjectAltName=DNS:{TLS_NAME}"],
                    check=True, capture_output=True,
                )
                tls_args = ["--cert", str(cert), "--key", str(key)]
            proc = subprocess.Popen(
                [sys.executable, str(ROOT / "runner.py"), "--port", str(port),
                 "--state-dir", str(state), "--work-dir", str(state), *tls_args],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                tok = wait_for_runner(proc, port, state / "operator.token",
                                      ca=cert, server_name=TLS_NAME)
                with Client("127.0.0.1", port, tok, ca=cert, server_name=TLS_NAME) as client:
                    info = client.ping()
                    self.assertEqual(info["version"], 1)
                    if has_crypto:
                        self.assertIsNotNone(cert_not_after(cert))
                        self.assertIsNotNone(info["tls_expires_utc"])
            finally:
                stop_proc(proc)


class SelfUpdateTests(unittest.TestCase):
    def _init_git_fixture(self, base, initial_files):
        origin = base / "origin.git"
        work = base / "work"
        clone = base / "runner-checkout"
        subprocess.run(["git", "init", "--bare", str(origin)], check=True, capture_output=True)
        subprocess.run(["git", "clone", str(origin), str(work)], check=True, capture_output=True)
        for cmd in (["git", "-C", str(work), "config", "user.email", "test@example.com"],
                    ["git", "-C", str(work), "config", "user.name", "Test"]):
            subprocess.run(cmd, check=True, capture_output=True)
        for name, content in initial_files.items():
            (work / name).write_bytes(content)
        subprocess.run(["git", "-C", str(work), "add", "."], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(work), "commit", "-m", "initial"], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(work), "push", "origin", "HEAD"], check=True, capture_output=True)
        subprocess.run(["git", "clone", str(origin), str(clone)], check=True, capture_output=True)
        return work, clone

    def test_native_update_rollback_and_active_worker_guard(self):
        files = {name: (ROOT / name).read_bytes()
                 for name in ("runner.py", "runner_worker.py", "runner_tls.py", "runner_client.py")}
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            work, clone = self._init_git_fixture(base, files)
            state = base / "state"
            port = free_port()
            proc = subprocess.Popen(
                [sys.executable, str(clone / "runner.py"), "--port", str(port),
                 "--state-dir", str(state), "--work-dir", str(clone)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                tok = wait_for_runner(proc, port, state / "operator.token")
                with Client("127.0.0.1", port, tok) as client:
                    # 1. Up-to-date no-op
                    res = client.update()
                    self.assertFalse(res["updated"])
                    old_rev = res["old_rev"]

                    # 2. Push a broken commit -> update must fail and roll back cleanly
                    (work / "runner.py").write_text("def broken(:\n")
                    subprocess.run(["git", "-C", str(work), "commit", "-am", "broken"], check=True, capture_output=True)
                    subprocess.run(["git", "-C", str(work), "push"], check=True, capture_output=True)
                    with self.assertRaisesRegex(RemoteError, "rolled back"):
                        client.update()
                    self.assertEqual(client.ping()["rev"], old_rev[:12])

                    # 3. Revert broken commit + push a valid update
                    (work / "runner.py").write_bytes(files["runner.py"] + b"\n# updated\n")
                    subprocess.run(["git", "-C", str(work), "commit", "-am", "valid update"], check=True, capture_output=True)
                    subprocess.run(["git", "-C", str(work), "push"], check=True, capture_output=True)
                    new_head = subprocess.run(["git", "-C", str(work), "rev-parse", "HEAD"],
                                              check=True, capture_output=True, text=True).stdout.strip()

                    # 4. Active worker blocks update without force
                    pid = client.request("upload", source="ctx.recv(timeout=10)\n")["program_id"]
                    rid = client.request("start", program_id=pid)["run_id"]
                    with self.assertRaisesRegex(RemoteError, "active workers"):
                        client.update()
                    client.request("stop", run_id=rid)

                    # 5. Valid update restarts and comes back on new_head
                    updated = client.update()
                    self.assertTrue(updated["updated"])
                    self.assertTrue(updated["restarting"])
                    self.assertEqual(updated["new_rev"], new_head)
                    self.assertEqual(updated["live"]["rev"], new_head[:12])
            finally:
                stop_proc(proc)


if __name__ == "__main__":
    unittest.main()
