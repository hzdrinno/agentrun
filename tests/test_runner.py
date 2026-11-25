import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from runner_client import Client, RemoteError

ROOT = Path(__file__).resolve().parents[1]


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
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            cls.port = sock.getsockname()[1]
        cls.server = subprocess.Popen(
            [sys.executable, str(ROOT / "runner.py"), "--state-dir", str(state),
             "--work-dir", str(state),
             "--read-token-file", str(reader_token)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        cls.addClassCleanup(cls.shutdown)
        cls.server.stdin.write(f"{cls.port}\n".encode())
        cls.server.stdin.flush()
        token_file = state / "operator.token"
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if cls.server.poll() is not None:
                raise RuntimeError(cls.server.stderr.read().decode())
            try:
                cls.token = token_file.read_text().strip()
                with cls.connect() as client:
                    client.request("ping")
                return
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("runner failed to start")

    @classmethod
    def shutdown(cls):
        try:
            cls.server.communicate(timeout=0.2)
        except subprocess.TimeoutExpired:
            cls.server.terminate()
            try:
                cls.server.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                cls.server.kill()
                cls.server.communicate()

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

    def test_worker_continues_after_disconnect_and_cursors_report_loss(self):
        source = """\
ctx.emit("started", ctx.config["tag"])
value = ctx.recv(timeout=5)
for index in range(8):
    ctx.emit("data", [value, index], timestamp={"sample": index})
"""
        with self.connect() as client:
            program = client.request("upload", source=source)["program_id"]
            run = client.request("start", program_id=program, config={"tag": "test"},
                                 buffer_records=5)["run_id"]
            first = client.request("read", run_id=run, wait_ms=3000)
            self.assertEqual(first["records"][0]["value"], "test")
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
                    client.request("stop", run_id=run)
                final = client.request("read", run_id=run, after=first["cursor"], wait_ms=3000)
                self.assertEqual(final["state"], "exited")
                self.assertEqual(final["reason"], reason)
                self.assertNotEqual(final["returncode"], 0)


if __name__ == "__main__":
    unittest.main()
