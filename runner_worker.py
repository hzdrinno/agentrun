"""Child process entry point. Uploaded scripts receive a global named ctx."""

from collections import deque
import json
import os
from pathlib import Path
import queue
import runpy
import sys
import threading
import time

MAX_OUTPUT = 64 * 1024
INBOX_LIMIT = 32


class Context:
    def __init__(self, config, control, events):
        self.config = config
        self.run_id = os.environ.get("AGENTRUN_RUN_ID")
        self.program_id = os.environ.get("AGENTRUN_PROGRAM_ID")
        self.stopping = threading.Event()
        self._events = events
        self._lock = threading.Lock()
        self._cond = threading.Condition()
        self._inbox = deque()
        self._closed = False
        threading.Thread(target=self._receive, args=(control,), daemon=True).start()

    def _receive(self, control):
        buffer = b""
        try:
            while chunk := control.read(4096):
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    value = json.loads(line)["value"]
                    with self._cond:
                        while len(self._inbox) >= INBOX_LIMIT and not self._closed:
                            self._cond.wait()
                        self._inbox.append(value)
                        self._cond.notify()
        finally:
            with self._cond:
                self._closed = True
                self.stopping.set()
                self._cond.notify_all()

    def recv(self, timeout=None):
        """Wait for one message. Raises queue.Empty on timeout or EOFError when closed."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while not self._inbox:
                if self._closed:
                    raise EOFError("runner closed the inbox")
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise queue.Empty
                    self._cond.wait(remaining)
                else:
                    self._cond.wait()
            value = self._inbox.popleft()
            self._cond.notify()
            return value

    def try_recv(self, default=None):
        """Return the next queued message, or default if none is ready. Raises EOFError when closed."""
        try:
            return self.recv(timeout=0)
        except queue.Empty:
            return default

    def emit(self, channel, value, timestamp=None):
        """Publish a JSON value. Multiple threads may emit concurrently."""
        if not isinstance(channel, str) or not 0 < len(channel) <= 128:
            raise ValueError("channel must be a nonempty string up to 128 characters")
        item = {"channel": channel, "value": value}
        if timestamp is not None:
            item["timestamp"] = timestamp
        encoded = json.dumps(item, allow_nan=False, separators=(",", ":")).encode() + b"\n"
        if len(encoded) > MAX_OUTPUT:
            raise ValueError(f"output record exceeds {MAX_OUTPUT} bytes")
        with self._lock:
            self._events.write(encoded)
            self._events.flush()


def main():
    script = Path(sys.argv[1])
    # Detach fd 0 from the control pipe so stray stdin reads cannot corrupt framing.
    control = os.fdopen(os.dup(sys.stdin.fileno()), "rb", buffering=0)
    devnull = os.open(os.devnull, os.O_RDONLY)
    try:
        os.dup2(devnull, sys.stdin.fileno())
    finally:
        os.close(devnull)
    config = json.loads(control.readline())
    sys.stdout.flush()
    with os.fdopen(os.dup(sys.stdout.fileno()), "wb") as events:
        # Keep print() and stderr out of the structured output stream.
        os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
        context = Context(config, control, events)
        sys.argv = [str(script)]
        sys.path[:1] = [str(Path.cwd()), str(script.parent)]
        runpy.run_path(str(script), init_globals={"ctx": context}, run_name="__main__")


if __name__ == "__main__":
    main()
