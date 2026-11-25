"""Child process entry point. Uploaded scripts receive a global named ctx."""

import json
import os
from pathlib import Path
import queue
import runpy
import sys
import threading

MAX_OUTPUT = 64 * 1024
EOF = object()


class Context:
    def __init__(self, config, control, events):
        self.config = config
        self._events = events
        self._lock = threading.Lock()
        self._inbox = queue.Queue(maxsize=32)
        threading.Thread(target=self._receive, args=(control,), daemon=True).start()

    def _receive(self, control):
        buffer = b""
        try:
            while chunk := control.read(4096):
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    self._inbox.put(json.loads(line)["value"])
        finally:
            self._inbox.put(EOF)

    def recv(self, timeout=None):
        """Wait for one message. Raises queue.Empty on timeout or EOFError when closed."""
        value = self._inbox.get(timeout=timeout)
        if value is EOF:
            self._inbox.put_nowait(EOF)
            raise EOFError("runner closed the inbox")
        return value

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
    # A raw pipe avoids a daemon thread holding a buffered I/O lock at exit.
    control = os.fdopen(os.dup(sys.stdin.fileno()), "rb", buffering=0)
    config = json.loads(control.readline())
    sys.stdout.flush()
    with os.fdopen(os.dup(sys.stdout.fileno()), "wb") as events:
        # Keep print() and stderr out of the structured output stream.
        os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
        context = Context(config, control, events)
        sys.argv = [str(script)]
        sys.path[:1] = [str(script.parent), str(Path.cwd())]
        runpy.run_path(str(script), init_globals={"ctx": context}, run_name="__main__")


if __name__ == "__main__":
    main()
