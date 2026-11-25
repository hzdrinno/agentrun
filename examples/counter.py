"""Emit a counter periodically. Send {"interval_s": 0.25} to change its cadence."""

import queue
import time

interval = float(ctx.config.get("interval_s", 1))
count = 0
due = time.monotonic()
while True:
    try:
        message = ctx.recv(timeout=max(0, due - time.monotonic()))
    except queue.Empty:
        count += 1
        ctx.emit("counter", count, timestamp={"monotonic_ns": str(time.monotonic_ns())})
        due = time.monotonic() + interval
    else:
        interval = float(message["interval_s"])
        if interval <= 0:
            raise ValueError("interval_s must be positive")
        due = time.monotonic() + interval
        ctx.emit("interval_s", interval)
