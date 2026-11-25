"""List a directory relative to the worker's working directory or by absolute path."""

from pathlib import Path

directory = Path(ctx.config.get("path", "."))
ctx.emit("files", sorted(entry.name for entry in directory.iterdir()))
