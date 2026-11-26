# AgentRun

Minimal remote Python execution and process control over a single TCP port, designed for **AI-agent-driven automation**.

Conceptually, AgentRun is **`SSH + tmux + a structured JSON event ring`** collapsed into one unprivileged, standard-library Python process. An agent connects over one forwarded or direct TCP port to upload arbitrary Python scripts on the fly, run them as detached subprocesses that survive disconnects, send live JSON commands into running workers, stream timestamped structured outputs with cursor/gap tracking, and pull or push files — without shell quoting, admin privileges, or framework lock-in.

## Why AgentRun?

If you already have Linux + OpenSSH + `tmux`, plain SSH goes a long way. AgentRun targets the gap where standard tools get awkward — especially unprivileged hosts (including Windows), single-port TCP proxies/tunnels, flaky links, and AI agents acting as the primary operators:

| Tool | What it's built for | Where AgentRun differs |
| --- | --- | --- |
| **OpenSSH (+ `tmux`)** | Interactive shells, port forwarding, SFTP | Requires OS service/admin setup (especially on Windows), lacks native `tmux` on Windows, and streams unstructured bytes where agents spend turns fighting shell quoting and ad-hoc log tailing. AgentRun runs unprivileged and speaks structured JSON with sequence cursors and explicit gap reporting. |
| **Jupyter Server / Kernel Gateway** | Interactive notebooks & remote Python over HTTP/WS | Heavy dependency tree and a stateful cell model that is awkward for detached long-running workers receiving live control messages while buffering output across disconnects. |
| **RPyC / Pyro5** | Pure-Python remote object RPC | Built around live proxy objects over a persistent socket, making them fragile across flaky links and reconnects compared to decoupled subprocesses + in-memory ring buffers. |
| **Ray / Dask / Nomad** | Distributed clusters & job scheduling | Multi-component cluster machinery — great at scale, heavy overkill for controlling a single remote workstation, edge box, or local-network host. |
| **Domain-specific RPC / automation servers** | Fixed command sets & rigid schemas | Require pre-defining endpoints ahead of time; AgentRun has zero domain logic in the supervisor so an agent can upload whatever Python logic a task requires. |

## Run locally

Python 3.11 or newer is required. The runner and client use only the standard library unless you generate a certificate with `--tls-auto`.

```text
python runner.py --port 7700
```

Omit `--port` to enter it interactively. By default the runner listens on loopback and puts its operator token and uploaded scripts in `~/.python-runner`. In another terminal:

```text
python runner_client.py --port 7700 exec --file examples/list_files.py
python runner_client.py --port 7700 exec -c "print('hello from the worker')"
python runner_client.py --port 7700 exec --file - <<'PY'
ctx.emit("hi", {"run": ctx.run_id})
PY
```

Optional editable install gives `agentrun` and `agentrun-client` commands on `PATH`:

```text
python -m pip install --no-build-isolation --no-deps -e .
```

Use `--work-dir` to select the worker's initial working directory (which is first on `sys.path`, so helper modules placed there can be imported) and `--worker-python` to use a specific Python environment.

## Access it remotely or through a proxy

Bind to the address your clients or TCP proxy target. To generate a self-signed certificate on the host, install `cryptography` once and start the runner with `--tls-auto`:

```text
python -m pip install --only-binary=:all: cryptography
python runner.py --host <bind-IP> --port 7700 --tls-auto
```

The runner creates `runner.pem` and `runner-key.pem` in its state directory and reuses them on future starts (valid for one year; expiry is reported in `ping` and logged when within 30 days). Give clients `runner.pem` for `--ca` and use `--server-name agentrun`. Keep `runner-key.pem` on the runner host. A direct non-loopback listener requires `--tls-auto` or `--cert` and `--key`.

### Client defaults and profiles

Avoid repeating connection flags by setting environment variables (`AGENTRUN_HOST`, `AGENTRUN_PORT`, `AGENTRUN_CA`, `AGENTRUN_SERVER_NAME`, `AGENTRUN_TOKEN_FILE`, `AGENTRUN_TOKEN`, `AGENTRUN_PROFILE`) or saving `~/.agentrun.json`:

```json
{
  "default": "remote",
  "profiles": {
    "remote": {
      "host": "192.0.2.10",
      "port": 7700,
      "ca": "/path/to/runner.pem",
      "server_name": "agentrun",
      "token_file": "/path/to/operator.token"
    }
  }
}
```

Then run commands directly (`python runner_client.py ping`, `list`, `exec`, `pull`, `push`, `update`) or use `runner_client.Client.from_env()` from Python.

Only trusted operators should receive the operator token; an operator can execute arbitrary Python under the runner's OS account. Pass `--read-token-file` with a separate random token (at least 32 ASCII characters) for read-only clients.

## Start a worker, stream, and transfer files

```text
python runner_client.py upload examples/counter.py
python runner_client.py start <program_id> --name counter
python runner_client.py send <run_id> '{"interval_s": 0.25}'
python runner_client.py read <run_id> --tail 10 --follow
python runner_client.py stop <run_id> --grace-s 5
python runner_client.py pull remote/output.csv local.csv
python runner_client.py push helper.py helper.py
python runner_client.py install numpy scipy
python runner_client.py install --relay numpy
```

Upload stores a UTF-8 Python file (retaining the 256 most recently used scripts) and returns its SHA-256 ID. Starting a program returns a unique run ID and passes `config` as `ctx.config` (plus `ctx.run_id`, `ctx.program_id`, and `ctx.stopping`).

- `ctx.recv(timeout=...)` returns the next input, raises `queue.Empty` on timeout, and raises `EOFError` when the runner closes the inbox. `ctx.try_recv(default=None)` is a non-blocking shorthand.
- `ctx.emit(channel, value, timestamp=...)` publishes JSON from any thread. Ordinary stdout and stderr become `log` records. Standard input (`fd 0`) is detached to `devnull`.
- Stopping a run (via `stop`, `timeout_s`, or runner shutdown) closes the worker's inbox first and waits up to `grace_s` (default 1.5 s) so `EOFError` / `ctx.stopping` / `finally:` cleanup can run before `terminate()`/`kill()`.
- `pull` and `push` stream arbitrary-size files through the existing protocol with flow control and SHA-256 verification.
- `install` runs `pip install --only-binary=:all:` directly into the host's Python (falling back to `--user` if global `site-packages` is read-only), so installed packages persist across runs and are shared with other Python apps on the host. Pass `--relay` when the host has no PyPI access to download matching wheels on the client, push them over the TCP link, and install offline.

## Self-update from Git

When the runner is started from a git checkout, an operator can update and restart it remotely:

```text
python runner_client.py update
```

The runner refuses to update while workers are running unless `--force` is passed, runs `git pull --ff-only`, validates the updated Python modules (automatically rolling back with `git reset --hard` on failure), restarts in place, and the client transparently reconnects until `ping` reports the new revision.

## TCP protocol

Send one UTF-8 JSON object per newline and read one reply object per request. The first message is `{"id":"1","op":"auth","token":"..."}`. Success replies contain `id` and `result`. Failures contain `id` and `error`. Requests within one connection are processed in order.

| Operation | Request fields |
| --- | --- |
| `ping`, `list` | None (`ping` returns runner metadata, revision, capacity, and TLS expiry) |
| `upload` | `source` (up to 256 KiB of source) |
| `start` | `program_id`, optional `name`, `config`, `timeout_s` (0 = no deadline), `buffer_records`, `buffer_bytes` |
| `send` | `run_id`, `value` |
| `read` | `run_id`, optional `after`, `limit` (up to 2000), `wait_ms` (up to 30000), `tail`, `channels` |
| `status` | `run_id` |
| `stop` | `run_id`, optional `grace_s` (0 to 25, default 1.5) |
| `update` | optional `force`, `restart` (operator only) |

Run the tests with `python -m unittest discover -s tests -v`.
