# Python Runner

Run Python on another PC through a single TCP port. The runner manages uploaded scripts, subprocesses, live input, and buffered output. Scripts decide what files, network endpoints, timings, and Python libraries to use. There is no device configuration in the runner.

## Run locally

Python 3.11 or newer is required. The runner and client use only the standard library unless you generate a certificate with `--tls-auto`.

```text
python runner.py
```

Enter the port at the prompt, or use `python runner.py --port 7700` to skip it. By default the runner listens on loopback and puts its operator token and uploaded scripts in `~/.python-runner`. In another terminal:

```text
python runner_client.py --port 7700 exec --file examples/list_files.py
python runner_client.py --port 7700 exec -c "print('hello from the worker')"
```

Use `--work-dir` to select the worker's initial working directory and `--worker-python` to use a specific Python environment. Prepare any third-party libraries in that environment before launching the worker.

## Access it through a proxy

Bind to the address your TCP proxy targets. To generate a self-signed certificate on the runner PC, install the optional certificate generation dependency once and start the runner with `--tls-auto`.

```text
python -m pip install --only-binary=:all: cryptography
python runner.py --port 12260 --tls-auto
```

This listens on loopback. For a proxy on another PC, add `--host` with the runner's reachable IP address. The runner creates `runner.pem` and `runner-key.pem` in its state directory and reuses them on future starts. Give clients `runner.pem` for `--ca` and use `--server-name agentrun`. Keep `runner-key.pem` on the runner PC. The generated certificate is valid for a year.

If you already have a TLS certificate and private key, supply the PEM files directly. A direct non-loopback listener requires TLS.

```text
python runner.py --port 7700 --host <bind-IP> --cert runner.pem --key runner-key.pem
```

Forward the TCP stream to that address and port. Give the client the trusted CA certificate, the name on the runner's certificate, and a copy of the operator token file. The name on the certificate can differ from the proxy address.

```text
python runner_client.py --host <proxy-host> --port <proxy-port> --ca ca.pem --server-name <certificate-name> --token-file operator.token list
```

Use TLS even for a loopback bind if remote clients reach it through a proxy. Only trusted operators should receive the operator token. An operator can execute arbitrary Python with the runner's OS account and its file and network permissions. Run under a dedicated account with the access those programs need. Child processes provide lifecycle separation and share the runner's account. To serve read-only clients, pass `--read-token-file` with a separate random token of at least 32 ASCII characters. Read-only clients can inspect all runs and their output.

## Start a worker and talk to it

```text
python runner_client.py --port 7700 upload examples/counter.py
python runner_client.py --port 7700 start <program_id>
python runner_client.py --port 7700 send <run_id> '{"interval_s": 0.25}'
python runner_client.py --port 7700 read <run_id> --after 0 --wait-ms 2000
python runner_client.py --port 7700 stop <run_id>
```

The commands above use Bash-style JSON quoting. Agents can use `runner_client.Client` directly to pass dictionaries without shell quoting.

Upload stores a UTF-8 Python file and returns its SHA-256 ID. Starting a program returns a unique run ID and accepts a JSON object as `config`. The script sees that object as `ctx.config`. `ctx.recv(timeout=...)` returns the next input, raises `queue.Empty` on timeout, and raises `EOFError` when the input closes. Standard input is reserved for the runner protocol. Use `ctx.emit(channel, value, timestamp=...)` from any thread to publish a JSON value with an optional source timestamp. Ordinary stdout and stderr become log records. A worker can run after its client disconnects.

Read returns records in sequence, receipt times on the host, a cursor for the next read, and a gap if older records have expired. Host monotonic nanoseconds are encoded as decimal strings. The buffer defaults to 2,048 records or 2 MiB per run, whichever fills first. A client can request a smaller buffer when starting a run. Buffers and run IDs live in memory, so they survive client reconnects and expire when the runner restarts. Uploads persist. Completed runs remain available until the run retention limit evicts them. The supervisor stops the worker itself. If a script starts additional OS processes, it should manage their lifetime.

## TCP protocol

Send one UTF-8 JSON object per newline and read one reply object per request. The first message is `{"id":"1","op":"auth","token":"..."}`. Success replies contain `id` and `result`. Failures contain `id` and `error`. Requests within one connection are processed in order. A client can open another connection to the same port for control while one waits for data.

| Operation | Request fields |
| --- | --- |
| `ping`, `list` | None |
| `upload` | `source` (up to 256 KiB of source) |
| `start` | `program_id`, optional `config`, `timeout_s` (zero means no deadline), `buffer_records`, and `buffer_bytes` |
| `send` | `run_id`, `value`. Acknowledges queueing; a worker can emit its own completion event. |
| `read` | `run_id`, optional `after`, `limit`, and `wait_ms` (up to 30 seconds) |
| `status`, `stop` | `run_id` |

For continuous reads, repeat `read` with the returned cursor and a positive `wait_ms`. Reauthenticate on reconnect and reuse the cursor. Oversized messages and a full input queue are reported as errors.

Run the integration tests from this directory with `python -m unittest discover -s tests -v`.
