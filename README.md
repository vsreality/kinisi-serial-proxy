# Kinisi Serial Proxy

Connect to a Kinisi motor controller over your local network using the
[jskinisi web client](https://github.com/vsreality/jskinisi).

Run the proxy on a Raspberry Pi or another computer connected to the controller
by USB. You can then use the web client from a separate computer. The proxy
forwards serial data without interpreting controller commands.

## Installation

Requires Python 3.9+. From the project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

On Windows, activate the environment with `.venv\Scripts\Activate.ps1` in
PowerShell. Use `python` instead of `python3` if that is your interpreter name.

The user running the proxy needs access to the serial device. On Raspberry Pi OS,
add the user to `dialout` if needed, then log out and back in:

```bash
sudo usermod -a -G dialout "$USER"
```

## Run

For a web client running on the same computer:

```bash
python3 proxy.py --allow-origin http://localhost:5173
```

For a Raspberry Pi accessed from another computer:

```bash
python3 proxy.py --host 0.0.0.0 \
  --allow-client "<client-ip>" \
  --allow-origin http://localhost:5173
```

Replace `<client-ip>` with the trusted computer's IP address and the origin with
the web client's actual page origin, including scheme and port, without a trailing
slash. Both allowlist options can be repeated. Client entries accept IP addresses
or CIDR networks; origins must match exactly.

The proxy defaults to localhost and checks access before accepting connections.
These settings are for trusted private networks; they do not provide user
authentication or encryption. Use a VPN or authenticated TLS gateway for remote
access across untrusted networks. HTTPS pages may require an HTTPS/WSS connection.

| Option | Default | Purpose |
| --- | --- | --- |
| `--host` | `127.0.0.1` | Listener address |
| `--port` | `8765` | HTTP and WebSocket port |
| `--serial-port` | none | Auto-open a device when a client connects |
| `--baud-rate` | `115200` | Default serial baud rate |
| `--serial-timeout` | `0.05` | Serial read timeout in seconds |
| `--allow-client` | loopback only | Additional trusted client IP or network |
| `--allow-origin` | none | Allowed browser origin |
| `--verbose` | off | Debug logging |

The web client normally selects the serial port. For a fixed device on Linux,
prefer a stable `/dev/serial/by-id/...` path. Keep the read timeout well below the
controller's heartbeat timeout so idle reads do not delay heartbeat writes.

## JavaScript client

See the [JavaScript client guide](docs/client.md) for connecting with jskinisi.

## Background service

On Linux with pidfd support (kernel 5.3+), use the included launcher:

```bash
PYTHON=.venv/bin/python HOST=0.0.0.0 \
  ALLOW_CLIENTS="<client-ip>" ALLOW_ORIGINS="http://localhost:5173" \
  bash proxyctl.sh start
bash proxyctl.sh status
bash proxyctl.sh logs
bash proxyctl.sh stop
```

Other settings are `PORT`, `BAUD`, `SERIAL_PORT`, and `SERIAL_TIMEOUT`. Allowlist
values are space-separated. Supply the same environment settings when restarting.
Logs are appended to `proxy.log`. The launcher survives logout but does not start
at boot. Stop an older launcher before upgrading; legacy numeric PID files are
not accepted by the new process manager.

For startup at boot, customize the user, paths and access settings in
[kinisi-serial-proxy.service](kinisi-serial-proxy.service), then install it:

```bash
sudo cp kinisi-serial-proxy.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kinisi-serial-proxy
```

Use either systemd or the launcher for a given listener. On Windows or macOS,
run `python3 proxy.py` directly.

## Proxy protocol

See the [protocol reference](docs/protocol.md) for HTTP endpoints and WebSocket messages.

## Tests

Run from the project directory:

```bash
python3 -m unittest discover -s tests -v
```

Tests use isolated loopback devices; no controller is required. Linux process
management tests are skipped on other platforms. For a standalone forwarding check:

```bash
python3 proxy.py --serial-port loop://   # first terminal
python3 -m tests.test_client            # second terminal
```

## License

[MIT](LICENSE).
