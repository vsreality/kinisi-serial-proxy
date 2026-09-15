# ----------------------------------------------------------------------------
# Filename: proxy.py
# Description: Low-level WebSocket <-> serial proxy for the Kinisi controller.
#
# The proxy runs on a Raspberry Pi (or any host with the controller plugged in
# over USB/serial) and exposes a WebSocket endpoint. A web client (jskinisi)
# tunnels its raw serial traffic through this WebSocket, and the proxy forwards
# the bytes to/from the physical serial port.
#
# The proxy is deliberately dumb: it knows nothing about the Kinisi command
# protocol. It only relays bytes, so it stays compatible with any firmware
# version without changes here.
#
# Wire protocol
# -------------
# Two message kinds share the single WebSocket, distinguished by the WebSocket
# frame type:
#
#   * Text frames  -> JSON control messages (open / close / list / status).
#   * Binary frames -> raw serial data, prefixed with a 1-byte opcode:
#
#       WRITE (0x01): [0x01, ...payload]
#           Proxy writes `payload` to the serial port. No response.
#
#       READ  (0x02): [0x02, len_lo, len_hi]
#           Proxy reads exactly `len` bytes (uint16, little-endian) from the
#           serial port and replies with a single binary frame containing those
#           bytes. On timeout it replies with the bytes read so far (possibly
#           empty), matching pyserial's read() behaviour.
#
# JSON control messages (client -> proxy), each replied to with JSON:
#
#     {"op": "open",  "port": "/dev/ttyUSB0", "baudRate": 115200}
#     {"op": "close"}
#     {"op": "list"}
#     {"op": "status"}
#
# Replies always echo "op" and include "ok": true|false, plus "error" on
# failure and op-specific fields (e.g. "ports" for list).
# ----------------------------------------------------------------------------

import argparse
import asyncio
import json
import logging
import ipaddress
import math
from urllib.parse import urlsplit
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus

import serial
import serial.tools.list_ports
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from websockets.datastructures import Headers, MultipleValuesError
from websockets.http11 import Response

OP_WRITE = 0x01
OP_READ = 0x02

logger = logging.getLogger("kinisi-serial-proxy")


class SupersededConnection(Exception):
    """A previous client tried to operate after a new client took ownership."""


class SerialProxy:
    """Owns the serial port and relays bytes for one WebSocket at a time."""

    def __init__(self, default_port=None, baud_rate=115200, timeout=0.05,
                 allowed_clients=None, allowed_origins=()):
        """
        Configure trusted client networks, exact browser origins and serial defaults.

        Args:
            default_port: Device path or administrator-selected serial URL; None lets the client choose.
            baud_rate: Default integer baud rate, from 1 through 4000000.
            timeout: Finite positive serial read timeout in seconds.
            allowed_clients: Trusted IP addresses or CIDR networks; None allows loopback only.
            allowed_origins: Exact HTTP(S) browser origins, without paths or wildcards.

        Raises:
            ValueError: A configuration value is invalid.

        Notes:
            Creates a serial worker executor but does not open hardware or start a listener.
        """
        self._validate_open(default_port, baud_rate, optional_port=True)
        if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("serial timeout must be a finite positive number")
        clients = ['127.0.0.0/8', '::1/128'] if allowed_clients is None else allowed_clients
        self.allowed_clients = tuple(ipaddress.ip_network(value, strict=False) for value in clients)
        self.allowed_origins = frozenset(allowed_origins)
        for origin in self.allowed_origins:
            parsed = urlsplit(origin)
            if (parsed.scheme not in ('http', 'https') or not parsed.hostname
                    or parsed.username or parsed.password or parsed.path
                    or parsed.query or parsed.fragment or '*' in origin):
                raise ValueError("allowed origins must be exact HTTP(S) origins without a path")
            _ = parsed.port  # Reject malformed port numbers during startup.
        self.default_port = default_port
        self.default_baud_rate = baud_rate
        self.timeout = timeout
        self.serial = None
        # Serialises access to the blocking pyserial object so overlapping
        # coroutines can't interleave a read, write, open or close.
        self.lock = asyncio.Lock()
        # All blocking serial calls run on this single worker thread. pyserial
        # objects are not safe to touch from multiple threads at once (a write
        # racing a close crashes on the internal abort pipes), so we confine
        # every serial operation to one thread.
        self._serial_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="serial-io"
        )
        # Only one client may drive the serial port at a time. A newer
        # connection takes over and supersedes the previous one.
        self.active_connection = None

    # -- serial lifecycle (blocking; run on the serial-io thread) -----------

    async def _run_serial(self, func, *args):
        """
        Run a blocking serial call on the dedicated serial-io thread.

        Args:
            func: Blocking callable to execute on the dedicated serial worker.
            *args: Positional arguments passed unchanged to func.

        Returns:
            The callable's result after completion.

        Raises:
            Exception: Exceptions raised by func propagate to the caller.

        Notes:
            Callers must hold the serial lock when accessing shared serial state.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._serial_executor, func, *args)

    def _open(self, port, baud_rate):
        """
        Reuse a healthy matching port; failed I/O invalidates it separately.

        Args:
            port: Validated serial device path or administrator-configured URL.
            baud_rate: Validated integer baud rate.

        Raises:
            serial.SerialException: The device cannot be opened or configured.
            OSError: The operating system rejects a serial operation.

        Notes:
            Run on the serial worker while holding the serial lock. Reuses a matching
            open handle; otherwise closes the previous handle before opening the new one.
        """
        # Avoid unnecessary serial reconfiguration and DTR transitions. Whether
        # DTR causes a reset depends on the board and its firmware.
        if (
            self.serial is not None
            and self.serial.is_open
            and self.serial.port == port
            and self.serial.baudrate == baud_rate
        ):
            logger.info("Serial port %s already open; reusing", port)
            return
        self._close()
        # serial_for_url accepts plain device paths (/dev/ttyUSB0, COM3) as well
        # as pyserial URL handlers such as loop:// (handy for testing without
        # hardware) and rfc2217://host:port (serial over the network).
        # Short reads release the shared worker promptly for heartbeat writes.
        # Keep a separate write budget so this does not also cut transfers to 50 ms.
        self.serial = serial.serial_for_url(
            port, baudrate=baud_rate, timeout=self.timeout,
            write_timeout=max(1.0, self.timeout)
        )
        logger.info("Opened serial port %s @ %d", port, baud_rate)

    def _close(self):
        """
        Close and invalidate the serial handle on its owning worker thread.

        Raises:
            OSError: The underlying close operation fails.

        Notes:
            Run on the serial worker while holding the serial lock. Clears self.serial
            even if closing raises; does nothing when no handle exists.
        """
        if self.serial is not None:
            try:
                self.serial.close()
            finally:
                logger.info("Closed serial port %s", self.serial.port)
                self.serial = None

    # -- HTTP (REST) endpoints ----------------------------------------------
    #
    # Served on the same host/port as the WebSocket. A plain HTTP request is
    # answered here; a WebSocket upgrade request returns None and proceeds to
    # the handshake / handle() below. The web client fetches the serial-port
    # list over HTTP *before* opening the WebSocket, then connects and switches
    # to the binary protocol.

    async def process_request(self, connection, request):
        """
        Authorize HTTP and WebSocket requests before they can claim a serial session.

        Args:
            connection: Incoming WebSocket server connection, including its TCP peer address.
            request: HTTP request with headers and path.

        Returns:
            An HTTP response for rejected access or discovery requests; None permits
            the WebSocket handshake to continue.

        Notes:
            Applies the same access rules to HTTP and WebSockets before ownership changes.
        """
        error = self._access_error(connection, request)
        if error:
            return self._json_response({'error': error}, HTTPStatus.FORBIDDEN)
        origin = request.headers.get('Origin')
        path = request.path.split("?")[0]

        if path == "/ports":
            # Port enumeration is independent of the (possibly busy or wedged)
            # serial connection, so it runs on the default thread pool, never
            # the single serial-io thread.
            ports = await asyncio.to_thread(self._list_ports)
            return self._json_response({"ports": ports}, origin=origin)

        if path == "/status":
            async with self.lock:
                status = self._status()
            return self._json_response(status, origin=origin)

        # Not an HTTP endpoint -> let the WebSocket handshake proceed.
        return None

    def _access_error(self, connection, request):
        """
        Trust the TCP peer address, never forwarding headers supplied by the client.

        Args:
            connection: Connection whose remote_address supplies the actual TCP peer.
            request: Request whose optional Origin header identifies the browser page.

        Returns:
            An error string for rejected access, or None when the peer and origin are allowed.

        Notes:
            Clients without an Origin header still require an allowed address.
            IPv4-mapped IPv6 addresses are normalized before checking the allowlist.
        """
        try:
            address = ipaddress.ip_address(connection.remote_address[0])
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
                address = address.ipv4_mapped
            if not any(address in network for network in self.allowed_clients):
                return 'client address is not allowed'
            origin = request.headers.get('Origin')
            if origin is not None and origin not in self.allowed_origins:
                return 'browser origin is not allowed'
        except (ValueError, TypeError, IndexError, MultipleValuesError):
            return 'invalid client address or origin'
        return None

    @staticmethod
    def _json_response(payload, status=HTTPStatus.OK, origin=None):
        """
        Expose HTTP responses only to an already-authorized browser origin.

        Args:
            payload: JSON-serializable response content.
            status: HTTP status code, defaulting to 200 OK.
            origin: Already-authorized browser origin, or None to omit CORS permission.

        Returns:
            A WebSocket server HTTP Response with JSON content and explicit content length.
        """
        body = (json.dumps(payload) + "\n").encode()
        headers = Headers(
            {
                "Content-Type": "application/json",
                "Content-Length": str(len(body)),
                "Vary": "Origin",
            }
        )
        if origin is not None:
            headers['Access-Control-Allow-Origin'] = origin
        return Response(status, status.phrase, headers, body)

    def _check_owner(self, websocket):
        """
        Reject queued work from old clients; call while holding the serial lock.

        Args:
            websocket: Client attempting to access the shared serial session.

        Raises:
            SupersededConnection: The client no longer owns the session.

        Notes:
            Check under the serial lock immediately before an operation. The receive
            loop also calls this to discard queued messages promptly.
        """
        if self.active_connection is not websocket:
            raise SupersededConnection()

    def _status(self):
        """
        Read a consistent serial status while holding the serial lock.

        Returns:
            A dictionary containing open, port, and baudRate; closed-port values are None.

        Notes:
            The caller must hold the serial lock while taking this snapshot.
        """
        return {'open': self.serial is not None,
                'port': self.serial.port if self.serial else None,
                'baudRate': self.serial.baudrate if self.serial else None}

    # -- WebSocket connection handler ---------------------------------------

    async def handle(self, websocket):
        """
        Transfer ownership atomically and discard queued operations from previous clients.

        Args:
            websocket: Accepted server connection with request and remote_address metadata.

        Notes:
            Rechecks access, transfers ownership under the serial lock, and closes the
            previous WebSocket. Discards superseded work. Serial failures close the
            WebSocket with code 1011; normal disconnects retain healthy serial handles.
        """
        if self._access_error(websocket, websocket.request):
            await websocket.close(1008, 'client is not allowed')
            return
        peer = getattr(websocket, "remote_address", "?")
        logger.info("Client connected: %s", peer)

        # Only one client drives the serial port. Claim it as the active
        # connection *before* closing any previous one, so the previous
        # handler's cleanup below sees it is no longer active and leaves the
        # (about to be reused) serial port alone.
        async with self.lock:
            previous = self.active_connection
            self.active_connection = websocket
        try:
            if previous is not None:
                logger.info("Superseding previous connection")
                await previous.close(1001, "superseded by a new connection")

            if self.default_port:
                async with self.lock:
                    self._check_owner(websocket)
                    await self._run_serial(
                        self._open, self.default_port, self.default_baud_rate
                    )
            async for message in websocket:
                self._check_owner(websocket)
                if isinstance(message, (bytes, bytearray)):
                    await self._handle_binary(websocket, message)
                else:
                    await self._handle_text(websocket, message)
        except (ConnectionClosed, SupersededConnection):
            pass
        except (serial.SerialException, OSError) as exc:
            # Serial I/O already discarded the failed handle under its lock.
            # End this session: silently dropping bytes would hide lost commands
            # and leave the SDK waiting for replies that can never arrive.
            logger.warning("Serial connection lost for %s: %s", peer, exc)
            await websocket.close(1011, "serial I/O failed; reconnect to the controller")
        finally:
            logger.info("Client disconnected: %s", peer)
            # Keep healthy handles across WebSocket reconnects. Failed handles
            # are closed by _serial_io so they cannot poison the next connection.
            async with self.lock:
                if self.active_connection is websocket:
                    self.active_connection = None

    # -- binary (data) frames -----------------------------------------------

    async def _handle_binary(self, websocket, message):
        """
        Forward one binary operation belonging to the current client.

        Args:
            websocket: Client that submitted the frame.
            message: Bytes containing the proxy opcode followed by its arguments.

        Raises:
            SupersededConnection: Ownership was lost before serial access.
            serial.SerialException: Serial I/O failed or no port is open.

        Notes:
            WRITE has no proxy reply. READ replies with bytes, possibly empty on timeout.
            Empty or unknown frames are ignored; truncated READ requests receive empty bytes.
        """
        if len(message) < 1:
            return
        opcode = message[0]

        if opcode == OP_WRITE:
            payload = bytes(message[1:])
            await self._serial_write(websocket, payload)

        elif opcode == OP_READ:
            if len(message) < 3:
                logger.warning("Malformed READ frame: %r", message)
                await websocket.send(b"")
                return
            length = message[1] | (message[2] << 8)  # uint16 little-endian
            data = await self._serial_read(websocket, length)
            await websocket.send(data)

        else:
            logger.warning("Unknown binary opcode: 0x%02x", opcode)

    async def _serial_write(self, websocket, payload):
        """
        Forward a complete write or invalidate the failed serial session.

        Args:
            websocket: Client whose ownership must still be valid.
            payload: Complete byte sequence to write.

        Raises:
            SupersededConnection: The client no longer owns the port.
            serial.SerialException: No port is open, the write fails, or only part is written.
            OSError: The operating system rejects I/O.

        Notes:
            A failed write invalidates the serial handle; bytes are never silently dropped.
        """
        await self._serial_io(websocket, "write", payload)

    async def _serial_read(self, websocket, length):
        """
        Return received bytes, including empty timeout reads, or fail the session.

        Args:
            websocket: Client whose ownership must still be valid.
            length: Maximum number of bytes requested from the serial device.

        Returns:
            Received bytes; a read timeout may return fewer bytes, including none.

        Raises:
            SupersededConnection: The client no longer owns the port.
            serial.SerialException: No port is open or the device fails.
            OSError: The operating system rejects I/O.
        """
        return await self._serial_io(websocket, "read", length)

    async def _serial_io(self, websocket, operation, argument):
        """
        Serialize I/O and discard a broken handle before another client can reuse it.

        Args:
            websocket: Client requesting access to the shared serial device.
            operation: Either read or write.
            argument: Requested byte count for read, or payload bytes for write.

        Returns:
            Received bytes for read, or the written byte count for write.

        Raises:
            SupersededConnection: Ownership changed before the serial lock was acquired.
            serial.SerialException: The port is absent, I/O fails, or a write is incomplete.
            OSError: The operating system rejects I/O.

        Notes:
            Keeps ownership checking, I/O and failure cleanup under one lock. Failed
            handles are discarded before any other client can reopen the device.
        """
        async with self.lock:
            self._check_owner(websocket)
            if self.serial is None:
                raise serial.SerialException("no open serial port")
            try:
                result = await self._run_serial(getattr(self.serial, operation), argument)
                if operation == "write" and result != len(argument):
                    raise serial.SerialException("incomplete serial write")
                return result
            except (serial.SerialException, OSError):
                try:
                    await self._run_serial(self._close)
                except (serial.SerialException, OSError) as exc:
                    logger.warning("Failed to close disconnected serial port: %s", exc)
                raise

    # -- text (control) frames ----------------------------------------------

    async def _handle_text(self, websocket, message):
        """
        Validate a JSON control envelope before dispatching its operation.

        Args:
            websocket: Client to receive the JSON control reply.
            message: JSON text expected to contain an object with a string op field.

        Notes:
            Malformed envelopes and unknown operations receive ok: false. Valid
            operations delegate to the corresponding control handler.
        """
        try:
            request = json.loads(message)
            if not isinstance(request, dict) or not isinstance(request.get('op'), str):
                raise ValueError('expected an object with a string op')
            op = request.get("op")
        except (ValueError, AttributeError):
            await websocket.send(json.dumps({"ok": False, "error": "invalid JSON"}))
            return

        if op == "open":
            await self._ctl_open(websocket, request)
        elif op == "close":
            await self._ctl_close(websocket)
        elif op == "list":
            await self._ctl_list(websocket)
        elif op == "status":
            await self._ctl_status(websocket)
        else:
            await websocket.send(
                json.dumps({"op": op, "ok": False, "error": f"unknown op '{op}'"})
            )

    async def _ctl_open(self, websocket, request):
        """
        Validate settings without changing a healthy port when input is malformed.

        Args:
            websocket: Client requesting the serial connection.
            request: Control object with optional port and baudRate settings.

        Raises:
            SupersededConnection: The client lost ownership before opening the device.

        Notes:
            Invalid settings and serial open failures produce an open reply with
            ok: false. Remote serial URLs must match the administrator-selected default.
        """
        port = request.get("port", self.default_port)
        baud_rate = request.get("baudRate", self.default_baud_rate)
        try:
            self._validate_open(port, baud_rate)
            # URL handlers are administrator-configured only; a remote client
            # must not redirect the proxy to arbitrary network endpoints or files.
            if '://' in port and port != self.default_port:
                raise ValueError('serial URLs must be configured with --serial-port')
            async with self.lock:
                self._check_owner(websocket)
                await self._run_serial(self._open, port, baud_rate)
            await websocket.send(
                json.dumps({"op": "open", "ok": True, "port": port, "baudRate": baud_rate})
            )
        except (serial.SerialException, ValueError, OSError) as exc:
            logger.warning("Serial open rejected: %s", exc)
            await websocket.send(json.dumps({"op": "open", "ok": False, "error": str(exc)}))

    @staticmethod
    def _validate_open(port, baud_rate, optional_port=False):
        """
        Reject coercions, non-finite values and malformed device names explicitly.

        Args:
            port: Device name to validate.
            baud_rate: Integer baud rate from 1 through 4000000; booleans are rejected.
            optional_port: Allow port=None when validating server defaults.

        Raises:
            ValueError: The port or baud rate has an invalid type, value, or length.

        Notes:
            Validation does not open, close, or otherwise modify a serial device.
        """
        if not (optional_port and port is None):
            if not isinstance(port, str) or not port.strip() or '\x00' in port or len(port) > 1024:
                raise ValueError('port must be a nonempty string of at most 1024 characters')
        if type(baud_rate) is not int or not 1 <= baud_rate <= 4000000:
            raise ValueError('baudRate must be an integer between 1 and 4000000')

    async def _ctl_close(self, websocket):
        """
        Close only the serial session still owned by this client.

        Args:
            websocket: Current client requesting closure and receiving the JSON reply.

        Raises:
            SupersededConnection: The client lost ownership before the close operation.
            OSError: Closing the serial device fails.

        Notes:
            Closing an already-closed port succeeds.
        """
        async with self.lock:
            self._check_owner(websocket)
            await self._run_serial(self._close)
        await websocket.send(json.dumps({"op": "close", "ok": True}))

    async def _ctl_list(self, websocket):
        """
        List serial devices without opening any of them.

        Args:
            websocket: Client to receive the list reply.

        Notes:
            Runs device enumeration off the event loop without occupying the serial worker.
        """
        ports = await asyncio.to_thread(self._list_ports)
        await websocket.send(json.dumps({"op": "list", "ok": True, "ports": ports}))

    @staticmethod
    def _list_ports():
        """
        Enumerate local serial device metadata.

        Returns:
            A list of dictionaries with device, description and hwid for each local port.

        Notes:
            Enumerates metadata only; no device is opened.
        """
        return [
            {"device": p.device, "description": p.description, "hwid": p.hwid}
            for p in serial.tools.list_ports.comports()
        ]

    async def _ctl_status(self, websocket):
        """
        Return a consistent snapshot without racing a worker-thread close.

        Args:
            websocket: Current client to receive open, port and baudRate fields.

        Raises:
            SupersededConnection: The client lost ownership before the snapshot.
        """
        async with self.lock:
            self._check_owner(websocket)
            status = self._status()
        await websocket.send(json.dumps({'op': 'status', 'ok': True, **status}))


async def main():
    """
    Validate configuration, serve authorized clients and release serial resources on exit.

    Raises:
        SystemExit: Command-line arguments or configuration are invalid.
        OSError: The listener cannot bind or a resource operation fails.

    Notes:
        Serves until cancelled. On shutdown, closes the serial handle and releases
        its worker executor after the WebSocket server exits.
    """
    parser = argparse.ArgumentParser(description="Kinisi WebSocket <-> serial proxy")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: localhost only)")
    parser.add_argument('--allow-client', action='append', default=[], metavar='IP_OR_CIDR',
                        help='Allow a trusted client IP/network, in addition to loopback; repeatable')
    parser.add_argument('--allow-origin', action='append', default=[], metavar='ORIGIN',
                        help='Allow an exact browser origin, e.g. http://localhost:5173; repeatable')
    parser.add_argument("--port", type=int, default=8765, help="WebSocket bind port")
    parser.add_argument(
        "--serial-port",
        default=None,
        help="Serial device to auto-open on connect (e.g. /dev/ttyUSB0). "
        "If omitted, the client must send an 'open' control message.",
    )
    parser.add_argument("--baud-rate", type=int, default=115200, help="Serial baud rate")
    parser.add_argument(
        "--serial-timeout", type=float, default=0.05,
        help="Serial read timeout in seconds (default 0.05 keeps idle heartbeat writes responsive)"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        if not 1 <= args.port <= 65535:
            raise ValueError('port must be between 1 and 65535')
        proxy = SerialProxy(
            default_port=args.serial_port, baud_rate=args.baud_rate, timeout=args.serial_timeout,
            allowed_clients=['127.0.0.0/8', '::1/128', *args.allow_client],
            allowed_origins=args.allow_origin,
        )
    except ValueError as exc:
        parser.error(str(exc))

    logger.info(
        "Starting proxy on ws://%s:%d (HTTP: GET /ports, /status)",
        args.host,
        args.port,
    )
    try:
        async with serve(proxy.handle, args.host, args.port, process_request=proxy.process_request):
            await asyncio.Future()  # run forever
    finally:
        async with proxy.lock:
            await proxy._run_serial(proxy._close)
        proxy._serial_executor.shutdown(wait=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
