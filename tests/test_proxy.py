# File: tests/test_proxy.py
"""Verify raw forwarding and idle read latency without connecting motor hardware."""

import asyncio
import json
import unittest
import threading
from types import SimpleNamespace
from unittest.mock import patch

import serial
import websockets
from websockets.asyncio.server import serve
from websockets.datastructures import Headers

from proxy import OP_READ, OP_WRITE, SerialProxy, SupersededConnection


class ProxyTests(unittest.IsolatedAsyncioTestCase):
    """Exercise the actual WebSocket server against pyserial's loopback device."""

    async def asyncSetUp(self):
        """
        Start an isolated proxy on an ephemeral local port.
        """
        self.proxy = SerialProxy(default_port='loop://')
        self.server = await serve(self.proxy.handle, '127.0.0.1', 0,
                                  process_request=self.proxy.process_request)
        port = self.server.sockets[0].getsockname()[1]
        self.url = f'ws://127.0.0.1:{port}'

    async def asyncTearDown(self):
        """
        Release WebSockets and the serial worker even after a failed assertion.
        """
        self.server.close()
        await self.server.wait_closed()
        await self.proxy._run_serial(self.proxy._close)
        self.proxy._serial_executor.shutdown(wait=True)

    async def test_idle_read_does_not_block_queued_write_for_watchdog_period(self):
        """
        An empty READ must yield before a 500 ms watchdog can expire.
        """
        async with websockets.connect(self.url) as socket:
            payload = bytes([3, 0x76, 1, 0])
            await socket.send(bytes([OP_READ, 1, 0]))
            await socket.send(bytes([OP_WRITE]) + payload)
            await socket.send(bytes([OP_READ, len(payload), 0]))
            self.assertEqual(await asyncio.wait_for(socket.recv(), 0.5), b'')
            self.assertEqual(await asyncio.wait_for(socket.recv(), 0.5), payload)
            self.assertEqual(self.proxy.serial.timeout, 0.05)
            self.assertGreaterEqual(self.proxy.serial.write_timeout, 1.0)

    async def test_raw_telemetry_and_control_messages(self):
        """
        Keep binary payloads intact without interpreting their Kinisi message type.
        """
        async with websockets.connect(self.url) as socket:
            await socket.send(json.dumps({'op': 'status'}))
            status = json.loads(await socket.recv())
            self.assertTrue(status['ok'])
            payload = bytes([37, 0x7C, 0, 0]) + bytes(range(34))
            await socket.send(bytes([OP_WRITE]) + payload)
            await socket.send(bytes([OP_READ, len(payload), 0]))
            self.assertEqual(await asyncio.wait_for(socket.recv(), 0.5), payload)

    async def test_failed_serial_io_closes_session_and_reconnect_opens_fresh_port(self):
        """
        USB loss, write timeout and partial writes must never reuse a broken handle.
        """
        cases = [
            ('read', {'side_effect': serial.SerialException('device disconnected')}, bytes([OP_READ, 1, 0])),
            ('write', {'side_effect': OSError(5, 'Input/output error')}, bytes([OP_WRITE, 42])),
            ('write', {'side_effect': serial.SerialTimeoutException('Write timeout')}, bytes([OP_WRITE, 42])),
            ('write', {'return_value': 0}, bytes([OP_WRITE, 42])),
        ]
        for operation, behavior, frame in cases:
            with self.subTest(operation=operation, behavior=behavior):
                async with websockets.connect(self.url) as socket:
                    await socket.send(json.dumps({'op': 'status'}))
                    await socket.recv()  # Wait for automatic serial open.
                    failed_port = self.proxy.serial
                    with patch.object(failed_port, operation, **behavior):
                        await socket.send(frame)
                        with self.assertRaises(websockets.ConnectionClosed) as raised:
                            await asyncio.wait_for(socket.recv(), 1)
                    self.assertEqual(raised.exception.rcvd.code, 1011)
                    self.assertIsNone(self.proxy.serial)
                    self.assertFalse(failed_port.is_open)

                async with websockets.connect(self.url) as socket:
                    await socket.send(json.dumps({'op': 'status'}))
                    self.assertTrue(json.loads(await socket.recv())['open'])
                    self.assertIsNot(self.proxy.serial, failed_port)
                    await socket.send(bytes([OP_WRITE, 42]))
                    await socket.send(bytes([OP_READ, 1, 0]))
                    self.assertEqual(await asyncio.wait_for(socket.recv(), 1), b'*')

    async def test_takeover_discards_old_buffered_commands(self):
        """
        A write queued behind a blocked old write must not execute after takeover.
        """
        async with websockets.connect(self.url) as old:
            await old.send(json.dumps({'op': 'status'})); await old.recv()
            entered, release = threading.Event(), threading.Event()
            writes = []

            def blocked_write(data):
                """
                Hold the first serial call while a replacement connects.

                Args:
                    data: Bytes submitted by the proxy serial worker.

                Returns:
                    The payload length after the first write is released.

                Raises:
                    RuntimeError: The test fails to release the blocked worker within two seconds.
                """
                writes.append(bytes(data))
                if len(writes) == 1:
                    entered.set()
                    if not release.wait(2):
                        raise RuntimeError('test did not release the serial worker')
                return len(data)

            with patch.object(self.proxy.serial, 'write', side_effect=blocked_write):
                try:
                    await old.send(bytes([OP_WRITE]) + b'first')
                    self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                    await old.send(bytes([OP_WRITE]) + b'stale')
                    async with websockets.connect(self.url) as new:
                        await asyncio.sleep(0.05)  # Let takeover wait on the occupied serial lock.
                        release.set()
                        await new.send(bytes([OP_WRITE]) + b'new')
                        await new.send(json.dumps({'op': 'status'}))
                        await asyncio.wait_for(new.recv(), 1)
                        self.assertEqual(writes, [b'first', b'new'])
                finally:
                    release.set()

    async def test_superseded_controls_cannot_close_or_reopen_new_port(self):
        """
        Check ownership again after waiting for the lock, for both OPEN and CLOSE.
        """
        async with websockets.connect(self.url) as ws:
            await ws.send(json.dumps({'op': 'status'})); await ws.recv()
            previous = self.proxy.active_connection
            for operation in ('open', 'close', 'read', 'write'):
                with self.subTest(operation=operation):
                    async with self.proxy.lock:
                        self.proxy.active_connection = previous
                        if operation == 'open':
                            pending = asyncio.create_task(self.proxy._ctl_open(previous, {'port': 'loop://'}))
                        elif operation == 'close':
                            pending = asyncio.create_task(self.proxy._ctl_close(previous))
                        else:
                            pending = asyncio.create_task(self.proxy._serial_io(previous, operation, 1 if operation == 'read' else b'x'))
                        await asyncio.sleep(0)
                        self.proxy.active_connection = object()
                    with self.assertRaises(SupersededConnection):
                        await pending
                    self.assertTrue(self.proxy.serial.is_open)
            self.proxy.active_connection = previous

    async def test_invalid_open_returns_error_without_losing_session(self):
        """
        Bad JSON field types must neither close a healthy serial port nor kill the socket.
        """
        cases = [{'baudRate': value} for value in ['invalid', '115200', None, True, 1.5, 0, -1, 4000001]]
        cases += [{'port': value} for value in [None, 42, [], '', ' ', 'bad\x00port', 'x' * 1025]]
        cases += [{'port': 'socket://untrusted.invalid:1234'}]
        async with websockets.connect(self.url) as ws:
            await ws.send(json.dumps({'op': 'status'})); await ws.recv()
            original = self.proxy.serial
            for invalid in cases:
                with self.subTest(request=invalid):
                    await ws.send(json.dumps({'op': 'open', 'port': 'loop://', **invalid}))
                    response = json.loads(await asyncio.wait_for(ws.recv(), 1))
                    self.assertEqual(response['op'], 'open')
                    self.assertFalse(response['ok'])
                    self.assertIs(self.proxy.serial, original)
                    self.assertTrue(original.is_open)
            await ws.send(bytes([OP_WRITE, 42])); await ws.send(bytes([OP_READ, 1, 0]))
            self.assertEqual(await ws.recv(), b'*')

    async def test_invalid_envelope_returns_error(self):
        """
        Arrays, scalars, missing operations and malformed JSON keep the connection usable.
        """
        async with websockets.connect(self.url) as ws:
            for invalid in ['{', '[]', 'null', '{}', '{"op":4}']:
                await ws.send(invalid)
                self.assertFalse(json.loads(await ws.recv())['ok'])
            await ws.send(json.dumps({'op': 'status'}))
            self.assertTrue(json.loads(await ws.recv())['ok'])

    async def test_disallowed_origin_cannot_evict_current_client(self):
        """
        Authorization happens during handshake, before serial ownership can change.
        """
        async with websockets.connect(self.url) as owner:
            await owner.send(json.dumps({'op': 'status'})); await owner.recv()
            active = self.proxy.active_connection
            with self.assertRaises(websockets.exceptions.InvalidStatus) as error:
                async with websockets.connect(self.url, origin='https://untrusted.example'):
                    self.fail('untrusted origin was accepted')
            self.assertEqual(error.exception.response.status_code, 403)
            self.assertIs(self.proxy.active_connection, active)
            await owner.send(json.dumps({'op': 'status'}))
            self.assertTrue(json.loads(await owner.recv())['ok'])

    async def test_explicit_origin_can_connect(self):
        """
        The same jskinisi transport can connect from an explicitly trusted page.
        """
        self.proxy.allowed_origins = frozenset(['http://localhost:5173'])
        async with websockets.connect(self.url, origin='http://localhost:5173') as ws:
            await ws.send(json.dumps({'op': 'status'}))
            self.assertTrue(json.loads(await ws.recv())['ok'])

    async def test_peer_rejection_applies_to_websocket_and_http(self):
        """
        A spoofed forwarding header cannot bypass TCP client-address restrictions.
        """
        self.proxy.allowed_clients = ()
        with self.assertRaises(websockets.exceptions.InvalidStatus) as error:
            async with websockets.connect(self.url, additional_headers={'X-Forwarded-For': '127.0.0.1'}):
                self.fail('blocked peer was accepted')
        self.assertEqual(error.exception.response.status_code, 403)
        response = await self.http_get('/ports')
        self.assertTrue(response.startswith(b'HTTP/1.1 403'))
        self.assertNotIn(b'Access-Control-Allow-Origin', response)

    async def http_get(self, path, origin=None):
        """
        Read a real HTTP response from the shared listener without third-party HTTP clients.

        Args:
            path: HTTP endpoint on the fixture listener.
            origin: Optional browser Origin header.

        Returns:
            Raw HTTP response bytes.

        Notes:
            Bounds the read to one second and closes the stream in all cases.
        """
        port = self.server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection('127.0.0.1', port)
        headers = f'GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n'
        if origin:
            headers += f'Origin: {origin}\r\n'
        writer.write((headers + '\r\n').encode()); await writer.drain()
        try:
            return await asyncio.wait_for(reader.read(), 1)
        finally:
            writer.close(); await writer.wait_closed()

    async def test_http_origin_and_cors(self):
        """
        Only exact allowed origins receive CORS permission on status and discovery.
        """
        self.proxy.allowed_origins = frozenset(['http://localhost:5173'])
        for path in ('/ports', '/status'):
            response = await self.http_get(path, 'http://localhost:5173')
            self.assertTrue(response.startswith(b'HTTP/1.1 200'))
            self.assertIn(b'Access-Control-Allow-Origin: http://localhost:5173', response)
            response = await self.http_get(path, 'http://localhost:5173.evil.example')
            self.assertTrue(response.startswith(b'HTTP/1.1 403'))
            self.assertNotIn(b'Access-Control-Allow-Origin', response)


class ConfigurationTests(unittest.TestCase):
    """Validate security and serial settings before any server or worker is started."""

    def test_invalid_configuration(self):
        """
        Reject invalid origins, address ranges and serial settings at startup.
        """
        for settings in [{'timeout': float('nan')}, {'timeout': float('inf')}, {'timeout': 0},
                         {'baud_rate': True}, {'default_port': 42},
                         {'allowed_clients': ['not-an-address']},
                         *({'allowed_origins': [origin]} for origin in
                           ['*', 'null', 'https://example.com/path', 'https://user:pass@example.com', 'http://example.com:bad'])]:
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                SerialProxy(**settings)

    def test_client_networks_and_ipv4_mapped_peers(self):
        """
        Allow exactly the configured network without trusting forwarding headers.
        """
        proxy = SerialProxy(allowed_clients=['192.0.2.0/24'])
        try:
            request = SimpleNamespace(headers=Headers({'X-Forwarded-For': '192.0.2.1'}))
            for ip, allowed in [('192.0.2.5', True), ('::ffff:192.0.2.5', True), ('198.51.100.5', False)]:
                error = proxy._access_error(SimpleNamespace(remote_address=(ip, 1234)), request)
                self.assertEqual(error is None, allowed)
        finally:
            proxy._serial_executor.shutdown(wait=True)


if __name__ == '__main__':
    unittest.main()
