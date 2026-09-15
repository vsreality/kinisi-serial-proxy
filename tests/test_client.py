# ----------------------------------------------------------------------------
# Filename: tests/test_client.py
# Description: Standalone smoke test for the Kinisi serial proxy.
#
# It connects to a running proxy over WebSocket and exercises the full wire
# protocol: the JSON control channel (status / list / open) and the binary
# data channel (WRITE + READ round-trip).
#
# Run the proxy against pyserial's loopback handler (no hardware needed):
#
#     python3 proxy.py --serial-port loop://
#
# then in another terminal:
#
#     python3 -m tests.test_client
#
# With loop://, everything WRITTEN to the port is echoed back on READ, so the
# round-trip assertion below verifies bytes flow both ways through the proxy.
# ----------------------------------------------------------------------------

import argparse
import asyncio
import json

import websockets

OP_WRITE = 0x01
OP_READ = 0x02


def write_frame(payload: bytes) -> bytes:
    """
    Prefix a loopback payload with the proxy WRITE opcode.

    Args:
        payload: Raw loopback test bytes.

    Returns:
        Bytes prefixed with the WRITE opcode.
    """
    return bytes([OP_WRITE]) + payload


def read_frame(length: int) -> bytes:
    """
    Request the expected number of echoed bytes.

    Args:
        length: Requested byte count, expected to fit an unsigned 16-bit integer.

    Returns:
        Three bytes containing the READ opcode and little-endian count.
    """
    return bytes([OP_READ, length & 0xFF, (length >> 8) & 0xFF])


async def control(ws, request: dict) -> dict:
    """
    Exchange and display one JSON control message.

    Args:
        ws: Connected proxy WebSocket.
        request: JSON-serializable control object.

    Returns:
        The decoded JSON reply after printing the exchange.
    """
    await ws.send(json.dumps(request))
    reply = json.loads(await ws.recv())
    print(f"  -> {request}  ==>  {reply}")
    return reply


async def main():
    """
    Verify a loopback-only proxy without sending arbitrary data to motor hardware.

    Raises:
        SystemExit: Arguments request a non-loopback device.
        RuntimeError: The proxy does not report an open loopback port.
        AssertionError: Opening fails or the echoed bytes differ.

    Notes:
        Connects to the configured proxy but writes test data only after verifying loop://.
    """
    parser = argparse.ArgumentParser(description="Smoke test for kinisi-serial-proxy")
    parser.add_argument("--url", default="ws://localhost:8765", help="Proxy WebSocket URL")
    parser.add_argument(
        "--open",
        default=None,
        help="Serial port to open before testing (skip if the proxy auto-opens one)",
    )
    args = parser.parse_args()
    if args.open is not None and args.open != 'loop://':
        parser.error('this smoke test supports only --open loop://')

    print(f"Connecting to {args.url} ...")
    async with websockets.connect(args.url) as ws:
        print("Connected.\n")

        print("[control] status / list:")
        await control(ws, {"op": "status"})
        await control(ws, {"op": "list"})

        if args.open:
            print("\n[control] open:")
            reply = await control(ws, {"op": "open", "port": args.open})
            assert reply.get("ok"), f"open failed: {reply}"

        status = await control(ws, {"op": "status"})
        if not status.get('open') or status.get('port') != 'loop://':
            raise RuntimeError('refusing to write test bytes: configure the proxy with --serial-port loop://')

        print("\n[data] WRITE + READ round-trip:")
        payload = b"\x03\x02\x00hello-kinisi"
        print(f"  writing {len(payload)} bytes: {payload!r}")
        await ws.send(write_frame(payload))

        await ws.send(read_frame(len(payload)))
        echoed = await ws.recv()
        echoed = bytes(echoed)
        print(f"  read back {len(echoed)} bytes: {echoed!r}")

        assert echoed == payload, 'loopback round-trip did not match'
        print("\nPASS: round-trip matched (proxy relays bytes both ways).")


if __name__ == "__main__":
    asyncio.run(main())
