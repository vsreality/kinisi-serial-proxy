# JavaScript client

Configure the proxy to allow your client computer and browser origin as described
in the [README](../README.md#run).

Use `KinisiWebSocketClient` from jskinisi. Always call `open()` before controller
commands, even when the proxy auto-opens the serial port: `connect()` opens the
WebSocket, while `open()` completes the board handshake and enables the SDK heartbeat.

```js
import { KinisiWebSocketClient } from './commands/kinisi_ws_client.js';

const controller = new KinisiWebSocketClient('ws://raspberrypi.local:8765');
if (!await controller.connect()) throw new Error(controller.lastError);

const ports = await controller.listPorts();
// Let the user select a device from ports and pass its path as selectedPort.
const result = await controller.open(selectedPort);
if (!result.ok) throw new Error(result.error);

await controller.ping();
// Perform application operations here; disconnect when finished.
await controller.disconnect();
```
