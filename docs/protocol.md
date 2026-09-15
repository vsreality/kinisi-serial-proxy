# Proxy protocol

HTTP discovery and WebSocket connections share the same port and access rules.
`GET /ports` lists device paths, descriptions and hardware IDs. `GET /status`
returns the serial port's open state, path and baud rate.

| WebSocket message | Behaviour |
| --- | --- |
| JSON `{"op":"open","port":"<device>","baudRate":115200}` | Open a serial device |
| JSON `{"op":"close"}` | Close the serial device |
| JSON `{"op":"list"}` | List available devices |
| JSON `{"op":"status"}` | Read serial connection status |
| Binary `[0x01, ...payload]` | Write raw serial bytes; no proxy reply |
| Binary `[0x02, len_lo, len_hi]` | Read up to the requested little-endian length and reply with bytes |

JSON replies include `op`, `ok`, and an `error` when an operation fails. Reads may
return fewer bytes on timeout. A new authorized client supersedes the previous
connection; queued commands from the previous client are discarded. Serial I/O
failures close the WebSocket with code 1011; reconnect through the SDK to recover.

Serial URL handlers, such as `loop://` or `rfc2217://`, must be explicitly selected
with `--serial-port` by the administrator. Remote clients cannot select other URL
handlers. Browser origins must be allowed for HTTP discovery as well as WebSockets.
Client-address checks use the actual TCP peer, so a gateway must enforce access
for its own clients too.
