#!/usr/bin/env bash
# File: proxyctl.sh
# Linux launcher for the verified process manager. See README.md.
# Settings: HOST, PORT, BAUD, SERIAL_PORT, SERIAL_TIMEOUT, PYTHON,
#           ALLOW_CLIENTS (space-separated IP/CIDR), ALLOW_ORIGINS (origins).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${PYTHON:-python3}" "$DIR/proxyctl.py" "$@"
