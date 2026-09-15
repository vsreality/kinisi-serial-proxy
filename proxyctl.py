# File: proxyctl.py
"""Manage the Linux background proxy using verified identities and stable pidfds."""

import argparse
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time

DIRECTORY = Path(__file__).resolve().parent
PIDFILE = DIRECTORY / 'proxy.pid'
LOGFILE = DIRECTORY / 'proxy.log'


def process_identity(pid):
    """
    Read process lifetime identifiers; reject zombies and unrelated executables.

    Args:
        pid: Integer process ID greater than one.

    Returns:
        A dictionary containing pid, start_ticks, and boot_id for this proxy process.

    Raises:
        ValueError: The PID or script argument does not identify this proxy.
        ProcessLookupError: The process is a zombie.
        OSError: Process metadata cannot be read, including when the process exits.
    """
    if type(pid) is not int or pid <= 1:
        raise ValueError('invalid PID')
    proc = Path('/proc') / str(pid)
    fields = (proc / 'stat').read_text().rpartition(') ')[2].split()
    if fields[0] == 'Z':
        raise ProcessLookupError('process has exited')
    command = (proc / 'cmdline').read_bytes().split(b'\0')
    expected = os.fsencode(str(DIRECTORY / 'proxy.py'))
    # The launcher always uses this exact Python invocation. Checking the script
    # argument also rejects a forged/stale file pointing to an unrelated process.
    if len(command) < 3 or command[1] != expected:
        raise ValueError('PID does not belong to this proxy')
    return {'pid': pid, 'start_ticks': fields[19],
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def verified_process():
    """
    Return a pidfd only when the saved identity still describes this exact process.

    Returns:
        An open Linux pidfd for the verified process, or None for a missing,
        stale, malformed, inaccessible, or unrelated process record.

    Notes:
        The caller owns the returned descriptor and must close it. A pidfd refers
        to one process lifetime even if its numeric PID is later reused.
    """
    descriptor = None
    try:
        saved = json.loads(PIDFILE.read_text())
        if not isinstance(saved, dict):
            return None  # Old numeric PID files are intentionally not trusted.
        pid = saved.get('pid')
        if type(pid) is not int or pid <= 1:
            return None
        descriptor = os.pidfd_open(pid)
        if process_identity(pid) != saved:
            raise ValueError('stale process identity')
        return descriptor
    except (OSError, ValueError, IndexError):
        if descriptor is not None:
            os.close(descriptor)
        return None


def proxy_arguments():
    """
    Translate launcher environment settings into literal CLI arguments.

    Returns:
        A list of literal command-line arguments derived from the launcher environment.

    Notes:
        Reads HOST, PORT, BAUD, SERIAL_TIMEOUT, SERIAL_PORT, ALLOW_CLIENTS and
        ALLOW_ORIGINS. Allowlist values are split on whitespace, never evaluated by a shell.
    """
    args = ['--host', os.environ.get('HOST', '127.0.0.1'),
            '--port', os.environ.get('PORT', '8765'),
            '--baud-rate', os.environ.get('BAUD', '115200'),
            '--serial-timeout', os.environ.get('SERIAL_TIMEOUT', '0.05')]
    if os.environ.get('SERIAL_PORT'):
        args += ['--serial-port', os.environ['SERIAL_PORT']]
    for variable, option in [('ALLOW_CLIENTS', '--allow-client'), ('ALLOW_ORIGINS', '--allow-origin')]:
        for value in os.environ.get(variable, '').split():
            args += [option, value]
    return args


def start():
    """
    Start one proxy and atomically save its identity after a successful startup.

    Returns:
        Zero if already running or started successfully; one if the child exits during startup.

    Raises:
        OSError: Launching the process or recording its identity fails.

    Notes:
        Call while holding the management lock. Starts a detached child, appends
        output to proxy.log and atomically replaces the identity file.
    """
    descriptor = verified_process()
    if descriptor is not None:
        os.close(descriptor)
        print('Proxy already running.')
        return 0
    with LOGFILE.open('ab') as log:
        process = subprocess.Popen([sys.executable, str(DIRECTORY / 'proxy.py'), *proxy_arguments()],
                                   cwd=DIRECTORY, stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
    try:
        time.sleep(1)
        if process.poll() is not None:
            print(f'Proxy failed to start; see {LOGFILE}', file=sys.stderr)
            return 1
        identity = process_identity(process.pid)
        temporary = PIDFILE.with_suffix('.pid.tmp')
        temporary.write_text(json.dumps(identity) + '\n')
        temporary.replace(PIDFILE)
    except BaseException:
        # This Popen still owns its unreaped child; no saved PID is used here.
        process.terminate()
        process.wait(timeout=5)
        raise
    print(f'Proxy started (PID {process.pid}). Logs: {LOGFILE}')
    return 0


def stop():
    """
    Signal a verified pidfd, never a PID that could have been reassigned.

    Returns:
        Zero after stopping the verified proxy, or when no verified process exists.

    Raises:
        TimeoutError: The verified process has not exited after termination and kill waits.
        OSError: Signalling or removing the identity file fails.

    Notes:
        Call while holding the management lock. Escalates from SIGTERM to SIGKILL
        through the same pidfd. Retains the identity file when termination times out.
    """
    descriptor = verified_process()
    if descriptor is None:
        print('No verified proxy process; no signal sent.')
    else:
        try:
            signal.pidfd_send_signal(descriptor, signal.SIGTERM)
            if not select.select([descriptor], [], [], 5)[0]:
                signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                if not select.select([descriptor], [], [], 2)[0]:
                    raise TimeoutError('proxy did not exit; retaining its identity file')
            print('Proxy stopped.')
        except ProcessLookupError:
            pass
        finally:
            os.close(descriptor)
    PIDFILE.unlink(missing_ok=True)
    return 0


def main():
    """
    Serialize management commands and fail safely on systems without pidfd support.

    Returns:
        The requested command's exit status; status returns one when no verified proxy exists.

    Raises:
        SystemExit: Arguments are invalid or required Linux/Python features are unavailable.

    Notes:
        Takes an exclusive file lock around status/start/stop/restart. Log following
        runs separately so it cannot block other management commands.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', nargs='?', default='start', choices=['start', 'stop', 'restart', 'status', 'logs'])
    args = parser.parse_args()
    if not sys.platform.startswith('linux') or not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        parser.error('background management requires Linux with pidfd support; use proxy.py directly elsewhere')
    if args.action == 'logs':
        return subprocess.call(['tail', '-n', '40', '-f', str(LOGFILE)])
    import fcntl
    with (DIRECTORY / 'proxy.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if args.action == 'status':
            descriptor = verified_process()
            if descriptor is None:
                print('not running (no verified proxy process)')
                return 1
            os.close(descriptor)
            print('running')
            return 0
        if args.action in ('stop', 'restart'):
            stop()
        if args.action in ('start', 'restart'):
            return start()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
