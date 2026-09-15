# File: tests/test_proxyctl.py
"""Test Linux process ownership using isolated loopback proxy processes."""

import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import unittest


@unittest.skipUnless(sys.platform.startswith('linux') and hasattr(os, 'pidfd_open')
                     and hasattr(signal, 'pidfd_send_signal'), 'Linux pidfd support required')
class ProcessControlTests(unittest.TestCase):
    """Never inspect, signal or restart the user's installed proxy."""

    def setUp(self):
        """
        Copy the launcher into an isolated directory with an unused TCP port.
        """
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        source = Path(__file__).resolve().parents[1]
        for name in ('proxy.py', 'proxyctl.py', 'proxyctl.sh'):
            shutil.copyfile(source / name, self.directory / name)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            self.port = listener.getsockname()[1]
        self.env = {**os.environ, 'HOST': '127.0.0.1', 'PORT': str(self.port),
                    'SERIAL_PORT': 'loop://', 'PYTHON': sys.executable,
                    'ALLOW_CLIENTS': '', 'ALLOW_ORIGINS': '', 'SERIAL_TIMEOUT': '0.05'}
        self.identity = None

    def tearDown(self):
        """
        Restore the test-owned identity, stop that process and remove fixture files.
        """
        if self.identity is not None:
            (self.directory / 'proxy.pid').write_text(json.dumps(self.identity))
            self.run_ctl('stop')
        self.temporary.cleanup()

    def run_ctl(self, action):
        """
        Run the real shell wrapper with bounded execution and captured diagnostics.

        Args:
            action: Launcher command to execute in the fixture directory.

        Returns:
            subprocess.CompletedProcess with captured text output and exit status.

        Raises:
            subprocess.TimeoutExpired: The command exceeds twelve seconds.
        """
        return subprocess.run(['bash', str(self.directory / 'proxyctl.sh'), action],
                              env=self.env, capture_output=True, text=True, timeout=12)

    def start_proxy(self):
        """
        Start only the fixture's loopback proxy and remember its verified identity.

        Returns:
            The saved identity dictionary for the fixture-owned proxy.

        Raises:
            AssertionError: The isolated loopback proxy fails to start.
        """
        result = self.run_ctl('start')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.identity = json.loads((self.directory / 'proxy.pid').read_text())
        return self.identity

    def test_start_status_and_stop(self):
        """
        Start is idempotent, status confirms ownership, and stop ends the fixture process.
        """
        self.start_proxy()
        original = self.identity.copy()
        self.assertEqual(self.run_ctl('status').returncode, 0)
        self.assertEqual(self.run_ctl('start').returncode, 0)
        self.assertEqual(json.loads((self.directory / 'proxy.pid').read_text()), original)
        self.assertEqual(self.run_ctl('stop').returncode, 0)
        self.assertFalse((self.directory / 'proxy.pid').exists())
        self.assertEqual(self.run_ctl('status').returncode, 1)

    def test_stale_lifetime_cannot_signal_process(self):
        """
        A reused PID, previous-boot record or old numeric PID file is never signalled.
        """
        identity = self.start_proxy()
        for stale in [identity['pid'], {**identity, 'start_ticks': '0'},
                      {**identity, 'boot_id': 'previous-boot'}]:
            with self.subTest(stale=stale):
                (self.directory / 'proxy.pid').write_text(json.dumps(stale))
                result = self.run_ctl('stop')
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn('no signal sent', result.stdout)
                (self.directory / 'proxy.pid').write_text(json.dumps(identity))
                self.assertEqual(self.run_ctl('status').returncode, 0)

    def test_unrelated_process_survives_forged_record(self):
        """
        Even matching boot/start metadata cannot authorize stopping another executable.
        """
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        try:
            proc = Path('/proc') / str(child.pid)
            fields = (proc / 'stat').read_text().rpartition(') ')[2].split()
            forged = {'pid': child.pid, 'start_ticks': fields[19],
                      'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
            (self.directory / 'proxy.pid').write_text(json.dumps(forged))
            self.assertEqual(self.run_ctl('stop').returncode, 0)
            self.assertIsNone(child.poll())
        finally:
            child.terminate()
            child.wait(timeout=5)

    def test_invalid_config_does_not_record_running_process(self):
        """
        A proxy that exits during startup must not leave a trusted PID record.
        """
        self.env['SERIAL_TIMEOUT'] = 'nan'
        self.assertEqual(self.run_ctl('start').returncode, 1)
        self.assertFalse((self.directory / 'proxy.pid').exists())


if __name__ == '__main__':
    unittest.main()
