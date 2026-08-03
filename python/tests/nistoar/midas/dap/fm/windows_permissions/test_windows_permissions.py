import os
import shutil
import tempfile
import unittest as test
from pathlib import Path

from nistoar.base.config import ConfigurationException
from nistoar.midas.dap.fm.windows_permissions import (
    WindowsPermissionsAppendError,
    WindowsPermissionsError,
    WindowsPermissionsIntegration,
)


PATH_PREFIX = "midastest_uploads/mdsdev/nextcloud/data/oar_api/files"
WINDOWS_PATH_PREFIX = r"midastest_uploads\mdsdev\nextcloud\data\oar_api\files"
WINDOWS_PATH_PREFIX_BYTES = b"midastest_uploads\\mdsdev\\nextcloud\\data\\oar_api\\files"


class WindowsPermissionsIntegrationTest(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="_test_winperm.")
        self.winperm_dir = Path(self.tmpdir) / "winperm"
        os.mkdir(self.winperm_dir)
        self.batch_file = self.winperm_dir / "permissions_batch.txt"

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _integration(self):
        return WindowsPermissionsIntegration.from_config({
            "batch_file_path": str(self.batch_file),
            "path_prefix": PATH_PREFIX,
        })

    def test_config_is_required(self):
        with self.assertRaises(ConfigurationException):
            WindowsPermissionsIntegration.from_config()
        with self.assertRaises(ConfigurationException):
            WindowsPermissionsIntegration.from_config({})

    def test_owner_command_writes_shared_drive_relative_path(self):
        integ = self._integration()
        command = integ.set_record_permission(
            "mds3:0000",
            "mds3:0000/mds3:0000",
            "ava1",
            "All",
            is_owner=True,
            event_type="record_creation",
        )

        self.assertEqual(
            command,
            rf"setpermissions {WINDOWS_PATH_PREFIX}\mds3:0000\mds3:0000 ava1 FullControl Allow",
        )
        with open(self.batch_file, "rb") as fd:
            self.assertEqual(
                fd.read(),
                b"setpermissions " + WINDOWS_PATH_PREFIX_BYTES +
                b"\\mds3:0000\\mds3:0000 ava1 FullControl Allow\r\n",
            )

    def test_permission_update_appends_after_existing_commands(self):
        with open(self.batch_file, "wb") as fd:
            fd.write(b"setpermissions " + WINDOWS_PATH_PREFIX_BYTES + b"\\old alice Read Allow\r\n")

        integ = self._integration()
        integ.set_record_permission("rec1", "rec1/rec1", "bob", "Write")

        with open(self.batch_file, "rb") as fd:
            self.assertEqual(
                fd.read(),
                b"setpermissions " + WINDOWS_PATH_PREFIX_BYTES + b"\\old alice Read Allow\r\n"
                b"setpermissions " + WINDOWS_PATH_PREFIX_BYTES + b"\\rec1\\rec1 bob Read,Write Allow\r\n",
            )

    def test_config_validation(self):
        with self.assertRaises(ConfigurationException):
            WindowsPermissionsIntegration.from_config({
                "batch_file_path": str(self.batch_file),
            })

        with self.assertRaises(ConfigurationException):
            WindowsPermissionsIntegration.from_config({
                "batch_file_path": str(self.winperm_dir / "permissions_batch.log"),
                "path_prefix": PATH_PREFIX,
            })

        with self.assertRaises(ConfigurationException):
            WindowsPermissionsIntegration.from_config({
                "batch_file_path": str(Path(self.tmpdir) / "missing" / "permissions_batch.txt"),
                "path_prefix": PATH_PREFIX,
            })

        sysdir = Path(self.tmpdir) / "sys"
        os.mkdir(sysdir)
        with self.assertRaises(ConfigurationException):
            WindowsPermissionsIntegration.from_config({
                "batch_file_path": str(sysdir / "permissions_batch.txt"),
                "path_prefix": PATH_PREFIX,
            })

        datadir = Path(self.tmpdir) / "data"
        os.mkdir(datadir)
        with self.assertRaises(ConfigurationException):
            WindowsPermissionsIntegration.from_config({
                "batch_file_path": str(datadir / "permissions_batch.txt"),
                "path_prefix": PATH_PREFIX,
            }, local_storage_root_dir=datadir)

    def test_rejects_values_that_cannot_be_safely_written_as_one_command(self):
        integ = self._integration()
        bad_calls = [
            lambda: integ.set_record_permission("rec1", "../rec1", "alice", "Read"),
            lambda: integ.set_record_permission("rec1", r"C:\Other\rec1", "alice", "Read"),
            lambda: integ.set_record_permission("rec1", "rec1/one\nsetpermissions two", "alice", "Read"),
            lambda: integ.set_record_permission("rec1", "rec1/rec1", "bad user", "Read"),
            lambda: integ.set_record_permission("rec1", "rec1/rec1", "alice", "None"),
            lambda: integ.set_record_permission("rec1", "rec1/rec1", "alice", "Bogus"),
        ]
        for bad_call in bad_calls:
            with self.assertRaises(WindowsPermissionsError):
                bad_call()

    def test_append_failure_is_propagated(self):
        integ = self._integration()
        os.remove(self.batch_file)
        os.mkdir(self.batch_file)

        with self.assertLogs("windows-permissions", level="ERROR"):
            with self.assertRaises(WindowsPermissionsAppendError):
                integ.set_record_permission("rec1", "rec1/rec1", "alice", "Read")

if __name__ == "__main__":
    test.main()
