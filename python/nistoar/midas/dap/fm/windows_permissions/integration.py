"""
High-level integration used by the MIDAS file-manager service.
"""

import logging
from collections.abc import Mapping
from pathlib import Path

from nistoar.base.config import ConfigurationException

from .commands import format_setpermissions, rights_for
from .errors import WindowsPermissionsAppendError
from .paths import join_windows_paths, windows_path
from .validation import is_under
from .writer import append_line, check_append_access


class WindowsPermissionsIntegration:

    def __init__(self, batch_file_path, path_prefix, log=None):
        self.batch_file_path = Path(batch_file_path)
        self.path_prefix = windows_path(path_prefix)
        self.log = log or logging.getLogger("windows-permissions")

    @classmethod
    def from_config(cls, config=None, log=None, local_storage_root_dir=None):
        if config is None:
            raise ConfigurationException("windows_permissions: configuration is required")
        if not isinstance(config, Mapping):
            raise ConfigurationException("windows_permissions: configuration must be an object")

        missing = [name for name in ("batch_file_path", "path_prefix") if not config.get(name)]
        if missing:
            raise ConfigurationException(
                "windows_permissions: missing required config parameter(s): " + ", ".join(missing)
            )

        batch_file_path = Path(config["batch_file_path"])
        validate_batch_file_path(batch_file_path, config, local_storage_root_dir)
        check_append_access(batch_file_path)

        return cls(batch_file_path, config["path_prefix"], log)

    def set_record_permission(self, record_id, storage_relative_path, principal,
                              permission_name, is_owner=False, event_type="permission_update"):
        rights = rights_for(permission_name, is_owner)
        target_path = join_windows_paths(self.path_prefix, storage_relative_path)
        command = format_setpermissions(target_path, principal, rights)

        self.log.info(
            "Appending Windows permission command: record_id=%s event_type=%s principal=%s "
            "target_path=%s rights=%s control_type=Allow",
            record_id,
            event_type,
            principal,
            target_path,
            ",".join(rights),
        )

        try:
            append_line(self.batch_file_path, command)
        except OSError as ex:
            self.log.error(
                "Failed to append Windows permission command: record_id=%s event_type=%s "
                "principal=%s target_path=%s rights=%s control_type=Allow batch_file_path=%s error=%s",
                record_id,
                event_type,
                principal,
                target_path,
                ",".join(rights),
                self.batch_file_path,
                str(ex),
            )
            raise WindowsPermissionsAppendError(
                "Unable to append Windows permission command to %s: %s" %
                (self.batch_file_path, str(ex))
            ) from ex

        return command


def validate_batch_file_path(batch_file_path, config, local_storage_root_dir=None):
    if batch_file_path.suffix.lower() != ".txt":
        raise ConfigurationException("windows_permissions.batch_file_path: file must use a .txt extension")
    if not batch_file_path.parent.is_dir():
        raise ConfigurationException(
            "windows_permissions.batch_file_path: parent directory does not exist: " +
            str(batch_file_path.parent)
        )
    if batch_file_path.is_dir():
        raise ConfigurationException(
            "windows_permissions.batch_file_path: path points to a directory: " + str(batch_file_path)
        )
    if any(part.lower() == "sys" for part in batch_file_path.parts):
        raise ConfigurationException(
            "windows_permissions.batch_file_path: path must not be under a Nextcloud sys directory"
        )

    protected_dirs = []
    if local_storage_root_dir:
        protected_dirs.append(("local_storage_root_dir", Path(local_storage_root_dir)))
    if config.get("nextcloud_data_dir"):
        protected_dirs.append(("nextcloud_data_dir", Path(config["nextcloud_data_dir"])))

    for name, protected_dir in protected_dirs:
        if is_under(batch_file_path, protected_dir):
            raise ConfigurationException("windows_permissions.batch_file_path: path must not be under " + name)
