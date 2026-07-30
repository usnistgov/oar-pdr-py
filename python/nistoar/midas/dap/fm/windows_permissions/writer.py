"""
Append commands to the Windows Permissions Manager batch file.
"""

import os
import fcntl

from nistoar.base.config import ConfigurationException


LINE_ENDING = "\r\n"
ENCODING = "utf-8"


def check_append_access(batch_file_path):
    try:
        with open(batch_file_path, "ab"):
            pass
    except OSError as ex:
        raise ConfigurationException(
            "windows_permissions.batch_file_path: unable to append to file: " + str(ex)
        ) from ex


def append_line(batch_file_path, command):
    with open(batch_file_path, "ab") as out:
        fcntl.flock(out.fileno(), fcntl.LOCK_EX)
        out.write((command + LINE_ENDING).encode(ENCODING))
        out.flush()
        os.fsync(out.fileno())
        fcntl.flock(out.fileno(), fcntl.LOCK_UN)
