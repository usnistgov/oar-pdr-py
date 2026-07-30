"""
MIDAS permission mapping and command formatting.
"""

from .errors import WindowsPermissionsError
from .validation import clean


PERMISSION_RIGHTS = {
    "Read": ["Read"],
    "Write": ["Read", "Write"],
    "Delete": ["Read", "Write", "Delete", "DeleteSubdirectoriesAndFiles"],
    "Share": ["Read", "Write", "Delete", "DeleteSubdirectoriesAndFiles"],
    "All": ["FullControl"],
}


def rights_for(permission_name, is_owner=False):
    if is_owner:
        return ["FullControl"]
    if permission_name == "None":
        raise WindowsPermissionsError(
            "Permission removal is not supported by the current Windows Permissions Manager protocol"
        )
    if permission_name not in PERMISSION_RIGHTS:
        raise WindowsPermissionsError("permission: unsupported MIDAS permission: " + str(permission_name))
    return PERMISSION_RIGHTS[permission_name]


def format_setpermissions(target_path, principal, rights):
    return "setpermissions %s %s %s Allow" % (
        clean("path", target_path),
        clean("principal", principal),
        ",".join(rights),
    )
