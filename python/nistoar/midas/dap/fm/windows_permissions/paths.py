"""
Build Windows relative paths for Windows Permissions Manager commands.
"""

from pathlib import PureWindowsPath

from .errors import WindowsPermissionsError
from .validation import clean


def windows_path(relative_path):
    relative_path = clean("path", relative_path)
    if "/" in relative_path and "\\" in relative_path:
        raise WindowsPermissionsError("path: mixed path separators are not allowed")

    path = PureWindowsPath(relative_path)
    if path.drive or path.root:
        raise WindowsPermissionsError("path: absolute paths are not allowed")

    clean_parts = []
    for part in path.parts:
        if part in ("", ".", ".."):
            raise WindowsPermissionsError("path: empty, current, or parent parts are not allowed")
        clean_parts.append(clean("path component", part))

    return "\\".join(clean_parts)


def join_windows_paths(prefix, relative_path):
    prefix = windows_path(prefix)
    relative_path = windows_path(relative_path)
    return prefix + "\\" + relative_path
