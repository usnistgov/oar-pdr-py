"""
Build Windows target paths from MIDAS file-manager storage paths.
"""

from pathlib import PureWindowsPath

from nistoar.base.config import ConfigurationException

from .errors import WindowsPermissionsError
from .validation import clean


def windows_root(value):
    root = PureWindowsPath(clean("windows_target_root", value))
    if not root.is_absolute():
        raise ConfigurationException("windows_permissions.windows_target_root: must be an absolute Windows path")
    return root


def windows_path(root, storage_relative_path):
    relpath = clean("storage_relative_path", storage_relative_path)
    if "/" in relpath and "\\" in relpath:
        raise WindowsPermissionsError("storage_relative_path: mixed path separators are not allowed")

    relpath = PureWindowsPath(relpath)
    if relpath.drive or relpath.root:
        raise WindowsPermissionsError("storage_relative_path: absolute paths are not allowed")

    parts = relpath.parts
    if any(part in ("", ".", "..") for part in parts):
        raise WindowsPermissionsError("storage_relative_path: empty, current, or parent parts are not allowed")

    parts = [clean("storage_relative_path component", part) for part in parts]
    return str(root.joinpath(*parts))
