"""
Small validation helpers for command values and paths.
"""

from .errors import WindowsPermissionsError


def clean(name, value):
    if value is None:
        raise WindowsPermissionsError(name + ": value is required")
    value = str(value)
    if not value or value != value.strip():
        raise WindowsPermissionsError(name + ": value is empty or has leading/trailing whitespace")
    if "\r" in value or "\n" in value or "\t" in value:
        raise WindowsPermissionsError(name + ": newline and tab characters are not allowed")
    if any(ch.isspace() for ch in value):
        raise WindowsPermissionsError(name + ": spaces are not supported by the batch command protocol")
    return value


def is_under(path, parent):
    try:
        path.resolve(strict=False).relative_to(parent.resolve(strict=False))
        return True
    except ValueError:
        return False
