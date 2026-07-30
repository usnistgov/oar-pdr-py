"""
Exceptions for the Windows permissions batch-file integration.
"""

from nistoar.midas.dap.fm.exceptions import FileManagerException


class WindowsPermissionsError(FileManagerException):
    pass


class WindowsPermissionsAppendError(WindowsPermissionsError):
    pass
