"""
Windows Permissions Manager batch-file integration for MIDAS DAP file spaces.
"""

from .errors import WindowsPermissionsAppendError, WindowsPermissionsError
from .integration import DEFAULT_BATCH_FILE_PATH, WindowsPermissionsIntegration
