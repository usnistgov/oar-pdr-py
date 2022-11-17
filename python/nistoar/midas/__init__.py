"""
midas:  A module providing infrastructure support for MIDAS applications.
"""
from nistoar.base import OARException, SystemInfoMixin, config

try:
    from .version import __version__
except ImportError:
    __version__ = "(unset)"

_MIDASSYSNAME = "MIDAS"
_MIDASSYSABBREV = "MIDAS"

class MIDASSystem(SystemInfoMixin):
    """
    A SystemInfoMixin representing the overall PDR system.
    """
    def __init__(self, subsysname="", subsysabbrev=""):
        super(MIDASSystem, self).__init__(_MIDASSYSNAME, _MIDASSYSABBREV,
                                          subsysname, subsysabbrev, __version__)

system = MIDASSystem()
    
class MIDASException(OARException):
    """
    An general base class for exceptions that occur while using MIDAS infrastructure or applications
    """
    pass

