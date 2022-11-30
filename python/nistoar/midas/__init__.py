"""
midas:  A module providing infrastructure support for MIDAS applications.

MIDAS, historically, stands for Manag... Invent... Digital Assets ....  In its first generation,
the system collectively provided a Data Management Plan (DMP) generation tool, an Enterprise Data
Inventory (EDI) record generation tool, reporting functionality, and the generation of the NIST 
Public Data Listing (PDL), the publicly viewable portion of the EDI (which is exported to data.gov).

This module represents a successor implementation of the first generation system.  It notably includes 
implementations of a DMP Authoring service and a Digital Asset Publication (DAP) Authoring service 
(successor to the EDI tool).
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

