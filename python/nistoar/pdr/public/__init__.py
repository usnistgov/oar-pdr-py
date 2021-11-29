"""
A module providing services and capabilities for the public side of the Public Data Repository
"""
from ..exceptions import *
from ... import pdr as _pdr

_SUBSYSNAME = "Services"
_SUBSYSABBREV = "services"

class PublicServicesSystem(_pdr.PDRSystem):
    """
    a SystemInfoMixin providing static information about the Preservation system
    """
    def __init__(self):
        super(PublicServicesSystem, self).__init__(_SUBSYSNAME, _SUBSYSABBREV)

system = PublicServicesSystem()

