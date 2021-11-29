"""
A module providing services and capabilities for the public side of the Public Data Repository
"""
from ...exceptions import *
from .... import pdr as _pdr

_SUBSYSNAME = "Resolver"
_SUBSYSABBREV = "resolver"

class ResolverSystem(_pdr.PDRSystem):
    """
    a SystemInfoMixin providing static information about the Preservation system
    """
    def __init__(self):
        super(ResolverSystem, self).__init__(_SUBSYSNAME, _SUBSYSABBREV)

system = ResolverSystem()

