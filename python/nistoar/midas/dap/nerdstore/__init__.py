"""
Interfaces and implementations to NERDm metadata storage.  

The drafting service collects bits of NERDm metadata that describes the SIP which it must 
persist as part of the draft SIP.  This package defines an abstract interface for pulling 
the metadata from storage and into memory, enabling new metadata to be merge in, and storing 
the result.  
"""
from collections.abc import Mapping
from logging import Logger

from .base import *
from nistoar.pdr.exceptions import ConfigurationException, StateException

from . import inmem
from . import fsbased
from . import fmfs

_def_store_map = {
    "inmem":    inmem.InMemoryResourceStorage,
    "fsbased":  fsbased.FSBasedResourceStorage,
    "fmfs":     fmfs.FMFSResourceStorage
}

class NERDResourceStorageFactory:
    """
    a factory class for creating :py:class:`~nistoar.midas.dap.nerdstore.NERDResourceStorage` instances.

    """

    def __init__(self, storemap: Mapping=None):
        if not storemap:
            storemap = _def_store_map
        self._byname = storemap

    def open_storage(self, config: Mapping, logger: Logger, implname: str=None) -> NERDResourceStorage:
        if not implname:
            implname = config.get("type")
        if not implname:
            raise ConfigurationException("Missing required configuration parameter: type")

        if implname not in self._byname:
            raise StateException("Unrecognized nerdstore implementation type: "+implname)

        return self._byname[implname].from_config(config, logger)

