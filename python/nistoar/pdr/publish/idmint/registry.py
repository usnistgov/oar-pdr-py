"""
An implementation of an IDRegistry that tuned for use with the PDR
"""
import os, logging, json, threading
from collections import OrderedDict, Mapping, ChainMap
from abc import ABCMeta, abstractmethod
from typing import Callable
from copy import deepcopy

from nistoar.id.minter import IDRegistry
from nistoar.pdr.exceptions import StateException
from nistoar.pdr.utils import LockedFile
from nistoar.pdr import ARK_NAAN
from nistoar.pdr.describe import MetadataClient
from nistoar.pdr.exceptions import StateException, ConfigurationException

from nistoar.pdr.publish import sys as _sys
syslog = logging.getLogger().getChild(_sys.system_abbrev).getChild(_sys.subsystem_abbrev)

__all__ = [ 'IDLoader', 'RMMLoader', 'CachingIDRegistry', 'PDRIDRegistry' ]

class IDLoader(object, metaclass=ABCMeta):
    """
    a class that loads a map of identifiers to data into memory.  Implementations control where 
    the IDs are loaded from.
    """
    @abstractmethod
    def iter(self):
        """
        load the identifiers and return them in the form of an iterator over id-data pairs
        """
        return {}.iteritems()

class RMMLoader(IDLoader):
    """
    a class that loads IDs from the PDR's Resource Metadata Manager (RMM)

    This class queries the RMM to extract all known identifiers, but only those matching 
    a configured prefix.  

    The following configurmation parameters are supported:
    :param Mapping metadata_service:  (required) the configuration for setting up the RMM client.
    :param str     metadata_service.service_endpoint:  (required) the base URL for the RMM API service.
    :param str     id_prefrix:  the identifier prefix used to select identifiers from the RMM; this 
                                value can be overridden by an argument provided to the constructor.
    """
    def __init__(self, config: Mapping, prefix: str=None, projection: Callable=None):
        """
        create an IDLoader that loads IDs from records pulled from the RMM.  

        :param Mapping config:  the configuration to set up an RMM client
        :param str     prefix:  an identifier prefix to require when initializing the registry 
                                  with known identifiers.  This is matched against a full identifier, 
                                  so it should include "ark:/..." if applicable.  If not provided,
                                  all identifiers in the RMM will be loaded.
        :param Callable projection:  a function that takes an RMM NERDm record as it sole argument
                                  returns a Mapping representing the data to be associated with the ID.
                                  If not provided, an empty map will be associated.  
        """
        self.cfg = config
        if not prefix:
            prefix = self.cfg.get("id_prefix")
        self.prefix = prefix
        self.project = projection
        if not self.project:
            self.project = lambda r: dict()

        if not self.cfg.get('metadata_service', {}).get('service_endpoint'):
            raise ConfigurationException("Missing required config parameter: "+
                                         "metadata_service.service_endpoint")
        self.cli = MetadataClient(self.cfg['metadata_service']['service_endpoint'])

    def iter(self):
        recs = self.cli.search({'include':'@id','exclude':'_id'})
        for r in recs:
            if self.prefix and not r.get('@id', '').startswith(self.prefix):
                continue
            if r.get('@id'):
                yield (r['@id'], self.project(r))
            
class CachingIDRegistry(IDRegistry):
    """
    a registry that persists its information to disk and initialized from the full set of IDs 
    published to the PDR.  

    The persisted registry is a file....

    This class can take a configuration dictionary on construction; the following parameters are 
    supported:
    :param str      id_store_file:  the name to give to the file where IDs are persisted 
                                      (default: 'issued-ids.xxxx')
    :param bool cache_on_register:  if True default), each newly registered ID is persisted 
                                      immediated upon call to registerID(); if False, the IDs will 
                                      only be persisted with a call to cache_data().
    :param dict repo_access:        a dictionary providing the details for accessing the PDR
                                      services, including the metadata service for determining the
                                      list of currently issued IDs.  
    """

    def __init__(self, parentdir: str=None, config: Mapping=None, initloader: IDLoader=None, name: str=None):
        """
        create an IDRegistry.  If registry store file does not exist will be created and loaded 
        with IDs currently known to the PDR (unless initcache=False).  
        :param str  parentdir:  a directory where the persistent registry can be saved; if not 
                                  provided, the registry will not be persisted.
        :param Mapping config:  the configuration for this registry
        :param str       name:  a name to give to this collection of registered identifiers; if 
                                  not provided, a default will be determined from idprefix value.
        :raise StateException:  if the given parent directory is not an existing directory
        """
        if parentdir and not os.path.isdir(parentdir):
            raise StateException("%s: Not an existing dictionary")

        if not config:
            config = {}
        if not isinstance(config, Mapping):
            raise TypeError("Configuration not a dictionary: " + str(type(config)))
        self.cfg = config
        self.cache_immediately = self.cfg.get('cache_on_register', True)

        self.cached = {}
        self.uncached = OrderedDict()
        self.data = ChainMap(self.uncached, self.cached)
        self.name = name

        nm = type(self).__name__
        if self.name:
            nm += ":" + self.name
        self.log = syslog.getChild(nm)
        self.lock = threading.RLock()

        # set up the registry disk storage
        self.store = None
        if parentdir:
            defnm = "issued-ids.tsv"
            if self.name:
                defnm = "%s-%s" % (self.name, defnm)
            self.store = os.path.join(parentdir, self.cfg.get('id_store_file', defnm))

        if self.store and os.path.exists(self.store):
            self.reload_data()
            if not self.data:
                self.log.warning("empty registry persistance restored")
        elif initloader:
            self.init_cache(initloader)

    def reload_data(self):
        """
        load the contents of the registry from its persisted store into memory
        """
        with self.lock:
            with LockedFile(self.store) as fd:
                self.cached = self._parse(fd)
            self.data = ChainMap(self.uncached, self.cached)

    def _parse(self, fstrm):
        # format is a two-column TSV file where the first column is the registered ID and the
        # second is the associated data
        out = {}
        for line in fstrm:
            (k, v) = line.strip().split("\t", 1)
            try:
                v = json.loads(v)
            except:
                pass
            out[k] = v
        return out

    def cache_data(self):
        """
        cache any pending registered IDs to disk
        """
        if not self.uncached:
            self.log.debug("No uncached identifiers detected")
        with LockedFile(self.store, 'a') as fd:
            for key in list(self.uncached.keys()):
                self._serialize(fd, key, self.uncached[key])
                self.cached[key] = self.uncached[key]
                del self.uncached[key]

    def _serialize(self, fd, key, val):
        # format is a two-column TSV file where the first column is the registered ID and the
        # second is the associated data
        fd.write(key)
        fd.write("\t")
        fd.write(json.dumps(self.uncached[key]))
        fd.write("\n")

    def init_cache(self, idloader: IDLoader = None) -> int:
        """
        initialize the internal cache with all identifiers provided by the given loader.  
        Note that this will override the data with any already-registered IDs
        :return: the number of matching IDs loaded
        :raises RMMServerError:   if the remote service is unavailable or has an unexpected error
        """
        if not idloader:
            idloader = self.initloader
        if not idloader:
            return 0

        with self.lock:
            n = len(self.uncached)
            self.uncached.update(idloader.iter())
            n = len(self.uncached) - n
            if self.store and self.cache_immediately:
                self.cache_data()
            return n

    def registerID(self, id, data=None):
        """
        register the given ID to reserve it from further use

        :param id str:     the ID to be reserved
        :param data dict:  any data to store with the identifier.
        :raises ValueError:  if the id has already exists in storage.
        """
        with self.lock:
            if id in self.data:
                raise ValueError("id is already registerd: " + id)
            if data is None:
                data = {}
            self.uncached[id] = data
            if self.cache_immediately:
                self.cache_data()
                
    def get_data(self, id):
        """
        return the data for a given ID or none if it is not registered

        :param str id:  the identifier of interest
        """
        return self.data.get(id)

    def registered(self, id):
        """
        return true if the given ID has already been registered

        :param str id:  the identifier string to check
        """
        return id in self.data

    def iter(self):
        """
        return an iterator for the set of registered IDs
        """
        return self.data.keys()

class PDRIDRegistry(CachingIDRegistry):
    """
    An IDRegistry that can initialize itself with the IDs currently assigned within the PDR.

    
    """

    def __init__(self, config, parentdir=None, idshldr=None, initcache=True):
        """
        create the IDRegistry.  If registry store file does not exist will be created and loaded 
        with IDs currently known to the PDR (unless initcache=False).  The IDs loaded can be 
        limited to those ARK IDs matching a shoulder prefix.  

        :param Mapping config:  the configuration for this registry
        :param str  parentdir:  a directory where the persistent registry can be saved; if not 
                                  provided, the registry will not be persisted.
        :param str    idshldr:  an ARK ID shoulder that this registry is intended provide registration 
                                  for.  When the registry is initialized, only IDs using this shoulder 
                                  will be loaded.  This value will override the ark_shoulder config 
                                  parameter.  If not specified, all known IDs will be loaded
        :param bool initcache:  if True (default) and is necessary, load all identifiers known to the 
                                  PDR matching the idshldr into the registry's cache.  If False, loading 
                                  will be delayed until init_cache() is called.  
        :raise StateException:  if the given parent directory is not an existing directory
        """
        if not parentdir:
            parentdir = config.get('store_dir')
        if not idshldr:
            idshldr = config.get('id_shoulder')
        prefix = None
        if idshldr:
            prefix = "ark:/%s/%s-" % (config.get('naan', ARK_NAAN), idshldr)

        def projector(md):
            out = {}
            if 'pdr:sipid' in md:
                out['sipid'] = md['pdr:sipid']
            if 'pdr:aipid' in md:
                out['aipid'] = md['pdr:aipid']
            if 'ediid' in md:
                out['ediid'] = md['ediid']
            return out

        initloader = None
        if 'metadata_service' in config:
            initloader = RMMLoader(config, prefix, projector)
        ldr = (initcache and initloader) or None

        super(PDRIDRegistry, self).__init__(parentdir, config, ldr, idshldr.rstrip("-/.:"))
        self.initloader = initloader

    def init_cache(self, idloader: IDLoader = None) -> int:
        """
        initialize the internal cache with all identifiers provided by the given loader.  
        Note that this will override the data with any already-registered IDs.  
        :return: the number of matching IDs loaded
        :raises RMMServerError:   if the remote service is unavailable or has an unexpected error
        """
        if not idloader:
            idloader = self.initloader
        if not idloader:
            return 0

        return super(PDRIDRegistry, self).init_cache(idloader)

