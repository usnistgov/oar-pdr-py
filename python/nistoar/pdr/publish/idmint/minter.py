"""
Module providing minters for use with the PDR
"""
import os, logging, re
from collections import Mapping
from copy import deepcopy

from pynoid import __checkdigit as checkdigit

from .registry import IDRegistry, PDRIDRegistry
from nistoar.id.minter import IDMinter, NoidMinter
from nistoar.pdr.constants import ARK_PFX_PAT, ARK_NAAN
from nistoar.pdr.exceptions import StateException

from nistoar.pdr.publish import sys as _sys
syslog = logging.getLogger().getChild(_sys.system_abbrev).getChild(_sys.subsystem_abbrev)

ARK_PFX_RE = re.compile(ARK_PFX_PAT)

__all__ = [ 'PDPMinter' ]

class PDPMinter(IDMinter):
    """
    A minter that is intended to be used with PDR Programmtic Data Publishing systems.  
    """

    def __init__(self, config: Mapping, shldr: str=None, seqstart: int=None, registry: IDRegistry=None):
        """
        create the minter

        :param Mapping config:  the configuration for this minter
        :param str      shldr:  an ARK ID shoulder that this registry is intended provide registration 
                                  for.  When the registry is initialized, only IDs using this shoulder 
                                  will be loaded.  This value will override the ark_shoulder config 
                                  parameter.  If not specified, all known IDs will be loaded
        :param IDRegistry registry:  an ID Registry to use with the minter.  If not provided, a default 
                                will be created from the configuration. 
        :param int    seqstart: the sequence number to start with when minting an ID without an input 
                                  SIP identifier.  If not specified, the value provided by the 
                                  'sequence_start' config parameter will be used, or 1 if not set.  
        """
        if config is None:
            config = {}
        self.cfg = config
        if not shldr:
            shldr = self.cfg.get('id_shoulder')
        if not shldr:
            raise ConfigurationException("Missing required config parameter: id_shoulder")
        self.shldr = shldr
        self.naan = self.cfg.get('naan', ARK_NAAN)

        if not registry:
            regcfg = deepcopy(self.cfg.get('registry', {}))

            regcfg['id_shoulder'] = self.shldr
            regcfg['naan'] = self.naan
            regcfg.setdefault('id_store_file', self.shldr.rstrip("-/.:") + "-issued-ids.tsv")

            pdir = regcfg.setdefault('store_dir', self.cfg.get('store_dir', self.cfg.get('working_dir')))
            if not pdir:
                raise ConfigurationException("PDRMinter Missing required parameter (for %s): store_dir" %
                                             self.shldr)
                
            registry = PDRIDRegistry(regcfg)   # will load all IDs if necessary/so-configured

        self.registry = registry

        if seqstart is None:
            seqstart = self.cfg.get('sequence_start', 1)
            if not isinstance(seqstart, int):
                raise ConfigurationException("sequence_start: not an int: "+str(seqstart))
        if not isinstance(seqstart, int):
            raise TypeError("PDPMinter: seqstart not an int: "+str(seqstart))
        self._seqminter = NoidMinter("zdddd", seqstart)
        self.baseondata = self.cfg.get('based_on_sipid', self.cfg.get('based_on_data', False))

    def mint(self, data=None):
        """
        return a (newly) registered identifier string.  If allowed by the configuration, this 
        will attempt to create an ID based on the input data.  If the generated ID is already
        registered, an exception is raised.
        """
        out = None
        with self.registry.lock:
            if data and self.baseondata:
                out = self.id_for_data(data)
                if out and self.issued(out):
                    raise StateException("Unable to mint %s: already registered" % out)
                    
            while not out:
                out = self._seqminter.mint()
                out = "%s/%s%ss" % (self.naan, self.shldr, out)
                out = "ark:/%s%s" % (out, checkdigit(out))
                if self.issued(out):
                    out = None

            try:
                self.registry.registerID(out, data)
            except ValueError as ex:
                raise StateException(out + ": Failed to register due to possible race condition", cause=ex)

        return out

    def id_for_data(self, data):
        """
        return the ID that would be assigned for the SIP with the given NERDm record data.  A full 
        record is not required; however, None is returned if the data is insufficient for mapping to 
        a unique ID.
        """
        if not self.baseondata:
            return None
        locid = self._make_localkey(data)
        if not locid:
            return None
        return "ark:/%s/%ss%s" % (self.naan, self.shldr, locid)

    def _make_localkey(self, data):
        """
        create a unique key from the given data that should be combined with the ARK ID template
        to create an identifier.  This can be overridden by subclasses to customize the calculation of 
        unique identifiers.  

        This implementation uses the value of the 'sipid' property or returns None if it does not exist.
        """
        if not isinstance(data, Mapping):
            return None
        sipid = ARK_PFX_RE.sub('', data.get('sipid', ''))
        sipid = re.sub(r'^'+self.shldr, '', sipid)
        if not sipid:
            return None
        return sipid

    def issued(self, id):
        return self.registry.registered(id)

    def datafor(self, id):
        return self.registry.get_data(id)

    def _matches(self, constraint, rec):
        if not constraint:
            return True
        if not rec:
            return False
        for k in constraint:
            if k not in rec or constraint[k] != rec[k]:
                return False
        return True

    def search(self, qdata):
        """
        return all ids whose data matches the given query data
        :param Mapping qdata:   a dictionary keys and values must apppear in an ID's data in order to match
        """
        return [k for k in self.registry.iter() if self._matches(qdata, self.registry.get_data(k))]

    def id_for_sipid(self, sipid):
        """
        return the ID that would be assigned for the SIP with the given SIP ID or None if there is 
        not a supported mapping for the ID.  
        """
        return self.id_for_data({"sipid": sipid})
        

            
                                     
        
        
