"""
This module provides the base interface and implementation for the SIPBagger
infrastructure.  At the center is the SIPBagger class that serves as an 
abstract base for subclasses that understand different input sources.
"""
import os, json, filelock
from collections import OrderedDict, Mapping
from abc import ABCMeta, abstractmethod, abstractproperty
from copy import deepcopy

from .. import PublishSystem, system
from .. import (PublishException, PublishingStateException, StateException)
from ...utils import read_nerd, read_pod, read_json, write_json
from ...preserve.bagit.builder import checksum_of
from ....base.config import merge_config
from ..prov import PubAgent, Action

def moddate_of(filepath):
    """
    return a file's modification time (as a float)
    """
    return os.stat(filepath).st_mtime

class SIPBagger(PublishSystem, metaclass=ABCMeta):
    """
    This class will prepare an SIP organized in a particular form 
    by re-organizing its contents into a working bag.  Subclasses adapt 
    different SIP formats.  This abstract class provides common code.  

    SIPBagger implementations should be written to be indepodent: running 
    it mutliple times on the same inputs and outputs should result
    in the same end state.  That is, if run a second time and nothing is 
    different in the inputs, nothing changes in the output bag.
    If a file is added to the inputs and the prepper is rerun, that
    new file will get added to the output directory.  

    SIPBagger implementations make use of a configuration dictionary to 
    control its behavior.  Most of the supported properties are defined by 
    the specific implementation class; however, this base class supports the 
    following properties:

    :prop relative_to_indir bool (False):  If True, the output bag directory 
       is expected to be under one of the input directories; this base class
       will then ensure that it has write permission to create the output 
       directory.  If False, the bagger may raise an exception if the 
       requested output bag directory is found within an input SIP directory,
       regardless of whether the process has permission to write there.  
    """
    BGRMD_FILENAME = "__bagger.json"   # default bagger metadata file; may be overridden

    def __init__(self, outdir, config):
        """
        initialize the class by setting its configuration and the 
        output working directory where the root bag directory can be created.  
        """
        super(SIPBagger, self).__init__()
        self.bagparent = outdir
        self.cfg = config
        self.lock = None
        self.isrevision = False

    @abstractproperty
    def bagdir(self):
        """
        The path to the output bag directory.
        """
        raise NotImplementedError()

    def ensure_bag_parent_dir(self):
        """
        Ensure that the directory where the bag is/will be located exists.

        This implementation requires that this directory already exist, but a sub-class can override 
        this method to create the directory under certain circumstances.  
        """
        if not os.path.exists(self.bagparent):
            raise PublishingStateError("Bag Workspace dir does not exist: " + self.bagparent)
                                       

    @abstractmethod
    def ensure_preparation(self, nodata: bool=False, who: PubAgent=None, _action: Action=None) -> None:
        """
        create and update the output working bag directory to ensure it is 
        a re-organized version of the SIP, ready for annotation.

        :param bool nodata:  if True, do not copy (or link) data files to the
                             output directory.
        :param PubAgent who: an actor identifier object, indicating who is requesting this action.  This 
                             will get recorded in the history data.  If None, an internal administrative 
                             identity will be assumed.  This identity may affect the identifier assigned.
        :param Action _action:  Intended primarily for internal use; if provided, any provence actions 
                             that should be recorded within this function should be added as a subaction
                             of this given one rather than recorded directly as a stand-alone action.
        """
        raise NotImplementedError()

    def ensure_filelock(self):
        """
        if necessary, create a file lock object that can be used to prevent 
        multiple processes from trying to bag the same SIP simultaneously.
        The lock object is saved to self.lock.  
        """
        if not self.lock:
            lockfile = self.bagdir + ".lock"
            self.ensure_bag_parent_dir()
            self.lock = filelock.FileLock(lockfile)

    def prepare(self, nodata=False, who=None, lock=True, _action: Action=None):
        """
        initialize the output working bag directory by calling 
        ensure_preparation().  This operation is wrapped in the acquisition
        of a file lock to prevent multiple processes from 

        :param nodata bool: if True, do not copy (or link) data files to the
                            output directory.
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :param lock bool:   if True (default), acquire a lock before executing
                            the preparation.
        """
        if lock:
            self.ensure_filelock()
            with self.lock:
                self.ensure_preparation(nodata, who, _action)

        else:
            self.ensure_preparation(nodata, who, _action)

    def finalize(self, who: PubAgent=None, lock=True, _action: Action=None):
        """
        Based on the current state of the bag, finalize its contents to a complete state according to 
        the conventions of this bagger implementation.  After a successful call, the bag should be in 
        a preservable state.
        :param PubAgent who: an actor identifier object, indicating who is requesting this action.  This 
                             will get recorded in the history data.  If None, an internal administrative 
                             identity will be assumed.  This identity may affect the identifier assigned.
        :param bool lock:    if True (default), acquire a lock before executing
                             the preparation.
        :param Action _action:  Intended primarily for internal use; if provided, any provence actions 
                             that should be recorded within this function should be added as a subaction
                             of this given one rather than recorded directly as a stand-alone action.
        """
        if lock:
            self.ensure_filelock()
            with self.lock:
                self.ensure_finalize(who, _action)

        else:
            self.ensure_finalize(who, _action)

    @abstractmethod
    def ensure_finalize(self, who: PubAgent=None, _action: Action=None):
        """
        Based on the current state of the bag, finalize its contents to a complete state according to 
        the conventions of this bagger implementation.  After a successful call, the bag should be in 
        a preservable state.
        :param PubAgent who:  an actor identifier object, indicating who is requesting this action.  This 
                              will get recorded in the history data.  If None, an internal administrative 
                              identity will be assumed.  This identity may affect the identifier assigned.
        :param Action _action:  Intended primarily for internal use; if provided, any provence actions 
                             that should be recorded within this function should be added as a subaction
                             of this given one rather than recorded directly as a stand-alone action.
        """
        raise NotImplementedError()

    def baggermd_file_for(self, destpath):
        """
        return the full path within the bag for bagger metadata file for the 
        given component filepath 

        Bagger metadata is a metadata that an SIPBagger may temporarily cache 
        into files within the bag while building it up.  It is expected that 
        the files will be removed during the finalization phase.
        """
        return os.path.join(self.bagdir,"metadata",destpath,self.BGRMD_FILENAME)

    def baggermd_for(self, destpath):
        """
        return the bagger-specific metadata associated with the particular 
        component.  Resource-level metadata can be updated by providing an empty
        string as the component filepath.  
        """
        mdfile = self.baggermd_file_for(destpath)
        if os.path.exists(mdfile):
            return read_json(mdfile)
        return OrderedDict()

    def update_bagger_metadata_for(self, destpath, mdata):
        """
        update the bagger-specific metadata.  

        (Note that this metadata is expected to be removed from the bag during 
        the finalization phase.)

        Resource-level metadata can be updated by providing an empty
        string as the component filepath.  The given metadata will be 
        merged with the currently saved metadata.  If there are no metadata
        yet saved for the filepath, the given metadata will be merged 
        with default metadata.

        When the metadata is merged, note that whole array values will be 
        replaced with corresponding arrays from the input metadata; the 
        arrays are not combined in any way.
        
        :param str filepath:   the filepath to the component to update.  An
                               empty string ("") updates the resource-level
                               metadata.  
        :param dict   mdata:   the new metadata to merge in
        """
        mdfile = self.baggermd_file_for(destpath)
        if os.path.exists(mdfile):
            out = read_json(mdfile)
        else:
            out = OrderedDict()

        out = self._update_md(out, mdata)
        write_json(out, mdfile)
        return out

    def _update_md(self, orig, updates):
        # update the values of orig with the values in updates
        # this uses the same algorithm as used to merge config data
        return merge_config(updates, orig)

    @abstractmethod
    def delete(self):
        """
        delete the working bag from store; this sets the bagger to a virgin state.
        """
        raise NotImplementedError()
    

class SIPBaggerFactory(PublishSystem, metaclass=ABCMeta):
    """
    a factory class for instantiating SIPBaggers.  The factory can be implemented to provide baggers that 
    support one bagging convention/type or multiple ones; the supports() method reveals if a particular 
    convention is supported.
    """
    def __init__(self, config=None):
        """
        initialize the factory.  This version saves the configuration (if provided) but does not specify
        what configuration is expected.
        """
        PublishSystem.__init__(self)
        self.cfg = config

    @abstractmethod
    def supports(self, siptype: str) -> bool:
        """
        return True if this factory can instantiate an SIPBagger that supports the given convention 
        or False, otherwise.  
        :rtype: bool
        """
        return False

    @abstractmethod
    def create(self, sipid, siptype: str, config: Mapping=None, minter=None) -> SIPBagger:
        """
        create a new instantiation of an SIPBagger that can process an SIP of the given type.  If config
        is provided, it may get merged in some way with the configuration set at construction time before
        being applied to the bagger.

        :param           sipid:  the ID for the SIP to create a bagger for; this is usually a str, 
                                 subclasses may support more complicated ID types.
        :param str     siptype:  the name given to the SIP convention supported by the SIP reference by sipid
        :param Mapping  config:  bagger configuration parameters that should override the default
        :param IDMinter minter:  an IDMinter instance that should be used to mint a new PDR-ID
        """
        raise NotImplementedError()

class BaseSIPBaggerFactory(SIPBaggerFactory):
    """
    This is a base implementation of the SIPBaggerFactory that adds the following assumptions beyond 
    SIPBaggerFactory:  (1) the configuration follows the multi-SIP configuration schema (described below), 
    (2) SIP identifiers are strings, and (3) that all SIPBagger implementations support the same constructor 
    signature.

    Configuration Schema:
    """

    def __init__(self, config=None, workdir=None):
        """
        initialize the factory.  

        Subclasses should override this constructor to configure the specific SIP types this factory 
        will provide baggers for.  

        :param Mapping config:  the factory's configuration from which it will derive the default 
                                configuration for the SIPBaggers produced.
        :param str    workdir:  the default base working directory for the output baggers; this overrides
                                the 'working_dir' value provided in the given config, but a 'working_dir'
                                value provided to the create() method will override this one.  This 
                                directory must exist.
        """
        if workdir:
            if not os.path.isdir(workdir):
                raise StateException("Requested working directory does not exist (as a directory): " +
                                     workdir)
            config = deepcopy(config)
            config['working_dir'] = workdir
        super(BaseSIPBaggerFactory, self).__init__(config)

        self._bgrcls = {}

    def supports(self, siptype: str) -> bool:
        """
        return True if this factory can instantiate an SIPBagger that supports the given convention 
        or False, otherwise.  
        :rtype: bool
        """
        return siptype in self._bgrcls

    def create(self, sipid: str, siptype: str, config: Mapping=None, minter=None) -> SIPBagger:
        """
        create a new instantiation of an SIPBagger that can process an SIP of the given type.  If provided,
        config will be merged with the default configuration provided by this factory, overriding the 
        defaults.  In particular, this factory will first derive a bagger configuration for the bagger from 
        its factory configuration; next, it will update the configuration parameters with values provided 
        by the given config parameter.  

        :param str       sipid:  the ID for the SIP to create a bagger for; this is usually a str, 
                                 subclasses may support more complicated ID types.
        :param str     siptype:  the name given to the SIP convention supported by the SIP reference 
                                 by sipid
        :param Mapping  config:  bagger configuration parameters that should override the default
        :param IDMinter minter:  an IDMinter instance that should be used to mint a new PDR-ID; if a 
                                 registered SIPBagger class's constructor does not accept a minter 
                                 argument, the constructor will be called without one.  
        """
        if not self.supports(siptype):
            raise PublishException("Factory does not support this SIP type: "+siptype, sys=self)
        outcfg = merge_config(self.derive_config(siptype), config)

        cls = None
        try:
            cls = self._bgrcls[siptype]
        except KeyError as ex:
            raise PublishException("No SIPBagger class specified for siptype="+siptype, sys=self)

        try:
            return cls(sipid, outcfg, minter=minter)
        except TypeError as ex:
            if "unexpected keyword argument 'minter'" in str(ex):
                return cls(sipid, outcfg)
            raise


    
        



