"""
This module provides a base implementations of the SIPBagger interface to support PDR's Programmatic Data 
Publishing (PDP) API.  In this framework, SIP inputs are primarily in the form of NERDm metadata.
"""
import os, re, logging, json
from collections import OrderedDict
from typing import Mapping, Union, List, Callable
from abc import abstractmethod, abstractproperty
from copy import deepcopy
from urllib.parse import urlparse
from pathlib import Path
from logging import Logger

import yaml, jsonpatch

from .. import (BadSIPInputError, SIPStateException, PublishingStateException,
                ConfigurationException, PublishException)
from ... import constants as const
from ....nerdm.constants import (CORE_SCHEMA_URI, PUB_SCHEMA_URI, EXP_SCHEMA_URI, SIP_SCHEMA_URI,
                                 core_schema_base)
from ....nerdm import utils as nerdutils
from ... import utils as utils
from ....nerdm import utils as nerdmutils
from ....nerdm.convert import latest
from ...preserve.bagit.builder import BagBuilder
from ... import def_etc_dir
from .base import SIPBagger, UNKNOWN_AGENT
from .prepupd import UpdatePrepService, PENDING_VERSION_SFX
from ..idmint import PDPMinter
from ...utils.prov import Action, Agent, dump_to_history
from nistoar.base.config import merge_config as merge_md_into

SIPEXT_RE = re.compile(core_schema_base + r'sip/(v[^/]+)#/definitions/\w+Submission')
ARK_PFX_RE = re.compile(const.ARK_PFX_PAT)
VER_DELIM = const.RELHIST_EXTENSION.lstrip('/')
FILE_DELIM = const.FILECMP_EXTENSION.lstrip('/')
LINK_DELIM = const.LINKCMP_EXTENSION.lstrip('/')
AGG_DELIM = const.AGGCMP_EXTENSION.lstrip('/')

ASSIGN_DOI_NEVER   = 'never'
ASSIGN_DOI_ALWAYS  = 'always'
ASSIGN_DOI_REQUEST = 'request'

def import_fs_files(bldr: BagBuilder, srcinfo: Mapping, filepaths: List[str],
                    include_all: bool, examine: bool=False, log: Logger=None,
                    _action: Action=None) -> List[str]:
    """
    import files found in an import directory.  

    This importer function will attempt to import files from a locally-mounted directory.  The files
    must be organized within the import directory in the hierarchy intended for the target bag.  
    Files that start with a "." or a "#" are ignored.  

    By default, hard links will be attempted first; if this fails (because the directory in not in the 
    same filesystem as the target bag), then a regular copy will be done.  (Hard links are preferred as 
    they require less space on the disk and make the import faster.)  

    The following properties will be looked for in the ``srcinfo`` argument:

    ``location``
         (str) _required_. The full path to the directory (on a locally mounted filesystem) where files 
         to be imported are located.  
    ``hard_link_data``
         (bool) _optional_. If True (default), attempt to import the files by creating a hard link 
         (falling back to a regular copy if not possible).  If False, all files will explicitly copied.  
    ``consumable``
         (bool) _optional_.  if True (default), this function will remove each source file from the 
         source directory after successfully importing it.  This can prevent the file from being 
         imported multiple times unnecessarily.  If False, the file will be kept intact in the source 
         directory.

    :param BagBuilder bldr:  the bag builder that can take in files
    :param dict    srcinfo:  the description of the source of files.  It requires only one property,
                             ``location``; see above for additional supported properties.
    :param list[str] filepaths:  a list of the filepaths that have already be registered with the 
                             bag (i.e. their metadata has already been added; see ``include_all``).
    :param bool include_all: if False, a file will only be imported if its path is given in filepaths;
                             if True, all files will be imported and default metadata will be 
                             initialized.
    :param bool     examine: if True, examine the file, extract metadata, and register the metadata
                             into the bag; this will fully replace any metadata already set for the 
                             filepath.  Note that this can incur a significant time cost for large or 
                             numerous files.  If False (default), only minimal metadata will be set if 
                             metadata does not already exist for it.
    :param Logger      log:  a logger to use to report messages 
    :param Action  _action:  a provenance action that this import is part of; if provided additional
                             sub actions will be added to record the loading of each file
    :return:  a list of filepaths that were found and imported
              :rtype: List[str]
    """
    location = srcinfo.get('location')
    if not location:
        raise PublishingStateException("location not set in fs source info dictionary")
    if not os.path.isdir(location):
        raise PublishingStateException("%s: import location not found as a directory" % location)
    hardlinks = srcinfo.get('hard_link_data', True)

    act = None
    out = []
    for dir, subdirs, files in os.walk(location):
        for file in files:
            fp = os.path.relpath(os.path.join(dir, file), location)
            if not include_all and fp not in filepaths:
                continue
            src = os.path.join(location, fp)
            
            bldr.add_data_file(fp, src, False, hardlinks, comptype='DataFile')
            if _action:
                act = _action.add_subaction(Action(Action.PUT, fp, _action.agent, "Add a data file"))

            if fp not in filepaths or examine:
                # add its metadata if we don't know about it
                bldr.register_data_file(fp, src, examine, comptype='DataFile')
                if act:
                    act.add_subaction(Action(Action.PUT, "#m", act.agent, "update file metadata"))

            out.append(fp)
            if act:
                _action.add_subaction(act)

            if srcinfo.get('consumable', True):
                try:
                    os.unlink(src)
                except Exception as ex:
                    log.error("%s: Failed to remove source data file as requested: %s", src, str(ex))

    return out

class NERDmBasedBagger(SIPBagger):
    """
    An abstract SIPBagger that accepts NERDm metadata as its primarily inputs.

    This base class will look for the following parameters in the configuration:
    :param Mapping repo_access:         the configuration describing the PDR's APIs 
    :param Mapping bag_builder:         the configuration for the BagBuilder instance that will be
                                        used by this bagger (see BagBuilder)
    :param str assign_doi:              One of three values that controls the assignment of a DOI:
                                         * `always` -- always assign a DOI; the NERDm DOI is set 
                                           according to convention as soon as possible and at least 
                                           by bag finalization time.
                                         * `never` -- automatic assignment should never be applied
                                           (calling :py:meth:`ensure_doi()` does not override this).
                                         * `request` -- (default) a DOI is only assigned by calling
                                           :py:meth:`ensure_doi`.
    :param bool hidden_comp_allowed:    if False (default), Hidden type components are not
                                        permitted to be included in the input NERDm metadata.
    :param bool checksum_comp_allowed:  if False (default), ChecksumFile type components are not
                                        permitted to be included in the input NERDm metadata.
    """
    
    _data_source_file = "__data_sources.lis"
    _file_importers = { 'fs': import_fs_files }

    def __init__(self, sipid: str, bagparent: str, config: Mapping, convention: str,
                 prepsvc: UpdatePrepService=None, id:str=None):
        """
        create a base SIPBagger instance

        :param str sipid:        the identifier for the SIP to process
        :param str bagparent:    the directory where the working bag can be created
        :param Mapping config:   the configuration for this bagger 
        :param str convention:   a label indicating the convention that this SIPBagger implements
        :param UpdatePrepService prepsvc:  the UpdatePrepService to use to initialize the working 
                                   bag from a previously published one.  This can be None if either 
                                   the working bag already exists or this type of initialization is 
                                   not necessary/supported.
        :param str id:           the PDR identifier that should be assigned to this SIP.  If not 
                                   provided and an ID is not yet assigned, idminter must be specified.
        """
        super(NERDmBasedBagger, self).__init__(bagparent, config)

        sipid = ARK_PFX_RE.sub('', sipid)
        self._sipid = sipid
        self._id = id
        self._conv = convention

        syslog = self.getSysLogger().getChild(convention)
        nm = self.sipid
        if len(nm) > 11:
            nm = nm[:4] + "..." + nm[-4:]
        self.log = syslog.getChild(nm)

        nm = sipid.replace('/', '_')
        self.bagbldr = BagBuilder(self.bagparent, nm, self.cfg.get('bag_builder', {}), logger=self.log)
        self.prepsvc = prepsvc
        if not self.prepsvc and not os.path.exists(self.bagdir):
            self.log.warning("Bagger operating without an UpdatePrepService!")
            # raise ValueError("NERDmBasedBagger: requires a UpdatePrepService instance for new bag")

        self.prepared = False
        self._nerdmcore_re = None
        if self.cfg.get('required_core_nerdm_version'):
            self._nerdmcore_re = re.compile(core_schema_base + r'(' + 
                                            self.cfg['required_core_nerdm_version'] + r')#')

        if not self.cfg.get('resolver_base_url') and self.cfg.get('repo_base_url'):
            self.cfg['resolver_base_url'] = self.cfg['repo_base_url'].rstrip('/') + "/od/id/"

        self._histfile = None

    def __del__(self):
        if self.bagbldr:
            del self.bagbldr

    @property
    def sipid(self):
        "the identifier of the SIP being operated on"
        return self._sipid

    @property
    def id(self):
        "the PDR identifier assigned to to this SIP; if None, one has not been assigned yet."
        return self._id

    @property
    def convention(self):
        """a label for the SIP convention that this bagger is assuming for the SIP.  It identifies 
        the forms of the inputs and how they should be applied."""
        return self._conv

    @property
    def bagdir(self):
        "The path to the bag being constructed from the SIP inputs"
        return self.bagbldr.bagdir

    @property
    def bag(self):
        """
        return a NISTBag instance for the bag being assembled, or None if the bag has not been 
        established yet
        """
        return self.bagbldr.bag

    def ensure_preparation(self, nodata: bool=False, who: Agent=None, _action: Action=None) -> None:
        """
        create and update the output working bag directory to ensure it is 
        a re-organized version of the SIP, ready for updates.

        :param nodata bool: if True, do not copy (or link) data files to the output directory.  
                            In this implementation, this parameter is ignored
        :param Agent    who: an actor identifier object, indicating who is requesting this action.  This 
                             will get recorded in the history data.  If None, an internal administrative 
                             identity will be assumed.  This identity may affect the identifier assigned.
        :param Action _action:  Intended primarily for internal use; if provided, any provenance actions 
                             that should be recorded within this function should be added as a subaction
                             of this given one rather than recorded directly as a stand-alone action.
        """
        if not self._id:
            self._id = self._id_for(self.sipid, True)
        self.ensure_base_bag(who, _action)

    @abstractmethod
    def _id_for(self, sipid, mint=False):
        """
        determine the PDR ID that should be associated with the SIP of the given ID.
        The details will depend on the SIP convention.  It can be determined by the SIP
        ID itself or minted anew as appropriate.  

        :param bool mint:  If True, a new identifier should be minted if possible if one is 
                           not currently registered.
        """
        raise NotImplementedError()

    @abstractmethod
    def _aipid_for(self, pdrid):
        """
        determine the AIP ID to be used for preserving datasets with the given PDR ID.
        The details will depend on the SIP convention.  It can be determined by the SIP
        ID itself or minted anew as appropriate.  
        """
        raise NotImplementedError()

    def ensure_base_bag(self, who=None, _action: Action=None) -> None:
        """
        Establish an initial working bag.  If a working bag already exists, it 
        will be used as is.  Otherwise, this method will check to see if a 
        resource with with the same SIP identifier has been published before;
        if so, its metadata (with version information updated) will be used to 
        create the initial bag.  If not, it is assumed that this is a new 
        SIP that has never been submitted before; a new bag directory will be 
        created and identifiers will be assigned to it.  

        :param Agent    who: an actor identifier object, indicating who is requesting this action.  This 
                             will get recorded in the history data.  If None, an internal administrative 
                             identity will be assumed.  This identity may affect the identifier assigned.
        :param Action _action:  Intended primarily for internal use; if provided, any provenance actions 
                             that should be recorded within this function should be added as a subaction
                             of this given one rather than recorded directly as a stand-alone action.
        """
        if not who:
            who = UNKNOWN_AGENT
        if os.path.exists(self.bagdir):
            self.bagbldr.ensure_bagdir()  # sets builders bag instance

            if not self.prepared:
                self.log.info("Refreshing previously established working bag")

        elif self.prepsvc:
            self.log.debug("Looking for previously published version of bag")
            prepper = self._get_prepper()
            if prepper.create_new_update(self.bagdir):
                self.isrevision = True
                self.log.info("Working bag initialized with metadata from previous "
                              "publication.")

                # lock some metadata by saving them as annotations
                self.bagbldr.ensure_bagdir()
                annotf = self.bagbldr.bag.annotations_file_for('')
                if os.path.exists(annotf):
                    os.remove(annotf)
                nerdm = self.bagbldr.bag.nerd_metadata_for('', False)
                locked_md = OrderedDict([("@id", self.id), ("pdr:sipid", self.sipid)])
                for prop in "pdr:aipid firstIssued bureauCode programeCode".split():
                    if prop in nerdm:
                        locked_md[prop] = nerdm[prop]
                if not 'pdr:aipid' in locked_md:
                    locked_md['pdr:aipid'] = self._aipid_for(self.id)
                self._add_publisher_md(locked_md)
                self._add_provider_md(locked_md)
                self.bagbldr.update_annotations_for('', locked_md,
                                                    message="locking convention metadata as annotations")

                # add a history record
                act = Action(Action.COMMENT, self.id, who, "Initialized update based on version " +
                             re.sub(r'\++( [\(\)\w]+)*$', '', nerdm.get('version', '1.0')))
                             
                if _action:
                    _action.add_subaction(act)
                else:
                    self.record_history(act)

        if not os.path.exists(self.bagdir):
            self.bagbldr.ensure_bag_structure()
            self.bagbldr.assign_id(self.id)

            # set some minimal metadata
            version = "1.0.0"
            minimal_md = OrderedDict([
                ("@id", self.id),
                ("version", version),
                ("pdr:sipid", self.sipid),
                ("pdr:aipid", self._aipid_for(self.id))
            ])
            self._add_publisher_md(minimal_md)
            self._add_provider_md(minimal_md)
            self.bagbldr.update_metadata_for('', minimal_md, message='initial minimal metadata established')

            del minimal_md['version']
            self.bagbldr.update_annotations_for('', minimal_md,
                                                message="locking convention metadata as annoations")

            act = Action(Action.CREATE, self.id, who,
                         "Initialized new submission as version "+version)
            if self.cfg.get('assign_doi') == ASSIGN_DOI_ALWAYS:
                self.ensure_doi(who, _action=act)
            
            # add a history record
            if _action:
                _action.add_subaction(act)
            else:
                self.record_history(act)

        else:
            self.bagbldr.ensure_bagdir()  # sets bag object, connects internal log file

        self.prepared = True
            
    def _get_prepper(self):
        if not self.prepsvc:
            return None
        return self.prepsvc.prepper_for(self.id, self._aipid_for(self.id),
                                        log=self.log.getChild("prepper"))

    @abstractmethod
    def _add_publisher_md(self, resmd) -> None:
        """
        add resource-level metadata that is specific to the publisher (e.g. NIST)
        """
        raise NotImplementedError()

    @abstractmethod
    def _add_provider_md(self, resmd) -> None:
        """
        add resource-level metadata that is standard to the specific provider.  The provider is 
        normally determined by the shoulder on the PDR identifier ("@id").  
        """
        raise NotImplementedError()

    def describe(self, relid: str = '', who: Agent = None, _action: Action=None) -> Mapping:
        """
        return a NERDm description for the part of the dataset pointed to by the given identifier
        relative to the dataset's base identifier.
        :param str relid:  the relative identifier for the part of interest.  If an empty string
                           (default), the full NERDm record will be returned.
        :param Agent  who: an actor identifier object, indicating who is requesting the data.  This 
                           request may trigger the restaging of previously published data, in which 
                           case who triggered it will get recorded.  If None, an internal administrative 
                           identity will be assumed.  
        :param Action _action:  Intended primarily for internal use; if provided, any provenance actions 
                             that should be recorded within this function should be added as a subaction
                             of this given one rather than recorded directly as a stand-alone action.
        """
        if not who:
            who = UNKNOWN_AGENT
        if not self.bagdir or not os.path.exists(self.bagdir):
            self.prepare(False, who, _action=_action)
        return self.bagbldr.bag.describe(relid)

    def set_res_nerdm(self, nerdm: Mapping, who: Agent = None, savefilemd: bool=True,
                      lock: bool=True, _action: Action=None) -> None:
        """
        set the resource metadata (which may optionally include file component metadata) for the SIP.  
        The input metadata should be as complete as is appropriate for the type of SIP being processed.  

        :param Mapping nerdm:  the resource-level NERDm metadata to save
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :param bool savefilemd:  if True (default), any DataFile or Subcollection metadata included will 
                                 be saved as well
        :param Action _action:  Intended primarily for internal use; if provided, any provenance actions 
                             that should be recorded within this function should be added as a subaction
                             of this given one rather than recorded directly as a stand-alone action.
        """
        if not who:
            who = UNKNOWN_AGENT
        with self._lock_when(lock):
            nerdm = self._check_res_schema_id(nerdm)   # creates a deep copy of the record

            hist = Action(Action.PUT, self.id, who, "Set resource metadata")
            if _action:
                _action.add_subaction(hist)
            self.ensure_preparation(True, who, hist)

            # modify the input: remove properties that cannot be set, add others
            handsoff = "@id @context publisher issued firstIssued revised annotated language " + \
                       "bureauCode programCode doi ediid releaseHistory "
            handsoff += " ".join([k for k in nerdm.keys() if k.startswith("pdr:")])
            for prop in handsoff.strip().split():
                if prop in nerdm:
                    del nerdm[prop]
            self._set_standard_res_modifications(nerdm)
            self._set_provider_res_modifications(nerdm)

            components = nerdm.get('components')
            if 'components' in nerdm:
                nerdm['components'] = []

            # set up history record (using who)
            what = "Setting resource metadata"
            if savefilemd and components:
                what += " with components"
            hist.add_subaction(self._history_comment("#m", who, what))

            try:
                old = self.bagbldr.bag.nerd_metadata_for('', True)   # for history record

                self.bagbldr.add_res_nerd(nerdm, False)

                new = self.bagbldr.bag.nerd_metadata_for('', True)   # for history record
                hist.add_subaction(self._putcreate_history_action("#m", who,
                                                                  "Set resource-level metadata",
                                                                  old, new))

                if savefilemd and components:
                    # clear out any previously saved components
                    oldcmps = self.bagbldr.bag.subcoll_children('')
                    if oldcmps:
                        for cmp in oldcmps:
                            self.bagbldr.remove_component(cmp)
                        hist.add_subaction(Action(Action.DELETE, FILE_DELIM, who,
                                                  "Cleared previously added components"))

                    for cmp in components:
                        self._set_comp_nerdm(cmp, who, hist, False)
                else:
                    hist = hist.subactions[0]

            except Exception as ex:
                self.log.warning("Bag left in possible incomplete state due to error: %s", str(ex))
                self.record_history(hist)
                hist = self._history_comment("#m", who, "Failed to complete %s action" % hist.type)
                raise

            finally:
                # record history record
                if not _action:
                    self.record_history(hist)

    def _check_res_schema_id(self, nerdm):
        if self._nerdmcore_re:
            if '_schema' in nerdm:
                raise BadSIPInputError("Required schema identifier property missing from input metadata: "+
                                       "_schema")
            if not self._nerdmcore_re.match(nerdm['_schema']):
                raise ValueError("Input metadata is not a NERDm record; schema: "+ nerdm['_schema'])
        elif '_schema' in nerdm:
            if not nerdm['_schema'].startswith(core_schema_base):
                raise BadSIPInputError("Input metadata is not a NERDm record; schema: "+ nerdm['_schema'])
        elif 'title' not in nerdm or 'contactPoint' not in nerdm:
            raise BadSIPInputError("Input metadata apparently is not a NERDm record (no schema specified)")

        nerdm = latest.update_to_latest_schema(nerdm, False)
        return nerdm

    def _set_standard_res_modifications(self, resmd):
        # update the types
        types = resmd.setdefault('@type', [])
#        while 'nrds:PDRSubmission' in types:
#            types.remove('nrds:PDRSubmission')
        if 'nrd:Resource' not in types:
            types = [t for t in types if not t.endswith(':Resource')]  # fixes unconventional prefixes
            types.append('nrd:Resource')
        if 'nrdp:PublicDataResource' not in types:
            types = [t for t in types if not t.endswith(':PublicDataResource')]
            nerdmutils._insert_before_val(types, 'nrdp:PublicDataResource',
                                          'nrds:PDRSubmission', 'nrd:Resource')
        if 'nrdp:PublicDataResource' in types:
            if 'nrdp:DataPublication' not in types and 'authors' in resmd and len(resmd['authors']) > 0:
                nerdmutils._insert_before_val(types, 'nrdp:DataPublication',
                                              'nrds:PDRSubmission', 'nrdp:PublicDataResource')
            if 'nrde:ExperimentalData' not in types:
                isexp = False
                for prop in "instrumentsUsed isPartOfProjects acquisitionStartTime hasAcquisitionStart acquisitionEndTime hasAcquisitionEnd".split():
                    if prop in resmd:
                        isexp = True
                        break
                if isexp:
                    types = [t for t in types if not t.endswith(':ExperimentalData')]  
                    nerdmutils._insert_before_val(types, 'nrde:ExperimentalData', 'nrdp:DataPublication',
                                                  'nrds:PDRSubmission', 'nrdp:PublicDataResource')
        resmd['@type'] = types

        extschs = set(resmd.get('_extensionSchemas', []))
        if nerdutils.is_type(resmd, 'DataPublication'):
            if not any([s for s in extschs if s.endswith('/definitions/DataPublication')]):
                extschs.add(PUB_SCHEMA_URI + "#/definitions/DataPublication")
            for s in [s for s in extschs if s.endswith('/definitions/PublicDataResource')]:
                extschs.remove(s)
        elif nerdutils.is_type(resmd, 'PublicDataResource'):
            if not any([s for s in extschs if s.endswith('/definitions/PublicDataResource')]):
                extschs.add(PUB_SCHEMA_URI + "#/definitions/PublicDataResource")
        if nerdutils.is_type(resmd, 'ExperimentalData'):
            if not any([s for s in extschs if s.endswith('/definitions/ExperimentalData')]):
                extschs.add(EXP_SCHEMA_URI + "#/definitions/ExperimentalData")
            for s in [s for s in extschs if s.endswith('/definitions/ExperimentalContext')]:
                extschs.remove(s)
        if extschs:
            resmd['_extensionSchemas'] = list(extschs)

        if 'contactPoint' in resmd:
            resmd['contactPoint']['@type'] = "vcard:Contact"

        if not resmd.get('accessLevel'):
            resmd['accessLevel'] = "public"

    def _set_provider_res_modifications(self, resmd: Mapping):
        """
        modify the given resource NERDm metadata provided by the client with changes 
        appropriate for the provider context.  The provider is normally determined by the 
        shoulder on the PDR identifier ("@id").  This should be overridden for by 
        convention-specific subclasses.
        """
        return

    def set_comp_nerdm(self, nerdm: Mapping, who: Agent=None, lock: bool=True,
                       _action: Action=None) -> None:
        """
        set the metadata for a component of the resource.  If the component represents a file or 
        a subcollection, it must contain a 'filepath' property.  
        :param Mapping nerdm:   the NERDm Component metadata.  
        """
        with self._lock_when(lock):
            return self._set_comp_nerdm(nerdm, who, _action=_action)

    def _set_comp_nerdm(self, nerdm: Mapping, who: Agent=None, _action=None, tolatest=True) -> None:
        if not who:
            who = UNKNOWN_AGENT
        nerdm = self._check_input_comp(nerdm, tolatest)   # copies nerdm

        hist = Action(Action.PUT, self.id, who, "Set some component metadata")
        self.ensure_preparation(True, who, hist)
        if hist.subactions_count() == 0:
            hist = None
        elif _action:
            _action.add_subaction(hist)

        # modify the input: remove properties that are not needed or allowed, add others
        remove = "_schema @context"
        for prop in remove.split():
            if prop in nerdm:
                del nerdm[prop]

        # figure out the type; any of these may raise BadSIPInputError
        self._set_comp_types(nerdm)
        self._setchk_missing_comp_props(nerdm)   
        self._set_comp_id(nerdm)

        # validate?

        old = self.bagbldr.bag.describe(nerdm['@id'])  # for history record
        
        if 'filepath' in nerdm:
            if not self.bagbldr.bag.has_component(nerdm['filepath']):
                self.bagbldr.register_data_file(nerdm['filepath'], comptype="DataFile")
            self.bagbldr.update_metadata_for(nerdm['filepath'], nerdm)
        else:
            # add to non-file list of components
            self.bagbldr.update_metadata_for("@id:"+nerdm['@id'], nerdm)

        act = self._putcreate_history_action('/'+nerdm['@id'].lstrip('/'), who, "Set component metadata",
                                             old, self.bagbldr.bag.describe(nerdm['@id']))
        if hist:
            hist.add_subaction(act)
        elif _action:
            _action.add_subaction(act)
        else:
            self.record_history(act)

        return nerdm['@id']

    def _check_legal_url(self, url):
        u = urlparse(url)
        u.port
        if not u.scheme:
            raise ValueError("Missing scheme")
        if u.scheme not in ["http", "https", "ftp"]:
            raise ValueError("Unsupported scheme: " + u.scheme)
        if not u.netloc:
            raise ValueError("Missing server address")


    def _check_input_comp(self, compmd, tolatest):
        if compmd.get('@type') and \
           not nerdutils.is_any_type(compmd, ["DataFile", "AccessPage", "Subcollection", "IncludedResource"]):
            raise BadSIPInputError("Does not include a supported component type: "+str(compmd.get('@type')))

        if not self.cfg.get('hidden_comp_allowed', False) and nerdutils.is_type(compmd, "Hidden"):
            raise BadSIPInputError("Hidden components not allowed: "+str(compmd('@type',[])))
                
        if not self.cfg.get('checksum_comp_allowed', False) and nerdutils.is_type(compmd, "ChecksumFile"):
            raise BadSIPInputError("Checksum components not allowed: "+str(compmd('@type',[])))

        if 'downloadURL' in compmd:
            try:
                self._check_legal_url(compmd['downloadURL'])
            except ValueError as ex:
                raise BadSIPInputError("Illegal downloadURL property: %s: %s" %
                                       (str(ex), str(compmd['downloadURL'])))
        if 'accessURL' in compmd:
            try:
                self._check_legal_url(compmd['accessURL'])
            except ValueError as ex:
                raise BadSIPInputError("Illegal downloadURL property: %s: %s" %
                                       (str(ex), str(compmd['downloadURL'])))

        if tolatest:
            if self._nerdmcore_re:
                if '_schema' in compmd:
                    raise BadSIPInputError("Required schema identifier property missing from input metadata: "+
                                           "_schema")
                if not self._nerdmcore_re.match(compmd['_schema']):
                    raise ValueError("Input metadata is not a NERDm record; schema: "+ compmd['_schema'])
            elif '_schema' in compmd:
                if not compmd['_schema'].startswith(core_schema_base):
                    raise BadSIPInputError("Input metadata is not a NERDm record; schema: "+ compmd['_schema'])

            compmd = latest.update_to_latest_schema(compmd, False)
        else:
            compmd = deepcopy(compmd)
                    
        return compmd
            

    def _set_comp_types(self, cmpmd):
        # we are assuming that if the component specifies a non-empty @type, it includes one of
        # the supported types.
        if not cmpmd.get('@type'):
            if 'downloadURL' in cmpmd:
                if 'accessURL' in cmpmd:
                    raise BadSIPInputError("Unable to determine component type: contains both "+
                                           "downloadURL and accessURL: "+str(cmpmd['downloadURL']))
                cmpmd['@type'] = ["nrdp:DataFile"]
            elif 'accessURL' in cmpmd:
                cmpmd['@type'] = ["nrdp:AccessPage"]
            # elif 'filepath' in cmpmd:
            #     cmpmd['@type'] = ["nrdp:Subcollection"]
            # elif 'proxyFor' in cmpmd:
            #     cmpmd['@type'] = ["nrd:IncludedResource"]
            else:
                raise BadSIPInputError("Unable to determine component type: must provide @type\n" +
                                       json.dumps(cmpmd))

        if (nerdutils.is_type(cmpmd, "DataFile") or nerdutils.is_type(cmpmd, "ChecksumFile")) \
           and not nerdutils.is_type(cmpmd, "DownloadableFile"):
            cmpmd['@type'].append("nrdp:DownloadableFile")
        if (nerdutils.is_type(cmpmd, "DataFile") or nerdutils.is_type(cmpmd, "AccessPage")) \
           and not nerdutils.is_type(cmpmd, "Distribution"):
            cmpmd['@type'].append("dcat:Distribution")

        # update _extensionSchemas
        extschs = set(cmpmd.get('_extensionSchemas',[]))
        if nerdutils.is_type(cmpmd, "DataFile"):
            if not any([s for s in extschs if s.endswith('/definitions/DataFile')]):
                extschs.add(PUB_SCHEMA_URI + "#/definitions/DataFile")
        elif nerdutils.is_type(cmpmd, "Subcollection"):
            if not any([s for s in extschs if s.endswith('/definitions/Subcollection')]):
                extschs.add(PUB_SCHEMA_URI + "#/definitions/Subcollection")
        elif nerdutils.is_type(cmpmd, "ChecksumFile"):
            if not any([s for s in extschs if s.endswith('/definitions/ChecksumFile')]):
                extschs.add(PUB_SCHEMA_URI + "#/definitions/ChecksumFile")
        elif nerdutils.is_type(cmpmd, "AccessPage"):
            if not any([s for s in extschs if s.endswith('/definitions/AccessPage')]):
                extschs.add(PUB_SCHEMA_URI + "#/definitions/AccessPage")
        elif nerdutils.is_type(cmpmd, "IncludedResource"):
            if not any([s for s in extschs if s.endswith('/definitions/IncludedResource')]):
                extschs.add(CORE_SCHEMA_URI + "#/definitions/IncludedResource")
        if nerdutils.is_type(cmpmd, 'AcquisitionActivity'):
            if not any([s for s in extschs if s.endswith('/definitions/AcquisitionActivity')]):
                extschs.add(EXP_SCHEMA_URI + "#/definitions/ExperimentalData")
        if extschs:
            cmpmd['_extensionSchemas'] = list(extschs)

    def _setchk_missing_comp_props(self, cmpmd):
        # this also checks for minimum metadata as required by this publishing convention
        if nerdutils.is_type(cmpmd, "AccessPage"):
            self._setchk_missing_access_props(cmpmd)
        elif nerdutils.is_type(cmpmd, "DataFile"):
            self._setchk_missing_datafile_props(cmpmd)
        elif nerdutils.is_type(cmpmd, "Subcollection"):
            self._setchk_missing_subcoll_props(cmpmd)
        elif nerdutils.is_type(cmpmd, "IncludedResource"):
            self._setchk_missing_inclres_props(cmpmd)

    def _setchk_missing_access_props(self, cmpmd):
        if 'accessURL' not in cmpmd:
            raise BadSIPInputError("AccessPage component is missing accessURL: "+json.dumps(cmpmd))

    def _setchk_missing_datafile_props(self, cmpmd):
        missing = []
        for prop in ["downloadURL"]:   # ["downloadURL", "size"]:
            if prop not in cmpmd:
                missing.append(prop)
        if missing:
            msg = "Missing DataFile component properties: "+str(missing)+": "+ \
                  str(('downloadURL' in cmpmd and cmpmd['downloadURL']) or json.dumps(cmpmd))
            raise BadSIPInputError(msg)

        if 'filepath' not in cmpmd:
            m = re.search(r'/od/ds/', cmpmd['downloadURL'])
            if m:
                cmpmd['filepath'] = cmpmd['downloadURL'][m.end():]
            else:
                cmpmd['filepath'] = re.sub(r'^.*/', '', cmpmd['downloadURL'])

    def _setchk_missing_subcoll_props(self, cmpmd):
        if 'filepath' not in cmpmd:
            raise BadSIPInputError("Subcollection component is missing filepath: "+json.dumps(cmpmd))
        
    def _setchk_missing_inclres_props(self, cmpmd):
        if 'proxyFor' not in cmpmd:
            raise BadSIPInputError("IncludedResource component is missing proxyFor: "+json.dumps(cmpmd))

        if 'location' not in cmpmd:
            if cmpmd['proxyFor'].startswith("doi:"):
                cmpmd['location'] = re.sub(r'^doi:', 'https://doi.org/', cmpmd['proxyFor'])
            elif cmpmd['proxyFor'].startswith("hdl:"):
                cmpmd['location'] = re.sub(r'^hdl:', 'https://handle.net/', cmpmd['proxyFor'])
            elif cmpmd['proxyFor'].startswith("https:") or cmpmd['proxyFor'].startswith("http:"):
                cmpmd['location'] = cmpmd['proxyFor']

    def _set_comp_id(self, cmpmd):
        if '@id' in cmpmd:
            if cmpmd['@id'].startswith(self.id):
                cmpmd['@id'] = cmpmd['@id'][len(self.id):]
            elif cmpmd['@id'].startswith(self.sipid):
                cmpmd['@id'] = cmpmd['@id'][len(self.id):]
        else: 
            if nerdutils.is_any_type(cmpmd, ["DataFile", "ChecksumFile", "Subcollection"]):
                cmpmd['@id'] = FILE_DELIM + '/' + cmpmd['filepath']

            elif nerdutils.is_type(cmpmd, "AccessPage"):
                cmpmd['@id'] = LINK_DELIM + '/'
                url = urlparse(cmpmd['accessURL'])
                if url.netloc == 'doi.org':
                    cmpmd['@id'] += "doi:"
                elif url.netloc == 'handle.net':
                    cmpmd['@id'] += "hdl:"
                else:
                    cmpmd['@id'] += netloc 
                if url.path and url.path != '/':
                    cmpmd['@id'] += url.path

            elif nerdutils.is_type(cmpmd, "IncludedResource"):
                cmpmd['@id'] = AGG_DELIM + '/'
                url = urlparse(cmpmd['proxyFor'])
                if url.netloc == 'doi.org':
                    cmpmd['@id'] += "doi:"
                elif url.netloc == 'handle.net':
                    cmpmd['@id'] += "hdl:"
                elif url.netloc:
                    cmpmd['@id'] += netloc 
                if url.netloc and url.path and url.path != '/':
                    cmpmd['@id'] += url.path
                else:
                    cmpmd['@id'] += cmpmd['proxyFor']

    def add_data_file(self, srcfile: Union[str,Path], filepath: str, mdata: Mapping=None, 
                      merge: bool=True, who: Agent=None, hardlink: bool=True, lock: bool=True,
                      _action: Action=None):
        """
        add a data file to the bag

        :param str|Path srcfile:  the path to the file to import
        :param str     filepath:  the path relative to the bag's data directory to import the 
                                  file into
        :param dict       mdata:  the NERDm component metadata describeing the file to register.
                                  if not provided and no metadata for the file has yet to be added
                                  (via :py:meth:`set_comp_nerdm`), only minimal default metadata 
                                  will be registered; otherwise, existing metadata will be unaltered.
        :param bool       merge:  if True (default), the provided metadata will be merged with any 
                                  existing metadata for the given filepath; if False, the given 
                                  metadata will replace any previously registered metadata.  In either
                                  case, this method will ensure that the save metadat includes the 
                                  necessary minimum.
        :param bool    hardlink:  If True (default), this method will attempt to import the file by 
                                  creating a hard link to the source file; if it fails (because the 
                                  filesystems for the source file and the bag are different), fallback 
                                  to copying the file normally.  Specify False to force a hard copy.  
        """
        if not who:
            who = UNKNOWN_AGENT

        with self._lock_when(lock):
            if not mdata and not self.bagbldr.bag.has_component(filepath):
                mdata = self.bagbldr.describe_data_file(srcfile, filepath, examine=False)
                mdata['filepath'] = filepath

            hist = Action(Action.PUT, self.id+const.FILECMP_EXTENSION+'/'+filepath,
                          who, "Add a data file")
            self.ensure_preparation(True, who, hist)

            message = f"Adding data file {filepath}"
            if mdata:
                message += " with metadata"
            self.bagbldr.add_data_file(filepath, srcfile, False, hardlink, message, comptype='DataFile')
                                    
            if mdata:
                if merge:
                    self.bagbldr.update_metadata_for(filepath, mdata, message='', comptype='DataFile')
                    hist.add_subaction(Action(Action.PATCH, "#m", who, "update file metadata"))
                else:
                    self.bagbldr.replace_metadata_for(filepath, mdata, message='', comptype='DataFile')
                    hist.add_subaction(Action(Action.PUT, "#m", who, "add file metadata"))

            if _action:
                _action.add_subaction(hist)
            else:
                self.record_history(hist)
                
                
    def import_data_files(self, srcinfo: Union[str, Mapping], include_all: bool=False, 
                          examine: bool=False, who: Agent=None, forcecopy: bool=False, 
                          lock: bool=True, _action: Action=None, _filepaths: List[str]=None) -> List[str]:
        """
        import data files found in a given data source.

        The implementation can support multiple ways of importing data files of importing files into
        the bag.  The where from and how of importing is specified in the ``srcinfo`` argument.  This 
        argument can take one of two forms.  The brief format is in the form of a string representing 
        a simple URN where the colon-delimited prefix indicates type type of source it is (e.g. "fs:"); 
        the remainder is a location specifier appropriate for that type. More complex sources are 
        described by by a dictionary that includes a ``type`` property indicating the type of the 
        source; the remaining properties provide the details needed to access the source.

        The types of sources that are supported is implementation-dependent.  This base implementation 
        supports only the ``fs`` type which allows data to be imported from a filesystem-accessible 
        directory.  In the brief format, the location specifier is the full path to the directory 
        containing data files.  In the dictionary format, the following properties (in addition to 
        ``type`` being set to ``fs``) are supported:

        ``location``
             (str) _required_.  the full path to the directory where files can be found
        ``hard_link_data``
             (bool) _optional_. If True (default), attempt to import the files by creating a hard link 
             (falling back to a regular copy if not possible).  If False, all files will explicitly 
             copied.  
        ``consumbable``
             (bool) _optional_.  If True (default), the file may be removed from the source directory
             after it has been loaded into the bag; this is recommended to prevent the file from being 
             inadvertantly reloaded with each call to :py:meth:`import_from_sources`.  False prevents
             the removal of the source file after successful import.

        See also :py:func:`import_fs_files` for more details on the import behavior for the ``fs`` type.

        :param str|dict srcinfo:  a description of how and from where files can be pulled from (see above)
        :param bool include_all:  if False (default), only files for which the bag holds file metadata 
                                  for already will be imported.  If True, all files found in the source 
                                  will be loaded; for any that the bag does not have metadata, default 
                                  metadata will be created for it (see also the ``examine`` argument).
        :param bool     examine:  If True, each file will be examined for extracting additional metadata
                                  (e.g. a checksum hash).  A True value may incur a significant 
                                  time cost.  If False (default), at most, only minimal metadata for the 
                                  file will be created if it doesn't exist already.  
        :return:  a list of the filepaths that were imported
                  :rtype: List[str]
        """
        # convert string form to dict; can raise TypeError, ValueError
        srcinfo = self._ensure_srcinfo_dict(srcinfo)
        srctype = srcinfo['type']

        if not who:
            who = UNKNOWN_AGENT

        with self._lock_when(lock):
            self.ensure_preparation(True, who)
            hmsg = "Importing data files from "+srcinfo.get('location', "source type="+srctype)
            hist = Action(Action.PUT, self.id+const.FILECMP_EXTENSION+'/', who, hmsg)

            if _filepaths is None:
                _filepaths = list(self.bagbldr.bag.iter_data_components())

            try:
                return self._file_importers[srcinfo['type']](self.bagbldr, srcinfo, _filepaths,
                                                             include_all, examine, self.log, hist)
            except PublishingStateException as ex:
                self.log.error("Unable to import data from source (type=%s): %s",
                               srcinfo['type'], str(ex))
            except PublishException as ex:
                raise
            except Exception as ex:
                raise PublishException("Unexpected failure while importing files from source "
                                       "type=%s: %s" % (srcinfo['type'], str(ex)))
                                          
            if _action:
                _action.add_subaction(hist)
            else:
                self.record_history(hist)

    def _ensure_srcinfo_dict(self, srcinfo):
        if isinstance(srcinfo, str) and ':' in srcinfo:
            # format should be type:location
            srctp, loc = srcinfo.split(':', 1)
            srcinfo = { 'type': srctp, 'location': loc }
        elif not isinstance(srcinfo, Mapping):
            raise TypeError("bagger: srcinfo not a str or Mapping")

        if not srcinfo.get('type'):
            raise ValueError("bagger: srcinfo dict is missing required 'type' property")
        elif srcinfo['type'] not in self._file_importers:
            raise ValueError("bagger: srcinfo type not supported: "+srcinfo['type'])

        return srcinfo

    def set_data_source(self, srcinfo: Union[str,Mapping], who: Agent=None,
                        lock: bool=True, _action: Action=None):
        """
        declare a source for loading data files into this bag.  

        Data files found at the source will be migrated into the bag when 
        :py:meth:`ensure_data_files` is called (which is called by :py:meth:`finalize`).  The 
        files themselves, therefore, do not need to be available at the source when this method
        is called; they just need to be there by the time :py:meth:`finalize` is called.

        :param str|dict srcinfo:  a description of the source.  The format is the same as ``srcinfo``
                                  supported by the :py:meth:`import_data_files`.
        :param Agent who:         an agent identifier object, indicating who is requesting this action.  
                                  This will get recorded in the history data.  If None, an internal 
                                  administrative identity will be assumed.  This identity may affect the 
                                  identifier assigned.
        """
        # convert string form to dict; can raise TypeError, ValueError
        srcinfo = self._ensure_srcinfo_dict(srcinfo)
        try:
            encoded = json.dumps(srcinfo)
        except TypeError as ex:
            raise TypeError("set_data_source: srcinfo is not a str or a JSON-encodable object (%s)" % \
                            str(ex))

        if not who:
            who = UNKNOWN_AGENT

        with self._lock_when(lock):
            hist = Action(Action.COMMENT, self.id, who, "Setting payload data source", encoded)
            self.ensure_preparation(True, who, hist)

            dsrcf = os.path.join(self.bagdir, self._data_source_file)
            try:
                with open(dsrcf, 'a') as fd:
                    fd.write(encoded)
                    fd.write('\n')
            except Exception as ex:
                raise PublishingStateException("Unable to write data source to SIP bag: "+str(ex))

            if _action:
                _action.add_subaction(hist)
            else:
                self.record_history(hist)
            
    def ensure_data_files(self, include_all: bool=False, examine: bool=False, who: Agent=None,
                          lock=True, _action: Action=None):
        """
        import all data found in the registered data sources into the bag

        Data sources are registered via :py:meth:`set_data_source`.  This method will iterate through
        the registered data sources and import the data files found there.  This method is called 
        by :py:meth:`finalize` to ensure that all data has been imported.  

        :param bool include_all:  if False (default), only files that currently have a component metadata 
                                  description will get migrated.  If True, all files found in the 
                                  sources will be imported; for those without a component metadata
                                  description, a default description will be created.
        :param bool     examine:  If True, each imported file will be examined for extracting additional 
                                  metadata (e.g. a checksum hash).  A True value may incur a significant 
                                  time cost if the files are large or numerous.  If False (default), at 
                                  most, only minimal metadata for the file will be created if it doesn't 
                                  exist already.  
        """
        if not who:
            who = UNKNOWN_AGENT

        with self._lock_when(lock):
            filepaths = list(self.bagbldr.bag.iter_data_components())

            hist = Action(Action.PATCH, self.id, who, "Ensuring all data imported")

            imported = set()
            dsrcf = os.path.join(self.bagdir, self._data_source_file)
            if os.path.isfile(dsrcf):
                with open(dsrcf) as fd:
                    for line in fd:
                        try:
                            srcinfo = json.loads(line.strip())
                        except ValueError as ex:
                            self.log.error("Corrupted data source entry: %s; skipping", line.strip())
                        else:
                            imported |= set(self.import_data_files(srcinfo, include_all, examine, who,
                                                                   False, False, hist, filepaths))

            if _action:
                _action.add_subaction(hist)
            else:
                self.record_history(hist)

            return list(imported)

    @classmethod
    def register_data_source_type(cls, type: str, importer: Callable):
        """
        add support for a mechanism for importing data files into a bag

        This function can be used to extend this class to support additional mechanisms for 
        importing data files.  The ``type`` value corresponds to a supported ``type`` property 
        in the ``srcinfo`` provided to :py:meth:`import_data_files`` and :py:meth:`set_data_source`.
        The mechanism implementation is given by ``importer`` which is a callable that must 
        support the following arguments:

        ``bldr``
             (BagBuilder)  the bag builder instance to use to load the file from the source
        ``srcinfo``
             (dict)  the description of the data source in which the properties controls how 
             and from where the files are loaded.  These properties are implementation-specific
             (so it is recommended that the function definition document what is supported).
        ``filepaths``
             (list of str)  a list of the filepaths whose metadata have already been committed
             to the bag.
        ``include_all``
             (bool)  if True, all eligible files found in the data source will be imported; 
             otherwise, only those files corresponding to those listed in ``filepaths`` will 
             be loaded.
        ``examine``
             (bool)  if True and if possible, each file should be examined for extractable metadata;
             otherwise, only minimal metadata will be set if none already exist in the bag.
        ``log``
             (Logger) a Logger that should be used to record log messages
        ``_action``
             (Action) a provenance Action instance representing an aggregate action that this import 
             function call is part of.  If provided, file loading actions will be recorded as 
             sub-action into this Action.  If None, no provenance actions are recorded. 

        The function must return a list of the filepaths that files were imported into.  The function
        may raise exceptions; they will be handled.  

        :param          str type:  the label that identifies the type of data source to support
        :param function importer:  a function that implements the import mechanism (see above for 
                                   description of the required function signature).  This will replace
                                   any previously registered function for this type.
        """
        cls._file_importers[type] = importer

    def delete(self, who: Agent=None, lock=True):
        """
        delete the working bag from store; this sets the bagger to a virgin state.
        """
        msg = "Deleting SIP bag by request"
        if who:
            msg += " by "+str(who)
        self.log.info(msg)
        with self._lock_when(lock):
            self.bagbldr.destroy()

    def ensure_doi(self, who: Agent=None, lock: bool=True, _action=None):
        """
        ensure that the NERDm resource metadata includes the `doi` property set according to this
        SIP's convention.  This method consults the value of the `assign_doi` configuration parameter:
        if the value is 'never', no DOI is set.

        This function calls :py:meth:`_determine_doi` which returns the appropriate DOI string to 
        assign to this SIP according the SIP's convention.
        """
        if self.cfg.get('assign_doi') == ASSIGN_DOI_NEVER:
            return
        with self._lock_when(lock):
            self._ensure_doi(who, _action)

    def _ensure_doi(self, who, _action=None, nerd=None):
        if not who:
            who = UNKNOWN_AGENT

        if not nerd:
            self.ensure_preparation(who, _action)
            nerd = self.bagbldr.bag.nerd_metadata_for('', True)
        if nerd.get('doi'):
            return

        doi = self._determine_doi()  # may raise SIPConflictException

        # lock in the DOI by setting it as an annotation
        self.bagbldr.update_annotations_for('', {'doi': doi},
                                            message="Setting DOI to "+doi)

        act = Action(Action.PATCH, "#m", who, "DOI assigned", doi)
        if _action:
            _action.add_subaction(act)
        else:
            self.record_history(act)

    @abstractmethod
    def _determine_doi(self):
        """
        return a DOI (in the format expected for the NERDm DOI property--i.e. "doi:...")
        that should be assigned to this SIP according to this SIP's convention.  
        """
        raise NotImplementedError()

    def finalize_version(self, who: Agent, incrfield: int=None, vermsg=None, _action=None) -> str:
        """
        set the version that this dataset should be published under.  The version property is only 
        updated if it has not already been set; that is, only if it is marked with a version string 
        ending in "+ (in edit)".  Otherwise, no changes are made.  Thus, once the version is finalized, 
        it will not be further updated via subsequent calls to this method.  

        If updated, the version is updated by incrementing one of version fields.  Which field should 
        be incremented is determined by _determine_update_level().  

        This method is intended to be called by :py:meth:`ensure_finalize`.

        :param Agent     who:  the agent requesting the finalization
        :param int incrfield:  the position of the version field that should be incremented; if None, 
                               the field will be determined based on what appears to have changed.
        :param str    vermsg:  a message to record in the release history, indicating what changed 
                               with this version.  If None, a default message is set based either on 
                               what apears to have changed or the value of `incrfield`.  It is 
                               recommended that this field be specified if also specifying incrfield.
        """
        hist = _action
        if not hist:
            hist = Action(Action.PATCH, self.id, who, "finalizing version")
            
        self.ensure_preparation(True, who, _action)
        nerd = self.bagbldr.bag.nerd_metadata_for('', True)
        oldver = nerd.get("version", "")
        oldnerdfile = os.path.join(self.bagdir, "__old_nerdm.json")
        oldnerd = None
        ver = None

        if not oldver:
            # this shouldn't happen (unless, possibly, it's never been published before)
            if not os.path.isfile(oldnerdfile):
                ver = "1.0.0"
                if not vermsg:
                    vermsg = "initial release"
            else:
                oldnerd = utils.read_nerd(oldnerdfile)
                oldver = oldnerd.get('version', '1.0.0') + PENDING_VERSION_SFX
        
        elif not oldver.endswith(PENDING_VERSION_SFX):
            self.log.info("finalized version already set as v%s", oldver)
            verrel = [r for r in nerd.get('releaseHistory',{}).get('hasRelease',[])
                        if r.get('version') == oldver]

            if verrel:
                # no need to update release history
                if _action:
                    _action.add_subaction(self._history_comment('#m', who,
                                                                "version already finalized as v%s" % oldver))
                return oldver

            # even though the version is set, we need to update the release history
            ver = oldver

        if oldver and oldver.endswith(PENDING_VERSION_SFX):
            # version ends with the pending suffix
            oldver = oldver[:(-1*len(PENDING_VERSION_SFX))]
            
            if incrfield is not None:
                if not isinstance(incrfield, int):
                    raise TypeError("finalize_version(): incrfield is not an int: "+type(incrfield))
                ver = self._increment_version(oldver, incrfield)
                if not vermsg and incrfield >= 0:
                    vermsg = "minor metadata update" if incrfield > 1 else "major data update"

            else:
                # determine how it will be incremented
                if not oldnerd and os.path.exists(oldnerdfile):
                    oldnerd = utils.read_nerd(oldnerdfile)

                try:
                    (incrfield, why) = self._determine_update_level(nerd, oldnerd)
                    ver = self._increment_version(oldver, incrfield)
                    if not vermsg:
                        vermsg = why
                except SIPStateException as ex:
                    raise SIPStateException("Don't know how to increment version: "+str(ex))

        rh = self._updated_release_history(nerd, ver, vermsg)
        updmd = { 'version': ver, 'releaseHistory': rh }

        self.bagbldr.update_metadata_for('', updmd, message="Setting version to "+ver)
        hist.add_subaction(Action(Action.PATCH, "#m", who, "Setting version to "+ver))
        if not _action:
            self.record_history(hist)
        return ver

    def _increment_version(self, oldver, lev):
        if lev < 0:
            # no change is requested
            return oldver

        parts = oldver.split('.')
        v = []
        for p in parts:
            # throw away non-integer characters
            p = re.sub(r'[^\d].*$', '', p)
            if not p:
                break
            try:
                v.append(int(p))
            except ValueError:
                break
        if not v:
            raise SIPStateException("Don't know how to increment unsupported version: "+oldver)
        for i in range(len(v), max(3, lev+1)):
            v.append(0)
        
        v[lev] += 1
        for i in range(lev+1, len(v)):
            v[i] = 0
        return '.'.join([str(i) for i in v])

    def _updated_release_history(self, nerd, version, message=None):
        defbaseloc = "https://data.nist.gov/od/id/"
        ltst = latest.NERDm2Latest(resolver=self.cfg.get('resolver_base_url', defbaseloc))
        rh = deepcopy(nerd.get('releaseHistory', {}))
        if not rh:
            rh = ltst.create_release_history(nerd)
        myver = [r for r in rh.get('hasRelease',[]) if r.get('version') == version]
        if not myver:
            myver = [ ltst.create_release_ref_for(version, nerd['@id']) ]
            rh['hasRelease'].append(myver[0])
        if message:
            myver[0]['description'] = message

        return rh
            

    @abstractmethod
    def _determine_update_level(self, newmd: Mapping, oldmd: Mapping=None):
        """
        compare the old metadata with an updated version to determine how to increment the version.
        What's expected to be in the metadata is convention-specific; however, generally these will be 
        the full old and revised NERDm Resource metadata.  
        :param Mapping newmd:  The metadata after it was updated
        :param Mapping oldmd:  The metadata before it was updated.  If not provided, attempt to 
                                 determine level based solely on the newmd
        :return:  an (int, str) tuple.  The integer indicates
                  which level of the version to increment where 0 indicates the
                  most significant (i.e. left-most) field should be incremented and 2 indicates the 
                  least significate (of a 3-field version). If the value is higher, the version should 
                  be expanded to at least that level.  A negative value indicates that version should 
                  not be changed; however, most conventions are not expected to support this possibilty. 
                  The string provides a default message indicating why that level was chosen.
        :raise: SIPStateException, if the inputs are insufficient for determining the level
        """
        raise NotImplementedError()

    def record_history(self, action: Action):
        """
        record the given action into the output bag's history log
        """
        if not self.bagdir or not os.path.exists(self.bagdir):
            self.ensure_preparation(True, action.agent)
        if not self._histfile:
            self._histfile = os.path.join(self.bagdir, self.cfg.get('history_filename', 'publish_history.yml'))

        with open(self._histfile, 'a') as fd:
            dump_to_history(action, fd)

    def _putcreate_history_action(self, relid, who, message, old=None, new=None):
        id = self.id + relid
        act = Action.PATCH
        obj = None
        if new is None:
            act = Action.PUT
        if old is None:
            if not relid.startswith('#') and not self.bagbldr.bag.has_component("@id:"+relid):
                act = Action.CREATE
        elif new:
            obj = self._jsondiff(old, new)

        return Action(act, id, who, message, obj)
            
    def _history_comment(self, subj, who, message):
        return Action(Action.COMMENT, subj, who, message)

    def _jsondiff(self, old, new):
        return {"jsonpatch": jsonpatch.make_patch(old, new)}

NERDmBasedBagger.register_data_source_type('fs', import_fs_files)

class PDPBagger(NERDmBasedBagger):
    """
    This bagger is a generic implementation of the NERDmBasedSIPBagger for the PDP API.  It implements 
    a base-level set of SIP assumptions that be used by many publishing clients with no special needs.
    The resulting publications are file-less: no data files are preserved; any data files described 
    are expected to be external.  

    This class will look for the following parameters in the configuration:
    :param str working_dir:             the directory where the default bag parent directory should 
                                           be located.
    :param str sip_bag_dir              the parent directory where the working bag should be located.
                                           If this is a relative directory, it is taken as relative to 
                                           the working directory.  (Default: "sipbags")
    :param Mapping repo_access:         the configuration describing the PDR's APIs; if not provided, 
                                           updating previous publications is not supported
    :param Mapping bag_builder:         the configuration for the BagBuilder instance that will be
                                        used by this bagger (see BagBuilder)
    :param bool hidden_comp_allowed:    if False (default), Hidden type components are not
                                        permitted to be included in the input NERDm metadata.
    :param bool checksum_comp_allowed:  if False (default), ChecksumFile type components are not
                                        permitted to be included in the input NERDm metadata.
    :param str assign_doi:              A flag indicating when a DOI should be assigned to a submission; 
                                        supported values are: "never", "always", "request" (default, i.e.,
                                        only requested on a per submission-basis).  
    :param str doi_naan:                The NAAN to use when determining the DOI to assign.  This is 
                                        required if `assign_doi` is not set to "never"
    :param Mapping publisher_metadata:  a dictionary of common, resource-level NERDm metadata reflective
                                        of the record publisher (e.g. NIST).  These will be added to the 
                                        input NERDm resource record (overriding the input values) before
                                        being saved.
    :param Mapping *_metadata:          a dictionary of resource-level NERDm metadata common to all records
                                        processed under a specific convention, where * is the name of the 
                                        convention.  These will be added to the input NERDm resource record 
                                        (overriding the input values) before being saved.
    :param str default_publisher_md_file:  the path to the file containing (in YAML or JSON format)
                                        a collection of common, resource-level NERDm metadata reflective
                                        of the record publisher (e.g. NIST).  If the path is relative,
                                        it is taken as relative to the OAR etc directory.  Values in this 
                                        file are overridden by metadata specified in "publisher_metadata"
    :param str default_*_md_file:       the path to the file containing (in YAML or JSON format)
                                        a collection of resource-level NERDm metadata common to all records
                                        processed under a specific convention, where * is the name of the 
                                        convention.  Values in this file are overridden by metadata specified 
                                        in "*_metadata".  
    :param Mapping finalize:            the configuration specific to the finalize() function.  See 
                                        BagBuilder.finalize for supported config subparameters; however,
                                        subclasses of this Bagger may support additional parameters.
    """

    _file_importers = { 'fs': import_fs_files }

    def __init__(self, sipid: str, config: Mapping, idminter: PDPMinter, prepsvc: UpdatePrepService=None, 
                 convention: str="pdp0", id:str=None):
        """
        create a base SIPBagger instance

        :param str sipid:        the identifier for the SIP to process
        :param Mapping config:   the configuration for this bagger 
        :param PDPMinter idminter:  the minter to use for creating new PDR identifiers
        :param UpdatePrepService prepsvc:  the UpdatePrepService to use to initialize the working 
                                   bag from a previously published one.  This can be None if either 
                                   the working bag already exists or this type of initialization is 
                                   not necessary/supported.
        :param str convention:   a label indicating the convention that this SIPBagger implements 
                                   (default: pdp0)
        :param str id:           the PDR identifier that should be assigned to this SIP.  If not 
                                   provided and an ID is not yet assigned, idminter must be specified.
        """
        bagparent = config.get('sip_bag_dir', 'sipbags')
        if not bagparent or not os.path.isabs(bagparent):
            if not 'working_dir' in config:
                raise ConfigurationException("PDPBagger: Missing required parameter: working_dir")
            workdir = config['working_dir']
            if not os.path.isdir(workdir):
                raise PublishingStateException("Working directory does not exist (as a directory): "+workdir)
            if not bagparent:
                bagparent = workdir
            else:
                bagparent = os.path.join(workdir, bagparent)
                if not os.path.exists(bagparent):
                    try:
                        os.mkdir(bagparent)
                    except OSError as ex:
                        raise PublishingStateException("Unable to create SIP Bag parent directory: %s: %s" %
                                                       (bagparent, str(ex)))
        if not os.path.isdir(bagparent):
            raise PublishingStateException("SIP Bag parent does not exist (as a directory): "+bagparent)

        if not convention:
            convention = "pdp0"
        super(PDPBagger, self).__init__(sipid, bagparent, config, convention, prepsvc, id)
        self._idmntr = idminter

        self.cfg.setdefault('assign_doi', ASSIGN_DOI_REQUEST)
        if not self.cfg.get('doi_naan') and self.cfg.get('assign_doi') != ASSIGN_DOI_NEVER:
            raise ConfigurationException("Missing configuration: doi_naan")

    def _id_for(self, sipid, mint=False) -> str:
        """
        determine the PDR ID that should be associated with the SIP of the given ID.

        This implementation uses the minter to look up the PDR ID or mint a new one.  

        :param bool mint:  If True, a new identifier should be minted if possible if one is 
                           not currently registered.
        """
        data = {'sipid': sipid}
        matches = self._idmntr.search(data)
        if not matches or len(matches) > 1:
            # no one-to-one match found; this should indicate that one has not been minted yet
            if mint:
                return self._idmntr.mint(data)
            return None

        return matches[0]

    def _aipid_for(self, pdrid):
        """
        determine the AIP ID to be used for preserving datasets with the given PDR ID.

        This implementation assumes PDR IDs take the form of ARK identifiers and the local part
        is used as the AIP ID
        """
        if not pdrid:
            raise PublishingStateException("Unable to determine AIP-ID: PDR-ID is not set yet")
        if not ARK_PFX_RE.match(pdrid):
            raise PublishingStateException("Unexpected PDR ID form: "+pdrid)
        return ARK_PFX_RE.sub('', pdrid).replace('/', ' ')

    def _determine_doi(self):
        localid = self._aipid_for(self.id)
        naan = self.cfg.get('doi_naan')
        if not naan:
            raise PublishingStateException("DOI NAAN not set in configuration")
        return "doi:%s/%s" % (naan, localid)

    def _add_publisher_md(self, resmd) -> None:
        """
        add resource-level metadata that is specific to the publisher (e.g. NIST).  This is pulled from
        system file.
        """
        self._add_std_res_md('publisher', resmd)

    def _add_provider_md(self, resmd: Mapping) -> None:
        """
        add resource-level metadata that is standard to the specific provider.  The provider is 
        normally determined by the shoulder on the PDR identifier ("@id").  
        """
        if '@id' not in resmd:
            raise PublishingStateException("PDR ID not set yet!")
        if not ARK_PFX_RE.match(resmd['@id']):
            raise PublishingStateException("Unexpected PDR ID form: "+resmd['@id'])

        aipid = ARK_PFX_RE.sub('', resmd['@id'])
        shldr = None
        m = re.match(r'^([a-zA-Z][a-zA-Z0-9]+)[-\./:]', aipid)
        if m:
            shldr = m.group(1)
        if not shldr:
            # some very early IDs did not feature a delimiter (e.g. pdr0, mds0)
            m = re.match(r'^[a-zA-Z]{3}\d', aipid)
            if m:
                shldr = m.group()
        if shldr:
            self._add_std_res_md(shldr, resmd)

    def _add_std_res_md(self, shldr: str, resmd: Mapping) -> None:
        shldr = shldr.rstrip('-:./')
        deffnm = 'default_%s_res_metadata.yml' % shldr
        deffnmk = 'default_%s_md_file' % shldr
        
        pubmd = None
        defpubmdfile = self.cfg.get(deffnmk, deffnm)
        defpubmdfile = os.path.join(def_etc_dir, defpubmdfile)
        if os.path.isfile(defpubmdfile):
            try:
                if defpubmdfile.endswith(".yml"):
                    with open(defpubmdfile) as fd:
                        pubmd = yaml.safe_load(fd)
                elif defpubmdfile.endswith(".json"):
                    pubmd = utils.read_json(defpubmdfile)
                else:
                    raise PublishingStateException("Unsupported format for "+defpubmdfile)
            except (OSError, ValueError) as ex:
                raise PublishingStateException("Trouble reading system metadata from "+defpubmdfile+
                                               ": "+str(ex))

        elif shldr == 'publisher' and 'publisher_metadata' not in self.cfg:
            raise ConfigurationException("No publisher metadata configured!")
        else:
            pubmd = OrderedDict()

        mdk = '%s_metadata' % shldr
        pubmd.update(self.cfg.get(mdk, {}))
        resmd.update(pubmd)

    def ensure_finalize(self, who=None, lock=True, _action: Action=None):
        """
        Based on the current state of the bag, finalize its contents to a complete state according to 
        the conventions of this bagger implementation.  After a successful call, the bag should be in 
        a preservable state.
        """
        hist = Action(Action.PATCH, self.id, who, "finalizing bag")
        if _action:
            _action.add_subaction(hist)
        act = Action(Action.COMMENT, self.id, who, "Finalized SIP bag for publishing")
        
        self.ensure_preparation(True, who, hist)

        try:
            # pull in any data still waiting to be imported
            self.ensure_data_files(lock=False, _action=hist)

            self.finalize_version(who, _action=hist)

            # remove use of Submission types
            nerd = self.bagbldr.bag.nerd_metadata_for('', True)
            updmd = {}
            tps = [t for t in nerd.get("@type", []) if not t.endswith("Submission")]
            if tps != nerd.get("@type", []):
                updmd['@type'] = tps
            exts = [s for s in nerd.get("_extensionSchemas", []) if not s.startswith(SIP_SCHEMA_URI)]
            if exts != nerd.get("_extensionSchemas", []):
                updmd['_extensionSchemas'] = exts
            if updmd:
                self.bagbldr.update_metadata_for('', updmd, message="finalize: remove SIP submission types")

            if self.cfg.get('assign_doi') == ASSIGN_DOI_ALWAYS:
                self._ensure_doi(who, hist, nerd)

            self.bagbldr.finalize_bag(self.cfg.get('finalize', {}), True)
            hist.add_subaction(act)

        except Exception as ex:
            self.log.warning("Failed to complete finalization: "+str(ex))
            act = Action(Action.COMMENT, self.id, who, "Failed to complete finalization request")
            self.record_history(act)
            raise

        finally:
            if not _action:
                self.record_history(hist)

    def _determine_update_level(self, newmd: Mapping, oldmd: Mapping=None):
        """
        compare the old metadata with an updated version to determine how to increment the version.
        What's expected to be in the metadata is convention-specific; however, generally these will be 
        the full old and revised NERDm Resource metadata.  
        :param Mapping newmd:  The metadata after it was updated
        :param Mapping oldmd:  The metadata before it was updated.  If not provided, attempt to 
                                 determine level based solely on the newmd
        :return:  an integer indicating which level of the version to increment where 0 indicates that 
                  the most significant (i.e. left-most) field should be incremented and 2 indicates the 
                  least significate (of a 3-field version). If the value is higher, the version should 
                  be expanded to at least that level.  A negative value indicates that version should 
                  not be changed; however, most conventions are not expected to support this possibilty. 
        :raise: SIPStateException, if the inputs are insufficient for determining the level
        """
        if not oldmd:
            # not built from a previous version
            return (-1, '')

        # data change (level 1):
        #  *  a file is updated (not applicable to pdp0)
        #  *  a file is added or subtracted
        #  *  an access page is added or deleted
        #
        oldfiles = set([c.get('@id','') for c in oldmd.get('components',[])
                                        if nerdutils.is_any_type(["DataFile", "AccessPage"])])
        newfiles = set([c.get('@id','') for c in oldmd.get('components',[]) 
                                        if nerdutils.is_any_type(["DataFile", "AccessPage"])])

        # any files in the data directory?
        updatedfiles = False
        for dir, sd, files in os.walk(self.bldr.bag.data_dir):
            if files:
                updatedfiles = True
                break

        if newfiles > oldfiles:
            return (1, "data links added")
        elif oldfiles > newfiles:
            return (1, "data links removed")
        elif oldfiles != newfiles:
            return (1, "data links added and removed")
        elif updatedfiles:
            return (1, "data files updated")

        # metadata change only
        return (2, "metadata updates only")
    
