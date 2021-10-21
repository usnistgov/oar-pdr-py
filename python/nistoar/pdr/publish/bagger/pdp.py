"""
This module provides a base implementations of the SIPBagger interface to support PDR's Programmatic Data 
Publishing (PDP) API.  In this framework, SIP inputs are primarily in the form of NERDm metadata.
"""
import os, re, logging, json
from collections import OrderedDict, Mapping
from abc import abstractmethod, abstractproperty
from copy import deepcopy
from urllib.parse import urlparse

import yaml, jsonpatch

from .. import BadSIPInputError, PublishingStateException
from ...constants import ARK_PFX_PAT
from ....nerdm.constants import CORE_SCHEMA_URI, PUB_SCHEMA_URI, EXP_SCHEMA_URI, core_schema_base
from ....nerdm import utils as nerdutils
from ....nerdm.convert.latest import update_to_latest_schema
from ...preserve.bagit.builder import BagBuilder
from ... import def_etc_dir
from .base import SIPBagger
from .prepupd import UpdatePrepService
from ..idmint import PDPMinter
from ..prov import Action, PubAgent, dump_to_history

SIPEXT_RE = re.compile(core_schema_base + r'sip/(v[^/]+)#/definitions/\w+Submission')
ARK_PFX_RE = re.compile(ARK_PFX_PAT)

def _insert_before_val(vals, inval, beforeval):
    try:
        vals.insert(vals.index(beforeval), inval)
    except ValueError:
        vals.append(inval)


class NERDmBasedBagger(SIPBagger):
    """
    An abstract SIPBagger that accepts NERDm metadata as its primarily inputs.

    This base class will look for the following parameters in the configuration:
    :param Mapping repo_access:         the configuration describing the PDR's APIs 
    :param Mapping bag_builder:         the configuration for the BagBuilder instance that will be
                                        used by this bagger (see BagBuilder)
    :param bool hidden_comp_allowed:    if False (default), Hidden type components are not
                                        permitted to be included in the input NERDm metadata.
    :param bool checksum_comp_allowed:  if False (default), ChecksumFile type components are not
                                        permitted to be included in the input NERDm metadata.
    """
    
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
        if not self.prepsvc and not os.path.exists(self.bagdir) and 'repo_access' in self.cfg:
            self.log.warn("Bagger operating without an UpdatePrepService!")
            # raise ValueError("NERDmBasedBagger: requires a UpdatePrepService instance for new bag")

        self.prepared = False
        self._nerdmcore_re = None
        if self.cfg.get('required_core_nerdm_version'):
            self._nerdmcore_re = re.compile(core_schema_base + r'(' + 
                                            self.cfg['required_core_nerdm_version'] + r')#')

        self._histfile = None

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

    def ensure_preparation(self, nodata: bool=False, who=None) -> None:
        """
        create and update the output working bag directory to ensure it is 
        a re-organized version of the SIP, ready for updates.

        :param nodata bool: if True, do not copy (or link) data files to the output directory.  
                            In this implementation, this parameter is ignored
        """
        if not self._id:
            self._id = self._id_for(self.sipid, True)
        self.ensure_base_bag(who)

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

    def ensure_base_bag(self, who=None) -> None:
        """
        Establish an initial working bag.  If a working bag already exists, it 
        will be used as is.  Otherwise, this method will check to see if a 
        resource with with the same SIP identifier has been published before;
        if so, its metadata (with version information updated) will be used to 
        create the initial bag.  If not, it is assumed that this is a new 
        SIP that has never been submitted before; a new bag directory will be 
        created and identifiers will be assigned to it.  
        """
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
                annotf = self.bagbldr.annotations_file_for('')
                if os.path.exists(annotf):
                    os.remove(annotf)
                nerdm = self.bagbldr.bag.nerd_metadata_for('', False)
                locked_md = OrderedDict([("pdr:sipid", self.sipid)])
                for prop in "pdr:aipid firstIssued bureauCode, programeCode".split():
                    if prop in nerdm:
                        locked_md = nerdm[prop]
                self._add_publisher_md(locked_md)
                self._add_provider_md(locked_md)
                self.bagbldr.update_annotations_for('', lockedmd,
                                                    message="locking convention metadata as annotations")

                # add a history record
                self.record_history(
                    Action(Action.COMMENT, '', who,
                           "Initialized update based on version " + nerdm.get('version', '1.0'))
                )

        if not os.path.exists(self.bagdir):
            self.bagbldr.ensure_bag_structure()
            self.bagbldr.assign_id(self.id)

            # set some minimal metadata
            minimal_md = OrderedDict([
                ("@id", self.id),
                ("version", "1.0.0"),
                ("pdr:sipid", self.sipid),
                ("pdr:aipid", self._aipid_for(self.id))
            ])
            self._add_publisher_md(minimal_md)
            self._add_provider_md(minimal_md)
            self.bagbldr.update_metadata_for('', minimal_md, message='initial minimal metadata established')

            del minimal_md['version']
            self.bagbldr.update_annotations_for('', minimal_md,
                                                message="locking convention metadata as annoations")

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

    def describe(self, relid: str = '', who: PubAgent = None) -> Mapping:
        """
        return a NERDm description for the part of the dataset pointed to by the given identifier
        relative to the dataset's base identifier.
        :param str relid:  the relative identifier for the part of interest.  If an empty string
                           (default), the full NERDm record will be returned.
        :param who:        an actor identifier object, indicating who is requesting the data.  This 
                           request may trigger the restaging of previously published data, in which 
                           case who triggered it will get recorded.  If None, an internal administrative 
                           identity will be assumed.  
        """
        if not self.bagdir or not os.path.exists(self.bagdir):
            self.prepare(False, who)
        return self.bag.describe(relid)

    def set_res_nerdm(self, nerdm: Mapping, who: PubAgent = None, savefilemd: bool=True,
                      lock: bool=True) -> None:
        """
        set the resource metadata (which may optionally include file component metadata) for the SIP.  
        The input metadata should be as complete as is appropriate for the type of SIP being processed.  

        :param Mapping nerdm:  the resource-level NERDm metadata to save
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :param bool savefilemd:  if True (default), any DataFile or Subcollection metadata included will 
                                 be saved as well
        """
        if lock:
            self.ensure_filelock()
            with self.lock:
                self._set_res_nerdm(nerdm, who, savefilemd)

        else:
            self._set_res_nerdm(nerdm, who, savefilemd)

    def _set_res_nerdm(self, nerdm: Mapping, who: PubAgent=None, savecompmd: bool=True) -> None:
        """
        set the resource metadata (which may optionally include file component metadata) for the SIP.  
        The input metadata should be as complete as is appropriate for the type of SIP being processed.  

        :param Mapping nerdm:  the resource-level NERDm metadata to save
        :param who:         an actor identifier object, indicating who is requesting this action.  This 
                            will get recorded in the history data.  If None, an internal administrative 
                            identity will be assumed.  This identity may affect the identifier assigned.
        :param bool savecompmd:  if True (default), any DataFile or Subcollection metadata included will 
                                 be saved as well, replacing all previously set components.
        """
        nerdm = self._check_res_schema_id(nerdm)   # creates a deep copy of the record

        self.ensure_preparation(True, who)

        # modify the input: remove properties that cannot be set, add others
        handsoff = "@id @context publisher issued firstIssued revised annotated language " + \
                   "bureauCode programCode doi"
        for prop in handsoff.split():
            if prop in nerdm:
                del nerdm[prop]
        self._set_standard_res_modifications(nerdm)
        self._set_provider_res_modifications(nerdm)

        components = nerdm.get('components')
        if 'components' in nerdm:
            nerdm['components'] = []

        # set up history record (using who)
        hist = self._putcreate_history_action("#m", who, "Set resource metadata")

        try:
            old = self.bagbldr.bag.nerd_metadata_for('', True)   # for history record

            self.bagbldr.add_res_nerd(nerdm, False)

            new = self.bagbldr.bag.nerd_metadata_for('', True)   # for history record
            hist.add_subaction(self._putcreate_history_action("#m", who, "Set resource-level metadata",
                                                              old, new))

            if savecompmd and components:
                # clear out any previously saved components
                oldcmps = self.bagbldr.bag.subcoll_children('')
                if oldcmps:
                    for cmp in oldcmps:
                        self.bagbldr.remove_component(cmp)
                    hist.add_subaction(Action(Action.DELETE, "pdr:f", who,
                                              "Cleared previously added components"))

                for cmp in components:
                    self._set_comp_nerdm(cmp, who, hist, False)
            else:
                hist = hist.subactions[0]

        except Exception:
            self.log.warning("Bag left in possible incomplete state due to error")
            self.record_history(hist)
            hist = self._history_comment("#m", who, "Failed to complete %s action" % hist.type)
            raise

        finally:
            # record history record
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

        nerdm = update_to_latest_schema(nerdm, False)
        return nerdm

    def _set_standard_res_modifications(self, resmd):
        # update the types
        types = resmd.setdefault('@type', [])
        while 'nrds:PDRSubmission' in types:
            types.remove('pdr:Submission')
        if 'nrd:Resource' not in types:
            types.append('nrd:Resource')
        if 'nrd:PublicDataResource' not in types:
            _insert_before_val(types, 'nrd:PublicDataResource', 'nrd:Resource')
        if nerdutils.is_type(resmd, 'PublicDataResource') and 'authors' in resmd and len(resmd['authors']) > 0:
            _insert_before_val(types, 'nrdp:DataPublication', 'nrd:PublicDataResource')

        extschs = set([s for s in resmd.get('_extensionSchemas', []) if not SIPEXT_RE.match(s)])
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
        if extschs:
            resmd['_extensionSchemas'] = list(extschs)

        if 'contactPoint' in resmd:
            resmd['contactPoint']['@type'] = "vcard:Contact"

    def _set_provider_res_modifications(self, resmd: Mapping):
        """
        modify the given resource NERDm metadata provided by the client with changes 
        appropriate for the provider context.  The provider is normally determined by the 
        shoulder on the PDR identifier ("@id").  This should be overridden for by 
        convention-specific subclasses.
        """
        return

    def set_comp_nerdm(self, nerdm: Mapping, who: PubAgent=None, lock: bool=True) -> None:
        """
        set the metadata for a component of the resource.  If the component represents a file or 
        a subcollection, it must contain a 'filepath' property.  
        :param Mapping nerdm:   the NERDm Component metadata.  
        """
        if lock:
            self.ensure_filelock()
            with self.lock:
                self._set_comp_nerdm(nerdm, who)

        else:
            self._set_comp_nerdm(nerdm, who)

    def _set_comp_nerdm(self, nerdm: Mapping, who: PubAgent=None, hist=None, tolatest=True) -> None:
        nerdm = self._check_input_comp(nerdm, tolatest)   # copies nerdm

        self.ensure_preparation(True, who)

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
        else:
            self.record_history(act)

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

            compmd = update_to_latest_schema(compmd, False)
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
                cmpmd['@id'] = "pdr:f" + '/' + cmpmd['filepath']

            elif nerdutils.is_type(cmpmd, "AccessPage"):
                cmpmd['@id'] = "pdr:see" + '/'
                url = urlparse(cmpmd['accessURL'])
                if url.netlog == 'doi.org':
                    cmpmd['@id'] += "doi:"
                elif url.netlog == 'handle.net':
                    cmpmd['@id'] += "hdl:"
                else:
                    cmpmd['@id'] += netloc 
                if url.path and url.path != '/':
                    cmpmd['@id'] += url.path

            elif nerdutils.is_type(cmpmd, "IncludedResource"):
                cmpmd['@id'] = "pdr:agg" + '/'
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

    def delete(self, lock=True):
        """
        delete the working bag from store; this sets the bagger to a virgin state.
        """
        if lock:
            self.ensure_filelock()
            with self.lock:
                self.bagbldr.destroy()
        else:
            self.bagbldr.destroy()

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
        if not ARK_PFX_RE.match(pdrid):
            raise PublishingStateException("Unexpected PDR ID form: "+pdrid)
        return ARK_PFX_RE.sub('', pdrid).replace('/', ' ')

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
                    pubmd = read_json(defpubmdfile)
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

    def ensure_finalize(self, who=None, lock=True):
        """
        Based on the current state of the bag, finalize its contents to a complete state according to 
        the conventions of this bagger implementation.  After a successful call, the bag should be in 
        a preservable state.
        """
        self.ensure_preparation(True, who)
        try:
            self.record_history(Action(Action.COMMENT, '', who, "Finalized SIP bag for publishing"))
            self.bagbldr.finalize_bag(self.cfg.get('finalize', {}), True)
        except Exception:
            self.record_history(Action(Action.COMMENT, '', who, "Failed to complete finalization request"))

    def record_history(self, action: Action):
        """
        record the given action into the output bag's history log
        """
        if not self.bagdir or not os.path.exists(self.bagdir):
            self.ensure_preparation(True, action.agent)
        if not self._histfile:
            self._histfile = os.path.join(self.bagdir, self.cfg.get('history_filename', 'publish.history'))

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



