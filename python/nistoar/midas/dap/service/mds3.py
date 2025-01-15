"""
The DAP Authoring Service implemented using the mds3 convention.  This convention represents the 
first DAP convention powered by the DBIO APIs.

The key features of the mds3 conventions are:
  * The data record being created/updated through the service is a NERDm Resource
  * The NERDm Resource record is stored separately from the DBIO 
    :py:class:`~nistoar.midas.dbio.base.ProjectRecord` (via the 
    :py:mod:`~nistoar.midas.dap.nerdstore` module).  The ``data`` property of the 
    :py:class:`~nistoar.midas.dbio.base.ProjectRecord` contains a summary (i.e. a subset of 
    properties) of the NERDm record.  
  * Conventions and heuristics are applied for setting default values for various NERDm 
    properties based on the potentially limited properties provided by the client during the 
    editing process.  (These conventions and hueristics are implemented the in various 
    ``_moderate_*`` functions in the :py:class:`DAPService` class.)

Support for the web service frontend is provided via :py:class:`DAPApp` class, an implementation
of the WSGI-based :ref:class:`~nistoar.pdr.publish.service.wsgi.ServiceApp`.
"""
import os, re, pkg_resources, random, string, time, math
from datetime import datetime
from logging import Logger
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Sequence, Callable
from typing import List, Union, Iterator
from copy import deepcopy
from urllib.parse import urlparse
from functools import reduce

from ...dbio import (DBClient, DBClientFactory, ProjectRecord, AlreadyExists, NotAuthorized, ACLs,
                     InvalidUpdate, ObjectNotFound, PartNotAccessible, NotEditable,
                     ProjectService, ProjectServiceFactory, DAP_PROJECTS)
from ...dbio.wsgi.project import (MIDASProjectApp, ProjectDataHandler, ProjectInfoHandler,
                                  ProjectSelectionHandler, ServiceApp)
from ...dbio import status
from nistoar.base.config import ConfigurationException, merge_config
from nistoar.nerdm import constants as nerdconst, utils as nerdutils
from nistoar.pdr import def_schema_dir, def_etc_dir, constants as const
from nistoar.pdr.utils import build_mime_type_map, read_json
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.nsd import NSDServerError

from . import validate
from .. import nerdstore
from ..nerdstore import NERDResource, NERDResourceStorage, NERDResourceStorageFactory, NERDStorageException
from ..fm import FileManager, FileSpaceNotFound, FileSpaceException

ASSIGN_DOI_NEVER   = 'never'
ASSIGN_DOI_ALWAYS  = 'always'
ASSIGN_DOI_REQUEST = 'request'

NERDM_PRE = "nrd"
NERDM_SCH_ID_BASE = nerdconst.core_schema_base
NERDM_SCH_VER = nerdconst.schema_versions[0]
NERDM_SCH_ID = NERDM_SCH_ID_BASE + NERDM_SCH_VER + "#"
NERDM_DEF = NERDM_SCH_ID + "/definitions/"
NERDM_CONTEXT = "https://data.nist.gov/od/dm/nerdm-pub-context.jsonld"

NERDMPUB_PRE = "nrdp"
NERDMPUB_SCH_ID_BASE = nerdconst.core_schema_base + "pub/"
NERDMPUB_SCH_VER = NERDM_SCH_VER
NERDMPUB_SCH_ID = NERDMPUB_SCH_ID_BASE + NERDMPUB_SCH_VER + "#"
NERDMPUB_DEF = NERDMPUB_SCH_ID + "/definitions/"

NERDMAGG_PRE = "nrda"
NERDMAGG_SCH_ID_BASE = nerdconst.core_schema_base + "agg/"
NERDMAGG_SCH_VER = nerdconst.agg_ver
NERDMAGG_SCH_ID = NERDMAGG_SCH_ID_BASE + NERDMAGG_SCH_VER + "#"
NERDMAGG_DEF = NERDMAGG_SCH_ID + "/definitions/"

NERDMEXP_PRE = "nrde"
NERDMEXP_SCH_ID_BASE = nerdconst.core_schema_base + "exp/"
NERDMEXP_SCH_VER = nerdconst.exp_ver
NERDMEXP_SCH_ID = NERDMEXP_SCH_ID_BASE + NERDMEXP_SCH_VER + "#"
NERDMEXP_DEF = NERDMEXP_SCH_ID + "/definitions/"

NERDMSW_PRE = "nrdw"
NERDMSW_SCH_ID_BASE = nerdconst.core_schema_base + "sw/"
NERDMSW_SCH_VER = NERDM_SCH_VER
NERDMSW_SCH_ID = NERDMSW_SCH_ID_BASE + NERDMSW_SCH_VER + "#"
NERDMSW_DEF = NERDMSW_SCH_ID + "/definitions/"

NERDMBIB_PRE = "nrdw"
NERDMBIB_SCH_ID_BASE = nerdconst.core_schema_base + "bib/"
NERDMBIB_SCH_VER = nerdconst.bib_ver
NERDMBIB_SCH_ID = NERDMBIB_SCH_ID_BASE + NERDMBIB_SCH_VER + "#"
NERDMBIB_DEF = NERDMBIB_SCH_ID + "/definitions/"

NIST_NAME = "National Institute of Standards and Technology"
NIST_ABBREV = "NIST"
NIST_ROR = "ror:05xpvk416"

VER_DELIM = const.RELHIST_EXTENSION.lstrip('/')
FILE_DELIM = const.FILECMP_EXTENSION.lstrip('/')
LINK_DELIM = const.LINKCMP_EXTENSION.lstrip('/')
AGG_DELIM = const.AGGCMP_EXTENSION.lstrip('/')
RES_DELIM = const.RESONLY_EXTENSION.lstrip('/')
EXTSCHPROP = "_extensionSchemas"

def random_id(prefix: str="", n: int=8):
    r = ''.join(random.choices(string.ascii_uppercase + string.digits, k=n))
    return prefix+r

class DAPProjectRecord(ProjectRecord):
    """
    a DBIO record specifically representing a DAP Project
    """

    def __init__(self, recdata: Mapping, dbclient: DBClient=None, fmclient: FileManager=None):
        super(DAPProjectRecord, self).__init__(DAP_PROJECTS, recdata, dbclient)
        self._fmcli = fmclient

    @classmethod
    def from_dap_record(cls, prec: ProjectRecord, fmclient: FileManager=None):
        return cls(prec._data, prec._cli, fmclient)

    def _initialize(self, rec: MutableMapping) -> MutableMapping:
        rec = super(DAPProjectRecord, self)._initialize(rec)
        
        if 'file_space' not in rec:
            rec['file_space'] = OrderedDict([
                ('id', rec.get('id')),
                ('action', ''),
                ('message', '')
            ])

        return rec

    @property
    def file_space(self):
        """
        a summary of the current status of the file management space associated with this record.
        An empty object indicates that the space may not exist, yet.
        """
        return self._data.get('file_space')

    def file_space_is_ready(self) -> bool:
        return bool(self.file_space and self.file_space.get('creator'))

    def ensure_file_space(self, who: str=None):
        """
        ensure that the file space holding user-uploaded files exist: if it doesn't, create it
        """
        if not self._fmcli:
            return
        if not self._data.get('file_space', {}).get('creator'):
            if not who:
                who = self._cli._who
            try:
                self._fmcli.get_record_space(self.id)
            except FileSpaceNotFound as ex:
                try:
                    self._data['file_space']['action'] = "create"
                    self._fmcli.create_record_space(who, self.id)
                    self._data['file_space']['created'] = \
                        datetime.fromtimestamp(math.floor(time.time())).isoformat()
                    self._data['file_space']['creator'] = who

                except FileSpaceException as ex:
                    self.log.error("Problem creating file space: %s", str(ex))
                    self._data['file_space']['message'] = "Failed to create file space"
                    raise
            else:
                self._data['file_space']['creator'] = who
                self._data['file_space']['id'] = self.id  # the ID of the space is the same as the rec

    def determine_uploads_url(self):
        """
        return the expected URL for the browser-based view of a record space's uploads directory.
        """
        fs = self.file_space
        if self._fmcli and fs and fs.get('uploads_dir_id'):
            return f"{self._fmcli.web_base}/{fs['uploads_dir_id']}?dir=/{self.id}/{self.id}"
        else:
            return f"/{self.id}/{self.id}"

    def to_dict(self):
        out = super().to_dict()
        if self._fmcli and out.get('file_space') and out['file_space'].get('id'):

            out['file_space']['location'] = self.determine_uploads_url()

            if self._fmcli.cfg.get('dav_base_url'):
                out['file_space']['uploads_dav_url'] = \
                    '/'.join([self._fmcli.cfg['dav_base_url'].rstrip('/'),
                              out['file_space'].get('id'), out['file_space'].get('id')])
        return out


to_DAPRec = DAPProjectRecord.from_dap_record


class DAPService(ProjectService):
    """
    a project record request broker class for DAP records.  

    This service allows a client to create and update DAP records in the form of NERDm Resource 
    records according to the mds3 conventions.  See this 
    :py:module:`module's documentation <nistoar.midas.dap.service.mds3>` for 
    a summary of the supported conventions.

    In addition to the configuration parameters supported by the parent class, this specialization
    also supports the following parameters:

    ``assign_doi``
        a label that indicates when a DOI should be assigned to new records.  Supported values include:
        * ``always``  -- assign a DOI to every newly created record
        * ``request`` -- assign a DOI on when requested by the system
        * ``never``   -- never assign a DOI to a record.
    ``doi_naan``
        the Name Assigning Authority Number to use for the DOI (given as a string)
    ``validate_nerdm``
        if True (default), validate the updates to NERDm metadata.  Incomplete NERDm records are 
        permitted.
    ``mimetype_files``
        a list of paths to files containing MIME-type to file extension maps used to assign a MIME-type
        (i.e. ``mediaType``) to a file component.  Any path given as a relative path will be assumed to 
        be relative to OAR_ETC_DIR.  If this parameter not set, a default map is loaded as a package 
        resource, ``data/mime.types``, under ``nistoar.pdr``.  The format is that supported by the Apache 
        and Nginx web servers.
    ``file_format_maps``
        a list of paths to files containing file extension to file format maps used to attach a file 
        format description to to a file component.  Any path given as a relative path will be assumed to 
        be relative to OAR_ETC_DIR.  If this parameter not set, a default map is loaded as a package 
        resource, ``data/fext2format.json``, under ``nistoar.pdr``.  The format of such files is a 
        JSON-encoded object with file extensions as the keys, and string descriptions of formats
        as values.  
    ``file_manager``
        a dictionary of properties configuring access to the file manager; if not used, a file
        manager will not be used to get file information.  
    ``default_responsible_org``
        a dictionary containing NERDm Affiliation metadata to be provided as the default to the 
        ``responsibleOrganization`` NERDm property. 

    Note that the DOI is not yet registered with DataCite; it is only internally reserved and included
    in the record NERDm data.  
    """

    def __init__(self, dbclient_factory: DBClientFactory, config: Mapping={}, who: Agent=None,
                 log: Logger=None, nerdstore: NERDResourceStorage=None, project_type=DAP_PROJECTS,
                 minnerdmver=(0, 6), fmcli=None, nsdsvc=None):
        """
        create the service
        :param DBClientFactory dbclient_factory:  the factory to create the DBIO service client from
        :param dict       config:  the service configuration tuned for the current type of project
        :param Agent         who:  the agent that describe who/what is using this service
        :param Logger        log:  the logger to use for log messages
        :param NERDResourceStorage nerdstore:  the NERD metadata storage backend to use; if None,
                                   a backend will be constructed based on the configuration
        :param str       project:  the type of project being accessed (default: DAP_PROJECTS)
        :param tuple minnerdmver:  a 2-tuple indicating the minimum version of the core NERDm schema
                                   required by this implementation; this is intended for use by 
                                   subclass constructors.
        :param FileManager fmcli:  The FileManager client to use; if None, one will be constructed 
                                   from the configuration.
        """
        super(DAPService, self).__init__(project_type, dbclient_factory, config, who, log,
                                         _subsys="Digital Asset Publication Authoring System",
                                         _subsysabbrev="DAP")

        self._fmcli = fmcli
        if config.get("file_manager"):
            if not self._fmcli:
                self._fmcli = self._make_fm_client(config['file_manager'])
            if config.get("nerdstorage") is not None and not config["nerdstorage"].get("file_manager"):
                config['nerdstorage']['file_manager'] = config["file_manager"]
        if not nerdstore:
            nerdstore = NERDResourceStorageFactory().open_storage(config.get("nerdstorage", {}), log)
        self._store = nerdstore

        self.cfg.setdefault('assign_doi', ASSIGN_DOI_REQUEST)
        if not self.cfg.get('doi_naan') and self.cfg.get('assign_doi') != ASSIGN_DOI_NEVER:
            raise ConfigurationException("Missing configuration: doi_naan")

        self._schemadir = self.cfg.get('nerdm_schema_dir', def_schema_dir)
        self._valid8r = None
        if self.cfg.get('validate_nerdm', True):
            if not self._schemadir:
                raise ConfigurationException("'validate_nerdm' is set but cannot find schema dir")
            self._valid8r = validate.create_lenient_validator(self._schemadir, "_")

        self._mediatypes = None
        self._formatbyext = None

        self._minnerdmver = minnerdmver

    def _make_fm_client(self, fmcfg):
        return FileManager(fmcfg)

    def _choose_mediatype(self, fext):
        defmt = 'application/octet-stream'

        if not self._mediatypes:
            mtfiles = [f if os.path.isabs(f) else os.path.join(def_etc_dir, f)
                         for f in self.cfg.get('mimetype_files', [])]
            if not mtfiles:
                mtfiles = [pkg_resources.resource_filename('nistoar.pdr', 'data/mime.types')]
            self._mediatypes = build_mime_type_map(mtfiles)

        return self._mediatypes.get(fext, defmt)

    def _guess_format(self, file_ext, mimetype=None):
        if not mimetype:
            mimetype = self._choose_mediatypes(file_ext)

        if self._formatbyext is None:
            fmtfiles = [f if os.path.isabs(f) else os.path.join(def_etc_dir, f)
                         for f in self.cfg.get('file_format_maps', [])]
            if not fmtfiles:
                fmtfiles = [pkg_resources.resource_filename('nistoar.pdr', 'data/fext2format.json')]
            self._formatbyext = {}
            for f in fmtfiles:
                try:
                    fmp = read_json(f)
                    if not isinstance(fmp, Mapping):
                        raise ValueError("wrong format for format-map file: contains "+type(fmp))
                    if fmp:
                        self._formatbyext.update(fmp)
                except Exception as ex:
                    self.log.warning("Unable to fead format-map file, %s: %s", f, str(ex))
            
        fmtd = self._formatbyext.get(file_ext)
        if fmtd:
            return { "description": fmtd }
        return None

    def get_record(self, id) -> ProjectRecord:
        """
        fetch the project record having the given identifier
        :raises ObjectNotFound:  if a record with that ID does not exist
        :raises NotAuthorized:   if the record exists but the current user is not authorized to read it.
        """
        return to_DAPRec(super().get_record(id), self._fmcli)

    def create_record(self, name, data=None, meta=None) -> ProjectRecord:
        """
        create a new project record with the given name.  An ID will be assigned to the new record.
        :param str  name:  the mnuemonic name to assign to the record.  This name cannot match that
                           of any other record owned by the user. 
        :param dict data:  the initial data content to assign to the new record.  
        :param dict meta:  the initial metadata to assign to the new record.  
        :raises NotAuthorized:  if the authenticated user is not authorized to create a record
        :raises AlreadyExists:  if a record owned by the user already exists with the given name
        :raises InvalidUpdate:  if the data given in either the ``data`` or ``meta`` parameters are
                                invalid (i.e. is not compliant with schemas and restrictions asociated
                                with this project type).
        """
        shoulder = self._get_id_shoulder(self.who)
        prec = to_DAPRec(self.dbcli.create_record(name, shoulder), self._fmcli)
        nerd = None

        # create the space in the file-manager
        if self._fmcli:
            prec.ensure_file_space(self.who.actor)
                
        try:
            if meta:
                meta = self._moderate_metadata(meta, shoulder)
                if prec.meta:
                    self._merge_into(meta, prec.meta)
                else:
                    prec.meta = meta
            elif not prec.meta:
                prec.meta = self._new_metadata_for(shoulder)

            # establish the version of NERDm we're using
            schemaid = None
            if data and data.get("_schema"):
                schemaid = data["_schema"]
                m = re.search(r'/v(\d+\.\d+(\.\d+)*)#?$', schemaid)
                if schemaid.startswith(NERDM_SCH_ID_BASE) and m:
                    ver = m.group(1).split('.')
                    for i in range(len(ver)):
                        if i >= len(self._minnerdmver):
                            break;
                        if ver[i] < self._minnerdmver[1]:
                            raise InvalidUpdate("Requested NERDm schema version, " + m.group(1) +
                                                " does not meet minimum requirement of " +
                                                ".".join(self._minnerdmver), sys=self)
                else:
                    raise InvalidUpdate("Unsupported schema for NERDm schema requested: " + schemaid,
                                        sys=self)
            
            # create a record in the metadata store
            if self._store.exists(prec.id):
                self.log.warning("NERDm data for id=%s unexpectedly found in metadata store", prec.id)
            self._store.load_from(self._new_data_for(prec.id, prec.meta, schemaid), prec.id)
            nerd = self._store.open(prec.id)
            if prec._data.get('file_space') and self._fmcli and hasattr(nerd.files, 'update_hierarchy'):
                try:
                    prec._data['file_space'].update(nerd.files.update_hierarchy())  # space should be empty
                    if prec.file_space.get('file_count', -2) < 0:
                        self.log.warning("Failed to initialize file listing from file manager")
                except Exception as ex:
                    self.log.exception("Failed to initialize file listing: problem accessing file manager: %s",
                                       str(ex))
            prec.data = self._summarize(nerd)

            if data:
                self._update_data(prec.id, data, prec=prec, nerd=nerd)  # this will call prec.save()
            else:
                prec.save()

        except Exception as ex:
            if nerd:
                try:
                    self._store.delete(prec.id)
                except Exception as ex:
                    self.log.error("Error while cleaning up NERDm data after create failure: %s", str(ex))
            try:
                self.dbcli.delete_record(prec.id)
            except Exception as ex:
                self.log.error("Error while cleaning up DAP record after create failure: %s", str(ex))
            raise

        self._record_action(Action(Action.CREATE, prec.id, self.who, prec.status.message))
        self.log.info("Created %s record %s (%s) for %s", self.dbcli.project, prec.id, prec.name, self.who)

        return prec

    def _new_data_for(self, recid, meta=None, schemaid=None):
        if not schemaid:
            schemaid = NERDM_SCH_ID
        out = OrderedDict([
            ("_schema", schemaid),
            ("@context", NERDM_CONTEXT),
            (EXTSCHPROP, [NERDMPUB_DEF + "PublicDataResource"]),
            ("@id", self._arkid_for(recid)),
            ("@type", [NERDMPUB_PRE + ":PublicDataResource", "dcat:Resource"])
        ])

        if self.cfg.get('assign_doi') == ASSIGN_DOI_ALWAYS:
            out['doi'] = self._doi_for(recid)

        if meta:
            if meta.get("resourceType"):
                addtypes = []
                if meta['resourceType'].lower() == "software":
                    addtypes = [":".join([NERDMSW_PRE, "SoftwarePublication"])]
                elif meta['resourceType'].lower() == "srd":
                    addtypes = [":".join([NERDMPUB_PRE, "SRD"])]
                out["@type"] = addtypes + out["@type"]

            if meta.get("softwareLink"):
                swcomp = self._get_sw_desc_for(meta["softwareLink"])
                if not 'components' in out:
                    out['components'] = []
                out['components'] = [swcomp] + out['components']

            # contact info
            if meta.get("creatorisContact"):
                cp = OrderedDict()
                if self.who.get_prop("userName") and self.who.get_prop("userLastName"):
                    cp['fn'] = f"{self.who.get_prop('userName')} {self.who.get_prop('userLastName')}"
                if self.who.get_prop("email"):
                    cp['hasEmail'] = self.who.get_prop("email")
                if cp:
                    out['contactPoint'] = cp
            elif meta.get("contactName"):
                out['contactPoint'] = self._moderate_contactPoint({"fn": meta["contactName"]}, doval=False)

            ro = deepcopy(self.cfg.get('default_responsible_org', {}))
            if self.dbcli.people_service and out.get('contactPoint', {}).get('hasEmail'):
                ps = self.dbcli.people_service
                try:
                    conrec = ps.get_person_by_email(email=out['contactPoint']['hasEmail'])
                except NSDServerError as ex:
                    self.log.warning("Unable to get org info on contact from NSD service: %s", str(ex))
                except Exception as ex:
                    self.log.exception("Unexpected error while accessing NSD service: %s", str(ex))
                else:
                    if conrec:
                        ro['subunits'] = []
                        if not ro.get('title'):
                            ro['title'] = conrec.get("ouName", "")
                        else:
                            ro['subunits'].append(conrec.get("ouName", ""))
                        ro['subunits'].append(conrec.get("divisionName", ""))
                        ro['subunits'].append(conrec.get("groupName", ""))
            if ro:
                out['responsibleOrganization'] = [ ro ]

        return out

    def _get_sw_desc_for(self, link):
        # id = link.rsplit('/', 1)[-1]
        # id = "%s/repo:%s" % (const.LINKCMP_EXTENSION.lstrip('/'), id)   # let moderate handle this
        out = OrderedDict([
            ("@type", ["nrdp:AccessPage"]),
            ("title", "Software Repository"),
            ("accessURL", link)
        ])
        if link.startswith("https://github.com/"):
            out['title'] += " in GitHub"
        return out

    def _doi_for(self, recid):
        naan = self.cfg.get('doi_naan')
        if not naan:
            raise PublishingStateException("DOI NAAN not set in configuration")
        return "doi:%s/%s" % (naan, self._aipid_for(recid))

    def _arkid_for(self, recid):
        return "ark:/%s/%s" % (const.ARK_NAAN, self._aipid_for(recid))

    def _aipid_for(self, recid):
        return '-'.join(recid.split(':', 1))

    def _moderate_metadata(self, mdata: MutableMapping, shoulder=None):
        # only accept expected keys
        allowed = "resourceType creatorisContact contactName willUpload provideLink softwareLink assocPageType".split()
        mdata = OrderedDict([p for p in mdata.items() if p[0] in allowed])

        out = super()._moderate_metadata(mdata, shoulder)
        if isinstance(out.get('creatorisContact'), str):
            out['creatorisContact'] = out['creatorisContact'].lower() == "true"
        elif out.get('creatorisContact') is None:
            out['creatorisContact'] = True

        return out
        
    def _new_metadata_for(self, shoulder=None):
        return OrderedDict([
            ("resourceType", "data"),
            ("creatorisContact", True)
        ])

    def get_nerdm_data(self, id: str, part: str=None):
        """
        return the full NERDm metadata.  This differs from the :py:method:`get_data` method which (in
        this implementation) only returns a summary of hte NERDm metadata.  
        :param str id:    the identifier for the record whose NERDm data should be returned. 
        :param str part:  a path to the part of the record that should be returned.  This can be the 
                          name of a top level NERDm property or one of the following special values:
                            * ``pdr:f`` -- returns only the file-like components (files and subcollections)
                            * ``pdr:see`` -- returns only the non-file components (like links)
        """
        prec = self.dbcli.get_record_for(id, ACLs.READ)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(prec.id)

        if not part:
            out = nerd.get_data()

        else:
            steps = part.split('/', 1)
            if len(steps) > 1:
                # part is of the form ppp/kkk and refers to an item in list, ppp, where
                # kkk is either an element identifier or an element index of the form,
                # [N]. 
                key = steps[1]
                m = re.search(r'^\[(\d+)\]$', key)
                if m:
                    try:
                        key = int(m.group(1))
                    except ValueError as ex:
                        raise PartNotAccessible(id, part, "Accessing %s not supported" % part)

                if steps[0] == "authors":
                    out = nerd.authors.get(key)
                elif steps[0] == "references":
                    out = nerd.references.get(key)
                elif steps[0] == LINK_DELIM:
                    out = nerd.nonfiles.get(key)
                elif steps[0] == FILE_DELIM:
                    out = nerd.files.get(key)
                elif steps[0] == "components":
                    out = None
                    try:
                        out = nerd.nonfiles.get(key)
                    except (KeyError, IndexError, nerdstore.ObjectNotFound) as ex:
                        pass
                    if not out:
                        try:
                            out = nerd.files.get_file_by_id(key)
                        except nerdstore.ObjectNotFound as ex:
                            raise ObjectNotFound(id, part, str(ex))
                else:
                    raise PartNotAccessible(id, part, "Accessing %s not supported" % part)

            elif part == "authors":
                out = nerd.authors.get_data()
            elif part == "references":
                out = nerd.references.get_data()
            elif part == "components":
                out = nerd.nonfiles.get_data() + nerd.files.get_data()
            elif part == FILE_DELIM:
                out = nerd.files.get_data()
            elif part == LINK_DELIM:
                out = nerd.nonfiles.get_data()
            elif part == RES_DELIM:
                out = nerd.get_res_data()
            elif part.startswith(FILE_DELIM+"/"):
                fprts = part.split('/', 1)
                try: 
                    out = nerd.files.get_file_by_path(fprts[1])
                except nerdstore.ObjectNotFound as ex:
                    raise ObjectNotFound(prec.id, part, str(ex))
            else:
                out = nerd.get_res_data()
                if part in out:
                    out = out[part]
                elif part in "description @type contactPoint title rights disclaimer landingPage".split():
                    raise ObjectNotFound(prec.id, part, "%s property not set yet" % part)
                else:
                    raise PartNotAccessible(prec.id, part, "Accessing %s not supported" % part)

        return out

    def replace_data(self, id, newdata, part=None):
        """
        Replace the currently stored data content of a record with the given data.  It is expected that 
        the new data will be filtered/cleansed via an internal call to :py:method:`moderate_data`.  
        :param str      id:  the identifier for the record whose data should be updated.
        :param str newdata:  the data to save as the new content.  
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             the given ``newdata`` is a value that should be set to the property pointed 
                             to by ``part``.  
        :param ProjectRecord prec:  the previously fetched and possibly updated record corresponding to 
                             ``id``.  If this is not provided, the record will by fetched anew based on 
                             the ``id``.
        :raises ObjectNotFound:  if no record with the given ID exists or the ``part`` parameter points 
                             to an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by ``id``.
        :raises PartNotAccessible:  if replacement of the part of the data specified by ``part`` is not 
                             allowed.
        :raises InvalidUpdate:  if the provided ``newdata`` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content.
        """
        return self._update_data(id, newdata, part, replace=True)

    def update_data(self, id, newdata, part=None, message="", _prec=None):
        """
        merge the given data into the currently save data content for the record with the given identifier.
        :param str      id:  the identifier for the record whose data should be updated.
        :param str newdata:  the data to save as the new content.  
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             the given ``newdata`` is a value that should be set to the property pointed 
                             to by ``part``.  
        :param str message:  an optional message that will be recorded as an explanation of the update.
        :raises ObjectNotFound:  if no record with the given ID exists or the ``part`` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by ``id``.  
        :raises PartNotAccessible:  if replacement of the part of the data specified by ``part`` is not 
                             allowed.
        :raises InvalidUpdate:  if the provided ``newdata`` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content.
        """
        return self._update_data(id, newdata, part, replace=False, message="", prec=_prec)

    def clear_data(self, id, part=None, message: str=None, _prec=None) -> bool:
        """
        remove the stored data content of the record and reset it to its defaults.  
        :param str      id:  the identifier for the record whose data should be cleared.
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             only that property will be cleared (either removed or set to an initial
                             default).
        :return:  True the data was properly cleared; return False if ``part`` was specified but does not
                  yet exist in the data.
        :param ProjectRecord prec:  the previously fetched and possibly updated record corresponding to 
                             ``id``.  If this is not provided, the record will by fetched anew based on 
                             the ``id``.  
        :raises ObjectNotFound:  if no record with the given ID exists or the ``part`` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by ``id``.  
        :raises PartNotAccessible:  if clearing of the part of the data specified by ``part`` is not 
                             allowed.
        """
        set_state = False
        if not _prec:
            set_state = True
            _prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized

        if not self._store.exists(id):
            self.log.warning("NERDm data for id=%s not found in metadata store", _prec.id)
            nerd = self._new_data_for(_prec.id, _prec.meta)
            self._store.load_from(nerd)
        nerd = self._store.open(id)

        provact = None
        try:
            if part:
                what = part
                if part == "authors":
                    if nerd.authors.count == 0:
                        return False
                    nerd.authors.empty()
                elif part == "references":
                    if nerd.references.count == 0:
                        return False
                    nerd.references.empty()
                elif part == FILE_DELIM:
                    if nerd.files.count == 0:
                        return False
                    what = "files"
                    nerd.files.empty()
                elif part == LINK_DELIM:
                    if nerd.nonfiles.count == 0:
                        return False
                    what = "links"
                    nerd.nonfiles.empty()
                elif part == "components":
                    if nerd.nonfiles.count == 0 and nerd.files.count == 0:
                        return False
                    nerd.files.empty()
                    nerd.nonfiles.empty()
                elif part in "title rights disclaimer description landingPage keyword".split():
                    resmd = nerd.get_res_data()
                    if part not in resmd:
                        return False
                    del resmd[part]
                    nerd.replace_res_data(resmd)
                else:
                    raise PartNotAccessible(_prec.id, part, "Clearing %s not allowed" % part)

                if not message:
                    message = "clearing "+what
                provact = Action(Action.PATCH, _prec.id, self.who, message)
                part = ("/"+part) if part.startswith("pdr:") else ("."+part)
                provact.add_subaction(Action(Action.DELETE, _prec.id+"#data"+part, self.who,
                                             message))
                _prec.status.act(self.STATUS_ACTION_CLEAR, "cleared "+what, self.who.actor)

            else:
                nerd.authors.empty()
                nerd.references.empty()
                nerd.files.empty()
                nerd.nonfiles.empty()
                nerd.replace_res_data(self._new_data_for(_prec.id, _prec.meta))

                if not message:
                    message = "clearing all NERDm data"
                provact = Action(Action.PATCH, _prec.id, self.who, message)
                _prec.status.act(self.STATUS_ACTION_CLEAR, "cleared all NERDm data", self.who.actor)

        except PartNotAccessible:
            # client request error; don't record action
            raise

        except Exception as ex:
            self.log.error("Failed to clear requested NERDm data, %s: %s", _prec.id, str(ex))
            self.log.warning("Partial update is possible")
            if provact:
                provact.message = "Failed to clear requested NERDm data"
                self._record_action(provact)
            
            _prec.status.act(self.STATUS_ACTION_CLEAR, "Failed to clear NERDm data", self.who.actor)
            _prec.set_state(status.EDIT)
            _prec.data = self._summarize(nerd)
            self._try_save(_prec)
            raise

        _prec.data = self._summarize(nerd)
        if set_state:
            _prec.status.set_state(status.EDIT)

        try:
            _prec.save()

        except Exception as ex:
            self.log.error("Failed to saved DBIO record, %s: %s", prec.id, str(ex))
            raise

        finally:
            self._record_action(provact)
        return True
            

    def _update_data(self, id, newdata, part=None, prec=None, nerd=None, replace=False, message=""):
        set_action = False
        if not prec:
            set_action = True  # setting the last action will NOT be the caller's responsibility
            prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized

        if prec.status.state not in [status.EDIT, status.READY]:
            raise NotEditable(id)

        if not nerd:
            if not self._store.exists(id):
                self.log.warning("NERDm data for id=%s not found in metadata store", prec.id)
                nerd = self._new_data_for(prec.id, prec.meta)
                if prec.data.get("title"):
                    nerd["title"] = prec.data.get("title")
                self._store.load_from(nerd)

            nerd = self._store.open(id)

        provact = Action(Action.PUT if not part and replace else Action.PATCH,
                         prec.id, self.who, message)

        if not part:
            # this is a complete replacement; save updated NERDm data to the metadata store
            try:
                # prep the provenance record
                data = self._update_all_nerd(prec, nerd, newdata, provact, replace)
            except InvalidUpdate as ex:
                ex.record_id = prec.id
                raise

        else:
            # replacing just a part of the data
            try:
                data = self._update_part_nerd(part, prec, nerd, newdata, replace)
            except InvalidUpdate as ex:
                ex.record_id = prec.id
                ex.record_part = part
                raise

        self._save_data(self._summarize(nerd), prec, message, set_action and self.STATUS_ACTION_UPDATE)
        self.log.info("Updated data for %s record %s (%s) for %s",
                      self.dbcli.project, prec.id, prec.name, self.who)
        return data

    def _summarize(self, nerd: NERDResource):
        resmd = nerd.get_res_data()
        out = OrderedDict()
        out["@id"] = resmd.get("@id")
        out["title"] = resmd.get("title","")
        out["_schema"] = resmd.get("_schema", NERDM_SCH_ID)
        out["@type"] = resmd.get("@type", ["nrd:Resource"])
        if 'doi' in resmd:
            out["doi"] = resmd["doi"]
        if 'contactPoint' in resmd:
            out["contactPoint"] = resmd["contactPoint"]
        if 'landingPage' in resmd:
            out["landingPage"] = resmd["landingPage"]
        out["keywords"] = resmd.get("keyword", [])
        out["theme"] = list(set(resmd.get("theme", []) + [t.get('tag') for t in resmd.get('topic', [])]))
        if resmd.get('responsibleOrganization'):
            out['responsibleOrganization'] = list(set(
                [org['title'] for org in resmd['responsibleOrganization']] + \
                reduce(lambda x, y: x+y,
                       [org.get('subunit',[]) for org in resmd['responsibleOrganization']], [])
            ))
        out["authors"] = nerd.authors.get_data()
        out["references"] = nerd.references.get_data()
#        out["author_count"] = nerd.authors.count
#        out["reference_count"] = nerd.references.count
        out["file_count"] = nerd.files.count
        out["nonfile_count"] = nerd.nonfiles.count
        if resmd.get('responsibleOrganization', [{}])[0].get('title'):
            out['responsibleOrganization'] = [ resmd['responsibleOrganization'][0]['title'] ] + \
                                             resmd['responsibleOrganization'][0].get('subunits', [])
        return out

    _handsoff = ("@id @context publisher issued firstIssued revised annotated version " + \
                 "bureauCode programCode systemOfRecords primaryITInvestmentUII "       + \
                 "doi ediid releaseHistory status").split()   # temporarily allow theme editing
#                 "doi ediid releaseHistory status theme").split()  

    def _update_all_nerd(self, prec: ProjectRecord, nerd: NERDResource,
                         data: Mapping, provact: Action, replace=False):

        # filter out properties that the user is not allow to update
        newdata = OrderedDict()
        for prop in data:
            if not prop.startswith("__") and prop not in self._handsoff:
                newdata[prop] = data[prop]

        errors = []
        authors = newdata.get('authors')
        if authors:
            del newdata['authors']
            try:
                authors = self._merge_objlist_for_update(nerd.authors, self._moderate_author,
                                                         authors, replace, True)
            except InvalidUpdate as ex:
                errors.extend(ex.errors)

        refs = newdata.get('references')
        if refs:
            del newdata['references']
            try:
                refs = self._merge_objlist_for_update(nerd.references, self._moderate_reference,
                                                      refs, replace, True)
            except InvalidUpdate as ex:
                errors.extend(ex.errors)

        comps = newdata.get('components')
        files = []
        nonfiles = []
        if comps:
            del newdata['components']
            try:
                files, nonfiles = self._merge_comps_for_update(nerd, comps, replace, True)
                comps = nonfiles + files
            except InvalidUpdate as ex:
                errors.extend(ex.errors)

        # handle resource-level data: merge the new data into the old and validate the result
        if replace:
            oldresdata = self._new_data_for(prec.id, prec.meta, newdata.get("_schema"))
        else:
            oldresdata = nerd.get_data(False)

        # merge and validate the resource-level data
        try:
            newdata = self._moderate_res_data(newdata, oldresdata, nerd, replace)
        except InvalidUpdate as ex:
            errors = ex.errors + errors

        if len(errors) > 1:
            raise InvalidUpdate("Input metadata data would create invalid record (%d errors detected)"
                                % len(errors), prec.id, errors=errors)
        elif len(errors) == 1:
            raise InvalidUpdate("Input validation error: "+str(errors[0]), prec.id, errors=errors)

        # all data is merged and validated; now commit
        try:
            old = nerd.get_res_data()
            nerd.replace_res_data(newdata)
            provact.add_subaction(Action(Action.PUT if replace else Action.PATCH,
                                         prec.id+"#data/pdr.r", self.who,
                                         "updating resource-level metadata",
                                         self._jsondiff(old, newdata)))

            if replace:
                old = nerd.authors.get_data()
                nerd.authors.empty()
                if authors:
                    provact.add_subaction(Action(Action.PUT, prec.id+"#data.authors", self.who,
                                                 "replacing authors", self._jsondiff(old, authors)))
                    nerd.authors.replace_all_with(authors)
                else:
                    provact.add_subaction(Action(Action.DELETE, prec.id+"#data.authors", self.who,
                                                 "removing authors"))

                old = nerd.references.get_data()
                nerd.references.empty()
                if refs:
                    provact.add_subaction(Action(Action.PUT, prec.id+"#data.references", self.who,
                                                 "replacing references", self._jsondiff(old, refs)))
                    nerd.references.replace_all_with(refs)
                else:
                    provact.add_subaction(Action(Action.DELETE, prec.id+"#data.references", self.who,
                                                 "removing references"))

                old = nerd.nonfiles.get_data()
                nerd.nonfiles.empty()
                if nonfiles:
                    provact.add_subaction(Action(Action.PUT, prec.id+"#data/pdr:see", self.who,
                                                 "replacing non-file components", self._jsondiff(old, nonfiles)))
                    nerd.nonfiles.replace_all_with(nonfiles)
                else:
                    provact.add_subaction(Action(Action.DELETE, prec.id+"#data/pdr:see", self.who,
                                                 "removing non-file components"))
                    
            else:
                def put_listitem_into(item, objlist):
                    if item.get("@id"):
                        objlist.set(item.get("@id"), item)
                    else:
                        objlist.append(item)
                def put_each_into(data, objlist):
                    for item in data:
                        put_listitem_into(item, objlist)

                if authors:
                    provact.add_subaction(Action(Action.PATCH, prec.id+"#data.authors", self.who,
                                                 "updating authors", self._jsondiff(old, nonfiles)))
                    put_each_into(authors, nerd.authors)
                if refs:
                    provact.add_subaction(Action(Action.PATCH, prec.id+"#data.references", self.who,
                                                 "updating references", self._jsondiff(old, refs)))
                    put_each_into(refs, nerd.references)
                if nonfiles:
                    provact.add_subaction(Action(Action.PATCH, prec.id+"#data/pdr:see", self.who,
                                                 "updating non-file components", self._jsondiff(old, nonfiles)))
                    put_each_into(nonfiles, nerd.nonfiles)

            if replace:
                provact.add_subaction(Action(Action.PUT, prec.id+"#data/pdr:f", self.who,
                                             "replacing non-file components"))
                nerd.files.empty()
            else:
                provact.add_subaction(Action(Action.PUT, prec.id+"#data/pdr:f", self.who,
                                             "replacing non-file components"))
            for fmd in files:
                nerd.files.set_file_at(fmd)

        except InvalidUpdate as ex:
            self.log.error("Invalid update to NERDm data not saved: %s: %s", prec.id, str(ex))
            if ex.errors:
                self.log.error("Errors include:\n  "+("\n  ".join([str(e) for e in ex.errors])))
            raise
        except Exception as ex:
            provact.message = "Failed to save NERDm data update due to internal error"
            self.log.error("Failed to save NERDm metadata: "+str(ex))
            self.log.warning("Failed NERDm save may have been partial")
            raise
        
        finally:
            self._record_action(provact)

        return nerd.get_data(True)

    def _update_part_nerd(self, path: str, prec: ProjectRecord, nerd: NERDResource, 
                          data: Union[list, Mapping], replace=False, doval=True):
        # update just part of the NERDm metadata as given by path.  The path identifies which
        # NERDm Resource property to update; the data parameter is expected to be of a JSONSchema 
        # type that matches that property.  Two special path values, FILE_DELIM ("pdr:f") and
        # LINK_DELIM ("pdr:see") are taken to refer to the list of file and non-file components,
        # respectively.

        schemabase = prec.data.get("_schema") or NERDMPUB_SCH_ID
        subacttype = Action.PUT if replace else Action.PATCH
        provact = Action(Action.PATCH, prec.id, self.who, "updating NERDm part")

        try:
            steps = path.split('/', 1)
            if len(steps) > 1:
                # path is of the form ppp/kkk and refers to an item in list, ppp, where
                # kkk is either an element identifier or an element index of the form,
                # [N]. 
                key = steps[1]
                m = re.search(r'^\[([\+\-]?\d+)\]$', key)
                if m:
                    try:
                        key = int(m.group(1))
                    except ValueError as ex:
                        raise PartNotAccessible(id, path, "Accessing %s not supported" % path)

                old = {}
                if steps[0] == "authors":
                    what = "adding author"
                    if key in nerd.authors:
                        old = nerd.authors.get(key)
                        what = "updating author"
                    # data["_schema"] = schemabase+"/definitions/Person"
                    data = self._update_listitem(nerd.authors, self._moderate_author, data, key,
                                                 replace, doval)
                    provact.add_subaction(Action(subacttype, "%s#data.authors[%s]" % (prec.id, str(key)), 
                                                 self.who, what, self._jsondiff(old, data)))
                    
                elif steps[0] == "references":
                    what = "adding reference"
                    if key in nerd.references:
                        old = nerd.references.get(key)
                        what = "updating reference"
                    data["_schema"] = schemabase+"/definitions/BibliographicReference"
                    data = self._update_listitem(nerd.references, self._moderate_reference, data, key,
                                                 replace, doval)
                    provact.add_subaction(Action(subacttype, "%s#data.references/%s" % (prec.id, str(key)), 
                                                 self.who, what, self._jsondiff(old, data)))
                    
                elif steps[0] == LINK_DELIM:
                    what = "adding link"
                    if key in nerd.nonfiles:
                        old = nerd.nonfiles.get(key)
                        what = "updating link"
                    data["_schema"] = schemabase+"/definitions/Component"
                    data = self._update_listitem(nerd.nonfiles, self._moderate_nonfile, data, key,
                                                 replace, doval)
                    provact.add_subaction(Action(subacttype, "%s#data/pdr:see[%s]" % (prec.id, str(key)), 
                                                 self.who, what, self._jsondiff(old, data)))
                    
                elif steps[0] == "components" or steps[0] == FILE_DELIM:
                    if ('filepath' not in data and key in nerd.nonfiles):
                        old = nerd.nonfiles.get(key)
                        what = "updating link"
                    elif key in nerd.files:
                        old = nerd.files.get(key)
                        what = "updating file"
                    else:
                        old = {}
                        what = "adding component"
                    data["_schema"] = schemabase+"/definitions/Component"
                    data = self._update_component(nerd, data, key, replace, doval=doval)
                    provact.add_subaction(Action(subacttype, "%s#data/pdr:f[%s]" % (prec.id, str(key)), 
                                                 self.who, what, self._jsondiff(old, data)))
                    
                else:
                    raise PartNotAccessible(prec.id, path, "Updating %s not allowed" % path)

            elif path == "authors":
                if not isinstance(data, list):
                    err = "authors data is not a list"
                    raise InvalidUpdate(err, id, path, errors=[err])
                old = nerd.authors.get_data()
                if replace:
                    data = self._replace_objlist(nerd.authors, self._moderate_author, data, doval)
                else:
                    data = self._update_objlist(nerd.authors, self._moderate_author, data, doval)
                provact.add_subaction(Action(subacttype, prec.id+"#data.authors", self.who, 
                                             "updating authors", self._jsondiff(old, data)))

            elif path == "references":
                if not isinstance(data, list):
                    err = "references data is not a list"
                    raise InvalidUpdate(err, id, path, errors=[err])
                old = nerd.references.get_data()
                if replace:
                    data = self._replace_objlist(nerd.references, self._moderate_reference, data, doval)
                else:
                    data = self._update_objlist(nerd.references, self._moderate_reference, data, doval)
                provact.add_subaction(Action(subacttype, prec.id+"#data.references", self.who, 
                                             "updating references", self._jsondiff(old, data)))

            elif path == LINK_DELIM:
                if not isinstance(data, list):
                    err = "non-file (links) data is not a list"
                    raise InvalidUpdate(err, id, path, errors=[err])
                old = nerd.nonfiles.get_data()
                if replace:
                    data = self._replace_objlist(nerd.nonfiles, self._moderate_nonfile, data, doval)
                else:
                    data = self._update_objlist(nerd.nonfiles, self._moderate_nonfile, data, doval)
                provact.add_subaction(Action(subacttype, prec.id+"#data/pdr:see", self.who, 
                                             "updating link list", self._jsondiff(old, data)))
                

            # elif path == FILE_DELIM:
            #     if not isinstance(data, list):
            #         err = "components data is not a list"
            #         raise InvalidUpdate(err, id, path, errors=[err])

            elif path == "components" or path == FILE_DELIM:
                if not isinstance(data, list):
                    err = "components data is not a list"
                    raise InvalidUpdate(err, id, path, errors=[err])
                oldn = nerd.nonfiles.get_data()
                oldf = nerd.files.get_files()
                files, nonfiles = self._merge_comps_for_update(nerd, data, replace, doval)
                if replace:
                    if path == "components":
                        nerd.nonfiles.empty()
                    nerd.files.empty()
                if path == "components":
                    for cmp in nonfiles:
                        if cmp.get("@id"):
                            nerd.nonfiles.set(cmp['@id'])
                        else:
                            nerd.nonfiles.append(cmp)

                provact.add_subaction(Action(subacttype, prec.id+"#data/pdr:f", self.who,
                                             "updating file list",
                                             self._jsondiff(oldn, nerd.nonfiles.get_data())))
                for cmp in files:
                    nerd.files.set_file_at(cmp)

                if path == "components":
                    provact.add_subaction(Action(subacttype, prec.id+"#data/pdr:see", self.who,
                                                 "updating link list",
                                                 self._jsondiff(oldf, nerd.nonfiles.get_data())))
                    data = nerd.nonfiles.get_data() + nerd.files.get_files()
                else:
                    data = nerd.files.get_files()

            elif path == "contactPoint":
                if not isinstance(data, Mapping):
                    raise InvalidUpdate("contactPoint data is not an object", sys=self)
                res = nerd.get_res_data()
                old = res.get('contactPoint')
                res['contactPoint'] = self._moderate_contactPoint(data, res, replace=replace, doval=doval)
                    # may raise InvalidUpdate
                provact.add_subaction(Action(subacttype, prec.id+"#data.contactPoint", self.who,
                                      "updating contact point", self._jsondiff(old, res['contactPoint'])))
                nerd.replace_res_data(res)
                data = res[path]
                
            elif path == "@type":
                if not isinstance(data, (list, str)):
                    raise InvalidUpdate("@type data is not a list of strings", sys=self)
                res = nerd.get_res_data()
                old = res.get('@type')
                res = self._moderate_restype(data, res, nerd, replace=replace, doval=doval)
                provact.add_subaction(Action(subacttype, prec.id+"#data.@type", self.who,
                                             "updating resource types", self._jsondiff(old, res['@type'])))
                nerd.replace_res_data(res)
                data = res.get(path)

            elif path == "description":
                if not isinstance(data, (list, str)):
                    raise InvalidUpdate(part+" data is not a list of strings", sys=self)
                res = nerd.get_res_data()
                old = res.get(path)

                res[path] = self._moderate_description(data, res, doval=doval)  # may raise InvalidUpdate
                provact.add_subaction(Action(Action.PUT, prec.id+"#data."+path, self.who, "updating "+path,
                                             self._jsondiff(old, res[path])))
                nerd.replace_res_data(res)
                data = res[path]

            elif path == "keyword":
                if not isinstance(data, (list, str)):
                    raise InvalidUpdate(part+" data is not a list of strings", sys=self)
                res = nerd.get_res_data()
                old = res.get(path)

                res[path] = self._moderate_keyword(data, res, doval=doval, replace=replace)  # InvalidUpdate
                provact.add_subaction(Action(Action.PUT if replace else Action.PATCH,
                                             prec.id+"#data."+path, self.who, "updating "+path,
                                             self._jsondiff(old, res[path])))
                nerd.replace_res_data(res)
                data = res[path]

            # NOTE!!: Temporary support for updating theme
            elif path == "theme":
                if not isinstance(data, (list, str)):
                    raise InvalidUpdate(part+" data is not a list of strings", sys=self)
                res = nerd.get_res_data()
                old = res.get(path)

                res[path] = self._moderate_keyword(data, res, doval=doval, replace=replace,
                                                   kwpropname='theme')  # may raise InvalidUpdate
                provact.add_subaction(Action(Action.PUT if replace else Action.PATCH,
                                             prec.id+"#data."+path, self.who, "updating "+path,
                                             self._jsondiff(old, res[path])))
                nerd.replace_res_data(res)
                data = res[path]

            elif path == "landingPage":
                if not isinstance(data, str):
                    raise InvalidUpdate("description data is not a string", sys=self)
                res = nerd.get_res_data()
                old = res.get('landingPage')
                res[path] = self._moderate_landingPage(data, res, doval)        # may raise InvalidUpdate
                provact.add_subaction(Action(Action.PUT, prec.id+"#data.landingPage", self.who,
                                             "updating landingPage",
                                             self._jsondiff(old, res['landingPage'])))
                nerd.replace_res_data(res)
                data = res[path]
                
            elif path in "title rights disclaimer".split():
                if not isinstance(data, str):
                    raise InvalidUpdate("%s value is not a string" % path, sys=self)
                res = nerd.get_res_data()
                old = res.get(path)
                res[path] = self._moderate_text(data, res, doval=doval)  # may raise InvalidUpdate
                provact.add_subaction(Action(subacttype, prec.id+"#data."+path, self.who, "updating "+path,
                                             self._jsondiff(old, res[path])))
                nerd.replace_res_data(res)
                data = res[path]
                
            else:
                raise PartNotAccessible(prec.id, path, "Updating %s not allowed" % path)

        except PartNotAccessible:
            # client request error; don't record action
            raise
        except InvalidUpdate as ex:
            self.log.error("Invalid update to NERDm data not saved: %s: %s", prec.id, str(ex))
            if ex.errors:
                self.log.error("Errors include:\n  "+("\n  ".join([str(e) for e in ex.errors])))
            raise
        except Exception as ex:
            self.log.error("Failed to save update to NERDm data, %s: %s", prec.id, str(ex))
            self.log.warning("Partial update is possible")
            provact.message = "Failed to update NERDm part"
            self._record_action(provact)
            raise
        else:
            self._record_action(provact)

        return data

            
    def set_file_component(self, id, filemd, filepath=None):
        """
        add a file to the specified dataset as described by the given metadata.  If the dataset 
        already has a file with the specified filepath, it will be replaced.
        :param str       id:  the identifier for the dataset to add the file to
        :param dict  filemd:  the NERDm file metadata describing the new file to add.  If
                              the "@id" property is set, it will be ignored.
        :param str filepath:  the path within the dataset to assign to the file.  If provided,
                              it will override the corresponding value in ``filemd``; if not 
                              provided, the ``filepath`` property must be set within ``filemd``.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        if filepath:
            data = deepcopy(data)
            data['filepath'] = filepath
        else:
            filepath = filemd.get("filepath")
        if not filepath:
            raise InvalidUpdate("filepath not set in the given file description to be added to " + id)
        nerd = self._store.open(id)

        oldfile = None
        try:
            oldfile = nerd.files.get_file_by_path(filepath)  # it must have na id
        except nerdstore.ObjectNotFound as ex:
            pass

        return self._update_file_comp(nerd, data, oldfile, replace=True, doval=True)

    def update_file_component_at(self, id: str, filemd: Mapping, filepath: str=None):
        """
        Update the metadata for a file component at a particular filepath.  The given metadata will 
        be merged with that of the existing file.  If a file is not currently registered at 
        that filepath, an exception is raised.  
        :param str       id:  the identifier for the dataset containing the file
        :param dict  filemd:  the file metadata to update 
        :param str filepath:  the path of the file within the dataset to update
        :raises ObjectNotFound:  if there does not exist a file at the given filepath
        :raises ValueError:  if filepath is not set in either the ``filepath`` argument or the 
                             filepath property.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        if not filepath:
            filepath = filemd.get("filepath")
        if not filepath:
            raise InvalidUpdate("filepath not set in the given file description to be added to " + id)
        if filemd.get("filepath") != filepath:
            filemd = deepcopy(filemd)
            filemd['filepath'] = filepath
            
        nerd = self._store.open(id)
        try:
            oldfile = nerd.files.get_file_by_path(filepath)
        except nerdstore.ObjectNotFound as ex:
            raise ObjectNotFound(id, FILE_DELIM+"/"+filepath, str(ex), self._sys)

        return self._update_file_comp(nerd, filemd, oldfile, replace=False, doval=True)

    def update_file_component(self, id: str, filemd: Mapping, fileid: str=None):
        """
        Update the metadata for a file component at a particular filepath.  The given metadata will 
        be merged with that of the existing file.  If a file is not currently registered at 
        that filepath, an exception is raised.  
        :param str       id:  the identifier for the dataset containing the file
        :param dict  filemd:  the file metadata to update 
        :param str   fileid:  the id of the file within the dataset to update
        :raises ObjectNotFound:  if there does not exist a resource with the given id
        :raises ValueError:  if id is not set in either the ``fileid`` argument or the ``filemd`` 
                             object's ``@id`` property.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        if not fileid:
            fileid = filemd.get("@id")
        if not fileid:
            raise InvalidUpdate("file-id not set in the given file description to be added to " + id)
        if filemd.get("@id") != fileid:
            filemd = deepcopy(filemd)
            filemd['@id'] = fileid

        nerd = self._store.open(id)
        try:
            oldfile = nerd.files.get_file_by_path(filepath)
        except nerdstore.ObjectNotFound as ex:
            raise ObjectNotFound(id, FILE_DELIM+"/"+filepath, str(ex), self._sys)

        return self._update_file_comp(nerd, filemd, oldfile, replace=False, doval=True)

    def replace_files(self, id: str, files: List[Mapping]):
        """
        replace all currently saved files and folder components with the given list.  Each component
        must include a ``filepath`` property.
        :param str       id:  the identifier for the dataset containing the file
        :raises ObjectNotFound:  if there does not exist a resource with the given id
        """
        if not isinstance(files, (list, tuple)):
            err = "components data is not a list"
            raise InvalidUpdate(err, id, "components", errors=[err])
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)

        errors = []
        newfiles = []
        nbad = 0
        for cmp in files:
            try:
                files[i] = self._moderate_file(cmp, True)
            except InvalidUpdate as ex:
                nbad += 1
                errors.extend(ex.errors)

        if errors:
            raise InvalidUpdate("%s: %d files in given list produced validation errors" % (id, nbad), 
                                errors=ex.errors)

        newfiles.sort(key=lambda cmp: cmp.get("filepath"))
        nerd.files.empty()
        for cmp in newfiles:
            nerd.files.set_file_at(cmp)

        
    def _update_file_comp(self, nerd: NERDResource, md: Mapping, oldmd: Mapping = None,
                          replace: bool=False, doval: bool=False):
        if oldmd and not replace:
            md = self._merge_into(md, oldmd)

        md = self._moderate_file(md, doval=doval)   # may raise InvalidUpdate

        id = nerd.files.set_file_at(md, md['filepath'], md.get('@id'))
        if not md.get('@id'):
            md['@id'] = id
        return md

    def sync_to_file_space(self, id: str) -> bool:
        """
        update the file metadata based on the contents in the file manager space
        :param str id:  the ID for the DAP project to sync
        :raises ObjectNotFound:  if the project with the given ID does not exist
        :raises NotAuthorized:   if the user does not write permission to make this update
        :raises FileSpaceException:  if syncing failed for an unexpected reason
        """
        if not self._fmcli:
            return {}
        prec = to_DAPRec(self.dbcli.get_record_for(id, ACLs.WRITE), self._fmcli)   # may raise exc
        nerd = self._store.open(id)

        if self._fmcli:
            prec.ensure_file_space(self.who.actor)
            files = nerd.files
            if hasattr(files, 'update_hierarchy'):
                prec.file_space['action'] = 'sync'
                if files.fm_summary.get('syncing') == "in_progress":
                    # a scan is still in progress, so just get the latests updates; don't start a new scan
                    prec.file_space.update(files.update_metadata())
                else:
                    prec.file_space.update(files.update_hierarchy())  # may raise FileSpaceException
                prec.save()
        return prec.to_dict().get('file_space', {})
            

    def add_nonfile_component(self, id: str, cmpmd: Mapping):
        """
        add a new non-file component to the specified dataset as described by the given metadata.  
        :param str     id:  the identifier for the dataset to add a new component to
        :param dict cmpmd:  the NERDm component metadata describing the new component to add.  If
                              the "@id" property is set, it will be ignored.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)

        return self._add_listitem(nerd.nonfiles, self._moderate_nonfile, cmpmd, doval=True)

    def update_nonfile_component(self, id: str, cmpmd: Mapping, idorpos=None, replace=False):
        """
        update the metadata for a non-file component in the specified dataset as identified either
        by its ID or position in the list of non-file components.  If identified component does not 
        exist, an exception is raised.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)

        return self._update_listitem(nerd.nonfiles, self._moderate_nonfile, cmpmd, idorpos, replace, True)
        
    def replace_nonfile_components(self, id: str, cmps: List[Mapping]):
        """
        replace all currently saved non-file components with the given list.  The order of given list 
        will be the order in which they are saved.
        """
        if not isinstance(cmps, (list, tuple)):
            err = "components data is not a list"
            raise InvalidUpdate(err, id, "components", errors=[err])
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)
        self._replace_objlist(nerd.nonfiles, self._moderate_nonfile, cmps, True)
        
    def add_author(self, id: str, authmd: Mapping):
        """
        add a new author to the specified dataset as described by the given metadata.  
        :param str      id:  the identifier for the dataset to add a new author to
        :param dict authmd:  the NERDm Person metadata describing the new author to add.  If
                              the "@id" property is set, it will be ignored.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)

        return self._add_listitem(nerd.authors, self._moderate_author, authmd, doval=True)

    def update_author(self, id: str, authmd: Mapping, idorpos=None, replace=False):
        """
        update the metadata for an author in the specified dataset as identified either
        by its ID or position in the list of authors.  If identified author does not 
        exist, an exception is raised.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)

        return self._update_listitem(nerd.authors, self._moderate_author, authmd, idorpos, replace, True)

    def replace_authors(self, id: str, authors: List[Mapping]):
        """
        replace all currently saved authors with the given list.  The order of given list will be 
        the order in which they are saved.
        """
        if not isinstance(authors, (list, tuple)):
            err = "authors data is not a list"
            raise InvalidUpdate(err, id, "authors", errors=[err])
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)
        self._replace_objlist(nerd.authors, self._moderate_author, authors, True)
        
    def add_reference(self, id: str, refmd: Mapping):
        """
        add a new author to the specified dataset as described by the given metadata.  
        :param str      id:  the identifier for the dataset to add a new reference to
        :param dict authmd:  the NERDm Reference metadata describing the new reference to add.  If
                              the "@id" property is set, it will be ignored.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)

        return self._add_listitem(nerd.references, self._moderate_reference, refmd, doval=True)

    def update_reference(self, id: str, refmd: Mapping, idorpos=None, replace=False):
        """
        update the metadata for a references in the specified dataset as identified either
        by its ID or position in the list of references.  If identified reference does not 
        exist, an exception is raised.
        """
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)

        return self._update_listitem(nerd.references, self._moderate_reference, refmd, idorpos, replace, True)
        
    def replace_references(self, id: str, refs: List[Mapping]):
        """
        replace all currently saved references with the given list.  The order of given list will be 
        the order in which they are saved.
        """
        if not isinstance(refs, (list, tuple)):
            err = "references data is not a list"
            raise InvalidUpdate(err, id, "references", errors=[err])
        prec = self.dbcli.get_record_for(id, ACLs.WRITE)   # may raise ObjectNotFound/NotAuthorized
        nerd = self._store.open(id)
        self._replace_objlist(nerd.references, self._moderate_reference, refs, True)
        
    def _add_listitem(self, objlist, moderate_func, data: Mapping, doval: bool=False):
        data = moderate_func(data)
        id = objlist.append(data)
        data['@id'] = id
        return data

    def _update_listitem(self, objlist, moderate_func, data: Mapping, idorpos=None,
                         replace: bool=False, doval: bool=False):
        try:
            olditem = objlist.get(idorpos)
            if not replace:
                data = self._merge_into(data, olditem)
        except IndexError as ex:
            raise ObjectNotFound("Item not found at position "+str(ex)) from ex
        except KeyError as ex:
            raise ObjectNotFound("Item not found with id="+str(ex)) from ex

        data = moderate_func(data, doval=doval)   # may raise InvalidUpdate
        objlist.set(olditem["@id"], data)
        if not data.get("@id"):
            data["@id"] = olditem["@id"]
        return data

    def _merge_comps_for_update(self, nerd: NERDResource, data: List[Mapping],
                                replace: bool=False, doval: bool=False):
        # the point of this function is to prep the data for update, collecting as many 
        # validation errors upfront as possible
        nonfiles = []
        files = []
        errors = []

        # collate the components
        for cmp in data:
            # is it a file or a non-file?
            cmplist = None
            if cmp.get("@id"):
                if cmp['@id'] in nerd.nonfiles.ids:
                    cmplist = nonfiles
                elif cmp['@id'] in nerd.files.ids:
                    cmplist = files

            if cmplist is None:
                if cmp.get('filepath'):
                    cmplist = files
                else:
                    cmplist = nonfiles

            cmplist.append(cmp)

        try:
            nonfiles = self._merge_objlist_for_update(nerd.nonfiles, self._moderate_nonfile, nonfiles,
                                                      replace, doval)
        except InvalidUpdate as ex:
            errors.extend(ex.errors)

        for i, cmp in enumerate(files):
            oldcmp = None
            if not replace:
                if cmp.get("@id") in nerd.files.ids:
                    oldcmp = nerd.files.get(cmp['@id'])
                elif cmp.get("filepath") and nerd.files.exists(cmp["filepath"]):
                    oldcmp = nerd.files.get(cmp['filepath'])

            if oldcmp:
                cmp = self._merge_into(cmp, oldcmp)
            elif cmp.get("@id"):
                cmp = deepcopy(cmp)
                del cmp["@id"]

            try:
                files[i] = self._moderate_file(cmp, doval)
            except InvalidUpdate as ex:
                errors.extend(ex.errors)
            
        if errors:
            raise InvalidUpdate("%d file validation errors detected" % len(ex.errors),
                                errors=ex.errors)

        files.sort(key=lambda cmp: cmp.get("filepath"))  # this places subcollections before their contents
        return files, nonfiles

    def _merge_objlist_for_update(self, objlist, moderate_func, data: List[Mapping],
                                  replace: bool=False, doval: bool=False):
        # the point of this function is to prep the data for update, collecting as many 
        # validation errors upfront as possible
        def merge_item(item):
            olditem = None
            if not replace and item.get("@id"):
                try:
                    olditem = objlist.get(item["@id"])
                except (KeyError, nerdstore.ObjectNotFound):
                    pass

            if olditem:
                item = self._merge_into(item, olditem)
            elif item.get("@id"):
                item = deepcopy(item)
                del item["@id"]
                
            return moderate_func(item, doval)

        out = []
        errors = []
        for item in data:
            try:
                out.append(merge_item(item))
            except InvalidUpdate as ex:
                errors.extend(ex.errors)
        if errors:
            raise InvalidUpdate("%d item validation errors detected" % len(errors), errors=errors)
        return out

    def _replace_objlist(self, objlist, moderate_func, data: List[Mapping], doval: bool=False):
        data = [ moderate_func(a, doval=doval) for a in data ]   # may raise InvalidUpdate
        objlist.empty()
        for item in data:
            objlist.append(item)
        return objlist.get_data()

    def _update_objlist(self, objlist, moderate_func, data: List[Mapping], doval: bool=False):
        # match the items in the given list to existing items currently store by their ids; for each 
        # match, the item metadata will be merged with the matching metadata.  If there is no
        # match, the item will be appended.  This method attempts to ferret out all errors
        # before updating any items
        newitems = []
        errors = []
        nbad = 0
        for item in data:
            olditem = None
            if item.get("@id"):
                olditem = objlist.get(item["@id"])
                item = self._merge_into(item, olditem)
            try:
                item = moderate_func(item, doval=doval)
            except InvalidUpdate as ex:
                errors.extend(ex.errors)
                nbad += 1
            newitems.append(item)

        if errors:
            raise InvalidUpdate("%d items contained validation errors" % nbad, errors=errors)

        for item in newitems:
            if item.get("@id"):
                objlist.set(item["@id"], item)
            else:
                objlist.append(item)
        return objlist.get_data()

        ## This is an implementation based on position rather than id
        # curcount = len(objlist)
        # for i, item in enumerate(data):
        #     olditem = None
        #     if i < curcount:
        #         olditem = objlist.get(i)
        #         data[i] = self._merge_into(data[i], olditem)
        #     data[i] = moderate_func(data[i], doval=doval)    # may raise InvalidUpdate
        # 
        # for i, item in enumerate(data):
        #     if i < curcount:
        #         objlist.set(i, item)
        #     else:
        #         objlist.append(item)
            

            
            
        
                        

#################

            
    def validate_json(self, json, schemauri=None):
        """
        validate the given JSON data record against the give schema, raising an exception if it 
        is not valid.

        :param dict json:      the (parsed) JSON data to validate
        :param str schemauri:  the JSONSchema URI to validate the input against. 
        :raises InvalidUpdate: if the data is found to be invalid against the schema; the exception's
                               ``errors`` property will list all the errors found.
        """
        errors = []
        if self._valid8r:
            if not schemauri:
                schemauri = json.get("_schema")
            if not schemauri:
                raise ValueError("validate_json(): No schema URI specified for input data")
            errors = self._valid8r.validate(json, schemauri=schemauri, strict=True, raiseex=False)
        else:
            self.log.warning("Unable to validate submitted NERDm data")

        if len(errors) > 0:
            raise InvalidUpdate("NERDm Schema validation errors found", errors=errors, sys=self)


    def _moderate_text(self, val, resmd=None, doval=True):
        # make sure input value is the right type, is properly encoded, and
        # does not contain any illegal bits
        if doval and not isinstance(val, str):
            raise InvalidUpdate("Text value is not a string", sys=self)
        return val

    def _moderate_description(self, val, resmd=None, doval=True, replace=True):
        # replace is ignored
        if val is None:
            val = []
        if isinstance(val, str):
            val = re.split(r'\n\n+', val)
        if not isinstance(val, Sequence):
            raise InvalidUpdate("description value is not a string or array of strings", sys=self)
        return [self._moderate_text(t, resmd, doval=doval) for t in val if t]

    def _moderate_keyword(self, val, resmd=None, doval=True, replace=True, kwpropname='keyword'):
        if val is None:
            val = []
        if isinstance(val, str):
            val = re.split(r'\n+', val)
        if not isinstance(val, Sequence):
            raise InvalidUpdate("keywords value is not a string or array of strings", sys=self)

        # uniquify list
        out = resmd.get(kwpropname, []) if resmd and not replace else []
        for v in val:
            if v not in out:
                out.append(self._moderate_text(v, resmd, doval=doval))

        return out

    def _moderate_landingPage(self, val, resmd=None, doval=True, replace=True):
        # replace is ignored
        if val is None:
            val = ""
        try: 
            url = urlparse(val)
            if url.scheme not in "https http".split() or not url.netloc:
                raise InvalidUpdate("landingPage: Not a complete HTTP URL")
        except ValueError as ex:
            raise InvalidUpdate("landingPage: Not a legal URL: "+str(ex))
        if resmd and doval:
            resmd['landingPage'] = val
            self.validate_json(resmd)
        return val
        
    _pfx_for_type = OrderedDict([
        ("ScienceTheme",        NERDMAGG_PRE),
        ("ExperimentalData",    NERDMEXP_PRE),
        ("DataPublication",     NERDMPUB_PRE),
        ("SoftwarePublication", NERDMSW_PRE),
        ("Aggregation",         NERDMAGG_PRE),
        ("PublicDataResource",  NERDMPUB_PRE),
        ("Resource",            NERDM_PRE)
    ])
    _schema_for_pfx = {
        NERDM_PRE:    NERDM_SCH_ID,
        NERDMPUB_PRE: NERDMPUB_SCH_ID,
        NERDMAGG_PRE: NERDMAGG_SCH_ID,
        NERDMSW_PRE:  NERDMSW_SCH_ID
    }

    def _moderate_restype(self, types, resmd, nerd=None, replace=True, doval=True):
        if types is None:
            types = []
        if not isinstance(types, list):
            types = [types]
        if any([not isinstance(t, str) for t in types]):
            raise InvalidUpdate("@type data is not a list of strings", sys=self)

        # separate NERDm Resource types and allowed non-NERDm types; throw away others
        if not replace:
            types = resmd.get("@type",[]) + types
        exttypes = []
        nrdtypes = set()
        for tp in types:
            parts = tp.split(':', 1)
            if parts[-1] in self._pfx_for_type and (len(parts) == 1 or parts[0].startswith("nrd")):
                nrdtypes.add(parts[-1])
            elif len(parts) == 2 and parts[0] in ["schema", "dcat"] and tp not in exttypes:
                exttypes.append(tp)

        # set some default types based on the presence of other metadata
        if nerd and nerd.authors.count > 0 and \
           "SoftwarePublication" not in nrdtypes and "DataPublication" not in nrdtypes:
            nrdtypes.add("DataPublication")

        if "ExperimentalData" not in nrdtypes:
            if self._has_exp_prop(resmd):
                nrdtypes.add("ExperimentalData")

        if not nrdtypes:
            nrdtypes.add("PublicDataResource")

        extschemas = []
        if "DataPublication" in nrdtypes:
            extschemas.append(NERDMPUB_DEF + "DataPublication")
        elif "PublicDataResource" in nrdtypes:
            extschemas.append(NERDMPUB_DEF + "PublicDataResource")
        if "SoftwarePublication" in nrdtypes:
            extschemas.append(NERDMSW_DEF + "SoftwarePublication")

        if "ScienceTheme" in nrdtypes:
            extschemas.append(NERDMAGG_DEF + "ScienceTheme")
        elif "Aggregation" in nrdtypes:
            extschemas.append(NERDMAGG_DEF + "Aggregation")

        if "ExperimentalData" in nrdtypes:
            extschemas.append(NERDMEXP_DEF + "ExperimentalData")

        # ensure proper prefixes and conventional order for NERDm types
        types = []
        for tp in self._pfx_for_type:
            if tp in nrdtypes:
                types.append("%s:%s" % (self._pfx_for_type[tp], tp))
        types += exttypes

        resmd["@type"] = types
        if extschemas:
            resmd[EXTSCHPROP] = extschemas

        if doval:
            self.validate_json(resmd)
        return resmd

    def _has_exp_prop(self, md):
        for prop in ("instrumentsUsed isPartOfProjects acquisitionStartTime hasAcquisitionStart "+
                     "acquisitionEndTime hasAcquisitionEnd").split():
            if prop in md:
                return True
        return False

    _contact_props = set("fn hasEmail postalAddress phoneNumber timezone proxyFor".split())
    def _moderate_contactPoint(self, info, resmd=None, replace=False, doval=True):
        if info is None:
            info = OrderedDict()
        if not isinstance(info, Mapping):
            raise InvalidUpdate("contactPoint data is not an object", sys=self)
        info = OrderedDict([(k,v) for k,v in info.items() if k in self._contact_props])
        if info.get('hasEmail') and not info['hasEmail'].startswith("mailto:"):
            info['hasEmail'] = "mailto:"+info['hasEmail'].strip()

        if not replace and resmd and resmd.get('contactPoint'):
            info = self._merge_into(info, resmd['contactPoint'])
        info['@type'] = "vcard:Contact"

        if doval:
            schemauri = NERDM_SCH_ID + "/definitions/ContactInfo"
            if resmd and resmd.get("_schema"):
                schemauri = resmd["_schema"] + "/definitions/ContactInfo"
            self.validate_json(info, schemauri)
            
        return info

    
    def _update_component(self, nerd: NERDResource, data: Mapping, key=None, replace=False, doval=False):
        if not isinstance(data, Mapping):
            raise InvalidUpdate("component data is not an object", sys=self)
        id = key if isinstance(key, str) else data.get("@id")
        filepath = data.get('filepath')

        oldfile = None
        if id:
            oldfile = nerd.files.get(id)
            if not filepath and oldfile:
                filepath = oldfile.get('filepath')
        pos = key if isinstance(key, int) else None

        if filepath:
            data = self._update_file_comp(nerd, data, oldfile, replace=replace, doval=doval)
        else:
            data = self._update_listitem(nerd.nonfiles, self._moderate_nonfile, data, pos, replace, doval)
        return data

    def _filter_props(self, obj, props):
        # remove all properties from obj that are not listed in props
        delprops = [k for k in obj if k not in props or (not obj.get(k) and obj.get(k) is not False)]
        for k in delprops:
            del obj[k]

    _authprops = set("_schema @id fn familyName givenName middleName orcid affiliation proxyFor".split())
    _affilprops = set("@id title abbrev proxyFor location label description subunits".split())
    
    def _moderate_author(self, auth, doval=True):
        # we are assuming that merging has already occured

        self._filter_props(auth, self._authprops)
        if not auth.get("@type"):
            auth["@type"] = "foaf:Person"
# Set fn at finalization
#        if not auth.get('fn') and auth.get('familyName') and auth.get('givenName'):
#            auth['fn'] = auth['familyName']
#            if auth.get('givenName'):
#                auth['fn'] += ", %s" % auth['givenName']
#            if auth.get('middleName'):
#                auth['fn'] += " %s" % auth['middleName']

        if isinstance(auth.get('affiliation',[]), str):
            auth['affiliation'] = [OrderedDict([('title', auth['affiliation'])])]
        elif not isinstance(auth.get('affiliation', []), list):
            del auth['affiliation']
        if auth.get('affiliation'):
            affils = auth['affiliation']
            for affil in affils:
                self._filter_props(affil, self._affilprops)
                affil["@type"] = "org:Organization"
                if affil.get("title") == "NIST":
                    affil["title"] = NIST_NAME
                if affil.get("title") == NIST_NAME:
                    affil["@id"] = NIST_ROR
                    if not affil.get("abbrev"):
                        affil["abbrev"] = [ NIST_ABBREV ]
                    else:
                        if not isinstance(affil["abbrev"], list):
                            raise InvalidUpdate("Affiliate abbrev property is not a list: "+
                                                str(affil["abbrev"]))
                        if NIST_ABBREV not in affil["abbrev"]:
                            affil["abbrev"].append(NIST_ABBREV)

        # Finally, validate (if requested)
        schemauri = NERDMPUB_SCH_ID + "/definitions/Person"
        if auth.get("_schema"):
            if not auth['_schema'].startswith(NERDMPUB_SCH_ID_BASE):
                raise InvalidUpdate("Unsupported author schema: "+auth['_schema'], sys=self)
            schemauri = auth['_schema']
            del auth['_schema']
        if doval:
            self.validate_json(auth, schemauri)

        return auth

    _refprops = set(("@id _schema _extensionSchemas title abbrev proxyFor location label "+
                     "description citation refType doi inPreparation vol volNumber pages "+
                     "authors publishYear").split())
    _reftypes = set(("IsDocumentedBy IsSupplementTo IsSupplementedBy IsCitedBy Cites IsReviewedBy "+
                     "IsReferencedBy References IsSourceOf IsDerivedFrom "+
                     "IsNewVersionOf IsPreviousVersionOf").split())
    def _moderate_reference(self, ref, doval=True):
        # QUESTION/TODO:  new properties?  doi?, inprep?
        # we are assuming that merging has already occured
        self._filter_props(ref, self._refprops)
        if not ref.get("refType"):
            ref["refType"] = "References"
        if not ref.get(EXTSCHPROP) and ref["refType"] in self._reftypes:
            ref.setdefault(EXTSCHPROP, [])
        try:
            # upgrade the version of the BIB extension
            for i, uri in enumerate(ref.get(EXTSCHPROP,[])):
                if uri.startswith(NERDMBIB_SCH_ID_BASE) and not uri.startswith(NERDMBIB_SCH_ID):
                    parts = ref[EXTSCHPROP][i].split('#', 1)
                    if len(parts) == 2:
                        ref[EXTSCHPROP][i] = NERDMBIB_SCH_ID + parts[1]
        except AttributeError as ex:
            raise InvalidUpdate("_extensionSchemas: value is not a list of strings", sys=self) from ex
        if ref.get("refType") in self._reftypes and NERDMBIB_DEF+"DCiteReference" not in ref[EXTSCHPROP]:
            ref[EXTSCHPROP].append(NERDMBIB_DEF+"DCiteReference")

        if not ref.get("@type"):
            ref["@type"] = ["deo:BibliographicReference"]

        try:
            if not ref.get("location") and ref.get("proxyFor"):
                if ref["proxyFor"].startswith("doi:"):
                    ref["location"] = "https://doi.org/" + ref["proxyFor"][4:]
                elif ref["proxyFor"].startswith("https://doi.org/"):
                    ref["location"] = ref["proxyFor"]
                    ref["proxyFor"] = "doi:" + ref["proxyFor"][len("https://doi.org/"):]
            elif not ref.get("proxyFor") and ref.get("location","").startswith("https://doi.org/"):
                ref["proxyFor"] = "doi:" + ref["location"][len("https://doi.org/"):]

        except AttributeError as ex:
            raise InvalidUpdate("location or proxyFor: value is not a string", sys=self) from ex

        # Penultimately, add an id if doesn't already have one
        if not ref.get("@id"):
            ref['@id'] = "REPLACE"
        #    ref['@id'] = random_id("ref:")

        # Finally, validate (if requested)
        schemauri = NERDM_SCH_ID + "/definitions/BibliographicReference"
        if ref.get("_schema"):
            if not ref['_schema'].startswith(NERDM_SCH_ID_BASE):
                raise InvalidUpdate("Unsupported schema for a reference: "+ref['_schema'], sys=self)
            schemauri = ref['_schema']
            del ref['_schema']
        if doval:
            self.validate_json(ref, schemauri)

        if ref.get("@id") == "REPLACE":
            del ref['@id']
        return ref

    def _moderate_file(self, cmp, doval=True):
        # Note private assumptions: cmp contains filepath property
        if '_extensionSchemas' not in cmp:
            cmp['_extensionSchemas'] = []
        if not isinstance(cmp.get('_extensionSchemas',[]), list) or \
           not all(isinstance(s, str) for s in cmp.get('_extensionSchemas',[])):
            msg = "Component "
            if cmp.get("filepath") or cmp.get("@id"):
                msg += "%s " % (cmp.get("filepath") or cmp.get("@id"))
            msg += "_extensionSchemas: not a list of strings"
            raise InvalidUpdate(msg, sys=self)

        # ensure @type is set to something recognizable
        if cmp.get('downloadURL'):
            if not nerdutils.is_type(cmp, "DownloadableFile"):
                nerdutils.insert_type(cmp, "nrdp:DownloadableFile", "dcat:Distribution")
            if not nerdutils.is_any_type(cmp, ["DataFile", "ChecksumFile"]):
                nerdutils.insert_type(cmp, "nrdp:DataFile", "nrdp:DownloadableFile", "dcat:Distribution")
        else:
            if not nerdutils.is_type(cmp, "Subcollection"):
                nerdutils.insert_type(cmp, "nrdp:Subcollection")

        if self._has_exp_prop(cmp):
            # contains experimental data
            nerdutils.insert_type(cmp, "nrde:AcquisitionActivity", "dcat:Distribution")

        # set the mediaType and format if needed:
        if nerdutils.is_type(cmp, "DownloadableFile"):
            filext = os.path.splitext(cmp.get("filepath",""))[-1].lstrip('.')
            if not cmp.get("mediaType"):
                cmp["mediaType"] = self._choose_mediatype(filext)
                                                        
            if not cmp.get("format"):
                fmt = self._guess_format(filext, cmp["mediaType"])
                if fmt:
                    cmp["format"] = fmt

        # make sure the _extensionSchemas list is filled out
        cmp.setdefault(EXTSCHPROP, [])
        if nerdutils.is_type(cmp, "DataFile"):
            if not any(s.endswith("#/definitions/DataFile") for s in cmp[EXTSCHPROP]):
                cmp[EXTSCHPROP].append(NERDMPUB_DEF+"DataFile")
        elif nerdutils.is_type(cmp, "ChecksumFile"):
            if not any(s.endswith("#/definitions/ChecksumFile") for s in cmp[EXTSCHPROP]):
                cmp[EXTSCHPROP].append(NERDMPUB_DEF+"ChecksumFile")
        elif nerdutils.is_type(cmp, "DownloadableFile"):
            if not any(s.endswith("#/definitions/DownloadableFile") for s in cmp[EXTSCHPROP]):
                cmp[EXTSCHPROP].append(NERDMPUB_DEF+"DownloadableFile")

        if nerdutils.is_type(cmp, "Subcollection") and \
           not any(s.endswith("#/definitions/Subcollection") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMPUB_DEF+"Subcollection")
        if nerdutils.is_type(cmp, "AcquisitionActivity") and \
           not any(s.endswith("#/definitions/AcquisitionActivity") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMEXP_DEF+"AcquisitionActivity")

        # Finally, validate (if requested)
        schemauri = NERDM_SCH_ID + "/definitions/Component"
        if cmp.get("_schema"):
            if not cmp['_schema'].startswith(NERDM_SCH_ID_BASE):
                raise InvalidUpdate("Unsupported component schema: "+cmp['_schema'], sys=self)
            schemauri = cmp['_schema']
            del cmp['_schema']
        if doval:
            self.validate_json(cmp, schemauri)

        return cmp

    def _moderate_nonfile(self, cmp, doval=True):
        if 'filepath' in cmp and not cmp.get('filepath'):
            del cmp['filepath']
        if not cmp:
            raise InvalidUpdate("Empty compomponent included: "+str(cmp))
        if cmp.get('filepath') or nerdutils.is_any_type(cmp, ["Subcollection", "DownloadableFile",
                                                              "DataFile", "ChecksumFile"]):
            msg = cmp.get("@id","")
            if msg:
                msg += ": "
            msg += "Non-file component includes some file component content"
            raise InvalidUpdate(msg, sys=self)

        # we make sure a specific @type is set.  First filter out in consequential ones.
        cmp.setdefault("@type", [])
        types = [t for t in cmp["@type"]
                   if not any(t.endswith(":"+p) for p in ["Component", "Distribution", "Document"])]

        # If a type is set, we'll make no assumptions as to the meaning of non-Component properties
        # (and we'll let validation detect issues).  Otherwise, guess the type based on properties.
        if not types:
            extschs = cmp.get(EXTSCHPROP, [])
            if cmp.get("accessURL"):
                # it's an access page of some kind
                cmp["@type"].insert(0, "nrdp:AccessPage")

            elif cmp.get("searchURL"):
                # it's a DynamicResourceSet
                cmp["@type"].insert(0, "nrdg:DynamicResourceSet")

            elif cmp.get("resourceType") or cmp.get("proxyFor"):
                # it's an included resource
                cmp["@type"].insert(0, "nrd:IncludedResource")

        if self._has_exp_prop(cmp) and not nerdutils.is_type(cmp, "AcquisitionActivity"):
            # points to experimental data
            nerdutils.insert_type(cmp, "nrde:AcquisitionActivity", "dcat:Distribution")

        cmp.setdefault(EXTSCHPROP, [])
        if nerdutils.is_type(cmp, "AccessPage") and \
           not any(s.endswith("#/definitions/AccessPage") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMPUB_DEF+"AccessPage")
        if nerdutils.is_type(cmp, "SearchPage") and \
           not any(s.endswith("#/definitions/SearchPage") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMPUB_DEF+"SearchPage")
        if nerdutils.is_type(cmp, "API") and \
           not any(s.endswith("#/definitions/API") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMSW_DEF+"API")
        if nerdutils.is_type(cmp, "DynamicResourceSet") and \
           not any(s.endswith("#/definitions/DynamicResourceSet") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMAGG_DEF+"DynamicResourceSet")
        if nerdutils.is_type(cmp, "IncludedResource") and \
           not any(s.endswith("#/definitions/IncludedResource") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDM_DEF+"IncludedResource")
        if nerdutils.is_type(cmp, "AcquisitionActivity") and \
           not any(s.endswith("#/definitions/AcquisitionActivity") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMEXP_DEF+"AcquisitionActivity")

        # Finally, validate (if requested)
        schemauri = NERDM_SCH_ID + "/definitions/Component"
        if cmp.get("_schema"):
            if not cmp['_schema'].startswith(NERDM_SCH_ID_BASE):
                raise InvalidUpdate("Unsupported component schema: "+cmp['_schema'], sys=self)
            schemauri = cmp['_schema']
            del cmp['_schema']
        if doval:
            self.validate_json(cmp, schemauri)

        return cmp

    def _moderate_res_data(self, resmd, basemd, nerd, replace=False, doval=True):
        if not resmd.get("_schema"):
            resmd["_schema"] = NERDM_SCH_ID

        restypes = resmd.get("@type", [])
        if not replace:
            restypes += basemd.get("@type", [])
        resmd = self._merge_into(resmd, basemd)
        resmd["@type"] = restypes

        errors = []
        for prop in "contactPoint description keyword landingPage".split():
            if prop in resmd:
                if resmd.get(prop) is None:
                    del resmd[prop]
                else:
                    try:
                        moderate = '_moderate_' + prop
                        if hasattr(self, moderate):
                            moderate = getattr(self, moderate)
                            resmd[prop] = moderate(resmd[prop], resmd, replace=True, doval=False)

                    except InvalidUpdate as ex:
                        errors.extend(ex.errors)

        resmd.setdefault("@type", [])
        try:
            resmd = self._moderate_restype(resmd["@type"], resmd, nerd, replace=True, doval=False)
        except InvalidUpdate as ex:
            errors.extend(ex.errors)

        if errors:
            raise InvalidUpdate(errors=errors, sys=self)

        if doval:
            self.validate_json(resmd)
        return resmd

                
class DAPServiceFactory(ProjectServiceFactory):
    """
    Factory for creating DAPService instances attached to a backend DB implementation and which act 
    on behalf of a specific user.  The configuration parameters that can be provided to this factory 
    is the union of those supported by the following classes:
      * :py:class:`DAPService` (``assign_doi`` and ``doi_naan``)
      * :py:class:`~nistoar.midas.dbio.project.ProjectService` (``clients`` and ``dbio``)
    """

    def __init__(self, dbclient_factory: DBClientFactory, config: Mapping={}, log: Logger=None,
                 nerdstore: NERDResourceStorage=None, project_coll: str=None):
        """
        create a service factory associated with a particulr DB backend.
        :param DBClientFactory dbclient_factory:  the factory instance to use to create a DBClient to 
                                 talk to the DB backend.
        :param Mapping  config:  the configuration for the service (see class-level documentation).  
        :param Logger      log:  the Logger to use in the service.  
        :param NERDResourceStorage nerdstore:  the NERDResourceStorage instance to use to access NERDm 
                                 records.  If not provided, one will be created based on the given 
                                 configuration (in the ``nerdstore`` parameter). 
        :param str project_coll: the project type (i.e. the DBIO project collection to access); 
                                 default: "dap".
        """
        if not project_coll:
            project_coll = DAP_PROJECTS
        self._nerdstore = nerdstore
        super(DAPServiceFactory, self).__init__(project_coll, dbclient_factory, config, log)

    def create_service_for(self, who: Agent=None):
        """
        create a service that acts on behalf of a specific user.  
        :param Agent who:    the user that wants access to a project
        """
        return DAPService(self._dbclifact, self._cfg, who, self._log, self._nerdstore, self._prjtype)

    
class DAPApp(MIDASProjectApp):
    """
    A MIDAS ServiceApp supporting a DAP service following the mds3 conventions
    """
    
    def __init__(self, dbcli_factory: DBClientFactory, log: Logger, config: dict={},
                 service_factory: ProjectServiceFactory=None, project_coll: str=None):
        if not project_coll:
            project_coll = DAP_PROJECTS
        uselog = log.getChild(project_coll)
        if not service_factory:
            service_factory = DAPServiceFactory(dbcli_factory, config, uselog, project_coll=project_coll)
        super(DAPApp, self).__init__(service_factory, uselog, config)
        self._data_update_handler = DAPProjectDataHandler
        self._info_update_handler = DAPProjectInfoHandler
        self._selection_handler = DAPProjectSelectionHandler

class DAPProjectDataHandler(ProjectDataHandler):
    """
    A :py:class:`~nistoar.midas.wsgi.project.ProjectDataHandler` specialized for editing NERDm records.

    Note that this implementation inherits its PUT, PATCH, and DELETE handling from its super-class.
    """
    _allowed_post_paths = "authors references components".split() + [FILE_DELIM, LINK_DELIM]

    def __init__(self, service: ProjectService, subapp: ServiceApp, wsgienv: dict, start_resp: Callable, 
                 who: Agent, id: str, datapath: str, config: dict=None, log: Logger=None):
        super(DAPProjectDataHandler, self).__init__(service, subapp, wsgienv, start_resp, who,
                                                    id, datapath, config, log)

    def do_GET(self, path, ashead=False):
        """
        respond to a GET request
        :param str path:  a path to the portion of the data to get.  This is the same as the `datapath`
                          given to the handler constructor.  This will be an empty string if the full
                          data object is requested.
        :param bool ashead:  if True, the request is actually a HEAD request for the data
        """
        try:
            out = self.svc.get_nerdm_data(self._id, path)
        except NotAuthorized as ex:
            return self.send_unauthorized()
        except ObjectNotFound as ex:
            if ex.record_part:
                return self.send_error_resp(404, "Data property not found",
                                            "No data found at requested property", self._id, ashead=ashead)
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found", self._id, ashead=ashead)
        except PartNotAccessible as ex:
            return self.send_error_resp(405, "Data property not retrieveable",
                                  "Requested data property cannot be retrieved independently of its ancestor")
        return self.send_json(out)

    def do_POST(self, path):
        """
        respond to a POST request.  Allowed paths include "authors", "references", "components", 
        "pdr:f" (for files), and "pdr:see" (for non-file components).  
        :param str path:  a path to the portion of the data to get.  This is the same as the `datapath`
                          given to the handler constructor.  This will be an empty string if the full
                          data object is requested.
        :param bool ashead:  if True, the request is actually a HEAD request for the data
        """
        try:
            newdata = self.get_json_body()
        except self.FatalError as ex:
            return self.send_fatal_error(ex)

        try:
            if not self.svc.dbcli.exists(self._id):
                return self.end_error_resp(404, "ID not found"
                                           "Record with requested identifier not found", self._id)

            if path == "authors":
                out = self.svc.add_author(self._id, newdata)
            elif path == "references":
                out = self.svc.add_reference(self._id, newdata)
            elif path == FILE_DELIM:
                out = self.svc.set_file_component(self._id, newdata)
            elif path == LINK_DELIM:
                out = self.svc.add_nonfile_component(self._id, newdata)
            elif path == "components":
                if 'filepath' in newdata:
                    out = self.svc.set_file_component(self._id, newdata)
                else:
                    out = self.svc.add_nonfile_component(self._id, newdata)

            else:
                return self.send_error_resp(405, "POST not allowed",
                                            "POST not supported on path")

        except NotAuthorized as ex:
            return self.send_unauthorized()
        except ObjectNotFound as ex:
            return send.send_error_resp(404, "Path not found",
                                        "Requested path not found within record", self._id) 
        except InvalidUpdate as ex:
            return self.send_error_resp(400, "Invalid Input Data", str(ex), self._id)
        except PartNotAccessible as ex:
            return self.send_error_resp(405, "Data part not updatable",
                                        "Requested part of data cannot be updated", self._id)

        return self.send_json(out, "Added", 201)

class DAPProjectInfoHandler(ProjectInfoHandler):
    """
    A :py:class:`~nistoar.midas.wsgi.project.ProjectInfoHandler` specialized for editing DAP records.
    In particular, it supporst PATCHing actions onto the ``file_space`` property to trigger
    synchronization with the associated space in the file manager.
    """
    FILE_SPACE = "file_space"

    def do_OPTIONS(self, path):
        if path == self.FILE_SPACE:
            return self.send_options(["GET", "PUT", "PATCH"])
        return self.send_options(["GET"])

    def do_PUT(self, path):
        return self.do_PATCH(path)

    def do_PATCH(self, path):
        if path != self.FILE_SPACE:
            return self.send_error_resp(405, "Method Not Allowed",
                                        f"This attribute of a draft record cannot be updated directly",
                                        self._id)

        # handle the PATCH on file_space
        # get the record
        try:
            prec = self.svc.get_record(self._id)
        except NotAuthorized as ex:
            return self.send_unauthorized()
        except ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found",
                                        "Record with requested identifier not found",
                                        self._id, ashead=ashead)

        # get the action request
        action = "sync"   # the default action (if there is not input doc)
        req = {}
        try:
            contlen = int(self._env.get('CONTENT_LENGTH', 0))
        except ValueError as ex:
            return self.send_error_resp(400, "Bad Content Length value")

        if contlen > 0:
            if self._env.get('CONTENT_TYPE') and "/json" not in self._env['CONTENT_TYPE']:
                return self.send_error_resp(400, "Input is not JSON",
                                            "Non-JSON content-type is not supported", self._id)
            try:
                req = self.get_json_body()
            except self.FatalError as ex:
                return self.send_fatal_error(ex)

        if req.get('action'):
            action = req['action']

        return self._apply_fs_action(action)

    def _apply_fs_action(self, action):
        fssumm = {}
        try:
            if action == "sync":
                fssumm = self.svc.sync_to_file_space(self._id)
            else:
                return self.send_error_resp(400, "Unrecognized action",
                                            "Unrecognized action requested")
        except NotAuthorized as ex:
            return self.send_unauthorized()
        except ObjectNotFound as ex:
            return self.send_error_resp(404, "ID not found")
        except NotEditable as ex:
            return self.send_error_resp(409, "Not in editable state", "Record is not in state=edit or ready")
        except (FileSpaceException, NERDStorageException) as ex:
            self.log.error("Trouble communicating with file manager: %s", str(ex))
            return self.send_error_resp(500, "File manager service error",
                                        "Trouble communicating with file manager")

        return self.send_json(fssumm)

class DAPProjectSelectionHandler(ProjectSelectionHandler):
    """
    A :py:class:`~nistoar.midas.wsgi.project.ProjectSelectionHandler` specialized for selecting DAP records.
    In particular, it ensures that the records returned from a search are full DAP records (including 
    the information computed on the fly).
    """

    def __init__(self, service: ProjectService, subapp: ServiceApp, wsgienv: dict, start_resp: Callable,
                 who: Agent, config: dict=None, log: Logger=None):
        super(DAPProjectSelectionHandler, self).__init__(service, subapp, wsgienv, start_resp, who,
                                                         config, log)
        self._fmcli = None
        if hasattr(service, '_fmcli'):
            self._fmcli = service._fmcli
        
    def _select_records(self, perms, **constraints) -> Iterator[ProjectRecord]:
        """
        submit a search query in a project specific way.  This implementation ensures that 
        DAPProjectRecords are returned.
        :return:  an iterator for the matched records
        """
        for rec in self._dbcli.select_records(perms, **constraints):
            yield to_DAPRec(rec, self._fmcli)

    def _adv_selected_records(self, filter, perms) -> Iterator[ProjectRecord]:
        """
        submit the advanced search query in a project-specific way. This implementation passes 
        the query directly to the generic DBClient instance.
        :return:  a generator that iterates through the matched records
        """
        for rec in self._dbcli.select_constraint_records(filter, perms):
            yield to_DAPRec(rec, self._fmcli)


    
