"""
The DAP Authoring Service implemented using the mds3 convention.  This convention represents the 
first DAP convention powered by the DBIO APIs.

Support for the web service frontend is provided as a 
WSGI :ref:class:`~nistoar.pdr.publish.service.wsgi.SubApp` implementation.
"""
import os
from logging import Logger
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Sequence, Callable
from typing import List

from ...dbio import (DBClient, DBClientFactory, ProjectRecord, AlreadyExists, NotAuthorized, ACLs,
                     InvalidUpdate, ProjectService, ProjectServiceFactory, DAP_PROJECTS)
from ...dbio.wsgi.project import MIDASProjectApp
from nistoar.base.config import ConfigurationException, merge_config
from nistoar.nerdm import constants as nerdconst, utils as nerdutils
from nistoar.pdr import def_schema_dir, def_etc_dir, constants as const
from nistoar.pdr.utils import build_mime_type_map, read_json
from nistoar.pdr.publish.prov import PubAgent

from . import validate
from ..nerdstore import NERDResource, NERDResourceStorage, NERDResourceStorageFactory

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

EXTSCHPROP = "_extensionSchemas"

class DAPService(ProjectService):
    """
    a project record request broker class for DAP records.  

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

    Note that the DOI is not yet registered with DataCite; it is only internally reserved and included
    in the record NERDm data.  
    """

    def __init__(self, dbclient_factory: DBClient, config: Mapping={}, who: PubAgent=None,
                 log: Logger=None, nerdstore: NERDResourceStorage=None, project_type=DAP_PROJECTS,
                 minnerdmver=(0, 6)):
        """
        create a request handler
        :param DBClient dbclient:  the DBIO client instance to use to access and save project records
        :param dict       config:  the handler configuration tuned for the current type of project
        :param dict      wsgienv:  the WSGI request context 
        :param Logger        log:  the logger to use for log messages
        """
        super(DAPService, self).__init__(project_type, dbclient_factory, config, who, log,
                                         _subsys="Digital Asset Publication Authoring System",
                                         _subsysabbrev="DAP")

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

        self._mediatypes = {
            "csv": "text/csv", "txt": "text/plain", "html": "text/html", "htm": "text/html",
            "sha256": "text/plain", "md5": "text/plain"
        }
        mimefiles = self.cfg.get('mimetype_files', [])
        if not isinstance(mimefiles, list):
            mimefiles = [mimefiles]
        if mimefiles:
            self._mediatypes = build_mime_type_map(mimefiles)
            
        self._formatbyext = {}
        if 'file_format_maps' in self.cfg:
            mimefiles = self.cfg.get('file_format_maps', [])
        else:
            mimefiles = os.path.join(def_etc_dir, "fext2format.json")
        if not isinstance(mimefiles, list):
            mimefiles = [mimefiles]
        for ffile in mimefiles:
            try:
                fmp = read_json(ffile)
                if not isinstance(fmp, Mapping):
                    raise ValueError("wrong format for format-map file: contains "+type(fmp))
                if fmp:
                    self._formatbyext.update(fmp)
            except Exception as ex:
                self.log.warning("Unable to read format-map file, %s: %s", ffile, str(ex))

        self._minnerdmver = minnerdmver

    def _guess_format(self, file_ext, mimetype=None):
        if not mimetype:
            mimetype = self._mediatypes.get(file_ext)
        fmtd = self._formatbyext.get(file_ext)
        if fmtd:
            return { "description": fmtd }
        return None

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
        prec = self.dbcli.create_record(name, shoulder)
        nerd = None

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
                    for v in range(len(ver)):
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
            prec.data = self._summarize(nerd)

            if data:
                self.update_data(prec.id, data, prec=prec, nerd=nerd)  # this will call prec.save()
            else:
                prec.save()

        except Exception as ex:
            if nerd:
                try:
                    nerd.delete()
                except Exception as ex:
                    self.log.error("Error while cleaning up NERDm data after create failure: %s", str(ex))
            try:
                prec.delete()
            except Exception as ex:
                self.log.error("Error while cleaning up DAP record after create failure: %s", str(ex))
            raise

        return prec

    def _new_data_for(self, recid, meta=None, schemaid=None):
        if not schemaid:
            schemaid = NERDM_SCH_ID
        out = OrderedDict([
            ("_schema", schemaid),
            ("@context", NERDM_CONTEXT),
            (EXTSCHPROP, [NERDMPUB_DEF + "PublicDataResource"]),
            ("@id", self._arkid_for(recid)),
            ("@type", [":".join([NERDMPUB_PRE, "PublicDataResource"]), "dcat:Resource"])
        ])

        if self.cfg.get('assign_doi') == ASSIGN_DOI_ALWAYS:
            out['doi'] = self._doi_for(recid)

        if meta:
            if meta.get("resourceType"):
                addtypes = []
                if meta['resourceType'].lower() == "software":
                    addtypes = [":".join([NERDPUB_PRE, "Software"])]
                elif meta['resourceType'].lower() == "srd":
                    addtypes = [":".join([NERDPUB_PRE, "SRD"])]
                out["@type"] = addtypes + out["@type"]

            if meta.get("softwareLink"):
                swcomp = self._get_sw_desc_for(meta["softwareLink"])
                if not 'components' in out:
                    out['components'] = []
                out['components'] = [swcomp] + out['components']

            # contact info

        return out

    def _get_sw_desc_for(self, link):
        id = link.rsplit('/', 1)[-1]
        id = "%s/repo:%s" % (const.LINKCMP_EXTENSION.lstrip('/'), id)
        out = OrderedDict([
            ("@id", id),
            ("@type", ["nrd:AccessPage", "dcat:Distribution"]),
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
            out['creatorisContact'] = true

        return out
        
    def _new_metadata_for(self, shoulder=None):
        return OrderedDict([
            ("resourceType", "data"),
            ("creatorisContact", True)
        ])

    def replace_data(self, id, newdata, part=None, prec=None, nerd=None):
        """
        Replace the currently stored data content of a record with the given data.  It is expected that 
        the new data will be filtered/cleansed via an internal call to :py:method:`dress_data`.  
        :param str      id:  the identifier for the record whose data should be updated.
        :param str newdata:  the data to save as the new content.  
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             the given `newdata` is a value that should be set to the property pointed 
                             to by `part`.  
        :param ProjectRecord prec:  the previously fetched and possibly updated record corresponding to `id`.
                             If this is not provided, the record will by fetched anew based on the `id`.  
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by `id`.  
        :raises PartNotAccessible:  if replacement of the part of the data specified by `part` is not allowed.
        :raises InvalidUpdate:  if the provided `newdata` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content.
        """
        return self._update_data(id, ndwdata, part, prec, nerd, True)

    def update_data(self, id, newdata, part=None, prec=None, nerd=None):
        """
        merge the given data into the currently save data content for the record with the given identifier.
        :param str      id:  the identifier for the record whose data should be updated.
        :param str newdata:  the data to save as the new content.  
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             the given `newdata` is a value that should be set to the property pointed 
                             to by `part`.  
        :param ProjectRecord prec:  the previously fetched and possibly updated record corresponding to `id`.
                             If this is not provided, the record will by fetched anew based on the `id`.  
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by `id`.  
        :raises PartNotAccessible:  if replacement of the part of the data specified by `part` is not allowed.
        :raises InvalidUpdate:  if the provided `newdata` represents an illegal or forbidden update or 
                             would otherwise result in invalid data content.
        """
        return self._update_data(id, newdata, part, prec, nerd, False)

    def clear_data(self, id, part=None, prec=None):
        """
        remove the stored data content of the record and reset it to its defaults.  
        :param str      id:  the identifier for the record whose data should be cleared.
        :param stt    part:  the slash-delimited pointer to an internal data property.  If provided, 
                             only that property will be cleared (either removed or set to an initial
                             default).
        :param ProjectRecord prec:  the previously fetched and possibly updated record corresponding to `id`.
                             If this is not provided, the record will by fetched anew based on the `id`.  
        :raises ObjectNotFound:  if no record with the given ID exists or the `part` parameter points to 
                             an undefined or unrecognized part of the data
        :raises NotAuthorized:   if the authenticated user does not have permission to read the record 
                             given by `id`.  
        :raises PartNotAccessible:  if clearing of the part of the data specified by `part` is not allowed.
        """
        if not prec:
            prec = self.dbcli.get_record_for(id, ACLs.WROTE)   # may raise ObjectNotFound/NotAuthorized

        if not self._store.exists(id):
            self.log.warning("NERDm data for id=%s not found in metadata store", prec.id)
            nerd = self._new_data_for(prec.id, prec.meta)
            self._store.load_from(nerd)
        nerd = self._store.open(id)

        if part:
            if part == "authors":
                nerd.authors.empty()
            elif part == "references":
                nerd.references.empty()
            elif part == "components":
                nerd.files.empty()
                nerd.nonfiles.empty()
            elif part in "title rights disclaimer description".split():
                resmd = nerd.get_res_data()
                del resmd[part]
                nerd.replace_res_data(resmd)
            else:
                raise PartNotAccessible(prec.id, path, "Clearing %s not allowed" % path)

        else:
            nerd.authors.empty()
            nerd.references.empty()
            nerd.files.empty()
            nerd.nonfiles.empty()
            nerd.replace_res_data(self._new_data_for(prec.id, prec.meta))


    def _update_data(self, id, newdata, part=None, prec=None, nerd=None, replace=False):
        if not prec:
            prec = self.dbcli.get_record_for(id, ACLs.WROTE)   # may raise ObjectNotFound/NotAuthorized

        if not nerd:
            if not self._store.exists(id):
                self.log.warning("NERDm data for id=%s not found in metadata store", prec.id)
                nerd = self._new_data_for(prec.id, prec.meta)
                if prec.data.get("title"):
                    nerd["title"] = prec.data.get("title")
                self._store.load_from(nerd)

            nerd = self._store.open(id)

        if not part:
            # this is a complete replacement; save updated NERDm data to the metadata store
            try:
                data = self._update_all_nerd(prec, nerd, newdata, replace)
            except InvalidUpdate as ex:
                ex.record_id = prec.id
                raise

        else:
            # replacing just a part of the data
            try:
                data = self._update_part_nerd(prec, nerd, part, newdata, replace)
            except InvalidUpdate as ex:
                ex.record_id = prec.id
                ex.record_part = part
                raise

        prec.data = self._summarize(nerd)
        prec.save()

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
        out["author_count"] = nerd.authors.count
        out["file_count"] = nerd.files.count
        out["nonfile_count"] = nerd.nonfiles.count
        out["reference_count"] = nerd.references.count
        return out

    _handsoff = ("@id @context publisher issued firstIssued revised annotated "   + \
                 "bureauCode programCode systemOfRecords primaryITInvestmentUII " + \
                 "doi ediid releaseHistory status theme").split()

    def _update_all_nerd(self, prec: ProjectRecord, nerd: NERDResource, data: Mapping, replace=False):
        # filter out properties that the user is not allow to update
        newdata = OrderedDict()
        for prop in data:
            if not prop.startswith("_") and prop not in self._handsoff:
                newdata[prop] = data[prop]

        errors = []
        authors = newdata.get('authors')
        if authors:
            del newdata['authors']
            authors = self._moderate_authors(authors, nerd, replace)
        refs = newdata.get('references')
        if refs:
            del newdata['references']
            refs = self._moderate_references(refs, nerd, replace)

        comps = newdata.get('components')
        files = []
        nonfiles = []
        if comps:
            del newdata['components']
            for cmp in comps:
                if 'filepath' in cmp:
                    files.append(self._moderate_file(cmp))
                else:
                    nonfiles.append(self._moderate_nonfile(cmp))
            comps = nonfiles + files

        # handle resource-level data: merge the new data into the old and validate the result
        if replace:
            oldresdata = self._new_data_for(prec.id, prec.meta, newdata.get("_schema"))
        else:
            oldresdata = nerd.get_data(False)

        # merge and validate the resource-level data
        newdata = self._moderate_res_data(newdata, oldresdata, nerd, replace)   # may raise InvalidUpdate

        # all data is merged and validated; now commit
        nerd.replace_res_data(newdata)
        if authors:
            self._update_part_nerd("authors", prec, nerd, authors, replace, doval=False)
        if refs:
            self._update_part_nerd("references", prec, nerd, refs, replace, doval=False)
        if comps:
            self._update_part_nerd("components", prec, nerd, comps, replace, doval=False)

        return nerd.get_data(True)

        
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


    def _update_part_nerd(self, path: str, prec: ProjectRecord, nerd: NERDResource, data: Mapping,
                          replace=False, doval=True):
        schemabase = prec.data.get("_schema") or NERDMPUB_SCH_ID
        
        m = re.search(r'^([a-z]+s)\[([\w\d]+)\]$', path)
        if m:
            # path is of the form xxx[k] and refers to an item in a list
            key = m.group(3)
            try:
                key = int(key)
            except ValueError:
                pass
            
            if m.group(1) == "authors":
                self._update_author(prec, nerd, data, replace, doval=doval)
            elif m.group(1) == "references":
                data["_schema"] = schemabase+"/definitions/BibliographicReference"
                self._update_reference(prec, nerd, data, replace, doval=doval)
            elif m.group(1) == "components":
                data["_schema"] = schemabase+"/definitions/Component"
                self._update_component(prec, nerd, data, replace, doval=doval)
            else:
                raise PartNotAccessible(prec.id, path, "Updating %s not allowed" % path)

        elif path == "authors":
            if replace:
                self._replace_authors(prec, nerd, data, doval=doval)
            else:
                self._update_authors(prec, nerd, data, doval=doval)
        elif path == "references":
            if replace:
                self._replace_references(prec, nerd, data, doval=doval)
            else:
                self._update_references(prec, nerd, data, doval=doval)
        elif path == "components":
            if replace:
                self._replace_components(prec, nerd, data, doval=doval)
            else:
                self._update_components(prec, nerd, data, doval=doval)

        elif path == "contactPoint":
            if not isinstance(data, Mapping):
                raise InvalidUpdate("contactPoint data is not an object", sys=self)
            res = nerd.get_res_data()
            res['contactPoint'] = self._moderate_contact(data, res, replace=replace, doval=doval)
                # may raise InvalidUpdate
            nerd.replace_res_data(res)
            
        elif path == "@type":
            if not isinstance(data, (list, str)):
                raise InvalidUpdate("@type data is not a list of strings", sys=self)
            res = nerd.get_res_data()
            res = self._moderate_restype(data, res, nerd, replace=replace, doval=doval)
            nerd.replace_res_data(res)

        elif path == "description":
            if not isinstance(data, (list, str)):
                raise InvalidUpdate("description data is not a list of strings", sys=self)
            res = nerd.get_res_data()
            res[path] = self._moderate_description(data, res, doval=doval)  # may raise InvalidUpdate
            nerd.replace_res_data(res)
            
        elif path in "title rights disclaimer".split():
            if not isinstance(data, str):
                raise InvalidUpdate("%s value is not a string" % path, sys=self)
            res = nerd.get_res_data()
            res[path] = self._moderate_text(data, res, doval=doval)  # may raise InvalidUpdate
            nerd.replace_res_data(res)
            
        else:
            raise PartNotAccessible(prec.id, path, "Updating %s not allowed" % path)
            
    def _moderate_text(self, val, resmd=None, doval=True):
        # make sure input value is the right type, is properly encoded, and
        # does not contain any illegal bits
        if doval and not isinstance(val, str):
            raise InvalidUpdate("Text value is not a string", sys=self)
        return val

    def _moderate_description(self, val, resmd=None, doval=True):
        if not isinstance(val, list):
            val = [val]
        return [self._moderate_text(t, resmd, doval=doval) for t in val if t != ""]

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
    def _moderate_contact(self, info, resmd=None, replace=False, doval=True):
        if not isinstance(info, Mapping):
            raise InvalidUpdate("contactPoint data is not an object", sys=self)
        info = OrderedDict([(k,v) for k,v in info.items() if k in self._contact_props])

        if not replace and resmd and resmd.get('contactInfo'):
            info = self._merge_into(info, resmd['contactInfo'])
        info['@type'] = "vcard:Contact"

        if doval:
            schemauri = NERDM_SCH_ID + "/definitions/ContactInfo"
            if resmd and resmd.get("_schema"):
                schemauri = resmd["_schema"] + "/definitions/ContactInfo"
            self.validate_json(info, schemauri)
            
        return info

    def _replace_authors(self, prec: ProjectRecord, nerd: NERDResource, data: List[Mapping]):
        if not isinstance(data, list):
            raise InvalidUpdate("authors data is not a list", sys=self)
        self._replace_listitems(nerd.authors, self._moderate_author, data)

    def _update_author(self, nerd: NERDResource, data: Mapping, pos: int=None, replace=False):
        if not isinstance(data, Mapping):
            raise InvalidUpdate("author data is not an object", sys=self)
        self._update_listitem(nerd.authors, self._moderate_author, data, pos, replace)

    def _update_authors(self, prec: ProjectRecord, nerd: NERDResource, data: List[Mapping]):
        if not isinstance(data, list):
            raise InvalidUpdate("authors data is not a list", sys=self)
        self._update_objlist(nerd.authors, self._moderate_author, data)

    def _replace_references(self, prec: ProjectRecord, nerd: NERDResource, data: List[Mapping]):
        if not isinstance(data, list):
            raise InvalidUpdate("references data is not a list", sys=self)
        self._replace_listitems(nerd.references, self._moderate_reference, data)

    def _update_reference(self, nerd: NERDResource, data: Mapping, pos: int=None, replace=False):
        if not isinstance(data, Mapping):
            raise InvalidUpdate("reference data is not an object", sys=self)
        self._update_listitem(nerd.references, self._moderate_reference, data, pos, replace)

    def _update_references(self, prec: ProjectRecord, nerd: NERDResource, data: List[Mapping]):
        if not isinstance(data, list):
            raise InvalidUpdate("references data is not a list", sys=self)
        self._update_objlist(nerd.references, self._moderate_reference, data)

    
    def _replace_listitems(self, objlist, moderate_func, data: List[Mapping]):
        data = [ moderate_func(a) for a in data ]   # may raise InvalidUpdate
        objlist.empty()
        for item in data:
            objlist.append(auth)

    def _update_listitem(self, objlist, moderate_func, data: Mapping, pos: int=None, replace=False):
        key = pos
        if key is None:
            key = data.get("@id")
        olditem = None
        if key:
            try:
                olditem = objlist.get(key)
                if not replace:
                    data = self._merge_into(data, olditem)
            except (KeyError, IndexError) as ex:
                pass

        data = moderate_func(data)   # may raise InvalidUpdate

        if olditem is None:
            objlist.append(data)
        else:
            objlist.set(key, data)

    def _update_objlist(self, objlist, moderate_func, data: List[Mapping]):
        # merge and validate all items before committing them
        for i, a in enumerate(data):
            olditem = None
            if a.get('@id'):
                try:
                    olditem = objlist.get(a['@id'])
                    data[i] = self._merge_into(a, olditem)
                except KeyError as ex:
                    pass
            data[i] = moderate_func(data[i])  # may raise InvalidUpdate

        # now commit
        for a in data:
            if a.get('@id'):
                objlist.set(a['@id'], a)
            else:
                objlist.append(a)

    def _replace_components(self, prec: ProjectRecord, nerd: NERDResource, data: List[Mapping]):
        if not isinstance(data, list):
            raise InvalidUpdate("authors data is not a list", sys=self)
        data = [ self._moderate_comp(a) for a in data ]   # may raise InvalidUpdate
        nerd.nonfiles.empty()
        nerd.files.empty()
        for cmp in data:
            if 'filepath' in cmp:
                nerd.files.set_file_at(cmp, cmp['filepath'])
            else:
                nerd.nonfiles.append(cmp)

    def _update_component(self, nerd: NERDResource, data: Mapping, pos: int=None, replace=False):
        if not isinstance(data, Mapping):
            raise InvalidUpdate("component data is not an object", sys=self)
        if 'filepath' in data:
            self.update_listitem(nerd.files, self._moderate_file, pos, replace)
        else:
            self.update_listitem(nerd.nonfiles, self._moderate_nonfile, pos, replace)

    def _update_components(self, prec: ProjectRecord, nerd: NERDResource, data: List[Mapping]):
        if not isinstance(data, list):
            raise InvalidUpdate("references data is not a list", sys=self)
        
        # merge and validate all items before committing them
        for i, cmp in enumerate(data):
            oldcmp = None
            if cmp.get('@id'):
                try:
                    oldcmp = objlist.get(a['@id'])
                    data[i] = self._merge_into(cmp, oldcmp)
                except KeyError as ex:
                    pass
            if 'filepath' in cmp:
                data[i] = self._moderate_file(data[i])  # may raise InvalidUpdate
            else:
                data[i] = self._moderate_nonfile(data[i])  # may raise InvalidUpdate

        # now commit
        for a in data:
            objlist = nerd.files if 'filepath' in cmp else nerd.nonfiles
            if a.get('@id'):
                objlist.set(a['@id'], a)
            else:
                objlist.append(a)

    def _filter_props(self, obj, props):
        delprops = [k for k in obj if k not in props or (not obj.get(k) and obj.get(k) is not False)]
        for k in delprops:
            del obj[k]

    _authprops = set("_schema fn familyName givenName middleName orcid affiliation proxyFor".split())
    _affilprops = set("@id title abbrev proxyFor location label description subunits".split())
    
    def _moderate_author(self, auth, doval=True):
        # we are assuming that merging has already occured

        self._filter_props(auth, self._authprops)
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
                     "description citation refType doi inprep").split())
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
                if any(s.startswith(NERDMBIB_SCH_ID_BASE) and s != NERDMBIB_SCH_ID
                       for s in ref[EXTSCHPROP]):
                    ref[EXTSCHPROP] = [NERDMBIB_SCH_ID if s.startswith(NERDMBIB_SCH_ID_BASE)
                                                                else s for s in ref[EXTSCHPROP]]
            except AttributeError as ex:
                raise InvalidUpdate("_extensionSchemas: value is not a list of strings", sys=self) from ex
            if NERDMBIB_SCH_ID not in ref[EXTSCHPROP]:
                ref[EXTSCHPROP].append(NERDMBIB_SCH_ID)

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

        # Finally, validate (if requested)
        schemauri = NERDM_SCH_ID + "/definitions/BibliographicReference"
        if ref.get("_schema"):
            if not ref['_schema'].startswith(NERDM_SCH_ID_BASE):
                raise InvalidUpdate("Unsupported schema for a reference: "+ref['_schema'], sys=self)
            schemauri = ref['_schema']
            del ref['_schema']
        if doval:
            self.validate_json(ref, schemauri)

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
                cmp["mediaType"] = self._mediatypes.get(filext, "application/octet-stream")
                                                        
            if not cmp.get("format"):
                fmt = self._guess_format(filext, cmp["mediaType"])
                if fmt:
                    cmp["format"] = fmt

        # make sure the _extensionSchemas list is filled out
        cmp.setdefault(EXTSCHPROP, [])
        if nerdutils.is_type(cmp, "DataFile") and \
           not any(s.endswith("#/definitions/DataFile") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMPUB_DEF+"DataFile")
        elif nerdutils.is_type(cmp, "ChecksumFile") and \
           not any(s.endswith("#/definitions/ChecksumFile") for s in cmp[EXTSCHPROP]):
            cmp[EXTSCHPROP].append(NERDMPUB_DEF+"ChecksumFile")
        elif nerdutils.is_type(cmp, "DownloadableFile") and \
           not any(s.endswith("#/definitions/DownloadableFile") for s in cmp[EXTSCHPROP]):
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
        restypes = resmd.get("@type", [])
        if not replace:
            restypes += basemd.get("@type", [])
        resmd = self._merge_into(resmd, basemd)
        resmd["@type"] = restypes

        errors = []
        if 'contactPoint' in resmd:
            if not resmd.get("contactPoint"):
                del resmd["contactPoint"]
            else:
                try:
                    resmd["contactPoint"] = self._moderate_contact(resmd["contactPoint"], resmd,
                                                                   replace=True, doval=False)
                except InvalidUpdate as ex:
                    errors.extend(ex.errors)

        if 'description' in resmd:
            if not resmd.get("description"):
                del resmd["description"]
            else:
                try:
                    resmd["description"] = self._moderate_description(resmd["description"], resmd,
                                                                      doval=False)
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
                 project_coll: str=None):
        """
        create a service factory associated with a particulr DB backend.
        :param DBClientFactory dbclient_factory:  the factory instance to use to create a DBClient to 
                                 talk to the DB backend.
        :param Mapping  config:  the configuration for the service (see class-level documentation).  
        :param Logger      log:  the Logger to use in the service.  
        :param str project_coll: the project type (i.e. the DBIO project collection to access); 
                                 default: "dap".
        """
        if not project_coll:
            project_coll = DAP_PROJECTS
        super(DAPServiceFactory, self).__init__(project_coll, dbclient_factory, config, log)

    def create_service_for(self, who: PubAgent=None):
        """
        create a service that acts on behalf of a specific user.  
        :param PubAgent who:    the user that wants access to a project
        """
        return DAPService(self._dbclifact, self._cfg, who, self._log, self._prjtype)

    
class DAPApp(MIDASProjectApp):
    """
    A MIDAS SubApp supporting a DAP service
    """
    
    def __init__(self, dbcli_factory: DBClientFactory, log: Logger, config: dict={}, project_coll: str=None):
        service_factory = DAPServiceFactory(dbcli_factory, config, project_coll)
        super(DAPApp, self).__init__(service_factory, log.getChild(DAP_PROJECTS), config)

