"""
The DAP Authoring Service implemented using the mdsx convention.  This convention represents an
implementation provided for development purposes and not intended for production use. 

Support for the web service frontend is provided as 
WSGI :ref:class:`~nistoar.pdr.publish.service.wsgi.SubApp` implementation.
"""
from logging import Logger
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Sequence, Callable

from ...dbio import (DBClient, DBClientFactory, ProjectRecord,
                     ProjectService, ProjectServiceFactory, DAP_PROJECTS)
from ...dbio.wsgi.project import MIDASProjectApp
from nistoar.base.config import ConfigurationException
from nistoar.nerdm.constants import core_schema_base, schema_versions
from nistoar.pdr import constants as const
from nistoar.pdr.publish.prov import PubAgent

ASSIGN_DOI_NEVER   = 'never'
ASSIGN_DOI_ALWAYS  = 'always'
ASSIGN_DOI_REQUEST = 'request'
NERD_PRE = "nrd"
NERDPUB_PRE = "nrdp"
NERDM_SCH_ID_BASE = core_schema_base
NERDMPUB_SCH_ID_BASE = core_schema_base + "pub/"
NERDM_SCH_VER = schema_versions[0]
NERDMPUB_SCH_VER = NERDM_SCH_VER
NERDM_SCH_ID = NERDM_SCH_ID_BASE + NERDM_SCH_VER + "#"
NERDMPUB_SCH_ID = NERDMPUB_SCH_ID_BASE + NERDMPUB_SCH_VER + "#"
NERDPUB_DEF = NERDMPUB_SCH_ID + "/definitions/"
NERDM_CONTEXT = "https://data.nist.gov/od/dm/nerdm-pub-context.jsonld"

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

    Note that the DOI is not yet registered with DataCite; it is only internally reserved and included
    in the record NERDm data.  
    """

    def __init__(self, dbclient_factory: DBClient, config: Mapping={}, who: PubAgent=None,
                 log: Logger=None, project_type=DAP_PROJECTS):
        """
        create a request handler
        :param DBClient dbclient:  the DBIO client instance to use to access and save project records
        :param dict       config:  the handler configuration tuned for the current type of project
        :param dict      wsgienv:  the WSGI request context 
        :param Logger        log:  the logger to use for log messages
        """
        super(DAPService, self).__init__(project_type, dbclient_factory, config, who, log)

        self.cfg.setdefault('assign_doi', ASSIGN_DOI_REQUEST)
        if not self.cfg.get('doi_naan') and self.cfg.get('assign_doi') != ASSIGN_DOI_NEVER:
            raise ConfigurationException("Missing configuration: doi_naan")

    def _new_data_for(self, recid, meta=None):
        out = OrderedDict([
            ("_schema", NERDM_SCH_ID),
            ("@context", NERDM_CONTEXT),
            ("_extensionSchemas", [NERDPUB_DEF + "PublicDataResource"]),
            ("@id", self._arkid_for(recid)),
            ("@type", [":".join([NERDPUB_PRE, "PublicDataResource"]), "dcat:Resource"])
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
        return OrderedDict([
            ("@id", id),
            ("@type", ["nrd:AccessPage", "dcat:Distribution"]),
            ("title", "Software Repository in GitHub"),
            ("accessURL", link)
        ])

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

