"""
Subapp supporting DAP
"""
from logging import Logger
from collections import OrderedDict
from collections.abc import Mapping, MutableMapping, Sequence, Callable

from ...dbio import DBClient, DBClientFactory, ProjectRecord
from ...dbio.wsgi.broker import ProjectRecordBroker
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

class DAPBroker(ProjectRecordBroker):
    """
    a project record request broker class for DAP records.  
    """

    def __init__(self, dbclient: DBClient, config: Mapping={}, who: PubAgent=None,
                 wsgienv: dict=None, log: Logger=None):
        """
        create a request handler
        :param DBClient dbclient:  the DBIO client instance to use to access and save project records
        :param dict       config:  the handler configuration tuned for the current type of project
        :param dict      wsgienv:  the WSGI request context 
        :param Logger        log:  the logger to use for log messages
        """
        super(DAPBroker, self).__init__(dbclient, config, who, wsgienv, log)

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

    
class DAPApp(MIDASProjectApp):
    """
    A MIDAS SubApp supporting a DAP service
    """

    def __init__(self, typename: str, log: Logger, dbcli_factory: DBClientFactory, config: dict={}):
        if not typename:
            typename = "dap"
        super(DAPApp, self).__init__(typename, log, dbcli_factory, config, DAPBroker)
    
                 
