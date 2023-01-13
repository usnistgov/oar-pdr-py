import os, sys, pdb, shutil, logging, json, time, re
from pathlib import Path

import unittest as test
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy

from nistoar.testing import *
from nistoar.pdr import utils
from nistoar.pdr.preserve.bagit import NISTBag
from nistoar.pdr.publish.bagger import utils as bagutils
import nistoar.pdr.preserve.bagit.builder as bldr
import nistoar.pdr.exceptions as exceptions
from nistoar.pdr.publish import prov

from nistoar.pdr.publish.service import pdp
from nistoar.pdr.publish.service import status

# datadir = nistoar/preserve/data
datadir = Path(__file__).parents[2] / 'preserve' / 'data'
datadir2 = Path(__file__).parents[1] / 'data'
simplenerd = datadir / '1491nerdm.json'

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
#    logging.basicConfig(filename=os.path.join(tmpdir(),"test_builder.log"),
#                        level=logging.INFO)
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_bagger.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter(bldr.DEF_BAGLOG_FORMAT))
    rootlog.addHandler(loghdlr)
    rootlog.setLevel(logging.DEBUG)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
        loghdlr.close()
        loghdlr = None
    rmtmpdir()

def to_dict(odict):
    # converts all OrderDict instances in a Mapping into plain dicts (so that they can be tested for equality)
    out = dict(odict)
    for prop in out:
        if isinstance(out[prop], OrderedDict):
            out[prop] = to_dict(out[prop])
        if isinstance(out[prop], (list, tuple)):
            for i in range(len(out[prop])):
                if isinstance(out[prop][i], OrderedDict):
                    out[prop][i] = to_dict(out[prop][i])
    return out

tstag = prov.PubAgent("test", prov.PubAgent.AUTO, "tester")
ncnrag = prov.PubAgent("ncnr", prov.PubAgent.AUTO, "tester")

class TestPDPublishingService(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()
        self.workdir = self.tf.mkdir("work")
        self.mintdir = self.tf.mkdir("idregs")
        self.bagparent = Path(self.workdir) / 'sipbags'
        bgrcfg = {
            "bag_builder": {
                "validate_id": True,
                "init_bag_info": {
                    'NIST-BagIt-Version': "X.3",
                    "Organization-Address": ["100 Bureau Dr.",
                                             "Gaithersburg, MD 20899"]
                },
                "ensure_nerdm_type_on_add": bldr.NERDM_SCH_ID_BASE + "v0.7"
            },
            "doi_naan": "10.18434",
            "assign_doi": "always",
            "finalize": {},
            "repo_base_url": "https://test.pdr.net/"
        }
            
        self.cfg = {
            "working_dir": self.workdir,
            "clients": {
                "ncnr": {
                    "default_shoulder": "ncnr0",
                    "localid_provider": True,
                    "auth_key": "NCNRdev"
                },
                "default": {
                    "default_shoulder": "pdp0",
                    "localid_provider": False,
                    "auth_key": "MIDASdev"
                }
            },
            "shoulders": {
                "ncnr0": {
                    "allowed_clients": [ "ncnr" ],
                    "bagger": {
                        "override_config_for": "pdp0",
                        "factory_function": "nistoar.pdr.publish.service.pdp.PDPBaggerFactory"
                    },
                    "id_minter": {
                        "naan": "88434",
                        "based_on_sipid": True,
                        "sequence_start": 21
                    }
                },
                "pdp0": {
                    "allowed_clients": [ "test" ],
                    "bagger": bgrcfg,
                    "id_minter": {
                        "naan": "88434",
                        "sequence_start": 17
                    }
                }
            }
        }
        self.pubsvc = pdp.PDPublishingService(self.cfg, 'pdp0')

    def tearDown(self):
        self.tf.clean()

    def test_ctor(self):
        self.assertEqual(self.pubsvc.workdir, self.workdir)
        self.assertEqual(self.pubsvc.idregdir, os.path.join(self.workdir, "idregs"))
        self.assertEqual(self.pubsvc.bagparent, str(self.bagparent))
        self.assertEqual(self.pubsvc.statusdir, os.path.join(self.workdir, "status"))
        self.assertEqual(self.pubsvc.convention, "pdp0")

        self.cfg['working_dir'] = "/tmp"
        self.cfg['sip_status_dir'] = "sip_stat"
        self.cfg['sip_bags_dir'] = self.mintdir

        self.pubsvc = pdp.PDPublishingService(self.cfg, 'pdp12', idregdir="ids")
        
        self.assertEqual(self.pubsvc.workdir, "/tmp")
        self.assertEqual(self.pubsvc.idregdir, "/tmp/ids")
        self.assertEqual(self.pubsvc.bagparent, self.mintdir)
        self.assertEqual(self.pubsvc.statusdir, "/tmp/sip_stat")
        self.assertEqual(self.pubsvc.convention, "pdp12")

        self.pubsvc = pdp.PDPublishingService(self.cfg, 'pdp12', self.tf.mkdir('pdr'))

        workdir = os.path.join(self.tf.root,'pdr')
        self.assertEqual(self.pubsvc.workdir, workdir)
        self.assertEqual(self.pubsvc.idregdir, os.path.join(workdir,"idregs"))
        self.assertEqual(self.pubsvc.bagparent, self.mintdir)
        self.assertEqual(self.pubsvc.statusdir, os.path.join(workdir,"sip_stat"))
        self.assertEqual(self.pubsvc.convention, "pdp12")

        with self.assertRaises(pdp.PublishingStateException):
            self.pubsvc = pdp.PDPublishingService(self.cfg, 'pdp12', "/oar/data/pdr")

    def test_get_id_shoulder(self):
        self.assertEqual(self.pubsvc._get_id_shoulder(tstag, "", True), "pdp0")
        self.assertEqual(self.pubsvc._get_id_shoulder(tstag, None, True), "pdp0")

        ncnr = prov.PubAgent("ncnr", prov.PubAgent.AUTO, "tester")
        self.assertEqual(self.pubsvc._get_id_shoulder(ncnr, "ncnr0:4200", True), "ncnr0")
        self.assertEqual(self.pubsvc._get_id_shoulder(ncnr, "", True), "ncnr0")

        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            self.pubsvc._get_id_shoulder(tstag, "ncnr0:4200", False)
        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            self.pubsvc._get_id_shoulder(tstag, "mds2:4200", False)
        with self.assertRaises(pdp.BadSIPInputError):
            self.pubsvc._get_id_shoulder(tstag, "pdp4200", False)
        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            self.pubsvc._get_id_shoulder(tstag, "ncnr0:4200", True)
        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            self.pubsvc._get_id_shoulder(tstag, "mds2:4200", True)
        with self.assertRaises(pdp.BadSIPInputError):
            self.pubsvc._get_id_shoulder(tstag, "pdp4200", True)
        
        self.pubsvc.cfg['shoulders']['pdp0']['allowed_clients'] = ["goob"]
        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            self.pubsvc._get_id_shoulder(tstag, "", True)
        
        self.pubsvc.cfg['shoulders']['pdp0']['allowed_clients'] = ["test"]
        self.assertEqual(self.pubsvc._get_id_shoulder(tstag, "", True), "pdp0")
        del self.pubsvc.cfg['clients']['default']
        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            self.pubsvc._get_id_shoulder(tstag, "", True)

    def test_get_minter(self):
        mntr = self.pubsvc._get_minter("pdp0")
        self.assertTrue(mntr)
        self.assertEqual(mntr.shldr, "pdp0")
        self.assertEqual(mntr.shldrdelim, "-")
        self.assertEqual(os.path.dirname(mntr.registry.store), self.pubsvc.idregdir)
        self.assertIs(self.pubsvc._get_minter("pdp0"), mntr)
        self.assertIsNot(self.pubsvc._get_minter("ncnr0"), mntr)
        

    def test_set_identifiers(self):
        mntr = self.pubsvc._get_minter("pdp0")
        nerd = { }
        self.pubsvc._set_identifiers(nerd, mntr, None)
        self.assertEqual(nerd['@id'], "ark:/88434/pdp0-0017sg")
        self.assertEqual(nerd['pdr:sipid'], "pdp0-0017")
        self.assertIs(self.pubsvc._get_minter("pdp0"), mntr)

        mntr = self.pubsvc._get_minter("ncnr0")
        nerd = { }
        self.pubsvc._set_identifiers(nerd, mntr, None)
        self.assertEqual(nerd['@id'], "ark:/88434/ncnr0-0021sh")
        self.assertEqual(nerd['pdr:sipid'], "ncnr0-0021")

        mntr = self.pubsvc._get_minter("ncnr0")
        nerd = { }
        self.pubsvc._set_identifiers(nerd, mntr, "ncnr0:fred")
        self.assertEqual(nerd['@id'], "ark:/88434/ncnr0-fredp7")
        self.assertEqual(nerd['pdr:sipid'], "ncnr0:fred")

    def test_status_of(self):
        stat = self.pubsvc.status_of("ncnr0:fred")
        self.assertEqual(stat.state, status.NOT_FOUND)
        self.assertEqual(stat.siptype, "")

        stat.start(self.pubsvc.convention)
        stat = self.pubsvc.status_of("ncnr0:fred")
        self.assertEqual(stat.state, status.PROCESSING)
        self.assertEqual(stat.siptype, "pdp0")
        
    def test_get_bagger(self):
        mntr = self.pubsvc._get_minter("pdp0")
        bgr = self.pubsvc._get_bagger_for("pdp0", "pdp0-0017sg", mntr)
        self.assertTrue(bgr)
        self.assertIs(self.pubsvc._get_bagger_for("pdp0", "pdp0-0017sg", mntr), bgr)
        self.assertEqual(bgr.sipid, "pdp0-0017sg")

        bgrn = self.pubsvc._get_bagger_for("ncnr0", "ncnr0:hello", mntr)
        self.assertTrue(bgr)
        self.assertIsNot(bgrn, bgr)
        self.assertEqual(bgrn.sipid, "ncnr0:hello")

    def test_accept_resource_metadata(self):
        nerd = utils.read_json(str(datadir2 / 'ncnrexp0.json'))

        with self.assertRaises(pdp.SIPConflictError):
            self.pubsvc.accept_resource_metadata(nerd, ncnrag, sipid="ncnr0:hello", create=False)

        sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, sipid="ncnr0:hello", create=True)
        self.assertEqual(sipid, "ncnr0:hello")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, sipid="ncnr0:hello", create=False)
        self.assertEqual(sipid, "ncnr0:hello")
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(bnerd["doi"], "doi:10.18434/ncnr0-hellopk")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        # nerdm record has some arbitrary value for '@id'
        nerd['@id'] = "ark:/88434/goob"
        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, create=True)

        del nerd['@id']
        sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, create=True)
        self.assertEqual(sipid, "ncnr0-0021")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-0021sh")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0-0021")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-0021sh")
        self.assertEqual(bnerd["doi"], "doi:10.18434/ncnr0-0021sh")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        # nerdm record has some arbitrary value for '@id'
        nerd = utils.read_json(str(simplenerd))
        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            sipid = self.pubsvc.accept_resource_metadata(nerd, tstag, create=True)

        del nerd['@id']
        sipid = self.pubsvc.accept_resource_metadata(nerd, tstag, create=True)
        self.assertEqual(sipid, "pdp0-0017")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/pdp0-0017sg")
        self.assertEqual(bnerd["pdr:sipid"], "pdp0-0017")
        self.assertEqual(bnerd["pdr:aipid"], "pdp0-0017sg")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertEqual(bnerd["accessLevel"], 'public')
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        # update by setting @id
        nerd['@id'] = sipid
        nerd['accessLevel'] = 'restricted public'
        sipid = self.pubsvc.accept_resource_metadata(nerd, tstag)
        self.assertEqual(sipid, "pdp0-0017")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/pdp0-0017sg")
        self.assertEqual(bnerd["pdr:sipid"], "pdp0-0017")
        self.assertEqual(bnerd["pdr:aipid"], "pdp0-0017sg")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertEqual(bnerd["accessLevel"], 'restricted public')
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        with self.assertRaises(pdp.UnauthorizedPublishingRequest):
            self.pubsvc.accept_resource_metadata(nerd, tstag, sipid="ncnr0:adieu", create=True)

        del nerd['@id']
        del nerd['components']
        sipid = self.pubsvc.accept_resource_metadata(nerd, tstag)
        self.assertEqual(sipid, "pdp0-0018")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/pdp0-0018s0")
        self.assertEqual(bnerd["pdr:sipid"], "pdp0-0018")
        self.assertEqual(bnerd["pdr:aipid"], "pdp0-0018s0")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertEqual(bnerd["accessLevel"], 'restricted public')
        self.assertEqual(bnerd['components'], [])

    def test_upsert_component_metadata(self):
        nerd = utils.read_json(str(simplenerd))
        comps = nerd['components']
        del nerd['@id']
        del nerd['components']
        schema = nerd['_schema'] + "/definitions/Component"

        sipid = self.pubsvc.accept_resource_metadata(nerd, tstag)
        self.assertEqual(sipid, "pdp0-0017")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(len(bnerd['components']), 0)

        comps[0]['_schema'] = schema
        compid = self.pubsvc.upsert_component_metadata(sipid, comps[0], tstag)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(len(bnerd['components']), 1)
        self.assertEqual(bnerd['components'][0]['@id'], compid)
        self.assertEqual(compid, "pdr:f/1491_optSortSphEvaluated20160701.cdf")
        self.assertEqual(bnerd['components'][0]['filepath'], "1491_optSortSphEvaluated20160701.cdf")
        self.assertNotIn('format', bnerd['components'][0])

        comps[0]['format'] = { 'description': 'CDF file', 'tag': 'CDF' }
        compid = self.pubsvc.upsert_component_metadata(sipid, comps[0], tstag)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(len(bnerd['components']), 1)
        self.assertEqual(bnerd['components'][0]['@id'], compid)
        self.assertEqual(compid, "pdr:f/1491_optSortSphEvaluated20160701.cdf")
        self.assertEqual(bnerd['components'][0]['format']['tag'], "CDF")
        self.assertEqual(bnerd['components'][0]['filepath'], "1491_optSortSphEvaluated20160701.cdf")

        comps[1]['_schema'] = schema
        compid = self.pubsvc.upsert_component_metadata(sipid, comps[1], tstag)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(len(bnerd['components']), 2)
        cmp = [c for c in bnerd['components'] if c['@id'] == compid]
        cmp = cmp[0] if len(cmp) > 0 else None
        self.assertTrue(cmp)
        self.assertEqual(compid, "pdr:f/1491_optSortSphEvaluated20160701.cdf.sha256")

        comps[2]['_schema'] = schema
        compid = self.pubsvc.upsert_component_metadata(sipid, comps[2], tstag)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(len(bnerd['components']), 3)
        cmp = [c for c in bnerd['components'] if c['@id'] == compid]
        cmp = cmp[0] if len(cmp) > 0 else None
        self.assertTrue(cmp)
        self.assertEqual(cmp['mediaType'], "application/zip")
        self.assertEqual(compid, "pdr:see/doi:/10.18434/T4SW26")

        cmp['mediaType'] = "text/html"
        compid = self.pubsvc.upsert_component_metadata(sipid, cmp, tstag)
        self.assertEqual(compid, "pdr:see/doi:/10.18434/T4SW26")
        bnerd = bag.nerdm_record(True)
        self.assertEqual(len(bnerd['components']), 3)
        comp = [c for c in bnerd['components'] if c['@id'] == compid]
        comp = comp[0] if len(comp) > 0 else None
        self.assertTrue(comp)
        self.assertEqual(comp['mediaType'], "text/html")
        
    def test_describe(self):
        nerd = utils.read_json(str(simplenerd))

        sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, sipid="ncnr0:hello", create=True)
        self.assertEqual(sipid, "ncnr0:hello")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        md = self.pubsvc.describe("ark:/88434/ncnr0-hellopk")
        self.assertTrue(md)
        self.assertEqual(md["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(md["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(md["title"], bnerd['title'])
        self.assertTrue(len(md.get('components',[])) > 0)
        self.assertIn('_schema', md)
        self.assertIn('@context', md)

        md = self.pubsvc.describe("ncnr0:hello")
        self.assertTrue(md)
        self.assertEqual(md["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(md["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(md["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(md["title"], bnerd['title'])
        self.assertTrue(len(md.get('components',[])) > 0)
        self.assertIn('_schema', md)
        self.assertIn('@context', md)

        md = self.pubsvc.describe("ark:/88434/ncnr0-hellopk", False)
        self.assertTrue(md)
        self.assertEqual(md["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(md["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(md["title"], bnerd['title'])
        self.assertEqual(len(md.get('components',[])), 0)
        self.assertIn('_schema', md)
        self.assertIn('@context', md)

        md = self.pubsvc.describe("ark:/88434/ncnr0-hellopk/pdr:f/1491_optSortSphEvaluated20160701.cdf")
        self.assertTrue(md)
        self.assertEqual(md['filepath'], "1491_optSortSphEvaluated20160701.cdf")
        self.assertIn('_schema', md)
        self.assertIn('@context', md)

        md = self.pubsvc.describe("ncnr0:hello/pdr:f/1491_optSortSphEvaluated20160701.cdf")
        self.assertTrue(md)
        self.assertEqual(md['filepath'], "1491_optSortSphEvaluated20160701.cdf")
        self.assertIn('_schema', md)
        self.assertIn('@context', md)

        with self.assertRaises(pdp.SIPNotFoundError):
            self.pubsvc.describe("ark:/88434/ncnr0-goober")

        with self.assertRaises(pdp.SIPNotFoundError):
            self.pubsvc.describe("ncnr0:goober")

        self.assertEqual(self.pubsvc.describe("ark:/88434/ncnr0-hellopk/goober"), {})
        self.assertEqual(self.pubsvc.describe("ncnr0:hello/goober"), {})
        
    def test_remove_component(self):
        nerd = utils.read_json(str(simplenerd))

        sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, sipid="ncnr0:hello", create=True)
        self.assertEqual(sipid, "ncnr0:hello")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        cmpid = "pdr:f/1491_optSortSphEvaluated20160701.cdf"
        cmp = [c for c in bnerd['components'] if c['@id'] == cmpid]
        self.assertEqual(len(cmp), 1)

        self.pubsvc.remove_component(sipid, cmpid, ncnrag)
        bnerd = bag.nerdm_record(True)
        cmp = [c for c in bnerd['components'] if c['@id'] == cmpid]
        self.assertEqual(len(cmp), 0)
        
        cmpid = "pdr:see/doi:/10.18434/T4SW26"
        cmp = [c for c in bnerd['components'] if c['@id'] == cmpid]
        self.assertEqual(len(cmp), 1)

        self.pubsvc.remove_component(sipid, cmpid, ncnrag)
        bnerd = bag.nerdm_record(True)
        cmp = [c for c in bnerd['components'] if c['@id'] == cmpid]
        self.assertEqual(len(cmp), 0)

    def test_delete(self):
        nerd = utils.read_json(str(simplenerd))

        self.assertEqual(self.pubsvc.status_of("ncnr0:hello").state, status.NOT_FOUND)

        sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, sipid="ncnr0:hello", create=True)
        self.assertEqual(sipid, "ncnr0:hello")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertTrue(len(bnerd.get('components',[])) > 0)

        self.assertEqual(self.pubsvc.status_of("ncnr0:hello").state, status.PENDING)

        self.pubsvc.delete(sipid, ncnrag)
        self.assertEqual(self.pubsvc.status_of("ncnr0:hello").state, status.NOT_FOUND)
        bagdir = self.bagparent / sipid
        self.assertFalse(bagdir.exists())

    def test_finalized(self):
        nerd = utils.read_json(str(simplenerd))
        nerd['version'] = "1.0.0+ (in edit)"

        self.assertEqual(self.pubsvc.status_of("ncnr0:hello").state, status.NOT_FOUND)

        sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, sipid="ncnr0:hello", create=True)
        self.assertEqual(sipid, "ncnr0:hello")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertTrue(len(bnerd.get('components',[])) > 0)
        self.assertEqual(bnerd['version'], "1.0.0+ (in edit)")

        self.assertEqual(self.pubsvc.status_of("ncnr0:hello").state, status.PENDING)

        self.pubsvc.finalize(sipid, ncnrag)
        self.assertEqual(self.pubsvc.status_of("ncnr0:hello").state, status.FINALIZED)

        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd['version'], '1.0.0')   # not built from a previous version

        self.pubsvc.finalize(sipid, ncnrag)
        self.assertEqual(self.pubsvc.status_of("ncnr0:hello").state, status.FINALIZED)

        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd['version'], '1.0.0')

    def test_publish(self):
        nerd = utils.read_json(str(simplenerd))
        nerd['version'] = "1.0.0+ (in edit)"

        self.assertEqual(self.pubsvc.status_of("ncnr0:hello").state, status.NOT_FOUND)

        sipid = self.pubsvc.accept_resource_metadata(nerd, ncnrag, sipid="ncnr0:hello", create=True)
        self.assertEqual(sipid, "ncnr0:hello")
        bagdir = self.bagparent / sipid
        self.assertTrue(bagdir.is_dir())
        bag = NISTBag(bagdir)
        bnerd = bag.nerdm_record(True)
        self.assertEqual(bnerd["@id"], "ark:/88434/ncnr0-hellopk")
        self.assertEqual(bnerd["pdr:sipid"], "ncnr0:hello")
        self.assertEqual(bnerd["pdr:aipid"], "ncnr0-hellopk")
        self.assertEqual(bnerd["title"], nerd['title'])
        self.assertTrue(len(bnerd.get('components',[])) > 0)
        self.assertEqual(bnerd['version'], "1.0.0+ (in edit)")

        # WARNING: Implmentation is not complete!
        self.pubsvc.publish(sipid, ncnrag)
        self.assertEqual(self.pubsvc.status_of(sipid).state, status.PUBLISHED)

                         
if __name__ == '__main__':
    test.main()
        
        
