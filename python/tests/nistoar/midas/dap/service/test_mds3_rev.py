"""
exercise the external review with the DAPService
"""
import os, json, pdb, logging, tempfile, pathlib
import unittest as test
from unittest.mock import Mock, patch, PropertyMock
from pathlib import Path

from nistoar.midas.dbio import inmem, base, AlreadyExists, InvalidUpdate, ObjectNotFound, PartNotAccessible
from nistoar.midas.dbio import project as prj, status
from nistoar.midas.dap.service import mds3
from nistoar.midas.dap.fm import FileSpaceNotFound
from nistoar.pdr.utils import prov
from nistoar.pdr.utils import read_nerd
from nistoar.nerdm.constants import CORE_SCHEMA_URI
from nistoar.pdr.utils import read_json, write_json

tmpdir = tempfile.TemporaryDirectory(prefix="_test_mds3.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_mds3.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

# test records
testdir = pathlib.Path(__file__).parents[0]
pdr2210 = testdir.parents[2] / 'pdr' / 'describe' / 'data' / 'pdr2210.json'
ncnrexp0 = testdir.parents[2] / 'pdr' / 'publish' / 'data' / 'ncnrexp0.json'
daptestdir = Path(__file__).parents[1] / 'data' 

nistr = prov.Agent("midas", prov.Agent.USER, "nstr1", "midas")

def read_scan(id=None):
    return read_json(daptestdir/"scan-report.json")

def read_scan_reply(id=None):
    return read_json(daptestdir/"scan-req-ack.json")

class TestMDS3DAPServiceWithExtRev(test.TestCase):

    def setUp(self):
        self.cfg = {
            "dbio": {
                "project_id_minting": {
                    "default_shoulder": {
                        "midas": "mdsy"
                    },
                    "allowed_shoulders": {
                        "midas": ["mds2"]
                    }
                }
            },
            "assign_doi": "always",
            "doi_naan": "10.88888",
            "nerdstorage": {
#                "type": "fsbased",
#                "store_dir": os.path.join(tmpdir.name)
                "type": "inmem",
            },
            "default_responsible_org": {
                "@type": "org:Organization",
                "@id": mds3.NIST_ROR,
                "title": "NIST"
            },
            "external_review": {
                "name": "simulated"
            },
            "reviewer_ids": [ "nstr1" ]
        }
        self.dbfact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdsy": 2 }})

    def create_service(self):
        fact = mds3.DAPServiceFactory(self.dbfact, self.cfg, rootlog.getChild("mds3"))
        self.svc = fact.create_service_for(nistr)
        self.nerds = self.svc._store
        self.revcli = self.svc._extrevcli
        return self.svc

    def test_create(self):
        self.create_service()
        self.assertTrue(self.svc.dbcli)
        self.assertEqual(self.svc.cfg, self.cfg)
        self.assertEqual(self.svc.who.actor, "nstr1")
        self.assertEqual(self.svc.who.agent_class, "midas")
        self.assertTrue(self.svc.log)
        self.assertTrue(self.svc._store)
        self.assertTrue(self.svc._valid8r)
        self.assertEqual(self.svc._minnerdmver, (0, 6))
        self.assertIsNotNone(self.svc._extrevcli)
        self.assertIs(self.revcli, self.svc._extrevcli)
        self.assertEqual(self.revcli.system_name, "simulated")
        self.assertIs(self.revcli.projsvc, self.svc)
        self.assertTrue(self.revcli.autoapp)

    def test_submit(self):
        self.create_service()
        self.revcli.autoapp = False
        self.assertEqual(self.revcli.projs, {})

        prec = self.svc.create_record("goob")
        id = prec.id
        self.svc.replace_authors(id, [
            { "familyName": "Cranston", "givenName": "Gurn", "middleName": "J." },
            { "fn": "Edgar Allen Poe", "affiliation": "NIST" },
            { "familyName": "Grant", "givenName": "U.", "middleName": "S." }
        ])
        prec = self.svc.get_record(id)
        self.assertEqual(prec.status.state, status.EDIT)

        stat = self.svc.submit(id)
        self.assertEqual(stat.state, status.SUBMITTED)
        self.assertIn(id, self.revcli.projs)
        self.assertEqual(self.revcli.projs[id]['phase'], "requested")
        self.assertIsNone(stat.get_review_from("sim"))

        self.revcli.update(id, "group")
        self.assertEqual(self.revcli.projs[id]['phase'], "group")
        prec = self.svc.get_record(id)
        rev = prec.status.get_review_from("simulated")
        self.assertIsNotNone(rev)
        self.assertEqual(rev['phase'], "group")
        self.assertEqual(prec.status.get_review_phases(), {"simulated": "group"})
        self.assertFalse(self.svc._sufficiently_reviewed(id))

        self.revcli.approve(id)
        self.assertEqual(self.revcli.projs[id]['phase'], "approved")
        prec = self.svc.get_record(id)
        rev = prec.status.get_review_from("simulated")
        self.assertIsNotNone(rev)
        self.assertEqual(rev['phase'], "approved")
        self.assertEqual(prec.status.get_review_phases(), {"simulated": "approved"})
        self.assertTrue(self.svc._sufficiently_reviewed(id))
        self.assertEqual(prec.status.state, status.ACCEPTED)
        

                         
if __name__ == '__main__':
    test.main()
        
        
        
