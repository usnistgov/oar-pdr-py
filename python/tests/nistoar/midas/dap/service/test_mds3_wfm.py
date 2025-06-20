import os, json, pdb, logging, tempfile, pathlib
import unittest as test
from unittest.mock import Mock, patch, PropertyMock
from pathlib import Path

from nistoar.midas.dbio import inmem, base, AlreadyExists, InvalidUpdate, ObjectNotFound, PartNotAccessible
from nistoar.midas.dbio import project as prj
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

class TestDAPProjectRecord(test.TestCase):

    def setUp(self):
        self.fm = None
        ack = read_scan_reply()
        with patch('nistoar.midas.dap.fm.FileManager') as mock:
            self.fm = mock.return_value
            self.fm.post_scan_files.return_value = ack
            self.fm.get_scan_files.return_value = read_scan()
            self.fm.get_record_space.return_value = {
                "fileid": "129",
                "type": "folder",
                "size": "0"
            }
            self.fm.get_uploads_directory.return_value = {
                "fileid": "130",
                "type": "folder",
                "size": "0"
            }
        self.fm.cfg = {
            'dap_app_base_url': 'http://localhost:5000/api',
            'auth': {
                'username': 'service_api',
                'password': 'service_pwd'
            },
            'dav_base_url': 'http://localhost:8000/remote.php/dav/files/oar_api'
        }

        self.cfg = {
            "dbio": {
                "project_id_minting": {
                    "default_shoulder": {
                        "midas": "mdsy"
                    },
                    "allowed_shoulders": {
                        "midas": ["mdsy", "spc1"]
                    }
                }
            }
        }
            
        self.dbfact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdsy": 2 }})
        dbcli = self.dbfact.create_client("dap", self.cfg.get("dbio", {}), nistr)
        rec = dbcli.create_record("goob")
        self.prec = mds3.DAPProjectRecord.from_dap_record(rec, self.fm)

    def test_ctor(self):
        self.assertTrue(self.prec._fmcli)
        fs = self.prec.file_space
        self.assertEqual(fs['action'], '')
        self.assertFalse(fs.get('creator'))
        self.assertFalse(self.prec.file_space_is_ready())

    def test_to_dict(self):
        fs = self.prec.file_space
        self.assertTrue(fs.get('id'))
        self.assertFalse(fs.get('creator'))

        rec = self.prec.to_dict()
        self.assertIn('id', rec)
        self.assertIn('meta', rec)
        self.assertIn('file_space', rec)
        self.assertFalse(rec['file_space'].get('creator'))
        self.assertTrue(rec['file_space'].get('uploads_dav_url'))
        self.assertEqual(rec['file_space'].get('location'), f"/{self.prec.id}/{self.prec.id}")

    def test_ensure_file_space(self):
        fs = self.prec.file_space
        self.assertTrue(fs.get('id'))
        self.assertFalse(fs.get('creator'))
        self.assertEqual(fs.get('action'), '')

        self.prec.ensure_file_space()
        self.assertTrue(fs.get('id'))
        self.assertEqual(fs.get('creator'), 'nstr1')
        self.assertFalse(fs.get('created'))
        self.assertEqual(fs.get('action'), '')

    def test_ensure_file_space2(self):
        fs = self.prec.file_space
        self.assertTrue(fs.get('id'))
        self.assertFalse(fs.get('creator'))
        self.assertFalse(fs.get('created'))
        self.assertEqual(fs.get('action'), '')

        self.fm.get_record_space.side_effect = FileSpaceNotFound(self.prec.id)
        self.prec.ensure_file_space()
        self.assertTrue(fs.get('id'))
        self.assertEqual(fs.get('creator'), 'nstr1')
        self.assertTrue(fs.get('created'))
        self.assertEqual(fs.get('action'), 'create')

        

        
        


class TestMDS3DAPServiceWithFM(test.TestCase):

    def setUp(self):
        self.cfg = {
            "dbio": {
                "project_id_minting": {
                    "default_shoulder": {
                        "midas": "mdsy"
                    },
                    "allowed_shoulders": {
                        "midas": ["mdsy", "spc1"]
                    }
                }
            },
            "assign_doi": "always",
            "doi_naan": "10.88888",
            "nerdstorage": {
                "type": "fmfs",
                "store_dir": os.path.join(tmpdir.name)
            }
        }
        self.fmcfg = {
            'dap_app_base_url': 'http://localhost:5000/api',
            'auth': {
                'username': 'service_api',
                'password': 'service_pwd'
            },
            'dav_base_url': 'http://localhost:8000/remote.php/dav/files/oar_api'
        }
        self.dbfact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdsy": 2 }})

        self.fm = None
        ack = read_scan_reply()
        with patch('nistoar.midas.dap.fm.FileManager') as mock:
            self.fm = mock.return_value
            self.fm.post_scan_files.return_value = ack
            self.fm.get_scan_files.return_value = read_scan()
            self.fm.get_record_space.return_value = {
                "fileid": "129",
                "type": "folder",
                "size": "0"
            }
            self.fm.get_uploads_directory.return_value = {
                "fileid": "130",
                "type": "folder",
                "size": "0"
            }
            type(self.fm).cfg = PropertyMock(return_value={'dav_base_url': 'base'})

    def create_service(self):
        self.svc = mds3.DAPService(self.dbfact, self.cfg, nistr, rootlog.getChild("mds3"))
        self.nerds = self.svc._store

        self.nerds._fmcli = self.fm
        self.svc._fmcli = self.fm
        self.svc._store._fmcli = self.fm
        return self.svc

    def test_ctor(self):
        self.create_service()
        self.assertTrue(self.svc.dbcli)
        self.assertTrue(self.svc._fmcli)
        self.assertEqual(self.svc.cfg, self.cfg)
        self.assertEqual(self.svc.who.actor, "nstr1")
        self.assertEqual(self.svc.who.agent_class, "midas")
        self.assertTrue(self.svc.log)
        self.assertTrue(self.svc._store)
        self.assertTrue(self.svc._valid8r)
        self.assertEqual(self.svc._minnerdmver, (0, 6))

    def test_create_record(self):
        self.create_service()
        self.assertTrue(not self.svc.dbcli.name_exists("goob"))
        self.fm.get_record_space.side_effect = FileSpaceNotFound("goob")
        
        prec = self.svc.create_record("goob")
        self.assertTrue(hasattr(prec, 'file_space'))
        fs = prec.file_space
        self.assertEqual(fs['id'], prec.id)
        self.assertEqual(fs['action'], 'create')
        self.assertEqual(fs['creator'], 'nstr1')

    def test_sync_to_file_space(self):
        self.create_service()
        self.assertTrue(not self.svc.dbcli.name_exists("goob"))
        
        prec = self.svc.create_record("goob")
        self.fm.get_scan_files.return_value = read_scan()

        self.assertTrue(self.svc.sync_to_file_space(prec.id))
        prec = self.svc.get_record(prec.id)
        fs = prec.file_space
        self.assertEqual(fs['id'], prec.id)
        self.assertEqual(fs['action'], 'sync')
        self.assertEqual(fs['creator'], 'nstr1')
        self.assertEqual(fs['file_count'], 7)
        self.assertEqual(fs['folder_count'], 2)

        files = self.svc.get_nerdm_data(prec.id, "pdr:f")
        self.assertEqual(len(files), 9)
        

                         
if __name__ == '__main__':
    test.main()
        
        
