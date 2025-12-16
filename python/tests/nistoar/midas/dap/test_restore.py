import os, json, logging, tempfile
from pathlib import Path
import unittest as test
from unittest import mock
from nistoar.midas.dbio import inmem, base, project, status
from nistoar.midas.dap import restore
from nistoar.pdr.utils.prov import Action, Agent
from nistoar.pdr.utils.io import read_json

import requests

testuser = Agent("dbio", Agent.AUTO, "tester", "test")
testdir = Path(__file__).parents[0]
nistoardir = testdir.parents[1]
datadir = nistoardir / "pdr" / "describe" / "data"
testnerd = datadir / "pdr2210.json"

nistr = Agent("midas", Agent.USER, "nstr1", "midas")

tmpdir = tempfile.TemporaryDirectory(prefix="_test_restore.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_restore.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
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

class TestAIPRestorer(test.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.nerd = read_json(testnerd)

    def setUp(self):
        self.cfg = {
            "dbio": {
                "superusers": ['nstr1'],
                "project_id_minting": {
                    "default_shoulder": {
                        "public": "pdr0"
                    }
                }
            },
            "restorer": {
                "nerdm_resolver": {
                    "service_endpoint": "https://data.nist.gov/od/id"
                }
            }
        }
        self.fact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdm1": 2 }})
        self.project = project.ProjectService(base.DMP_PROJECTS, self.fact, self.cfg, nistr,
                                              rootlog.getChild("project"))

        self.mockresp = mock.Mock()
        self.mockresp.json = mock.Mock(return_value=self.nerd)
        self.mockresp.status_code = 200
        self.mockresp.reason = "OK"

    def test_ctor(self):
        restr = restore.AIPRestorer("pdr2210", self.cfg['restorer'])
        self.assertEqual(restr.aipid, "pdr2210")
        self.assertTrue(restr._urestorer)
        self.assertEqual(restr._urestorer._src, "https://data.nist.gov/od/id/pdr2210")
        self.assertIsNone(restr._urestorer._data)
        restr.free()
        self.assertTrue(restr._urestorer)
        self.assertEqual(restr._urestorer._src, "https://data.nist.gov/od/id/pdr2210")
        self.assertIsNone(restr._urestorer._data)

    def test_recover(self):
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.AIPRestorer("pdr2210", self.cfg['restorer'])
            self.assertIsNone(restr._urestorer._data)

            restr.recover()
            self.assertIsNotNone(restr._urestorer._data)
            self.assertEqual(restr._urestorer._data, self.nerd)

        restr.free()
        self.assertIsNone(restr._urestorer._data)
        
        self.mockresp.status_code = 404
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.AIPRestorer("pdr2210", self.cfg['restorer'])
            with self.assertRaises(restore.ObjectNotFound):
                restr.recover()

        self.mockresp.status_code = 401
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.AIPRestorer("pdr2210", self.cfg['restorer'])
            with self.assertRaises(restore.NotAuthorized):
                restr.recover()

        self.mockresp.status_code = 406
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.AIPRestorer("pdr2210", self.cfg['restorer'])
            with self.assertRaises(restore.NotAuthorized):
                restr.recover()

        self.mockresp.status_code = 500
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.AIPRestorer("pdr2210", self.cfg['restorer'])
            with self.assertRaises(restore.DBIOException):
                restr.recover()

    def test_data1(self):
        """
        test access to get_data() and restore(), in that order
        """
        prec = self.project.create_record("goob", {"season": "autumn"})
        prec.status.set_state(status.PUBLISHED)
        prec.save()
        id = prec.id

        restr = None
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.AIPRestorer("pdr2210", self.cfg['restorer'])
            data = restr.get_data()
            self.assertEqual(data, self.nerd)
            self.assertIsNotNone(restr._urestorer._data)

        restr.restore(prec, dofree=True)
        self.assertEqual(prec.data, self.nerd)
        self.assertIsNone(restr._urestorer._data)

    def test_data2(self):
        """
        test access to restore() and get_data(), in that order
        """
        prec = self.project.create_record("goob", {"season": "autumn"})
        prec.status.set_state(status.PUBLISHED)
        prec.save()
        id = prec.id

        restr = None
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.AIPRestorer("pdr2210", self.cfg['restorer'])
            restr.restore(prec)
            self.assertEqual(prec.data, self.nerd)
            self.assertIsNotNone(restr._urestorer._data)

        data = restr.get_data()
        self.assertEqual(data, self.nerd)
        self.assertIsNotNone(restr._urestorer._data)

        restr.free()
        self.assertIsNone(restr._urestorer._data)
        
    def test_from_archived_at(self):
        prec = self.project.create_record("goob", {"season": "autumn"})
        prec.status.set_state(status.PUBLISHED)

        restr = restore.AIPRestorer.from_archived_at("aip:pdr2210", self.project.dbcli,
                                                     self.cfg['restorer'])
        self.assertTrue(isinstance(restr, restore.AIPRestorer))
        self.assertEqual(restr.aipid, "pdr2210")
        self.assertTrue(restr._urestorer)
        self.assertEqual(restr._urestorer._src, "https://data.nist.gov/od/id/pdr2210")
        self.assertIsNone(restr._urestorer._data)
        
        

        


if __name__ == '__main__':
    test.main()
