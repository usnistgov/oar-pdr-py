import os, json, pdb, logging, tempfile
from pathlib import Path
import unittest as test
from unittest import mock
from nistoar.midas.dbio import inmem, base, project, status, restore
from nistoar.pdr.utils.prov import Action, Agent

import requests

testuser = Agent("dbio", Agent.AUTO, "tester", "test")
testdir = Path(__file__).parents[0]
datadir = testdir / "data"

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

class TestDBIORestorer(test.TestCase):

    def setUp(self):
        self.cfg = {
            "dbio": {
                "superusers": ['nstr1'],
                "project_id_minting": {
                    "default_shoulder": {
                        "public": "pdr0"
                    }
                }
            }
        }
        self.fact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdm1": 2 }})
        self.project = project.ProjectService(base.DMP_PROJECTS, self.fact, self.cfg, nistr,
                                              rootlog.getChild("project"))

    def test_ctor(self):
        prec = self.project.create_record("goob", {"color": "yellow"})
        restr = restore.DBIORestorer(prec._cli, prec._coll, prec.id)
        self.assertEqual(restr.pubid, prec.id)
        self.assertIsNone(restr._pubrec)
        restr.free()
        self.assertEqual(restr.pubid, prec.id)
        self.assertIsNone(restr._pubrec)

    def test_recover(self):
        prec = self.project.create_record("goob", {"color": "yellow"})
        prec.status.set_state(status.SUBMITTED)
        prec.save()
        id = prec.id
        self.project.publish(id)
        prec = self.project.get_record(id)
        self.assertEqual(prec.status.state, status.PUBLISHED)
        pubid = prec.status.published_as
        self.assertEqual(pubid, "ark:/88434/pdr0-0001")

#        prec.data['color'] = "red"
#        self.assertEqual(prec.data, {"color": "red"})

        restr = restore.DBIORestorer(prec._cli, "dmp_latest", pubid)
        restr.recover()
        self.assertIsNotNone(restr._pubrec)
        self.assertEqual(restr._pubrec.data, {"color": "yellow"})
        self.assertEqual(restr._pubrec.id, pubid)

    def test_data1(self):
        """
        test access to get_data() and restore(), in that order
        """
        prec = self.project.create_record("goob", {"color": "yellow"})
        prec.status.set_state(status.SUBMITTED)
        prec.save()
        id = prec.id
        self.project.publish(id)
        prec = self.project.get_record(id)
        self.assertEqual(prec.status.state, status.PUBLISHED)
        pubid = prec.status.published_as
        self.assertEqual(pubid, "ark:/88434/pdr0-0001")

        prec.data['color'] = "red"
        self.assertEqual(prec.data, {"color": "red"})

        restr = restore.DBIORestorer(prec._cli, "dmp_latest", pubid)
        
        data = restr.get_data()
        self.assertEqual(data, {"color": "yellow"})
        self.assertIsNotNone(restr._pubrec)

        restr.restore(prec, dofree=True)
        self.assertEqual(prec.data, {"color": "yellow"})
        self.assertIsNone(restr._pubrec)

    def test_data2(self):
        """
        test access to restore() and get_data(), in that order
        """
        prec = self.project.create_record("goob", {"color": "yellow"})
        prec.status.set_state(status.SUBMITTED)
        prec.save()
        id = prec.id
        self.project.publish(id)
        prec = self.project.get_record(id)
        self.assertEqual(prec.status.state, status.PUBLISHED)
        pubid = prec.status.published_as
        self.assertEqual(pubid, "ark:/88434/pdr0-0001")

        prec.data['color'] = "red"
        self.assertEqual(prec.data, {"color": "red"})

        restr = restore.DBIORestorer(prec._cli, "dmp_latest", pubid)
        
        restr.restore(prec)
        self.assertEqual(prec.data, {"color": "yellow"})
        self.assertIsNotNone(restr._pubrec)

        data = restr.get_data()
        self.assertEqual(data, {"color": "yellow"})
        self.assertIsNotNone(restr._pubrec)
        restr.free()
        self.assertIsNone(restr._pubrec)

    def test_from_archived_at(self):
        prec = self.project.create_record("goob", {"color": "yellow"})
        restr = restore.DBIORestorer.from_archived_at("dbio_store:dmp_latest/ark:/88434/pdr0-0001",
                                                      self.project.dbcli)
        self.assertTrue(isinstance(restr, restore.DBIORestorer))
        self.assertEqual(restr.pubid, "ark:/88434/pdr0-0001")
        self.assertEqual(restr.pubcli._projcoll, "dmp_latest")
        self.assertIsNone(restr._pubrec)
        restr.free()
        self.assertEqual(restr.pubcli._projcoll, "dmp_latest")
        self.assertIsNone(restr._pubrec)

mockdata = { "season": "autumn" }

class TestURLRestorer(test.TestCase):

    def setUp(self):
        self.cfg = {
            "dbio": {
                "superusers": ['nstr1'],
                "project_id_minting": {
                    "default_shoulder": {
                        "public": "pdr0"
                    }
                }
            }
        }
        self.fact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdm1": 2 }})
        self.project = project.ProjectService(base.DMP_PROJECTS, self.fact, self.cfg, nistr,
                                              rootlog.getChild("project"))

        self.mockresp = mock.Mock()
        self.mockresp.json = mock.Mock(return_value=mockdata)
        self.mockresp.status_code = 200
        self.mockresp.reason = "OK"

    def test_ctor(self):
        with self.assertRaises(ValueError):
            restore.URLRestorer("")
        with self.assertRaises(ValueError):
            restore.URLRestorer(None)
        with self.assertRaises(ValueError):
            restore.URLRestorer("dbio_store:dmp_latest/goober")
        with self.assertRaises(ValueError):
            restore.URLRestorer("https://:goober")

        url = "https://archive/data.json"
        restr = restore.URLRestorer(url)
        self.assertEqual(restr._src, url)
        self.assertIsNone(restr._id)
        self.assertIsNone(restr._data)
        restr.free()
        self.assertEqual(restr._src, url)
        self.assertIsNone(restr._id)
        self.assertIsNone(restr._data)

        url = "http://archive/data.json"
        restr = restore.URLRestorer(url)
        self.assertEqual(restr._src, url)
        self.assertIsNone(restr._id)
        self.assertIsNone(restr._data)
        restr.free()
        self.assertEqual(restr._src, url)
        self.assertIsNone(restr._id)
        self.assertIsNone(restr._data)


    def test_recover(self):
        url = "http://archive/data.json"

        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.URLRestorer(url)
            self.assertIsNone(restr._data)

            restr.recover()
            self.assertIsNotNone(restr._data)
            self.assertEqual(restr._data, mockdata)

        self.mockresp.status_code = 404
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.URLRestorer(url)
            with self.assertRaises(restore.ObjectNotFound):
                restr.recover()

        self.mockresp.status_code = 401
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.URLRestorer(url)
            with self.assertRaises(restore.NotAuthorized):
                restr.recover()

        self.mockresp.status_code = 406
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.URLRestorer(url)
            with self.assertRaises(restore.NotAuthorized):
                restr.recover()

        self.mockresp.status_code = 500
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.URLRestorer(url)
            with self.assertRaises(restore.DBIOException):
                restr.recover()

    def test_data1(self):
        """
        test access to get_data() and restore(), in that order
        """
        url = "http://archive/data.json"

        prec = self.project.create_record("goob", {"season": "autumn"})
        prec.status.set_state(status.PUBLISHED)
        prec.save()
        id = prec.id

        prec.data['season'] = "fall"
        self.assertEqual(prec.data, {"season": "fall"})

        restr = None
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.URLRestorer(url)
            data = restr.get_data()
            self.assertEqual(data, {"season": "autumn"})
            self.assertIsNotNone(restr._data)

        restr.restore(prec, dofree=True)
        self.assertEqual(prec.data, {"season": "autumn"})
        self.assertIsNone(restr._data)

    def test_data2(self):
        """
        test access to restore() and get_data(), in that order
        """
        url = "http://archive/data.json"

        prec = self.project.create_record("goob", {"season": "autumn"})
        prec.status.set_state(status.PUBLISHED)
        prec.save()
        id = prec.id

        prec.data['season'] = "fall"
        self.assertEqual(prec.data, {"season": "fall"})

        restr = None
        with mock.patch('requests.get', return_value=self.mockresp) as mockget:
            restr = restore.URLRestorer(url)
            restr.restore(prec)
            self.assertEqual(prec.data, {"season": "autumn"})
            self.assertIsNotNone(restr._data)

        data = restr.get_data()
        self.assertEqual(data, {"season": "autumn"})
        self.assertIsNotNone(restr._data)

        restr.free()
        self.assertIsNone(restr._data)
        
    def test_from_archived_at(self):
        url = "https://archive/data.json"
        prec = self.project.create_record("goob", {"color": "yellow"})
        prec.status.set_state(status.PUBLISHED)
        
        restr = restore.URLRestorer.from_archived_at(url, self.project.dbcli)
        self.assertTrue(isinstance(restr, restore.URLRestorer))
        self.assertIsNone(restr._id)
        self.assertIsNone(restr._data)
        restr.free()
        self.assertIsNone(restr._data)

        

        


if __name__ == '__main__':
    test.main()
