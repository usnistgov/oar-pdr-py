import os, pdb, requests, logging, time
import unittest as test
from copy import deepcopy

from nistoar.testing import *
# import tests.nistoar.pdr.describe.sim_describe_svc as desc

testdir = os.path.dirname(os.path.abspath(__file__))
datadir = os.path.join(testdir, 'data')

import imp
simsrvrsrc = os.path.join(testdir, "sim_describe_svc.py")
with open(simsrvrsrc, 'r') as fd:
    desc = imp.load_module("sim_describe_svc", fd, simsrvrsrc,
                           (".py", 'r', imp.PY_SOURCE))

basedir = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir)))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)

def startService(authmeth=None):
    tdir = tmpdir()
    arcdir = os.path.join(tdir, "archive")
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/describe/sim_describe_svc.py"
    cmd = "uwsgi --daemonize {0} --plugin python3 --http-socket :{1} " \
          "--wsgi-file {2} --pidfile {3} --set-ph archive_dir={4}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), srvport,
                     os.path.join(basedir, wpy), pidfile, arcdir)
    os.system(cmd)
    time.sleep(0.5)

def stopService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(tdir,
                                                 "simsrv"+str(srvport)+".pid"))
    os.system(cmd)
    time.sleep(1)

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_simsrv.log"))
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
    rmtmpdir()

class TestArchive(test.TestCase):

    def setUp(self):
        self.dir = datadir
        self.arch = desc.SimArchive(self.dir)

    def test_ctor(self):
        self.assertEqual(self.arch.dir, datadir)
        self.assertEqual(self.arch.lu, {"ABCDEFG": "pdr02d4t",
                                        "ark:/88434/pdr2210": "pdr2210",
                                        "pdr2210": "pdr2210"})

    def test_ediid_to_id(self):
        self.assertEqual(self.arch.ediid_to_id("ABCDEFG"), "pdr02d4t")

    def test_ids(self):
        ids = self.arch.ids();
        self.assertEqual(len(ids), 2)
        self.assertIn("pdr02d4t", ids)
        self.assertIn("pdr2210", ids)

class TestSimService(test.TestCase):

    @classmethod
    def setUpClass(cls):
        tdir = tmpdir()
        adir = os.path.join(tdir, "archive")
        shutil.copytree(datadir, adir)
        startService()

    @classmethod
    def tearDownClass(cls):
        stopService()

    def test_found_ediid(self):
        resp = requests.get(baseurl+"ABCDEFG")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertEqual(data["@id"], "ark:/88434/pdr02d4t")

    def test_found_ark(self):
        resp = requests.get(baseurl+"?@id=ark:/88434/pdr02d4t")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertEqual(data['ResultData'][0]["ediid"], "ABCDEFG")

    def test_post_rec(self):
        id = "ark:/55121/mds1-1000"
        rec = {
            'ediid': id,
            '@id': id
        }
        resp = requests.post(baseurl, json=rec)
        self.assertEqual(resp.status_code, 201)

        resp = requests.get(baseurl+id)
        data = resp.json()
        self.assertEqual(data["@id"], id)
        
    def test_search_all(self):
        resp = requests.get(baseurl)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertIn("ResultData", data)
        ids = [d['@id'] for d in data["ResultData"] if '@id' in d]

        # the exact # of results depends on whether test_post_rec runs before or after this test
        self.assertGreaterEqual(len(data["ResultData"]), 2)
        self.assertGreaterEqual(data["ResultCount"], 2)
        self.assertGreaterEqual(data["PageSize"], 2)
        self.assertLessEqual(len(data["ResultData"]), 3)
        self.assertLessEqual(data["ResultCount"], 3)
        self.assertLessEqual(data["PageSize"], 3)

        self.assertIn("ark:/88434/pdr02d4t", ids)
        self.assertIn("ark:/88434/edi00hw91c", ids)
        if len(ids) > 2:
            self.assertIn("ark:/55121/mds1-1000", ids)



if __name__ == '__main__':
    test.main()

