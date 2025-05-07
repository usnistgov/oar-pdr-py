import os, pdb, requests, logging, time, json, sys
import unittest as test
from copy import deepcopy

from nistoar.testing import *
# import tests.nistoar.pdr.distrib.sim_distrib_srv as dstrb

testdir = os.path.dirname(os.path.abspath(__file__))
datadir = os.path.join(testdir, 'data')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir)))))

def import_file(path, name=None):
    if not name:
        name = os.path.splitext(os.path.basename(path))[0]
    import importlib.util as imputil
    spec = imputil.spec_from_file_location(name, path)
    out = imputil.module_from_spec(spec)
    sys.modules["sim_distrib_srv"] = out
    spec.loader.exec_module(out)
    return out

import importlib
simsrvrsrc = os.path.join(testdir, "sim_distrib_srv.py")
dstrb = import_file(simsrvrsrc)

port = 9091
baseurl = "http://localhost:{0}/".format(port)

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/distrib/sim_distrib_srv.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --pidfile {4}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile)
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
    startService()

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    stopService()
    rmtmpdir()

class TestFunc(test.TestCase):

    def test_version_of(self):
        self.assertEqual(dstrb.version_of("pdr2210.1_0.mbag0_3-0.zip"), [1, 0])
        self.assertEqual(dstrb.version_of("pdr2210.2.mbag0_3-1.zip"), [2])
        self.assertEqual(dstrb.version_of("pdr2210.3_1_3.mbag0_3-4.zip"), [3, 1, 3])
        self.assertEqual(dstrb.version_of("pdr2210.mbag0_3-0.zip"), [1])

    def test_seq_of(self):
        self.assertEqual(dstrb.seq_of("pdr2210.1_0.mbag0_3-0.zip"), 0)
        self.assertEqual(dstrb.seq_of("pdr2210.2.mbag0_3-1.zip"), 1)
        self.assertEqual(dstrb.seq_of("pdr2210.3_1_3.mbag0_3-4.zip"), 4)
        self.assertEqual(dstrb.seq_of("pdr2210.mbag0_3-0.zip"), 0)
        self.assertEqual(dstrb.seq_of("pdr1010.mbag0_3-1.zip"), 1)
        self.assertEqual(dstrb.seq_of("pdr1010.mbag0_3-2.zip"), 2)

class TestArchive(test.TestCase):

    def setUp(self):
        self.dir = datadir
        self.arch = dstrb.SimArchive(self.dir)

    def test_ctor(self):
        self.assertIn("pdr1010", self.arch._aips)
        self.assertIn("pdr2210", self.arch._aips)
        self.assertIn("1491", self.arch._aips)
        self.assertEqual(len(self.arch._aips), 4)
                
        self.assertIn("1", self.arch._aips['pdr1010'])
        self.assertEqual(len(self.arch._aips['pdr1010']), 1)

        self.assertIn("1.0", self.arch._aips['pdr2210'])
        self.assertIn("2", self.arch._aips['pdr2210'])
        self.assertIn("3.1.3", self.arch._aips['pdr2210'])
        self.assertEqual(len(self.arch._aips['pdr2210']), 3)
        self.assertIn("1.0", self.arch._aips['1491'])
        self.assertEqual(len(self.arch._aips['1491']), 1)

        self.assertIn("pdr1010.mbag0_3-1.zip", self.arch._aips['pdr1010']['1'])
        self.assertIn("pdr1010.mbag0_3-2.zip", self.arch._aips['pdr1010']['1'])
        self.assertEqual(len(self.arch._aips['pdr1010']['1']), 2)

        self.assertIn("pdr2210.1_0.mbag0_3-0.zip",
                      self.arch._aips['pdr2210']['1.0'])
        self.assertIn("pdr2210.1_0.mbag0_3-1.zip",
                      self.arch._aips['pdr2210']['1.0'])
        self.assertEqual(len(self.arch._aips['pdr2210']['1.0']), 2)
        self.assertIn("pdr2210.2.mbag0_3-2.zip",
                      self.arch._aips['pdr2210']['2'])
        self.assertEqual(len(self.arch._aips['pdr2210']['2']), 1)
        self.assertIn("pdr2210.3_1_3.mbag0_3-5.zip",
                      self.arch._aips['pdr2210']['3.1.3'])
        self.assertEqual(len(self.arch._aips['pdr2210']['3.1.3']), 1)

        self.assertIn("1491.1_0.mbag0_4-0.zip",
                      self.arch._aips['1491']['1.0'])
        self.assertEqual(len(self.arch._aips['1491']['1.0']), 1)
        

    def test_aipids(self):
        self.assertEqual(self.arch.aipids, ['1491', 'mds2-7223', 'pdr1010', 'pdr2210'])

    def test_versions_for(self):
        self.assertEqual(self.arch.versions_for('pdr1010'), ['1'])
        vers = self.arch.versions_for('pdr2210')
        self.assertIn('1.0', vers)
        self.assertIn('2', vers)
        self.assertIn('3.1.3', vers)
        self.assertEqual(len(vers), 3)

    def test_list_bags(self):
        self.assertEqual([f['name'] for f in self.arch.list_bags('pdr1010')],
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])
        self.assertEqual([f['name'] for f in self.arch.list_bags('pdr2210')],
                      ["pdr2210.1_0.mbag0_3-0.zip", "pdr2210.1_0.mbag0_3-1.zip", 
                       "pdr2210.2.mbag0_3-2.zip", "pdr2210.3_1_3.mbag0_3-5.zip"])
        self.assertEqual(self.arch.list_bags('pdr1010')[0],
                         {'name': 'pdr1010.mbag0_3-1.zip', 'aipid': 'pdr1010', 
                          'contentLength': 375, 'sinceVersion': '1',
                          'contentType': "application/zip",
                          "serialization": "zip",
                          'checksum': {'algorithm':"sha256",
     'hash': '9e70295bd074a121d720e2721ab405d7003e46086912cd92f012748c8cc3d6ad'},
                          'multibagSequence' : 1, "multibagProfileVersion" :"0.3"
                       })
        self.assertEqual([f['name'] for f in self.arch.list_bags('mds2-7223')],
                         ["mds2-7223.1_0_0.mbag0_4-0.zip", "mds2-7223.1_1_0.mbag0_4-1.zip"])

    def test_list_for_version(self):
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr1010', '1')],
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr1010', '2.1')], [])

        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr2210', '1.0')],
                      ["pdr2210.1_0.mbag0_3-0.zip", "pdr2210.1_0.mbag0_3-1.zip"])
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr2210', '2')],
                         ["pdr2210.2.mbag0_3-2.zip"])
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr2210', '3.1.3')],
                         ["pdr2210.3_1_3.mbag0_3-5.zip"])
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr2210', '3.1.2')], [])

    def test_list_for_latest_version(self):
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr1010', 'latest')],
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr1010')],
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr2210', 'latest')],
                         ["pdr2210.3_1_3.mbag0_3-5.zip"])
        self.assertEqual([f['name'] for f in
                          self.arch.list_for_version('pdr2210')],
                         ["pdr2210.3_1_3.mbag0_3-5.zip"])

    def test_head_for(self):
        self.assertEqual(self.arch.head_for('pdr1010', '1')['name'],
                         "pdr1010.mbag0_3-2.zip")
        self.assertEqual(self.arch.head_for('pdr2210', '1.0')['name'],
                         "pdr2210.1_0.mbag0_3-1.zip")
        self.assertEqual(self.arch.head_for('pdr2210', '2')['name'],
                         "pdr2210.2.mbag0_3-2.zip")
        self.assertEqual(self.arch.head_for('pdr2210', '3.1.3')['name'],
                         "pdr2210.3_1_3.mbag0_3-5.zip")
        self.assertIsNone(self.arch.head_for('pdr2210', '3'))

    def test_head_for_latest(self):
        self.assertEqual(self.arch.head_for('pdr1010', 'latest')['name'],
                         "pdr1010.mbag0_3-2.zip")
        self.assertEqual(self.arch.head_for('pdr1010')['name'],
                         "pdr1010.mbag0_3-2.zip")

class TestSimService(test.TestCase):

    def test_aipids(self):
        resp = requests.get(baseurl)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "AIP Identifiers")
        self.assertEqual(resp.json(), ["1491", "mds2-7223", "pdr1010", "pdr2210"])

    def test_list_all(self):
        resp = requests.get(baseurl+"/pdr1010")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "AIP Identifier exists")
        self.assertEqual(resp.json(), ["pdr1010"])

        resp = requests.get(baseurl+"/pdr2210")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "AIP Identifier exists")
        self.assertEqual(resp.json(), ["pdr2210"])

        resp = requests.get(baseurl+"/pdr2222")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.reason, "resource does not exist")

    def test_list_bags(self):
        resp = requests.get(baseurl+"/pdr1010/_aip")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "All bags for ID")
        self.assertEqual([f['name'] for f in resp.json()], 
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])
        self.assertEqual(resp.json()[0],
                         {'name': 'pdr1010.mbag0_3-1.zip', 'aipid': 'pdr1010',
                          'contentLength': 375, 'sinceVersion': '1', 
                          'contentType': "application/zip",
                          "serialization": "zip",
                          'checksum': {'algorithm':"sha256",
    'hash': '9e70295bd074a121d720e2721ab405d7003e46086912cd92f012748c8cc3d6ad'},
                          'multibagSequence' : 1, "multibagProfileVersion" :"0.3",
                          'downloadURL': "http://localhost/_aip/pdr1010.mbag0_3-1.zip"
                      })

        resp = requests.get(baseurl+"/pdr2210/_aip")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "All bags for ID")
        self.assertEqual([f['name'] for f in resp.json()], 
                      ["pdr2210.1_0.mbag0_3-0.zip", "pdr2210.1_0.mbag0_3-1.zip", 
                       "pdr2210.2.mbag0_3-2.zip", "pdr2210.3_1_3.mbag0_3-5.zip"])

    def test_versions_for(self):
        resp = requests.get(baseurl+"/pdr1010/_aip/_v")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "versions for ID")
        self.assertEqual(resp.json(), ["1"])

        resp = requests.get(baseurl+"/pdr2210/_aip/_v/")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "versions for ID")
        self.assertEqual(resp.json(), ["1.0", "2", "3.1.3"])

    def test_list_for_version(self):
        resp = requests.get(baseurl+"/pdr1010/_aip/_v/1")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "All bags for ID/vers")
        self.assertEqual([f['name'] for f in resp.json()], 
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])

        resp = requests.get(baseurl+"/pdr1010/_aip/_v/2")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.reason, "resource does not exist")

        resp = requests.get(baseurl+"/pdr2210/_aip/_v/1.0")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "All bags for ID/vers")
        self.assertEqual([f['name'] for f in resp.json()],
                    ["pdr2210.1_0.mbag0_3-0.zip", "pdr2210.1_0.mbag0_3-1.zip"])
                         
        resp = requests.get(baseurl+"/pdr2210/_aip/_v/2")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "All bags for ID/vers")
        self.assertEqual([f['name'] for f in resp.json()],
                         ["pdr2210.2.mbag0_3-2.zip"])

        resp = requests.get(baseurl+"/pdr2210/_aip/_v/3.1.3")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "All bags for ID/vers")
        self.assertEqual([f['name'] for f in resp.json()],
                         ["pdr2210.3_1_3.mbag0_3-5.zip"])

    def test_list_for_latest_version(self):
        resp = requests.get(baseurl+"/pdr1010/_aip/_v/latest")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "All bags for ID/vers")
        self.assertEqual([f['name'] for f in resp.json()], 
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])

        resp = requests.get(baseurl+"/pdr2210/_aip/_v/latest")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "All bags for ID/vers")
        self.assertEqual([f['name'] for f in resp.json()],
                         ["pdr2210.3_1_3.mbag0_3-5.zip"])

    def test_head(self):
        resp = requests.get(baseurl+"/pdr1010/_aip/_v/1/_head")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Head bags for ID/vers")
        self.assertEqual(resp.json()['name'], "pdr1010.mbag0_3-2.zip")
        
        resp = requests.get(baseurl+"/pdr2210/_aip/_v/1.0/_head")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Head bags for ID/vers")
        self.assertEqual(resp.json()['name'], "pdr2210.1_0.mbag0_3-1.zip")
                         
        resp = requests.get(baseurl+"/pdr2210/_aip/_v/2/_head")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Head bags for ID/vers")
        self.assertEqual(resp.json()['name'], "pdr2210.2.mbag0_3-2.zip")

        resp = requests.get(baseurl+"/pdr2210/_aip/_v/3.1.3/_head")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Head bags for ID/vers")
        self.assertEqual(resp.json()['name'], "pdr2210.3_1_3.mbag0_3-5.zip")

        resp = requests.get(baseurl+"/pdr1010/_aip/_v/2/_head")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.reason, "resource does not exist")

    def test_head_latest(self):
        resp = requests.get(baseurl+"/pdr1010/_aip/_v/latest/_head")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Head bags for ID/vers")
        self.assertEqual(resp.json()['name'], "pdr1010.mbag0_3-2.zip")
        
        resp = requests.get(baseurl+"/pdr2210/_aip/_v/latest/_head")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Head bags for ID/vers")
        self.assertEqual(resp.json()['name'], "pdr2210.3_1_3.mbag0_3-5.zip")

    def test_download(self):
        out = os.path.join(tmpdir(), "bag.zip")
        resp = requests.get(baseurl+"/_aip/pdr1010.mbag0_3-2.zip",
                            stream=True)
        with open(out, "wb") as fd:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    fd.write(chunk)

        self.assertTrue(os.path.isfile(out))
        dlcs = dstrb.checksum_of(out)
        refcs = dstrb.checksum_of(os.path.join(datadir,"pdr1010.mbag0_3-2.zip"))
        self.assertEqual(refcs, dlcs)

# this class doesn't test anything different from TestSimService; it exists mainly for debugging purposes.
# It runs the service in the current python process (rather through uwsgi).
class TestSimServiceServer(test.TestCase):

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def setUp(self):
        self.svc = dstrb.application
        self.resp = []

    def no_test_list_all(self):
        req = {
            'PATH_INFO': "/pdr1010",
            'REQUEST_METHOD': 'GET'
        }
        body = self.svc(req, self.start)

        self.assertGreater(len(self.resp), 0)
        self.assertIn("200 ", self.resp[0])
        self.assertIn("AIP Identifier exists", self.resp[0])
        self.assertEqual(json.loads("".join([e.decode() for e in body])), ["pdr1010"])

    def no_test_no_head(self):
        req = {
            'PATH_INFO': "/pdr1010/_aip/_v/2/_head",
            'REQUEST_METHOD': 'GET'
        }
        body = self.svc(req, self.start)

        self.assertIn("404 ", self.resp[0])
        self.assertIn("does not exist", self.resp[0])

    def no_test_list_noexist(self):
        req = {
            'PATH_INFO': "/goob",
            'REQUEST_METHOD': 'GET'
        }
        body = self.svc(req, self.start)

        self.assertGreater(len(self.resp), 0)
        self.assertIn("404 ", self.resp[0])
        self.assertIn("resource does not exist", self.resp[0])


if __name__ == '__main__':
    test.main()
