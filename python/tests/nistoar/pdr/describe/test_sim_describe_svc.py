import os, sys, pdb, requests, logging, time, shutil, json
import unittest as test
from copy import deepcopy

from nistoar.testing import *
# import tests.nistoar.pdr.describe.sim_describe_svc as desc

testdir = os.path.dirname(os.path.abspath(__file__))
datadir = os.path.join(testdir, 'data', 'rmm-test-archive')

def import_file(path, name=None):
    if not name:
        name = os.path.splitext(os.path.basename(path))[0]
    import importlib.util as imputil
    spec = imputil.spec_from_file_location(name, path)
    out = imputil.module_from_spec(spec)
    sys.modules["sim_describe_svc"] = out
    spec.loader.exec_module(out)
    return out

simsrvrsrc = os.path.join(testdir, "sim_describe_svc.py")
desc = import_file(simsrvrsrc)

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
        self.assertEqual(self.arch.records, {
            'ark:/88434/mds003r0x6': '1E0F15DAAEFB84E4E0531A5706813DD8436',
            'ark:/88434/mds00qdrz9': '19A9D7193F868BDDE0531A57068151D2431',
            'ark:/88434/mds00sxbvh': '1E651A532AFD8816E0531A570681A662439',
            "ark:/88434/mds2-2106": "mds2-2106",
            "ark:/88434/mds2-2107": "mds2-2107",
            "ark:/88434/mds2-2110": "mds2-2110"
        })
        self.assertEqual(self.arch.releaseSets, {
            'ark:/88434/mds003r0x6/pdr:v': '1E0F15DAAEFB84E4E0531A5706813DD8436',
            'ark:/88434/mds00qdrz9/pdr:v': '19A9D7193F868BDDE0531A57068151D2431',
            'ark:/88434/mds00sxbvh/pdr:v': '1E651A532AFD8816E0531A570681A662439',
            "ark:/88434/mds2-2106/pdr:v": "mds2-2106",
            "ark:/88434/mds2-2107/pdr:v": "mds2-2107",
            "ark:/88434/mds2-2110/pdr:v": "mds2-2110"
        })
        self.assertIn("ark:/88434/mds00sxbvh/pdr:v/1.0.4", self.arch.versions)
        self.assertIn("ark:/88434/mds2-2106/pdr:v/1.2.0",  self.arch.versions)
        self.assertEqual(len(self.arch.versions), 16)

    def test_pdrid2aipid(self):
        self.assertEqual(self.arch.pdrid2aipid("records", "ark:/88434/mds00sxbvh"),
                         "1E651A532AFD8816E0531A570681A662439")
        self.assertEqual(self.arch.pdrid2aipid("releaseSets", "ark:/88434/mds2-2107/pdr:v"), "mds2-2107")
        self.assertIsNone(self.arch.pdrid2aipid("releaseSets", "ark:/88434/mds2-2107"))
        self.assertEqual(self.arch.pdrid2aipid("versions", "ark:/88434/mds2-2106/pdr:v/1.1.0"),
                         "mds2-2106-v1_1_0")
        
    def test_ids(self):
        ids = self.arch.aipids();
        self.assertEqual(len(ids), 6)
        self.assertIn("mds2-2106", ids)
        self.assertIn("mds2-2107", ids)
        self.assertIn("mds2-2110", ids)
        self.assertIn("1E651A532AFD8816E0531A570681A662439", ids)
        self.assertIn("19A9D7193F868BDDE0531A57068151D2431", ids)
        self.assertIn("1E0F15DAAEFB84E4E0531A5706813DD8436", ids)

        ids = self.arch.aipids("releaseSets");
        self.assertEqual(len(ids), 6)
        self.assertIn("mds2-2106", ids)
        self.assertIn("mds2-2107", ids)
        self.assertIn("mds2-2110", ids)
        self.assertIn("1E651A532AFD8816E0531A570681A662439", ids)
        self.assertIn("19A9D7193F868BDDE0531A57068151D2431", ids)
        self.assertIn("1E0F15DAAEFB84E4E0531A5706813DD8436", ids)

        ids = self.arch.aipids("versions");
        self.assertEqual(len(ids), 16)
        self.assertIn("mds2-2106-v1_2_0", ids)
        self.assertIn("mds2-2107-v1_0_0", ids)
        self.assertIn("mds2-2110-v1_0_1", ids)
        self.assertIn("1E651A532AFD8816E0531A570681A662439-v1_0_4", ids)
        self.assertIn("19A9D7193F868BDDE0531A57068151D2431-v1_0_0", ids)
        self.assertIn("1E0F15DAAEFB84E4E0531A5706813DD8436-v1_0_0", ids)

class TestSimRMMHandler(test.TestCase):

    archdir = None
    arch = None

    @classmethod
    def setUpClass(cls):
        tdir = tmpdir()
        cls.archdir = os.path.join(tdir, "archive")
        shutil.copytree(datadir, cls.archdir)
        cls.arch = desc.SimArchive(cls.archdir)

    @classmethod
    def tearDownClass(cls):
        if cls.archdir:
            shutil.rmtree(cls.archdir)
            cls.archdir = None
        cls.arch = None
        
    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def gethandler(self, env):
        return desc.SimRMMHandler(self.arch, env, self.start, False)

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        self.hdlr = None
        self.resp = []

    def test_get_root(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Ready")

    def test_get_all_records(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 6)
        self.assertEqual(len(data['ResultData']), 6)
        self.assertEqual(data['ResultData'][0]['accessLevel'], "public")
        self.assertTrue(not any(['/pdr:v' in r['@id'] for r in data['ResultData']]))
        
    def test_get_all_releaseSets(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/releaseSets/"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 6)
        self.assertEqual(len(data['ResultData']), 6)
        self.assertIn('hasRelease', data['ResultData'][0])
        self.assertTrue(all(['hasRelease' in r for r in data['ResultData']]))
        self.assertTrue(all([r['@id'].endswith('/pdr:v') for r in data['ResultData']]))
        
    def test_get_all_versions(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/versions/"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 16)
        self.assertEqual(len(data['ResultData']), 16)
        self.assertEqual(data['ResultData'][0]['accessLevel'], "public")
        self.assertTrue(all(['/pdr:v/1.' in r['@id'] for r in data['ResultData']]))
        
    def test_get_record_by_ediid(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records/19A9D7193F868BDDE0531A57068151D2431"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['@id'], "ark:/88434/mds00qdrz9")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records/mds00qdrz9"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 mds00qdrz9 does not exist in records")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records/ark:/88434/mds00qdrz9"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['@id'], "ark:/88434/mds00qdrz9")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records/mds2-2107"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ediid'], "ark:/88434/mds2-2107")
        self.assertEqual(data['@id'], "ark:/88434/mds2-2107")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records/ark:/88434/mds2-2107"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ediid'], "ark:/88434/mds2-2107")
        self.assertEqual(data['@id'], "ark:/88434/mds2-2107")
        
    def test_get_releaseSet_by_ediid(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/releaseSets/19A9D7193F868BDDE0531A57068151D2431"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['@id'], "ark:/88434/mds00qdrz9/pdr:v")
        self.assertIn('hasRelease', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/releaseSets/mds00qdrz9"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 mds00qdrz9 does not exist in releaseSets")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/releaseSets/mds2-2107"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ediid'], "ark:/88434/mds2-2107")
        self.assertEqual(data['@id'], "ark:/88434/mds2-2107/pdr:v")
        
    def test_get_record_by_id(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "@id=ark:/88434/mds00qdrz9"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultData'][0]['@id'], "ark:/88434/mds00qdrz9")
        self.assertEqual(data['ResultData'][0]['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "@id=ark:/88434/mds2-2107"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultData'][0]['ediid'], "ark:/88434/mds2-2107")
        self.assertEqual(data['ResultData'][0]['@id'], "ark:/88434/mds2-2107")
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)
        
    def test_get_releaseSet_by_id(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/releaseSets",
            'QUERY_STRING': "@id=ark:/88434/mds00qdrz9/pdr:v"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultData'][0]['@id'], "ark:/88434/mds00qdrz9/pdr:v")
        self.assertEqual(data['ResultData'][0]['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/releaseSets",
            'QUERY_STRING': "@id=ark:/88434/mds2-2107/pdr:v"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultData'][0]['ediid'], "ark:/88434/mds2-2107")
        self.assertEqual(data['ResultData'][0]['@id'], "ark:/88434/mds2-2107/pdr:v")
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)
        
    def test_get_version_by_id(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/versions",
            'QUERY_STRING': "@id=ark:/88434/mds00qdrz9/pdr:v/1.0.0"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultData'][0]['@id'], "ark:/88434/mds00qdrz9/pdr:v/1.0.0")
        self.assertEqual(data['ResultData'][0]['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/versions",
            'QUERY_STRING': "@id=ark:/88434/mds00qdrz9"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 ark:/88434/mds00qdrz9 does not exist in versions")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/versions",
            'QUERY_STRING': "@id=ark:/88434/mds2-2106/pdr:v/1.4.0"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultData'][0]['ediid'], "ark:/88434/mds2-2106")
        self.assertEqual(data['ResultData'][0]['@id'], "ark:/88434/mds2-2106/pdr:v/1.4.0")
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)
        
        

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
        resp = requests.get(baseurl+"records/1E651A532AFD8816E0531A570681A662439")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertEqual(data["@id"], "ark:/88434/mds00sxbvh")

    def test_found_ark(self):
        resp = requests.get(baseurl+"records?@id=ark:/88434/mds00sxbvh")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertEqual(data['ResultData'][0]["ediid"], "1E651A532AFD8816E0531A570681A662439")

    def test_post_rec(self):
        id = "ark:/55121/mds1-1000"
        rec = {
            'ediid': id,
            '@id': id
        }
        resp = requests.post(baseurl+"records", json=rec)
        self.assertEqual(resp.status_code, 201)

        resp = requests.get(baseurl+"records/mds1-1000")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertEqual(data["@id"], id)        
        
        resp = requests.get(baseurl+"records?@id=ark:/55121/mds1-1000")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertEqual(data['ResultData'][0]["@id"], id)        

        
    def test_search_all_records(self):
        resp = requests.get(baseurl+"records")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertIn("ResultData", data)
        ids = [d['@id'] for d in data["ResultData"] if '@id' in d]

        # the exact # of results depends on whether test_post_rec runs before or after this test
        self.assertGreaterEqual(len(data["ResultData"]), 6)
        self.assertGreaterEqual(data["ResultCount"], 6)
        self.assertGreaterEqual(data["PageSize"], 6)
        self.assertLessEqual(len(data["ResultData"]), 7)
        self.assertLessEqual(data["ResultCount"], 7)
        self.assertLessEqual(data["PageSize"], 7)

        self.assertIn("ark:/88434/mds00sxbvh", ids)
        self.assertIn("ark:/88434/mds2-2106", ids)
        self.assertIn("ark:/88434/mds2-2107", ids)
        self.assertIn("ark:/88434/mds2-2110", ids)
        if len(ids) > 6:
            self.assertIn("ark:/55121/mds1-1000", ids)



if __name__ == '__main__':
    test.main()


