import os, sys, pdb, requests, logging, time, shutil, json, tempfile, re, io
import unittest as test
from copy import deepcopy
from pathlib import Path

from nistoar.pdr.public.sim.db import SimDb
from nistoar.pdr.public.sim.rmm import SimRMM, SimRMMHandler
from nistoar.pdr.describe.rmm import MetadataClient as rmm
from nistoar.testing import *
# import tests.nistoar.pdr.describe.sim_describe_svc as desc

releasesets = rmm.COLL_RELEASES
versions = rmm.COLL_VERSIONS
records = rmm.COLL_LATEST

testdir = Path(__file__).resolve().parent
descdir = testdir.parents[1]/"describe"
datadir = descdir/'data'/'rmm-test-archive'

tmpdir = tempfile.TemporaryDirectory(prefix="_test_sim.")
def tearDownModule():
    tmpdir.cleanup()

class TestSimRMMHandler(test.TestCase):

    tmpdir = None
    archdir = None
    arch = None

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.TemporaryDirectory(prefix="archive.")
        cls.archdir = Path(cls.tmpdir.name)/"archive"
        shutil.copytree(datadir, cls.archdir)

        # remove mds2-7223 so that we can add it later
        # cls.eject_aipid("mds2-7223")
        
        cls.arch = SimDb(cls.archdir, False)

    @classmethod
    def eject_aipid(cls, aipid):
        mdf = cls.archdir/records/(aipid+".json")
        if mdf.is_file():
            mdf.unlink()
        mdf = cls.archdir/releasesets/(aipid+".json")
        if mdf.is_file():
            mdf.unlink()
        for f in os.listdir(cls.archdir/versions):
            if f.startswith(aipid):
                (cls.archdir/versions/f).unlink()

    @classmethod
    def tearDownClass(cls):
        cls.tmpdir.cleanup()
        cls.arch = None
        cls.archdir = None

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def gethandler(self, env):
        return SimRMMHandler(self.arch, env, self.start, False)

    def setUp(self):
        self.eject_aipid("mds2-7223")
        self.hdlr = None
        self.resp = []

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

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "include=@id&exclude=_id"
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
            'PATH_INFO': "/releasesets/"
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
        self.assertEqual(self.resp[0], "200 Identifier exists")
        self.assertEqual(data['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['@id'], "ark:/88434/mds00qdrz9")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records/goober"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 goober does not exist in records")

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
            'PATH_INFO': "/releasesets/19A9D7193F868BDDE0531A57068151D2431"
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
            'PATH_INFO': "/releasesets/mds00qdrz9"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        self.assertEqual(data['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['@id'], "ark:/88434/mds00qdrz9/pdr:v")
        self.assertIn('hasRelease', data)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/releasesets/goober"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 goober does not exist in releasesets")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/releasesets/mds2-2107"
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
            'PATH_INFO': "/releasesets",
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
            'PATH_INFO': "/releasesets",
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
        
    def test_search(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "accessLevel=public"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 6)
        self.assertEqual(len(data['ResultData']), 6)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "accessLevel=public&accessLevel=private"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 6)
        self.assertEqual(len(data['ResultData']), 6)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "accessLevel=private"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 0)
        self.assertEqual(len(data['ResultData']), 0)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "goob=private"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 0)
        self.assertEqual(len(data['ResultData']), 0)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "ediid=ark:/88434/mds003r0x6&ediid=19A9D7193F868BDDE0531A57068151D2431"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "ediid=ark:/88434/mds2-2110&ediid=19A9D7193F868BDDE0531A57068151D2431"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 2)
        self.assertEqual(len(data['ResultData']), 2)

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records",
            'QUERY_STRING': "ediid=19A9D7193F868BDDE0531A57068151D2431"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)

    def test_version_search(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/versions",
            'QUERY_STRING': "ediid=ark:/88434/mds2-2106&version=1.4.0"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)
        self.assertEqual(data['ResultData'][0]['ediid'], "ark:/88434/mds2-2106")
        self.assertEqual(data['ResultData'][0]['@id'], "ark:/88434/mds2-2106/pdr:v/1.4.0")
        self.assertEqual(data['ResultData'][0]['version'], "1.4.0")

        self.resp = []
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/versions",
            'QUERY_STRING': "ediid=1E651A532AFD8816E0531A570681A662439&version=1.0.4"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['ResultCount'], 1)
        self.assertEqual(len(data['ResultData']), 1)
        self.assertEqual(data['ResultData'][0]['ediid'], "1E651A532AFD8816E0531A570681A662439")
        self.assertEqual(data['ResultData'][0]['@id'], "ark:/88434/mds00sxbvh/pdr:v/1.0.4")
        self.assertEqual(data['ResultData'][0]['version'], "1.0.4")

    def test_ingest(self):
        # self.eject_aipid("mds2-7223")

        # prove that mds2-7223 does not exist
        get = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/records/mds2-7223"
        }
        self.hdlr = self.gethandler(get)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 mds2-7223 does not exist in records")

        infile = datadir/versions/"mds2-7223-v1_0_0.json"
        self.assertTrue(infile.is_file())

        self.resp = []
        req = {
            'REQUEST_METHOD': "POST",
            'PATH_INFO': "/ingest",
            'HTTP_AUTHORIZATION': "Bearer key"
        }
        with open(infile) as fd:
            nerd = json.load(fd)
        nerd['@id'] = re.sub(r'/pdr:v.*$', '', nerd['@id'])
        infd = io.BytesIO(json.dumps(nerd).encode())
        req['wsgi.input'] = infd
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "201 Accepted")

        self.resp = []
        self.hdlr = self.gethandler(get)
        body = self.hdlr.handle()
        self.assertTrue(self.resp[0].startswith("200 "))
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['@id'], "ark:/88434/mds2-7223")
        self.assertEqual(data['version'], "1.0.0")
        
        self.resp = []
        get['PATH_INFO'] = "/releasesets/ark:/88434/mds2-7223/pdr:v"
        self.hdlr = self.gethandler(get)
        body = self.hdlr.handle()
        self.assertTrue(self.resp[0].startswith("200 "))
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['@id'], "ark:/88434/mds2-7223/pdr:v")
        self.assertEqual(data['version'], "1.0.0")
        
        self.resp = []
        get['PATH_INFO'] = "/versions/ark:/88434/mds2-7223/pdr:v/1.0.0"
        self.hdlr = self.gethandler(get)
        body = self.hdlr.handle()
        self.assertTrue(self.resp[0].startswith("200 "))
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['@id'], "ark:/88434/mds2-7223/pdr:v/1.0.0")
        self.assertEqual(data['version'], "1.0.0")
    
        infile = datadir/versions/"mds2-7223-v1_1_0.json"
        self.assertTrue(infile.is_file())

        self.resp = []
        req = {
            'REQUEST_METHOD': "POST",
            'PATH_INFO': "/ingest",
            'HTTP_AUTHORIZATION': "Bearer key"
        }
        with open(infile) as fd:
            nerd = json.load(fd)
        nerd['@id'] = re.sub(r'/pdr:v.*$', '', nerd['@id'])
        infd = io.BytesIO(json.dumps(nerd).encode())
        req['wsgi.input'] = infd
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "201 Accepted")

        self.resp = []
        get['PATH_INFO'] = "/records/mds2-7223"
        self.hdlr = self.gethandler(get)
        body = self.hdlr.handle()
        self.assertTrue(self.resp[0].startswith("200 "))
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['version'], "1.1.0")
        self.assertEqual(data['@id'], "ark:/88434/mds2-7223")
        
        self.resp = []
        get['PATH_INFO'] = "/releasesets/ark:/88434/mds2-7223/pdr:v"
        self.hdlr = self.gethandler(get)
        body = self.hdlr.handle()
        self.assertTrue(self.resp[0].startswith("200 "))
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['@id'], "ark:/88434/mds2-7223/pdr:v")
        self.assertEqual(data['version'], "1.1.0")
        
        self.resp = []
        get['PATH_INFO'] = "/versions/ark:/88434/mds2-7223/pdr:v/1.0.0"
        self.hdlr = self.gethandler(get)
        body = self.hdlr.handle()
        self.assertTrue(self.resp[0].startswith("200 "))
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['@id'], "ark:/88434/mds2-7223/pdr:v/1.0.0")
        self.assertEqual(data['version'], "1.0.0")
        
        self.resp = []
        get['PATH_INFO'] = "/versions/ark:/88434/mds2-7223/pdr:v/1.1.0"
        self.hdlr = self.gethandler(get)
        body = self.hdlr.handle()
        self.assertTrue(self.resp[0].startswith("200 "))
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['@id'], "ark:/88434/mds2-7223/pdr:v/1.1.0")
        self.assertEqual(data['version'], "1.1.0")
    
    def test_add_to_coll(self):
        # self.eject_aipid("mds2-7223")

        try:
            infile = datadir/versions/"mds2-7223-v1_0_0.json"
            self.assertTrue(infile.is_file())

            self.resp = []
            req = {
                'REQUEST_METHOD': "POST",
                'PATH_INFO': "/versions",
                'QUERY_STRING': "auth=key"
            }
            with open(infile) as fd:
                nerd = json.load(fd)
            infd = io.BytesIO(json.dumps(nerd).encode())
            req['wsgi.input'] = infd
            self.hdlr = self.gethandler(req)
            body = self.hdlr.handle()
            self.assertEqual(self.resp[0], "201 Accepted")

            self.resp = []
            get = {
                'REQUEST_METHOD': "GET",
                'PATH_INFO': "/versions/ark:/88434/mds2-7223/pdr:v/1.0.0"
            }
            self.hdlr = self.gethandler(get)
            body = self.hdlr.handle()
            self.assertEqual(self.resp[0], "200 Identifier exists")
            data = json.loads("\n".join([ln.decode() for ln in body]))
            self.assertEqual(data['@id'], "ark:/88434/mds2-7223/pdr:v/1.0.0")
            self.assertEqual(data['version'], "1.0.0")
        
            self.resp = []
            get['PATH_INFO'] = "/records/mds2-7223"
            self.hdlr = self.gethandler(get)
            body = self.hdlr.handle()
            self.assertEqual(self.resp[0], "404 mds2-7223 does not exist in records")

        finally:
            pass
            # self.resp = []
            # req = {
            #     'REQUEST_METHOD': "POST",
            #     'PATH_INFO': "/ingest"
            # }
            # nerd['@id'] = re.sub(r'/pdr:v.*$', '', nerd['@id'])
            # infd = io.BytesIO(json.dumps(nerd).encode())
            # req['wsgi.input'] = infd
            # self.hdlr = self.gethandler(req)
            # body = self.hdlr.handle()
            # self.assertEqual(self.resp[0], "201 Accepted")

            # infile = datadir/versions/"mds2-7223-v1_1_0.json"
            # self.assertTrue(infile.is_file())
            # with open(infile) as fd:
            #     nerd = json.load(fd)
            # self.resp = []
            # nerd['@id'] = re.sub(r'/pdr:v.*$', '', nerd['@id'])
            # infd = io.BytesIO(json.dumps(nerd).encode())
            # req['wsgi.input'] = infd
            # self.hdlr = self.gethandler(req)
            # body = self.hdlr.handle()
            # self.assertEqual(self.resp[0], "201 Accepted")

    def test_invalid(self):
        infile = datadir/records/"mds2-7223.json"
        self.assertTrue(infile.is_file())

        req = {
            'REQUEST_METHOD': "POST",
            'PATH_INFO': "/invalid",
            'QUERY_STRING': "auth=key"
        }
        with open(infile) as fd:
            nerd = json.load(fd)
        nerd['@id'] = re.sub(r'/pdr:v.*$', '', nerd['@id'])
        infd = io.BytesIO(json.dumps(nerd).encode())
        req['wsgi.input'] = infd
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "400 Input record is not valid")

    def test_strict_fail(self):
        infile = datadir/records/"mds2-7223.json"
        self.assertTrue(infile.is_file())

        req = {
            'REQUEST_METHOD': "POST",
            'PATH_INFO': "/ingest",
            'QUERY_STRING': "strictness=abusive"
        }
        with open(infile) as fd:
            nerd = json.load(fd)
        nerd['@id'] = re.sub(r'/pdr:v.*$', '', nerd['@id'])
        infd = io.BytesIO(json.dumps(nerd).encode())
        req['wsgi.input'] = infd
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "400 Input record is not valid")

    def test_unauthorized(self):
        infile = datadir/records/"mds2-7223.json"
        self.assertTrue(infile.is_file())

        req = {
            'REQUEST_METHOD': "POST",
            'PATH_INFO': "/ingest"
        }
        with open(infile) as fd:
            nerd = json.load(fd)
        nerd['@id'] = re.sub(r'/pdr:v.*$', '', nerd['@id'])
        infd = io.BytesIO(json.dumps(nerd).encode())
        req['wsgi.input'] = infd
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "401 Not Authorized")
        
            

if __name__ == '__main__':
    test.main()



    

