import os, pdb, requests, logging, time, json, sys, tempfile
import unittest as test
from copy import deepcopy
from pathlib import Path

from nistoar.testing import *
from nistoar.pdr.public.sim import distrib as dstrb

testdir = Path(__file__).resolve().parent
distdir = testdir.parents[1]/"distrib"
datadir = distdir/'data'

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

class TestSimDistribHandler(test.TestCase):

    archdir = None
    arch = None

    @classmethod
    def setUpClass(cls):
        cls.archdir = datadir
        cls.arch = dstrb.SimArchive(cls.archdir)

    @classmethod
    def tearDownClass(cls):
        cls.archdir = None
        cls.arch = None

    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers:
            self.resp.append("{0}: {1}".format(head[0], head[1]))

    def gethandler(self, env):
        env['SIM_BASEURL'] = "http://localhost/"
        return dstrb.SimDistribHandler(self.arch, env, self.start)

    def setUp(self):
        self.hdlr = None
        self.resp = []
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_testdistrib.")

    def tearDown(self):
        self.hdlr = None
        self.resp = []
        self.tmpdir.cleanup()

    def test_aipids(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 AIP Identifiers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data, ["1491", "mds2-7223", "pdr1010", "pdr2210"])

    def test_list_all(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/pdr1010"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 AIP Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data, ["pdr1010"])

        self.resp = []
        req['PATH_INFO'] = "/pdr2210"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 AIP Identifier exists")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data, ["pdr2210"])

        self.resp = []
        req['PATH_INFO'] = "/pdr2222"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 resource does not exist")

    def test_list_bags(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/pdr1010/_aip"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 All bags for ID")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual([f['name'] for f in data], 
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])
        self.assertEqual(data[0], 
                         {'name': 'pdr1010.mbag0_3-1.zip', 'aipid': 'pdr1010',
                          'contentLength': 375, 'sinceVersion': '1', 
                          'contentType': "application/zip",
                          "serialization": "zip",
                          'checksum': {'algorithm':"sha256",
    'hash': '9e70295bd074a121d720e2721ab405d7003e46086912cd92f012748c8cc3d6ad'},
                          'multibagSequence' : 1, "multibagProfileVersion" :"0.3",
                          'downloadURL': "http://localhost/_aip/pdr1010.mbag0_3-1.zip"
                      })

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 All bags for ID")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual([f['name'] for f in data], 
                      ["pdr2210.1_0.mbag0_3-0.zip", "pdr2210.1_0.mbag0_3-1.zip", 
                       "pdr2210.2.mbag0_3-2.zip", "pdr2210.3_1_3.mbag0_3-5.zip"])

    def test_versions_for(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/pdr1010/_aip/_v"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 versions for ID")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data, ["1"])

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 versions for ID")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data, ["1.0", "2", "3.1.3"])

    def test_list_for_version(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/pdr1010/_aip/_v/1"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 All bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual([f['name'] for f in data], 
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])

        self.resp = []
        req['PATH_INFO'] = "/pdr1010/_aip/_v/2"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 resource does not exist")

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v/1.0"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 All bags for ID/vers")
        self.assertEqual(self.resp[0], "200 All bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual([f['name'] for f in data], 
                         ["pdr2210.1_0.mbag0_3-0.zip", "pdr2210.1_0.mbag0_3-1.zip"])

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v/2"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 All bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual([f['name'] for f in data], 
                         ["pdr2210.2.mbag0_3-2.zip"])

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v/3.1.3"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 All bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual([f['name'] for f in data], 
                         ["pdr2210.3_1_3.mbag0_3-5.zip"])

    def test_list_for_latest_version(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/pdr1010/_aip/_v/latest"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 All bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual([f['name'] for f in data], 
                         ["pdr1010.mbag0_3-1.zip", "pdr1010.mbag0_3-2.zip"])

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v/latest"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 All bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual([f['name'] for f in data], 
                         ["pdr2210.3_1_3.mbag0_3-5.zip"])

    def test_head(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/pdr1010/_aip/_v/1/_head"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Head bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['name'], "pdr1010.mbag0_3-2.zip")

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v/1.0/_head"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Head bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['name'], "pdr2210.1_0.mbag0_3-1.zip")

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v/2/_head"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Head bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['name'], "pdr2210.2.mbag0_3-2.zip")

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v/3.1.3/_head"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Head bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['name'], "pdr2210.3_1_3.mbag0_3-5.zip")

        self.resp = []
        req['PATH_INFO'] = "/pdr1010/_aip/_v/2/_head"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "404 resource does not exist")

    def test_head_latest(self):
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/pdr1010/_aip/_v/latest/_head"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Head bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['name'], "pdr1010.mbag0_3-2.zip")

        self.resp = []
        req['PATH_INFO'] = "/pdr2210/_aip/_v/latest/_head"
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertEqual(self.resp[0], "200 Head bags for ID/vers")
        data = json.loads("\n".join([ln.decode() for ln in body]))
        self.assertEqual(data['name'], "pdr2210.3_1_3.mbag0_3-5.zip")

    def test_download(self):
        out = os.path.join(self.tmpdir.name, "bag.zip")
        req = {
            'REQUEST_METHOD': "GET",
            'PATH_INFO': "/_aip/pdr1010.mbag0_3-2.zip"
        }
        self.hdlr = self.gethandler(req)
        body = self.hdlr.handle()
        self.assertTrue(self.resp[0].startswith("200 "))
        with open(out, 'wb') as fd:
            for chunk in body:
                if chunk:
                    fd.write(chunk)

        self.assertTrue(os.path.isfile(out))
        dlcs = dstrb.checksum_of(out)
        refcs = dstrb.checksum_of(os.path.join(datadir,"pdr1010.mbag0_3-2.zip"))
        self.assertEqual(refcs, dlcs)
        

        
        

        

if __name__ == '__main__':
    test.main()
