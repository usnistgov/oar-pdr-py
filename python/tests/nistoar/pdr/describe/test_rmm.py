import os, pdb, sys, json, requests, logging, time, re, hashlib
import unittest as test

from nistoar.testing import *
from nistoar.pdr.describe import rmm

testdir = os.path.join(os.path.dirname(os.path.abspath(__file__)))
datadir = os.path.join(testdir, 'data')
basedir = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.dirname(testdir)))))

port = 9091
baseurl = "http://localhost:{0}/".format(port)

def startService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/describe/sim_describe_svc.py"
    cmd = "uwsgi --daemonize {0} --plugin python3 --http-socket :{1} " \
          "--wsgi-file {2} --pidfile {3}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), srvport,
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

class TestMetadataClient(test.TestCase):

    def setUp(self):
        self.baseurl = baseurl
        self.cli = rmm.MetadataClient(self.baseurl)

    def test_describe_ark(self):
        data = self.cli.describe("ark:/88434/mds003r0x6")
        self.assertEqual(data['@id'], 'ark:/88434/mds003r0x6')
        self.assertEqual(data['ediid'], '1E0F15DAAEFB84E4E0531A5706813DD8436')

        data = self.cli.describe("ark:/88434/mds2-2110")
        self.assertEqual(data['@id'], 'ark:/88434/mds2-2110')
        self.assertEqual(data['ediid'], 'ark:/88434/mds2-2110')

    def test_describe_pdr(self):
        data = self.cli.describe("1E0F15DAAEFB84E4E0531A5706813DD8436")
        self.assertEqual(data['@id'], 'ark:/88434/mds003r0x6')
        self.assertEqual(data['ediid'], '1E0F15DAAEFB84E4E0531A5706813DD8436')

        with self.assertRaises(rmm.IDNotFound):
            data = self.cli.describe("mds2-2110")

    def test_search(self):
        data = self.cli.search()
        self.assertEqual(len(data), 6)

        ids = [d['ediid'] for d in data if 'ediid' in d]
        self.assertIn("ark:/88434/mds2-2106", ids)
        self.assertIn("ark:/88434/mds2-2107", ids)
        self.assertIn("ark:/88434/mds2-2110", ids)
        self.assertIn("1E651A532AFD8816E0531A570681A662439", ids)
        self.assertIn("19A9D7193F868BDDE0531A57068151D2431", ids)
        self.assertIn("1E0F15DAAEFB84E4E0531A5706813DD8436", ids)
        

        


if __name__ == '__main__':
    test.main()


