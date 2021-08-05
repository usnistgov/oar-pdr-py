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

    def test_urls(self):
        id = "pdr02d4t"
        self.assertEqual(self.cli._url_for_pdr_id(id),
                         self.baseurl + "records?@id=" +id)

        self.assertEqual(self.cli._url_for_ediid(id),
                         self.baseurl + "records/" +id)

    def test_describe_ark(self):
        data = self.cli.describe("ark:/88434/pdr02d4t")
        self.assertEqual(data['ediid'], 'ABCDEFG')

    def test_describe_pdr(self):
        data = self.cli.describe("ABCDEFG")
        self.assertEqual(data['@id'], 'ark:/88434/pdr02d4t')

    def test_search(self):
        data = self.cli.search()
        self.assertEqual(len(data), 2)

        ids = [d['@id'] for d in data if '@id' in d]
        self.assertIn("ark:/88434/pdr02d4t", ids)
        self.assertIn("ark:/88434/edi00hw91c", ids)
        

        


if __name__ == '__main__':
    test.main()


