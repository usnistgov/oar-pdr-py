import os, pdb, requests, logging, time, json, sys, tempfile, shutil
import unittest as test
from copy import deepcopy
from pathlib import Path

from nistoar.testing import *
from nistoar.pdr.public.sim import repo

testdir = Path(__file__).resolve().parent
distdir = testdir.parents[1]/"distrib"
bagdir  = distdir/'data'
descdir = testdir.parents[1]/"describe"
archdir   = descdir/'data'/'rmm-test-archive'

uwsgiscript = repo.__file__
port = 9091
baseurl = "http://localhost:{0}/".format(port)

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(logdir):
    srvport = port
    pidfile = os.path.join(logdir,"simsrv"+str(srvport)+".pid")
    dbdir = os.path.join(logdir, "archive")
    
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} --wsgi-file {3} --set-ph readonly=False " \
          "--pidfile {4} --set-ph archive_dir={5} --set-ph bagdir={6} --set-ph baseurl={7} " \
          "--post-buffering=1"
    cmd = cmd.format(os.path.join(logdir,"simsrv.log"), uwsgi_opts, srvport,
                     uwsgiscript, pidfile, dbdir, bagdir, baseurl)
    os.system(cmd)
    time.sleep(0.5)

def stopService(logdir):
    srvport = port
    pidfile = os.path.join(logdir,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(logdir, "simsrv"+str(srvport)+".pid"))
    os.system(cmd)
    time.sleep(1)

tmpdir = None
def setUpModule():
    global tmpdir
    tmpdir = tempfile.TemporaryDirectory(prefix="_test_simsrv.")
    dbdir = Path(tmpdir.name)/"archive"
    shutil.copytree(archdir, dbdir)
    startService(tmpdir.name)

def tearDownModule():
    global tmpdir
    stopService(tmpdir.name)
    tmpdir.cleanup()

class TestSimRepo(test.TestCase):

    def test_ready(self):
        resp = requests.get(baseurl)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Ready")

    def test_distrib(self):
        resp = requests.get(baseurl+"ds/")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "AIP Identifiers")
        self.assertEqual(resp.json(), ["1491", "mds2-7223", "pdr1010", "pdr2210"])

    def test_describe(self):
        resp = requests.get(baseurl+"records/mds00qdrz9")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.reason, "Identifier exists")
        data = resp.json()
        self.assertEqual(data['ediid'], "19A9D7193F868BDDE0531A57068151D2431")
        self.assertEqual(data['@id'], "ark:/88434/mds00qdrz9")

    def test_ingest(self):
        nerdf = archdir/"records"/"mds2-7223.json"
        self.assertTrue(nerdf.is_file())

        with open(nerdf, 'r') as fd:
            data = json.load(fd)
        try:
            resp = requests.post(baseurl+"ingest/?auth=token", json=data)
        except Exception as ex:
            self.fail("request post failed: "+str(ex))
        self.assertEqual(resp.reason, "Accepted")
        self.assertEqual(resp.status_code, 201)

        
    






if __name__ == '__main__':
    test.main()
