import os, pdb, sys, json, logging, time, re, tempfile, shutil
import unittest as test
from pathlib import Path
from collections.abc import Mapping

from nistoar.testing import *
from nistoar.midas.dap.fm.clients import fm
from nistoar.midas.dap.fm.exceptions import *

port = 8999
baseurl = f"http://localhost:{port}/mfm1"
tmpdir = tempfile.TemporaryDirectory(prefix="test_fmclient.")
testdir = Path(__file__).parents[0]
basedir = testdir.parents[6]
fmdatadir = os.path.join(tmpdir.name, 'svc', "fmdata")
jobdir =  os.path.join(tmpdir.name, 'svc', "jobqueue")

nistoardir = testdir.parents[3]
sampdatadir = nistoardir / 'pdr' / 'preserve' / 'data' 

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(authmeth=None):
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tmpdir.name,"simsrv"+str(srvport)+".pid")

    wrkdir = os.path.join(tmpdir.name, "svc")
    if not os.path.exists(wrkdir):
        os.mkdir(wrkdir)
    
    wpy = "python/tests/nistoar/midas/dap/fm/sim_fmflask_srv.py"
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} --enable-threads " \
          "--wsgi-file {3} --pidfile {4} --set-ph workdir={5}"
    cmd = cmd.format(os.path.join(tmpdir.name,"simsrv.log"), uwsgi_opts, srvport,
                     os.path.join(basedir, wpy), pidfile, wrkdir)
    os.system(cmd)
    time.sleep(0.5)

def stopService(authmeth=None):
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tmpdir.name,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(tmpdir.name,
                                                 "simsrv"+str(srvport)+".pid"))
    os.system(cmd)
    time.sleep(1)

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_fm.log"))
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
    tmpdir.cleanup()

class MIDASFileManagerClientTest(test.TestCase):

    def setUp(self):
        self.config = {
            'dap_app_base_url': baseurl,
            'auth': {
                'user': 'admin'
            }
        }

        self.cli = fm.FileManager(self.config)

    def tearDown(self):
        if os.path.exists(fmdatadir):
            shutil.rmtree(fmdatadir)
            os.mkdir(fmdatadir)
        if os.path.exists(jobdir):
            shutil.rmtree(jobdir)
            os.mkdir(jobdir)

    def test_test(self):
        self.assertTrue(self.cli.test())
        self.assertEqual(self.cli.space_ids(), [])

    def test_spaces(self):
        summ = self.cli.create_space("mds3:0010", 'ava1')
        self.assertEqual(summ['file_count'], -1)
        self.assertEqual(summ['created_by'], 'ava1')
        self.assertEqual(summ['users'], ['ava1'])
        self.assertEqual(summ['id'], "mds3:0010")
        self.assertEqual(self.cli.space_ids(), ["mds3:0010"])

        summ = self.cli.create_space("mds3:0020", 'ava1')
        self.assertEqual(summ['file_count'], -1)
        self.assertEqual(summ['created_by'], 'ava1')
        self.assertEqual(summ['users'], ['ava1'])
        self.assertEqual(summ['id'], "mds3:0020")
        ids = self.cli.space_ids()
        self.assertIn("mds3:0010", ids)
        self.assertIn("mds3:0020", ids)
        self.assertEqual(len(ids), 2)

        with self.assertRaises(fm.FileManagerOpConflict):
            self.cli.create_space("mds3:0020", 'ava1')

        summ = self.cli.summarize_space("mds3:0010")
        self.assertEqual(summ['file_count'], -1)
        self.assertEqual(summ['created_by'], 'ava1')
        self.assertEqual(summ['users'], ['ava1'])
        self.assertEqual(summ['id'], "mds3:0010")

        self.assertIs(self.cli.delete_space("mds3:0010"), True)
        self.assertFalse(self.cli.space_exists("mds3:0010"))
        with self.assertRaises(FileManagerResourceNotFound):
            self.cli.summarize_space("mds3:0010")

    def test_scans(self):
        spid = "mds3:0000"
        userid = 'ava1'
        summ = self.cli.create_space(spid, userid)
        self.assertTrue(self.cli.space_exists(spid))

        screp = self.cli.start_scan(spid)
        self.assertTrue(isinstance(screp, Mapping))
        self.assertIn('scan_id', screp)
        scid = screp['scan_id']
        self.assertEqual(screp['space_id'], spid)
        self.assertEqual(len(screp['contents']), 0)

        if not screp.get('is_complete'):
            time.sleep(0.5)
            screp = self.cli.get_scan(spid, scid)
            self.assertTrue(screp.get('is_complete'))

        self.assertEqual(self.cli.last_scan_id(spid), scid)

        # add some files
        uplfldr = os.path.join(fmdatadir, spid, spid)
        shutil.copytree(sampdatadir/'simplesip', uplfldr, dirs_exist_ok=True)
        os.rename(os.path.join(uplfldr, '_nerdm.json'), os.path.join(uplfldr, '#nerdm.json'))
        self.assertTrue(os.path.isdir(os.path.join(uplfldr, 'trial3')))
        self.assertTrue(os.path.isfile(os.path.join(uplfldr, '#nerdm.json')))
        
#        time.sleep(0.25)
        screp = self.cli.start_scan(spid)
        self.assertTrue(isinstance(screp, Mapping))
        self.assertIn('scan_id', screp)
        self.assertEqual(screp['space_id'], spid)
        self.assertEqual(len(screp['contents']), 5)
        self.assertNotEqual(screp['scan_id'], scid)
        scid = screp['scan_id']

        time.sleep(0.5)
        screp = self.cli.get_scan(spid, scid)
        i = 0
        while not screp.get('is_complete') and i < 5:
            time.sleep(0.25)
            i += 1
            screp = self.cli.get_scan(spid, scid)
        self.assertTrue(screp.get('is_complete'))
        self.assertTrue(isinstance(screp, Mapping))
        self.assertIn('scan_id', screp)
        self.assertEqual(screp['space_id'], spid)
        self.assertEqual(screp['scan_id'], scid)
        self.assertEqual(len(screp['contents']), 5)
        files = [f for f in screp['contents'] if f['resource_type'] == 'file']
        self.assertTrue(not any(os.path.basename(f['path']).startswith('#') for f in files))
        self.assertIn('size', files[0])
        self.assertIn('checksum', files[0])
        
    def test_perms(self):
        spid = "mds3:0000"
        userid = 'ava1'
        summ = self.cli.create_space(spid, userid)
        self.assertTrue(self.cli.space_exists(spid))

        # get initial permissions
        resp = self.cli.get_space_permissions(spid)
        self.assertEqual(resp, {"ava1": "All"})

        upd = {"gurn": 'Read'}
        resp = self.cli.set_space_permissions(spid, upd)
        self.assertEqual(resp, {"ava1": "All", "gurn": 'Read'})
        resp = self.cli.get_space_permissions(spid)
        self.assertEqual(resp, {"ava1": "All", "gurn": 'Read'})

        upd = {"ava1": "Write", "alice": "Share"}
        resp = self.cli.set_space_permissions(spid, upd)
        self.assertEqual(resp, {"ava1": "Write", "gurn": 'Read', "alice": "Share"})

        upd = {"ava1": "None"}
        resp = self.cli.set_space_permissions(spid, upd)
        self.assertEqual(resp, {"ava1": "None", "gurn": 'Read', "alice": "Share"})
        
        
        

if __name__ == "__main__":
    test.main()




