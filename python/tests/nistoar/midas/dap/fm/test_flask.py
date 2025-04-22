import json, tempfile, shutil, os, sys, time, logging
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path

from nistoar.midas.dap.fm import service as fm, sim, flask as fmflask
from nistoar.midas.dap.fm.scan import base as scan, simjobexec

execdir = Path(__file__).parents[0]
datadir = execdir.parents[0] / 'data'
certpath = datadir / 'clientAdmin.crt'
keypath = datadir / 'clientAdmin.key'
capath = datadir / 'serverCa.crt'

nistoardir = execdir.parents[2]
fdatadir = nistoardir / 'pdr' / 'preserve' / 'data' 

tmpdir = tempfile.TemporaryDirectory(prefix="_test_fm_service.")
rootdir = Path(os.path.join(tmpdir.name, "fmdata"))
jobdir = Path(os.path.join(tmpdir.name, "jobqueue"))

def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_fmsvc.log"))
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
    tmpdir.cleanup()

class TestFlaskApp(test.TestCase):

    def setUp(self):
        if not os.path.exists(rootdir):
            os.mkdir(rootdir)
        if not os.path.exists(jobdir):
            os.mkdir(jobdir)
        self.config = {
            'flask': {
                'SECRET_KEY': 'XXXXXXX'
            },
            'service': {
                'nextcloud_base_url': 'http://mocknextcloud/nc',
                'webdav': {
                    'service_endpoint': 'http://mockservice/api',
                },
                'generic_api': {
                    'service_endpoint': 'http://mockservice/api',
                },
                'authentication': {
                    'client_cert_path': str(certpath),
                    'client_key_path':  str(keypath)
                },
                'local_storage_root_dir': str(rootdir),
                'admin_user': 'admin',
                'authentication': {
                    'user': 'admin',
                    'pass': 'pw'
                },
                'scan_queue': {
                    'jobdir': str(jobdir)
                }
            }
        }

        nccli = sim.SimNextcloudApi(rootdir, self.config.get('generic_api',{}))
        wdcli = sim.SimFMWebDAVClient(rootdir, self.config.get('webdav',{}))
        self.svc = fm.MIDASFileManagerService(self.config['service'], nccli=nccli, wdcli=wdcli)

        self.app = fmflask.create_app(self.config, self.svc)
        self.ctx = self.app.app_context()
        self.ctx.push()
        self.client = self.app.test_client()
        self.authhdrs = {
            'X_CLIENT_VERIFY': "SUCCESS",
            'X_CLIENT_CN': "admin"
        }

    def tearDown(self):
        self.ctx.pop()
        if os.path.exists(rootdir):
            shutil.rmtree(rootdir)
        if os.path.exists(jobdir):
            shutil.rmtree(jobdir)

    def test_ctor(self):
        self.assertIs(self.app.service, self.svc)

    def test_spaces(self):
        resp = self.client.get("/mfm1/spaces", headers=self.authhdrs)
        data = resp.json
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(isinstance(data, list))
        self.assertEqual(data, [])

        sp = self.svc.create_space_for("mds3:0000", 'ava1')
        resp = self.client.get("/mfm1/spaces", headers=self.authhdrs)
        data = resp.json
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(isinstance(data, list))
        self.assertEqual(data, [sp.id])
        
        resp = self.client.head("/mfm1/spaces/mds3:0010", headers=self.authhdrs)
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.text, '')
        resp = self.client.get("/mfm1/spaces/mds3:0010", headers=self.authhdrs)
        self.assertEqual(resp.status_code, 404)
        data = resp.json
        self.assertIn("message", data)
        self.assertIn("intent", data)

        resp = self.client.post("/mfm1/spaces",
                                json={'id':"mds3:0010", 'for_user': 'ava1'},
                                headers=self.authhdrs)
        data = resp.json
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(data['file_count'], -1)
        self.assertEqual(data['created_by'], 'ava1')
        self.assertEqual(data['users'], ['ava1'])
        self.assertEqual(data['id'], "mds3:0010")
        
        resp = self.client.head("/mfm1/spaces/mds3:0010", headers=self.authhdrs)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.text, '')

        resp = self.client.put("/mfm1/spaces/mds3:0020",
                               json={'for_user': 'ava1'},
                               headers=self.authhdrs)
        data = resp.json
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(data['file_count'], -1)
        self.assertEqual(data['created_by'], 'ava1')
        self.assertEqual(data['users'], ['ava1'])
        self.assertEqual(data['id'], "mds3:0020")
        
        resp = self.client.get("/mfm1/spaces", headers=self.authhdrs)
        data = resp.json
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(isinstance(data, list))
        self.assertIn("mds3:0010", data)
        self.assertIn("mds3:0020", data)
        self.assertIn("mds3:0000", data)
        self.assertEqual(len(data), 3)

    def _set_scan_queue(self):
        scan.set_slow_scan_queue(jobdir, resume=False)
        scan.slow_scan_queue.mod = simjobexec
        
    def test_scans(self):
        self._set_scan_queue()
        spid = "mds3:0000"
        userid = 'ava1'
        sp = self.svc.create_space_for(spid, userid)
        self.assertTrue((sp.root_dir/sp.uploads_folder).is_dir())
        self.assertTrue((sp.root_dir/sp.system_folder).is_dir())

        # start a scan, get a report
        resp = self.client.post("/mfm1/spaces/"+spid+"/scans", headers=self.authhdrs)
        rep = resp.json

        # review the empty report
        self.assertIn('scan_id', rep)
        scid = rep['scan_id']
        self.assertEqual(rep['space_id'], spid)
        self.assertEqual(len(rep['contents']), 0)

        # add some files
        shutil.copytree(fdatadir/'simplesip', sp.root_dir/sp.uploads_folder, dirs_exist_ok=True)
        self.assertTrue((sp.root_dir/sp.uploads_folder/'trial3').is_dir())
        self.assertTrue((sp.root_dir/sp.uploads_folder/'_nerdm.json').is_file())

        # rescan
        resp = self.client.post("/mfm1/spaces/"+spid+"/scans", headers=self.authhdrs)
        rep = resp.json
        scid = rep['scan_id']
        self.assertEqual(rep['space_id'], spid)
        self.assertEqual(len(rep['contents']), 4)
        files = [f for f in rep['contents'] if f['resource_type'] == 'file']
        self.assertTrue(not any(os.path.basename(f['path']).startswith('_') for f in files))
        self.assertIn('size', files[0])
        self.assertNotIn('checksum', files[0])  # not in fast result

        # now get slow_scan results
        # time.sleep(5)
        sp._get_scan_queue().runner.runthread.join(2.0)
        resp = self.client.get("/mfm1/spaces/"+spid+"/scans/"+scid, headers=self.authhdrs)
        rep = resp.json
        self.assertEqual(rep['scan_id'], scid)
        self.assertEqual(rep['space_id'], spid)
        self.assertEqual(len(rep['contents']), 4)
        files = [f for f in rep['contents'] if f['resource_type'] == 'file']
        self.assertTrue(not any(os.path.basename(f['path']).startswith('_') for f in files))
        self.assertIn('size', files[0])
        self.assertIn('checksum', files[0])

    def test_perms(self):
        
        self._set_scan_queue()
        spid = "mds3:0000"
        userid = 'ava1'
        sp = self.svc.create_space_for(spid, userid)
        self.assertTrue((sp.root_dir/sp.uploads_folder).is_dir())
        self.assertTrue((sp.root_dir/sp.system_folder).is_dir())

        # get initial permissions
        resp = self.client.get("/mfm1/spaces/"+spid+"/perms", headers=self.authhdrs)
        rep = resp.json

        self.assertEqual(rep, {"ava1": "All"})

        # add a user
        upd = {"gurn": 'Read'}
        resp = self.client.patch("/mfm1/spaces/"+spid+"/perms",
                                 json=upd, headers=self.authhdrs)
        rep = resp.json
        self.assertEqual(rep, {"ava1": "All", "gurn": "Read"})
        
        resp = self.client.get("/mfm1/spaces/"+spid+"/perms", headers=self.authhdrs)
        rep = resp.json
        self.assertEqual(rep, {"ava1": "All", "gurn": "Read"})

        upd = {"ava1": "Write", "alice": "Share"}
        resp = self.client.patch("/mfm1/spaces/"+spid+"/perms",
                                 json=upd, headers=self.authhdrs)
        rep = resp.json
        self.assertEqual(rep, {"ava1": "Write", "gurn": "Read", "alice": "Share"})

        
        
        
        
        

if __name__ == "__main__":
    test.main()

