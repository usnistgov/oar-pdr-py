import json, tempfile, shutil, os, sys, time
import unittest as test
from unittest.mock import patch, Mock
from pathlib import Path
from logging import Logger
from copy import deepcopy
from collections.abc import Mapping
import requests

from nistoar.midas.dap.fm import service as fm
from nistoar.midas.dap.fm.clients.nextcloud import NextcloudApi
from nistoar.midas.dap.fm.exceptions import *
from nistoar.base.config import ConfigurationException
from nistoar.midas.dap.fm import sim
from nistoar.midas.dap.fm.scan import base as scan, simjobexec

execdir = Path(__file__).parents[0]
datadir = execdir.parents[0] / 'data'
certpath = datadir / 'clientAdmin.crt'
keypath = datadir / 'clientAdmin.key'
capath = datadir / 'serverCa.crt'

tmpdir = tempfile.TemporaryDirectory(prefix="_test_fm_service.")
rootdir = Path(os.path.join(tmpdir.name, "fmdata"))
jobdir = Path(os.path.join(tmpdir.name, "jobqueue"))

def tearDownModule():
    tmpdir.cleanup()

class MIDASFileManagerServiceTest(test.TestCase):

    def setUp(self):
        if not os.path.exists(rootdir):
            os.mkdir(rootdir)
        if not os.path.exists(jobdir):
            os.mkdir(jobdir)
        self.config = {
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

        nccli = sim.SimNextcloudApi(rootdir, self.config.get('generic_api',{}))
        wdcli = sim.SimFMWebDAVClient(rootdir, self.config.get('webdav',{}))
        self.cli = fm.MIDASFileManagerService(self.config, nccli=nccli, wdcli=wdcli)

    def tearDown(self):
        if os.path.exists(rootdir):
            shutil.rmtree(rootdir)
        if os.path.exists(jobdir):
            shutil.rmtree(jobdir)

    def test_ctor(self):
        self.cli = fm.MIDASFileManagerService(self.config)
        self.assertEqual(self.cli._ncbase, self.config['nextcloud_base_url']+'/')
        self.assertEqual(self.cli._ncfilesurl, self.cli._ncbase+"apps/files/files")
        self.assertTrue(self.cli.nccli)
        self.assertTrue(self.cli.wdcli)
        self.assertEqual(self.cli.nccli.base_url, self.config['generic_api']['service_endpoint']+'/')
        self.assertEqual(self.cli.wdcli._wdcopts['webdav_root'], "/api")
        self.assertEqual(self.cli.nccli.authkw, {'auth': ("admin", "pw")})

        del self.config['webdav']
        del self.config['generic_api']
        self.cli = fm.MIDASFileManagerService(self.config)
        self.assertEqual(self.cli.nccli.base_url, self.cli._ncbase+"api/genapi.php/")
        self.assertEqual(self.cli.wdcli._wdcopts['webdav_root'], "/nc/remote.php/dav/files/admin")

        del self.config['admin_user']
        with self.assertRaises(ConfigurationException):
            self.cli = fm.MIDASFileManagerService(self.config)
        self.config['admin_user'] = 'admin'

        os.rmdir(rootdir)
        with self.assertRaises(ConfigurationException):
            self.cli = fm.MIDASFileManagerService(self.config)

#    @patch('requests.request')
    def test_test(self):  # , mock_request):
#        mock_resp = Mock()
#        mock_resp.status_code = 200
#        mock_request.return_value = mock_resp
        self.assertEqual(self.cli.test(), True)

    def test_ensure_user(self):
        self.assertTrue(not self.cli.nccli.is_user('ava1'))
        self.cli.ensure_user('ava1')
        self.assertTrue(self.cli.nccli.is_user('ava1'))
        self.cli.ensure_user('ava1')
        self.assertTrue(self.cli.nccli.is_user('ava1'))

    def test_space(self):
        id = "mdst:XXX1"
        self.assertTrue(not (rootdir/id).exists())
        self.assertTrue(not self.cli.space_exists(id))
        with self.assertRaises(FileManagerResourceNotFound):
            self.cli.get_space(id)

        sp = self.cli.create_space_for(id, 'ava1')
        self.assertTrue(isinstance(sp, fm.FMSpace))
        self.assertTrue(self.cli.nccli.is_user('ava1'))
        self.assertTrue((rootdir/id).is_dir())
        self.assertTrue((rootdir/id/id).is_dir())
        self.assertTrue((rootdir/id/(id+"-sys")).is_dir())
        self.assertTrue((rootdir/id/id/'#TRASH').is_dir())
        self.assertTrue((rootdir/id/id/'#HIDE').is_dir())

        self.cli.wdcli.is_directory("/".join((id, id, '#TRASH',)))
        self.assertEqual(sp.id, id)
        self.assertTrue(sp.resource_exists(id))
        self.assertTrue(sp.resource_exists(id+"-sys"))
        self.assertTrue(sp.resource_exists(id+"/#HIDE"))
        self.assertTrue(sp.resource_exists(id+"/#TRASH"))

        self.assertEqual(sp.creator, 'ava1')
        self.assertEqual(sp.get_known_users(), ['ava1'])
        sp._add_user('goober')
        self.assertEqual(sp.get_known_users(), ['ava1', 'goober'])

        sp = self.cli.get_space(id)
        self.assertTrue(isinstance(sp, fm.FMSpace))
        self.assertEqual(sp.id, id)
        self.assertTrue(sp.resource_exists(id))
        self.assertTrue(sp.resource_exists(id+"-sys"))
        self.assertTrue(sp.resource_exists(id+"/#HIDE"))
        self.assertTrue(sp.resource_exists(id+"/#TRASH"))

        self.cli.delete_space(id)
        self.assertTrue(not sp.resource_exists(id))
        self.assertTrue(not sp.resource_exists(id+"-sys"))
        self.assertTrue(not sp.resource_exists(id+"/#HIDE"))
        self.assertTrue(not sp.resource_exists(id+"/#TRASH"))
        self.assertTrue(not self.cli.space_exists(id))
        with self.assertRaises(FileManagerResourceNotFound):
            self.cli.get_space(id)
        self.assertTrue(not (rootdir/id).exists())

    def test_fmspace(self):
        id = "mdst:XXX1"
        sp = self.cli.create_space_for(id, 'ava1')
        self.assertTrue(isinstance(sp, fm.FMSpace))

        self.assertEqual(sp.id, id)
        self.assertEqual(sp.root_dir, rootdir/id)
        self.assertIsNotNone(sp._uploads_fileid)
        self.assertIsNotNone(sp.uploads_file_id)

        self.assertEqual(sp.root_davpath, id)
        self.assertEqual(sp.uploads_davpath, '/'.join((id,id,)))
        self.assertEqual(sp.hide_davpath, '/'.join((id,id,"#HIDE",)))
        self.assertEqual(sp.trash_davpath, '/'.join((id,id,"#TRASH",)))
        self.assertEqual(sp.system_davpath, '/'.join((id,id+"-sys",)))

        self.assertEqual(sp.uploads_folder, id)
        self.assertEqual(sp.system_folder, id+"-sys")

        info = sp.get_resource_info(id+"-sys")
        self.assertIn('size', info)
        self.assertIn('fileid', info)
        self.assertEqual(info['path'], f"/{id}/{id}-sys")

        self.assertEqual(sp.get_permissions_for(id, 'ava1'), fm.PERM_ALL)
        self.assertEqual(sp.get_permissions_for(id, 'gurn'), fm.PERM_NONE)
        sp.set_permissions_for(id, "gurn", fm.PERM_READ)
        self.assertEqual(sp.get_permissions_for(id, 'ava1'), fm.PERM_ALL)
        self.assertEqual(sp.get_permissions_for(id, 'gurn'), fm.PERM_READ)
        
        self.assertEqual(sp.uploads_file_id, "100")

        md = sp.summarize()
        self.assertEqual(md['uploads_dir_id'], "100")  # simulated value
        self.assertEqual(md['file_count'], -1)
        self.assertEqual(md['folder_count'], -1)
        self.assertEqual(md['created_by'], 'ava1')
        self.assertEqual(md['syncing'], 'unsynced')

    def _set_scan_queue(self):
        scan.set_slow_scan_queue(jobdir, resume=False)
        scan.slow_scan_queue.mod = simjobexec
        
    def test_fmspace_scan(self):
        self._set_scan_queue()
        id = "mdst:XXX1"
        sp = self.cli.create_space_for(id, 'ava1')
        scmd = sp.launch_scan()
        time.sleep(0.5)
        self.assertTrue(isinstance(scmd, Mapping))
        self.assertIn('scan_id', scmd)
        self.assertEqual(scmd.get('space_id'), id)
        self.assertEqual(len(scmd['contents']), 0)

        repfile = sp.root_dir/sp.system_folder/sp.scan_report_filename_for(scmd['scan_id'])
        self.assertTrue(repfile.is_file())

        scid = scmd['scan_id']
        scmd = sp.get_scan(scid)
        self.assertTrue(isinstance(scmd, Mapping))
        self.assertEqual(scmd['scan_id'], scid)
        self.assertEqual(scmd.get('space_id'), id)
        self.assertEqual(len(scmd['contents']), 0)

        md = sp.summarize()
        self.assertEqual(md['uploads_dir_id'], "100")  # simulated value
        self.assertEqual(md['file_count'], 0)
        self.assertEqual(md['folder_count'], 0)
        self.assertEqual(md['created_by'], 'ava1')
        self.assertEqual(md['syncing'], 'synced')

        sp.delete_scan(scid)
        repfile = sp.root_dir/sp.system_folder/sp.scan_report_filename_for(scid)
        self.assertTrue(not repfile.exists())

        with self.assertRaises(FileNotFoundError):
            sp.get_scan(scid)
        sp.delete_scan(scid)

    def test_format_previous_files(self):
        files = [
            { "size": 16340,  "checksum": "XXXX", "name": "README.txt" },
            { "size": 220341, "checksum": "YYYY", "name": "bagels.csv" }
        ]
        lines = []
        for f in files:
            lines.append(self.cli._format_published_file(f))
        self.assertEqual(lines[0], "#keep\tREADME.txt\t16340\tXXXX")
        self.assertEqual(lines[1], "#keep\tbagels.csv\t220341\tYYYY")

        table = self.cli._filelist2listfile(lines)
        self.assertTrue(table.startswith("# Below are"))
        self.assertTrue(table.split('\n')[-1].startswith("#keep\tbagels.csv"))
        
    def test_revive_space_for(self):
        files = [
            { "filepath": "README.txt",       "size": 16340,  "checksum": "XXXX" },
            { "filepath": "bkfst/bagels.csv", "size": 220341, "checksum": "YYYY" }
        ]
        self.cli.revive_space_for("mdsx:8888", "nstr", files)

        upldir = rootdir/"mdsx:8888"/"mdsx:8888"
        self.assertTrue(upldir.is_dir())
        prev = upldir/"#previously_published_files.tsv"
        self.assertTrue(prev.is_file())
        with open(prev) as fd:
            lines = fd.readlines()
        self.assertEqual(len([n for n in lines if "README.txt" in n]), 1)
        self.assertEqual(len([n for n in lines if "bagels.csv" in n]), 0)

        self.assertTrue((upldir/"bkfst").is_dir())
        prev = upldir/"bkfst"/"#previously_published_files.tsv"
        self.assertTrue(prev.is_file())
        with open(prev) as fd:
            lines = fd.readlines()
        self.assertEqual(len([n for n in lines if "README.txt" in n]), 0)
        self.assertEqual(len([n for n in lines if "bagels.csv" in n]), 1)
        
    
        
        
    
        

if __name__ == "__main__":
    test.main()
