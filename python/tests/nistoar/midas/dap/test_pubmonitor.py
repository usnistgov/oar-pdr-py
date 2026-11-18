import os, sys, shutil, logging, tempfile, re, time
from pathlib import Path

import unittest as test

from nistoar.pdr.publish.service import monitor, status as pstatus
from nistoar.pdr.publish.service import status as pstatus
from nistoar.midas import dbio
from nistoar.midas.dbio import status as mstatus
from nistoar.midas.dbio.inmem import InMemoryDBClientFactory
from nistoar.midas.dbio.project import ProjectService
from nistoar.midas.dap import pubmonitor
from nistoar.pdr.utils.io import read_nerd
from nistoar.pdr.utils.prov import Agent


tmpdir = tempfile.TemporaryDirectory(prefix="_test_monitor.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_pubmonitor.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
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

testdir = Path(__file__).parent
nistoardir = testdir.parents[1]
testnerdm = nistoardir / "pdr"/"publish"/"data"/"ncnrexp0.json"

class TestMIDASPublishingMonitor(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_work.")
        self.statusdir = os.path.join(self.tmpdir.name, "status")
        os.mkdir(self.statusdir)
        self.qfile = os.path.join(self.tmpdir.name, "monitorq.tsv")
        self.who = Agent("TestMIDASPublishingMonitor", Agent.AUTO, dbio.AUTOADMIN, Agent.ADMIN)
        svccfg = { "project_id_minting": { "default_shoulder": { "public": "pdr0" } } }
        self.dbfact = InMemoryDBClientFactory(svccfg)
        self.db = self.dbfact._db
        self.dapsvc = ProjectService(dbio.DAP_PROJECTS, self.dbfact, {})
        self.monitor = pubmonitor.MIDASPublishingMonitor(self.dbfact, self.statusdir, self.qfile)
        self.client = monitor.FileBasedPublishingMonitorClient(self.qfile)

    def tearDown(self):
        self.tmpdir.cleanup()

    def create_dap(self):
        nerdm = read_nerd(testnerdm)
        prec = self.dapsvc.create_record("test", nerdm)
        return prec.id

    def test_create_dap(self):
        id = self.create_dap()
        self.assertTrue(self.dapsvc.exists(id))

        stat = self.dapsvc.get_status(id)
        self.assertEqual(stat.state, mstatus.EDIT)

    def test_update_status(self):
        id = self.create_dap()
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.EDIT)
        self.monitor.update_status(id, pstatus.SUBMITTED, "submitted...")
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.ACCEPTED)
        self.assertEqual(self.dapsvc.get_status(id).message, "submitted...")
        self.monitor.update_status(id, pstatus.PROCESSING, "processing...")
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.INPRESS)
        self.assertNotEqual(self.dapsvc.get_status(id).message, "processing...")
        self.monitor.update_status(id, pstatus.PUBLISHED, "published!")
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.PUBLISHED)
        self.assertEqual(self.dapsvc.get_status(id).message, "published!")
        self.monitor.update_status(id, pstatus.FAILED, "failed!")
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.UNWELL)
        self.assertNotEqual(self.dapsvc.get_status(id).message, "published!")
        self.monitor.update_status(id, pstatus.ONHOLD, "err...")
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.INPRESS)
        self.assertNotEqual(self.dapsvc.get_status(id).message, "err...")
        self.monitor.update_status(id, pstatus.PENDING, "err...")          # ignored
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.INPRESS)
        self.assertNotEqual(self.dapsvc.get_status(id).message, "err...")
        self.monitor.update_status(id, pstatus.AWAITING, "err...")          # ignored
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.INPRESS)
        self.assertNotEqual(self.dapsvc.get_status(id).message, "err...")

    def test_monitor_once(self):
        id = self.create_dap()

        self.client.watch(id)

        # rig a status file
        statusdata = {
            'sys': {},
            'user': {
                'id': id,
                'state': pstatus.SUBMITTED,
                'siptype': 'dap',
                'authorized': [],
                'message': "Preparing to publish"
            }
        }
        pstatus.SIPStatus(id, self.statusdir, _data=statusdata).cache()

        self.monitor.update_statuses_in_queue()
        self.assertEqual(self.dapsvc.get_status(id).state, mstatus.ACCEPTED)
        self.assertEqual(self.dapsvc.get_status(id).message, "Preparing to publish")
        

        

if __name__ == '__main__':
    test.main()

    

        
    
