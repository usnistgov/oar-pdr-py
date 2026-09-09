import json, sys, os, logging, tempfile, time
import unittest as test
from unittest.mock import patch, MagicMock
from copy import deepcopy
from pathlib import Path
from subprocess import Popen

from nistoar.midas.dap.extrev.nps1 import ExternalReviewFeedbackClient
from nistoar.midas.dap.extrev import ExternalReviewException
from nistoar.base.config import ConfigurationException

testdir = Path(__file__).parents[0]
testserver = testdir / "testfb_server.py"
tmpdir = tempfile.TemporaryDirectory(prefix="_nps1_fbcli.")

PORT = 9990
RECID = "mds3:0001"

# uwsgi_opts = "--plugin python3"
uwsgi_opts = ""
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startService(workdir):
    pidfile = os.path.join(workdir, "testfb-server.pid")
    logfile = os.path.join(workdir, "testfb-server.log")
    wpy = os.path.join(testdir, "testfb_uwsgi.py")
    cmd = f"uwsgi --daemonize {logfile} {uwsgi_opts} --http-socket :{PORT} --wsgi-file {wpy} --pidfile {pidfile} --set-ph workdir={workdir}"
    os.system(cmd)
    time.sleep(0.5)

def stopService(workdir):
    pidfile = os.path.join(workdir, "testfb-server.pid")
    cmd = f"uwsgi --stop {pidfile}"
    os.system(cmd)
    time.sleep(1)

loghdlr = None
rootlog = None
def setUpModule():
    global tmpdir
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_feedback.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    rootlog.addHandler(loghdlr)
    startService(tmpdir.name)

def tearDownModule():
    global tmpdir
    global loghdlr
    stopService(tmpdir.name)
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()
    

class TestExternalReviewFeedbackClient(test.TestCase):

    def setUp(self):
        self.cfg = {
            "service_endpoint": f"http://localhost:{PORT}/nps/leg/",
            "auth_key": "secret"
        }
        self.cli = ExternalReviewFeedbackClient(self.cfg)

    def tearDown(self):
        self.cli.send_feedback(RECID, "requested", info_url='')

    def test_get_review(self):
        rev = self.cli.get_review(RECID)
        self.assertTrue(isinstance(rev, dict))
        self.assertEqual(rev['phase'], 'requested')
        self.assertEqual(rev['system'], 'nps1')
#        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertNotIn('info_at', rev)
        self.assertNotIn('feedback', rev)

    def test_cancel(self):
        rev = self.cli.cancel(RECID)
        self.assertTrue(isinstance(rev, dict))
        self.assertEqual(rev['phase'], 'canceled')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertNotIn('info_at', rev)

        rev = self.cli.get_review(RECID)
        self.assertEqual(rev['phase'], 'canceled')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertNotIn('info_at', rev)

    def test_send_feedback_info_url(self):
        rev = self.cli.send_feedback(RECID, 'technical', info_url="https://nps.example.com/nps/1")
        self.assertEqual(rev['phase'], 'technical')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertEqual(rev['info_at'], 'https://nps.example.com/nps/1')

        rev = self.cli.get_review(RECID)
        self.assertEqual(rev['phase'], 'technical')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertEqual(rev['info_at'], 'https://nps.example.com/nps/1')
        
        rev = self.cli.send_feedback(RECID, 'division', info_url='')
        self.assertEqual(rev['phase'], 'division')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertEqual(rev['info_at'], 'https://nps.example.com/nps/1')

    def test_send_feedback_feedback(self):
        fb = {
            "reviewer": "el techno",
            "type": "comment",
            "description": "this makes me feel sad."
        }
        rev = self.cli.send_feedback(RECID, 'technical', fb)
        self.assertEqual(rev['phase'], 'technical')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertNotIn('info_at', rev)
        self.assertTrue(rev.get('feedback'), fb)

        rev = self.cli.get_review(RECID)
        self.assertEqual(rev['phase'], 'technical')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertNotIn('info_at', rev)
        self.assertTrue(rev.get('feedback'), fb)

        fb = [ fb, {
            "reviewer": "peanut gallery",
            "type": "warn",
            "description": "This will never sell."
        }]
        rev = self.cli.send_feedback(RECID, 'technical', fb)
        self.assertEqual(rev['phase'], 'technical')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertNotIn('info_at', rev)
        self.assertTrue(rev.get('feedback'), fb)

        rev = self.cli.send_feedback(RECID, 'division')
        self.assertEqual(rev['phase'], 'division')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertNotIn('info_at', rev)
        self.assertTrue(rev.get('feedback'), fb)

        rev = self.cli.send_feedback(RECID, 'ou', request_changes=True)
        self.assertEqual(rev['phase'], 'ou')
        self.assertEqual(rev['system'], 'nps1')
        self.assertEqual(rev['@id'], '1')
        self.assertTrue(rev['updated'])
        self.assertNotIn('info_at', rev)
        self.assertTrue(rev.get('feedback'), fb)

        










if __name__ == '__main__':
    test.main()


    
