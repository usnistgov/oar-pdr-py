import os, json, pdb, logging, tempfile
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import state as st
from nistoar.pdr.preserve import PreservationStateError
from nistoar.base import config
from nistoar.pdr.publish.service import status

tmpdir = tempfile.TemporaryDirectory(prefix="_test_state.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_state.log"))
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

class TestPreservationStateManager(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_test_state.", dir=tmpdir.name)
        self.cfg = {
            "stage_dir":  "stage",
            "persist_in": self.tmpdir.name
        }
        self.statefile = os.path.join(self.tmpdir.name, "goober_state.json")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_ctor(self):
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")

        # test the common setUp
        self.assertTrue(self.state)
        self.assertTrue(self.state._keepfresh)
        self.assertIsNone(self.state._pubstat)
        self.assertEqual(self.state.aipid, "goober")
        self.assertEqual(str(self.state._cachefile), self.statefile)
        self.assertTrue(os.path.isfile(self.state._cachefile))
        self.assertEqual(self.state._data, {
            "_aipid": "goober",
            "_orig_sip": "goober/bag",
            "_stage_dir": "stage",
            "_work_dir": None,
            "_completed": 0,
            "_message": st.UNSTARTED_PROGRESS
        })
        self.assertEqual(self.state.get_sip(), "goober/bag")
        self.assertEqual(self.state.get_stage_dir(), "stage")
        self.assertIsNone(self.state.get_working_dir())
        self.assertEqual(self.state.steps_completed, 0)
        with open(self.state._cachefile) as fd:
            self.assertEqual(json.load(fd), self.state._data)
        
        # test reload of state by just providing state file
        self.state = st.JSONPreservationStateManager.from_file(self.state._cachefile)
        self.assertTrue(self.state._keepfresh)
        self.assertEqual(self.state.aipid, "goober")
        self.assertEqual(self.state._data, {
            "_aipid": "goober",
            "_orig_sip": "goober/bag",
            "_stage_dir": "stage",
            "_work_dir": None,
            "_completed": 0,
            "_message": st.UNSTARTED_PROGRESS
        })
        self.assertEqual(self.state.get_sip(), "goober/bag")
        self.assertEqual(self.state.get_stage_dir(), "stage")
        self.assertEqual(self.state.steps_completed, 0)
        with open(self.state._cachefile) as fd:
            self.assertEqual(json.load(fd), self.state._data)

        # test reload of existing state from configured state file
        self.state.mark_completed(self.state.FINALIZED, "call it finalized for testing")
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")
        self.assertTrue(self.state)
        self.assertTrue(self.state._keepfresh)
        self.assertIsNone(self.state._pubstat)
        self.assertEqual(self.state.aipid, "goober")
        self.assertEqual(str(self.state._cachefile), self.statefile)
        self.assertTrue(os.path.isfile(self.state._cachefile))
        self.assertEqual(self.state._data, {
            "_aipid": "goober",
            "_orig_sip": "goober/bag",
            "_stage_dir": "stage",
            "_work_dir": None,
            "_completed": self.state.FINALIZED,
            "_message": self.state.message
        })
        self.assertIn("finalized", self.state.message)
        self.assertEqual(self.state.get_sip(), "goober/bag")
        self.assertEqual(self.state.get_stage_dir(), "stage")
        self.assertIsNone(self.state.get_working_dir())
        self.assertEqual(self.state.steps_completed, self.state.FINALIZED)
        with open(self.state._cachefile) as fd:
            self.assertEqual(json.load(fd), self.state._data)

        # test use of clear_state
        self.state._data["_completed"] = 3
        self.state._data["foo"] = "bar"
        self.state._cache()
        self.state._load()
        self.assertEqual(self.state.steps_completed, 3)
        self.assertEqual(self.state._data["foo"], "bar")
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")
        self.assertEqual(self.state.steps_completed, 3)
        self.assertEqual(self.state._data["foo"], "bar")
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag",
                                                             clear_state=True)
        self.assertEqual(self.state.steps_completed, 0)
        self.assertNotIn("foo", self.state._data)

        # test some error conditions
        with self.assertRaises(config.ConfigurationException):
            st.JSONPreservationStateManager.for_aip({}, "goober", "goober/bag")
        with self.assertRaises(PreservationStateError):
            st.JSONPreservationStateManager.from_file("goober.json")

    def test_get_working_dir(self):
        self.cfg['working_dir'] = "work"
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")
        self.assertEqual(self.state.get_working_dir(), "work")

    def test_mark_completed(self):
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")
        self.assertEqual(self.state.steps_completed, 0)
        self.assertEqual(self.state.completed, "unstarted")

        self.state.mark_completed(self.state.SERIALIZED)
        self.assertEqual(self.state.steps_completed, 8)
        self.assertEqual(self.state.completed, "serialized")

        self.state.mark_completed(self.state.FINALIZED|self.state.VALIDATED)
        self.assertEqual(self.state.steps_completed, 14)
        self.assertEqual(self.state.completed, "serialized")

        self.state.mark_completed(self.state.SUBMITTED)
        self.assertEqual(self.state.steps_completed, 30)
        self.assertEqual(self.state.completed, "submitted to archive")

    def test_finalized_aip(self):
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")
        self.assertIsNone(self.state.get_finalized_aip())
        
        self.state.set_finalized_aip(self.state.get_sip())
        self.assertEqual(self.state.get_finalized_aip(), "goober/bag")

        self.state.set_finalized_aip("goober/bag.1_0_0-0")
        self.assertEqual(self.state.get_finalized_aip(), "goober/bag.1_0_0-0")

        with open(self.state._cachefile) as fd:
            self.assertEqual(json.load(fd)["_finalized_aip"], "goober/bag.1_0_0-0")
        self.state = st.JSONPreservationStateManager.from_file(self.state._cachefile)
        self.assertEqual(self.state.get_finalized_aip(), "goober/bag.1_0_0-0")

    def test_serialized_aip_files(self):
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")
        self.assertIsNone(self.state.get_serialized_files())

        with self.assertRaises(ValueError):
            self.state.set_serialized_files([])
        with self.assertRaises(TypeError):
            self.state.set_serialized_files("stage/goover-1_0_0-1.zip")
        with self.assertRaises(TypeError):
            self.state.set_serialized_files(["stage/goover-1_0_0-1.zip", None])

        self.state.set_serialized_files(["stage/goober-1_0_0-1.zip"])
        self.assertEqual(self.state.get_serialized_files(),
                         ["stage/goober-1_0_0-1.zip"])

        self.state.set_serialized_files(["foo/goober-1.zip", "bar/goober-2.zip"])
        self.assertEqual(self.state.get_serialized_files(),
                         ["foo/goober-1.zip", "bar/goober-2.zip"])

        self.state.set_serialized_files(("foo/goober-1.zip", "bar/goober-2.zip"))
        self.assertEqual(self.state.get_serialized_files(),
                         ["foo/goober-1.zip", "bar/goober-2.zip"])

        with open(self.state._cachefile) as fd:
            self.assertEqual(json.load(fd)["_serialized_files"],
                             ["foo/goober-1.zip", "bar/goober-2.zip"])
        self.state = st.JSONPreservationStateManager.from_file(self.state._cachefile)
        self.assertEqual(self.state.get_serialized_files(),
                         ["foo/goober-1.zip", "bar/goober-2.zip"])
        
        self.state.set_serialized_files(None)
        self.assertIsNone(self.state.get_serialized_files())
        
    def test_state_property(self):
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")
        self.assertIsNone(self.state.get_state_property("foo"))
        self.assertEqual(self.state.get_state_property("foo", "bar"), "bar")

        self.state.set_state_property("foo", "gurn")
        self.assertEqual(self.state.get_state_property("foo", "bar"), "gurn")
        self.assertEqual(self.state.get_state_property("foo"), "gurn")

        self.state.set_state_property("gary", "indiana")
        self.state.set_state_property("studio", 54)
        self.state.set_state_property("config", {"foo": "bar"})
        self.assertEqual(self.state.get_state_property("foo"), "gurn")
        self.assertEqual(self.state.get_state_property("gary"), "indiana")
        self.assertEqual(self.state.get_state_property("studio"), 54)
        self.assertEqual(self.state.get_state_property("config"), {"foo": "bar"})
        with open(self.state._cachefile) as fd:
            data = json.load(fd)
            self.assertEqual(data["foo"], "gurn")
            self.assertEqual(data["gary"], "indiana")
            self.assertEqual(data["studio"], 54)
            self.assertEqual(data["config"], {"foo": "bar"})
            self.assertEqual(data["_aipid"], "goober")
            
        with self.assertRaises(TypeError):
            self.state.set_state_property("can't", Exception("ya"))
        with self.assertRaises(TypeError):
            self.state.set_state_property("can't", Exception)
        
    def test_record_progress(self):
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")
        self.assertEqual(self.state.message, st.UNSTARTED_PROGRESS)

        self.state.record_progress("just twiddling")
        self.assertEqual(self.state.message, "just twiddling")
        self.state.record_progress("waiting to get started")
        self.assertEqual(self.state.message, "waiting to get started")

        self.state.mark_completed(self.state.FINALIZED, "validating finalized bag")
        self.assertEqual(self.state.message, "validating finalized bag")

    def test_record_progress_with_sipstatus(self):
        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag")

        sipcache = Path(self.tmpdir.name) / "sipstatus"
        sipcache.mkdir(exist_ok=True)
        sipstatf = sipcache / "goober.json"
        if sipstatf.exists():
            sipstatf.unlink()

        sipstat = status.SIPStatus("goober", {'cachedir': str(sipcache)})
        sipstat.update(status.SUBMITTED, "submitted for preservation")

        self.state = st.JSONPreservationStateManager.for_aip(self.cfg, "goober", "goober/bag",
                                                             pubstat=sipstat)
        self.assertEqual(self.state.message, st.UNSTARTED_PROGRESS)

        self.state.record_progress("just twiddling")
        self.assertEqual(self.state.message, "just twiddling")
        self.assertEqual(sipstat.message, "just twiddling")
        self.state.record_progress("waiting to get started")
        self.assertEqual(self.state.message, "waiting to get started")
        self.assertEqual(sipstat.message, "waiting to get started")

        self.state.mark_completed(self.state.FINALIZED, "validating finalized bag")
        self.assertEqual(self.state.message, "validating finalized bag")
        self.assertEqual(sipstat.message, "validating finalized bag")
        
        self.state.mark_completed(self.state.PUBLISHED, "preservation completed successfully")
        self.assertEqual(self.state.message, "preservation completed successfully")
        self.assertEqual(sipstat.message, "Submission was successfully published")

        
if __name__ == '__main__':
    test.main()




        
