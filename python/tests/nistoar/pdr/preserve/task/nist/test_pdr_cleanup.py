import os, json, pdb, logging, tempfile, zipfile, shutil
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import state as st
from nistoar.pdr.preserve.task.nist import pdr
from nistoar.pdr.preserve.task import framework as fw
from nistoar.base import config

pdrdir = Path(__file__).resolve().parents[2] 
storedir = pdrdir / "distrib" / "data"
basedir = pdrdir.parents[3]

tmpdir = tempfile.TemporaryDirectory(prefix="_test_cleanup.")
testbag = Path(tmpdir.name) / "mds2-7223.1_0_0.mbag0_4-0"
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_cleanup.log"))
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

class TestPDRPreservationCleanup(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="work.", dir=tmpdir.name)
        self.workdir = Path(self.tmpdir.name)
        self.aipid = "mds8-2222"
        self.cfg = { }

        self.mbags = [f"{self.aipid}.1_0_0.mbag0_4-{str(i)}" for i in range(3)]
        self.pstate = {
            "_aipid": self.aipid,
            "_orig_aip": str(self.workdir/self.aipid),
            "_work_dir": str(self.workdir),
            "_stage_dir": str(self.workdir/"stage"),
            "_serialized_files": [str(self.workdir/"stage"/f"{f}.zip") for f in self.mbags],
            "_completed": fw.PreservationStepsAware._all_steps,
            "_message": "Publication complete"
        }
        self._init_data()
        with open(self.workdir/f"{self.aipid}_state.json", 'w') as fd:
            json.dump(self.pstate, fd, indent=2)
        self.mgr = st.JSONPreservationStateManager({"working_dir": str(self.workdir),
                                                    "persist_in":  str(self.workdir)}, self.aipid)

        self.cln = pdr.PDRPreservationCleanup(self.cfg)

    def _make_bag(self, bagdir):
        os.mkdir(bagdir)
        os.mkdir(bagdir/"data")
        os.mkdir(bagdir/"metadata")
        with open(bagdir/"metadata"/"nerdm.json", 'w') as fd:
            pass
        with open(bagdir/"preserve.log", 'w') as fd:
            pass

    def _make_file(self, filepath):
        with open(filepath, 'w') as fd:
            pass

    def _init_data(self):
        if not os.path.exists(self.pstate['_stage_dir']):
            os.mkdir(self.pstate['_stage_dir'])

        for bg in self.pstate['_serialized_files']:
            self._make_file(bg)

        if not os.path.exists(self.workdir/"multibag"):
            os.mkdir(self.workdir/"multibag")

        for mb in self.mbags:
            if not (self.workdir/"multibag"/mb).exists():
                self._make_bag(self.workdir/"multibag"/mb)

        if not (self.workdir/self.pstate["_orig_aip"]).exists():
            self._make_bag(self.workdir/self.pstate["_orig_aip"])

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_setup(self):
        self.assertTrue((self.workdir/f"{self.aipid}_state.json").is_file())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        mbags = [mbdir/f for f in os.listdir(mbdir)]
        self.assertEqual(len(mbags), 3)
        self.assertTrue(all(b.is_dir() for b in mbags))

        self.assertTrue(os.path.isdir(self.mgr.get_stage_dir()))
        self.assertTrue(all(os.path.isfile(f) for f in self.mgr.get_serialized_files()))

        self.assertEqual(self.mgr.get_serialized_files(), self.pstate['_serialized_files'])

        self.assertFalse(self.cln.revert(self.mgr))

    def test_disabled_all(self):
        self.cfg["disabled"] = True
        self.cln = pdr.PDRPreservationCleanup(self.cfg)

        self.cln.apply(self.mgr)
        self.test_setup()

    def test_cancel_serialized_bags(self):
        self.cln.clean_serialized_bags(self.mgr, True)

        files = self.pstate['_serialized_files'] 
        self.assertTrue(os.path.isdir(os.path.dirname(files[0])))
        self.assertTrue(all(not os.path.exists(f) for f in files))
        self.assertIsNone(self.mgr.get_serialized_files())

        self.assertTrue((self.workdir/self.aipid).is_dir())

        # indempodent
        self.cln.clean_serialized_bags(self.mgr, True)
        self.assertTrue((self.workdir/self.aipid).is_dir())

    def test_clean_serialized_bags(self):
        hb = self.workdir/"stage"/f"{self.aipid}.1_0_0.mbag0_4-2.zip"
        self.assertTrue(hb.is_file())
        staged = self.pstate['_serialized_files']
        staged.remove(str(hb))
        self.assertEqual(len(staged), 2)

        self.cln.clean_serialized_bags(self.mgr)
        self.assertTrue(hb.is_file())
        self.assertTrue(all(not os.path.exists(f) for f in staged))
        self.assertTrue((self.workdir/self.aipid).is_dir())
        self.assertEqual(self.mgr.get_serialized_files(), [str(hb)])

        self.assertTrue((self.workdir/self.aipid).is_dir())

        # indempodent
        self.cln.clean_serialized_bags(self.mgr)
        self.assertTrue(hb.is_file())
        self.assertTrue(all(not os.path.exists(f) for f in staged))
        self.assertTrue((self.workdir/self.aipid).is_dir())

        self.assertTrue((self.workdir/self.aipid).is_dir())

    def test_clean_multibags(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        mbags = [mbdir/f for f in os.listdir(mbdir)]
        self.assertEqual(len(mbags), 3)

        self.cln.clean_multibags(self.mgr)
        self.assertTrue(not mbdir.exists())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        # indempodent
        self.cln.clean_multibags(self.mgr)
        self.assertTrue((self.workdir/self.aipid).is_dir())

    def test_cancel_multibags(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        mbags = [mbdir/f for f in os.listdir(mbdir)]
        self.assertEqual(len(mbags), 3)

        self.cln.clean_multibags(self.mgr, True)
        self.assertTrue(not mbdir.exists())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        # indempodent
        self.cln.clean_multibags(self.mgr)
        self.assertTrue((self.workdir/self.aipid).is_dir())

    def test_clean_original_aip(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        self.cln.clean_original_aip(self.mgr)
        self.assertTrue(not (self.workdir/self.aipid).exists())
        self.assertTrue(mbdir.is_dir())

        # indempodent
        self.cln.clean_original_aip(self.mgr)
        self.assertTrue(not (self.workdir/self.aipid).exists())
        self.assertTrue(mbdir.is_dir())
        
    def test_cancel(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        self.cln.apply(self.mgr, cancel=True)

        self.assertFalse(mbdir.exists())
        self.assertTrue((self.workdir/self.aipid).is_dir())
        self.assertIsNone(self.mgr.get_serialized_files())

        
    def test_apply(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        self.cln.apply(self.mgr)

        self.assertFalse(mbdir.exists())
        self.assertFalse((self.workdir/self.aipid).exists())
        staged = self.mgr.get_serialized_files()
        self.assertIsNotNone(staged)
        self.assertEqual(len(staged), 1)
        self.assertIn(staged[0], self.pstate['_serialized_files'])

    def test_apply_disable_original(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        self.cfg["disabled"] = ["original"]
        self.cln = pdr.PDRPreservationCleanup(self.cfg)

        self.cln.apply(self.mgr)

        self.assertFalse(mbdir.exists())
        self.assertTrue((self.workdir/self.aipid).is_dir())
        staged = self.mgr.get_serialized_files()
        self.assertIsNotNone(staged)
        self.assertEqual(len(staged), 1)
        self.assertIn(staged[0], self.pstate['_serialized_files'])

    def test_apply_disable_multbag(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        self.cfg["disabled"] = ["multibag"]
        self.cln = pdr.PDRPreservationCleanup(self.cfg)

        self.cln.apply(self.mgr)

        self.assertTrue(mbdir.is_dir())
        self.assertFalse((self.workdir/self.aipid).exists())
        staged = self.mgr.get_serialized_files()
        self.assertIsNotNone(staged)
        self.assertEqual(len(staged), 1)
        self.assertIn(staged[0], self.pstate['_serialized_files'])
    
    def test_apply_disable_serialized(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        self.cfg["disabled"] = ["serialized"]
        self.cln = pdr.PDRPreservationCleanup(self.cfg)

        self.cln.apply(self.mgr)

        self.assertFalse(mbdir.exists())
        self.assertFalse((self.workdir/self.aipid).exists())
        staged = self.mgr.get_serialized_files()
        self.assertIsNotNone(staged)
        self.assertEqual(staged, self.pstate['_serialized_files'])
        self.assertTrue(all(os.path.isfile(f) for f in staged))

    def test_apply_disable2(self):
        mbdir = self.workdir/"multibag"
        self.assertTrue(mbdir.is_dir())
        self.assertTrue((self.workdir/self.aipid).is_dir())

        self.cfg["disabled"] = ["multibag", "original"]
        self.cln = pdr.PDRPreservationCleanup(self.cfg)

        self.cln.apply(self.mgr)

        self.assertTrue(mbdir.is_dir())
        self.assertTrue((self.workdir/self.aipid).is_dir())
        staged = self.mgr.get_serialized_files()
        self.assertIsNotNone(staged)
        self.assertEqual(len(staged), 1)
        self.assertIn(staged[0], self.pstate['_serialized_files'])
    
        


if __name__ == '__main__':
    test.main()
