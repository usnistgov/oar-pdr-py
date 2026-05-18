import os, json, pdb, logging, tempfile, zipfile, shutil
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import validate as val, state as st
from nistoar.base import config

pdrdir = Path(__file__).resolve().parents[2] 
storedir = pdrdir / "distrib" / "data"
basedir = pdrdir.parents[3]

tmpdir = tempfile.TemporaryDirectory(prefix="_test_state.")
testbag = Path(tmpdir.name) / "mds2-7223.1_0_0.mbag0_4-0"
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_state.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)

    with zipfile.ZipFile(storedir/"mds2-7223.1_0_0.mbag0_4-0.zip") as zip:
        zip.extractall(os.path.join(tmpdir.name))
    # (Path(tmpdir.name)/"mds2-7223.1_0_0.mbag0_4-0").rename(testbag)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class TestNISTBagValidation(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="work.", dir=tmpdir.name)
        self.cfg = {
            "check_data_files": False,
        }
        self.smcfg = {
            "working_dir": self.tmpdir.name,
            "persist_in": self.tmpdir.name
        }
        self.val = val.NISTBagValidation(self.cfg)
        self.mgr = st.JSONPreservationStateManager.for_aip(self.smcfg, "mds2-7223", str(testbag))
        self.mgr.set_finalized_aip(str(testbag))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_ctor(self):
        self.assertEqual(self.val.cfg, self.cfg)
        self.assertEqual(self.mgr.aipid, "mds2-7223")
        self.assertEqual(self.mgr.get_finalized_aip(), str(testbag))

    def test_no_ops(self):
        self.val.revert(self.mgr)
        self.val.clean_up(self.mgr)

    def test_apply(self):
        self.val.apply(self.mgr)
        resfile = Path(self.tmpdir.name)/"validation_results.json"
        self.assertTrue(resfile.is_file())
        with open(resfile) as fd:
            data = json.load(fd)

        self.assertIs(data['is_valid'], True)
        self.assertIn('failed', data)
        self.assertNotIn('passed', data)
        self.assertEqual(len(data['failed']), 0)

    def test_noresults(self):
        self.mgr = st.JSONPreservationStateManager.for_aip({"persist_in": self.tmpdir.name},
                                                           "mds2-7223", str(testbag), clear_state=True)
        self.mgr.set_finalized_aip(str(testbag))
        self.val.apply(self.mgr)
        resfile = Path(self.tmpdir.name)/"validation_results.json"
        self.assertTrue(not resfile.exists())

    def test_record_passed(self):
        self.val.cfg['record_passed'] = True
        self.val.apply(self.mgr)
        
        resfile = Path(self.tmpdir.name)/"validation_results.json"
        self.assertTrue(resfile.is_file())
        with open(resfile) as fd:
            data = json.load(fd)

        self.assertIs(data['is_valid'], True)
        self.assertIn('failed', data)
        self.assertIn('passed', data)
        self.assertEqual(len(data['failed']), 0)
        

    def test_failure(self):
        bag = Path(self.tmpdir.name)/"mds2-7223"
        shutil.copytree(testbag, bag)
        self.mgr = st.JSONPreservationStateManager.for_aip(self.smcfg, "mds2-7223", 
                                                           str(bag), clear_state=True)
        self.mgr.set_finalized_aip(str(bag))
        with self.assertRaises(val.AIPValidationError):
            self.val.apply(self.mgr)
        
        resfile = Path(self.tmpdir.name)/"validation_results.json"
        self.assertTrue(resfile.is_file())
        with open(resfile) as fd:
            data = json.load(fd)

        self.assertIs(data['is_valid'], False)
        self.assertIn('failed', data)
        self.assertNotIn('passed', data)
        self.assertEqual(len(data['failed']), 1)
        



if __name__ == '__main__':
    test.main()
