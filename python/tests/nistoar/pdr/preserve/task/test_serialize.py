import os, json, pdb, logging, tempfile, zipfile, shutil
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import serialize as ser, state as st
from nistoar.base import config

pdrdir = Path(__file__).resolve().parents[2] 
storedir = pdrdir / "distrib" / "data"
basedir = pdrdir.parents[3]

tmpdir = tempfile.TemporaryDirectory(prefix="_test_serialize.")
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

class TestNISTBagSerialization(test.TestCase):

    def setUp(self):
        self.workdir = tempfile.TemporaryDirectory(prefix="work.", dir=tmpdir.name)
        self.stagedir = Path(self.workdir.name) / "stage"
        self.stagedir.mkdir()
        self.stcfg = {
            "working_dir": self.workdir.name,
            "stage_dir": self.stagedir,
            "persist_in": self.workdir.name
        }
        self.cfg = {
            "multibag": {
                "validate": True
            }
        }
        self.ser = ser.NISTBagSerialization(self.cfg)
        self.mgr = st.JSONPreservationStateManager.for_aip(self.stcfg, "mds2-7223", str(testbag))
                                                   
        self.mgr.set_finalized_aip(str(testbag))

    def tearDown(self):
        self.workdir.cleanup()

    def test_ctor(self):
        self.assertTrue(self.ser.cfg)
        self.assertTrue(self.ser._ser)
        self.assertEqual(self.mgr.get_stage_dir(), self.stagedir)
        self.assertEqual(self.mgr.get_working_dir(), self.workdir.name)

    def test_apply_no_multibag(self):
        self.assertEqual(len([f for f in os.listdir(self.stagedir)]), 0)
        self.assertIsNone(self.mgr.get_serialized_files())

        self.ser.apply(self.mgr)
        serfiles = [f for f in os.listdir(self.stagedir)]
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip.sha256", serfiles)
        self.assertEqual(len(serfiles), 2)
                         
        mbdir = Path(self.workdir.name) / "multibag"
        self.assertFalse(mbdir.exists())

        serfiles = self.mgr.get_serialized_files()
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-0.zip"), serfiles)
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-0.zip.sha256"), serfiles)
        self.assertEqual(len(serfiles), 2)

        self.ser.clean_up(self.mgr)
        serfiles = [f for f in os.listdir(self.stagedir)]
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip.sha256", serfiles)
        self.assertEqual(len(serfiles), 2)

        self.ser.revert(self.mgr)
        self.assertEqual(len([f for f in os.listdir(self.stagedir)]), 0)
        self.assertIsNone(self.mgr.get_serialized_files())

    def test_apply_small(self):
        self.assertEqual(len([f for f in os.listdir(self.stagedir)]), 0)
        self.assertIsNone(self.mgr.get_serialized_files())

        self.ser.cfg['multibag']['max_bag_size'] = 200000000  # 200 MB
        self.ser.apply(self.mgr)
        serfiles = [f for f in os.listdir(self.stagedir)]
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip.sha256", serfiles)
        self.assertEqual(len(serfiles), 2)
                         
        mbdir = Path(self.workdir.name) / "multibag"
        self.assertFalse(mbdir.exists())

        serfiles = self.mgr.get_serialized_files()
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-0.zip"), serfiles)
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-0.zip.sha256"), serfiles)
        self.assertEqual(len(serfiles), 2)

    def test_apply_split(self):
        self.assertEqual(len([f for f in os.listdir(self.stagedir)]), 0)
        self.assertIsNone(self.mgr.get_serialized_files())

        self.ser.cfg['multibag']['max_bag_size'] = 3000  # 2 kB
        self.ser.apply(self.mgr)
        serfiles = [f for f in os.listdir(self.stagedir)]
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip.sha256", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-1.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-1.zip.sha256", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-2.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-2.zip.sha256", serfiles)
        self.assertEqual(len(serfiles), 6)
                         
        mbdir = Path(self.workdir.name) / "multibag"
        self.assertTrue(mbdir.is_dir())
        mbags = [f for f in os.listdir(mbdir)]
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0", mbags)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-1", mbags)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-2", mbags)
        self.assertEqual(len(mbags), 3)

        self.ser.clean_up(self.mgr)
        self.assertFalse(mbdir.exists())
        serfiles = [f for f in os.listdir(self.stagedir)]
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip.sha256", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-1.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-1.zip.sha256", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-2.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-2.zip.sha256", serfiles)
        self.assertEqual(len(serfiles), 6)

        serfiles = self.mgr.get_serialized_files()
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-0.zip"), serfiles)
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-0.zip.sha256"), serfiles)
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-1.zip"), serfiles)
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-1.zip.sha256"), serfiles)
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-2.zip"), serfiles)
        self.assertIn(str(self.stagedir/"mds2-7223.1_0_0.mbag0_4-2.zip.sha256"), serfiles)
        self.assertEqual(len(serfiles), 6)

        self.ser.revert(self.mgr)
        self.assertIsNone(self.mgr.get_serialized_files())
        self.assertEqual(len([f for f in os.listdir(self.stagedir)]), 0)

    def test_run(self):
        self.assertEqual(len([f for f in os.listdir(self.stagedir)]), 0)
        self.assertIsNone(self.mgr.get_serialized_files())

        self.ser.cfg['multibag']['max_bag_size'] = 3000  # 2 kB
        self.ser.run(self.mgr)
        serfiles = [f for f in os.listdir(self.stagedir)]
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-0.zip.sha256", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-1.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-1.zip.sha256", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-2.zip", serfiles)
        self.assertIn("mds2-7223.1_0_0.mbag0_4-2.zip.sha256", serfiles)
        self.assertEqual(len(serfiles), 6)
                         
        mbdir = Path(self.workdir.name) / "multibag"
        self.assertFalse(mbdir.exists())

        self.ser.run(self.mgr)

if __name__ == '__main__':
    test.main()

