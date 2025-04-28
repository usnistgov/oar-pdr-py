import os, sys, json, pdb, logging, tempfile, re, tempfile, shutil
from pathlib import Path
import unittest as test
import importlib

import nistoar.jobmgt as jobmgt
import nistoar.jobmgt.exec as jobexe

def import_file(path, name=None):
    if not name:
        name = os.path.splitext(os.path.basename(path))[0]
    import importlib.util as imputil
    spec = imputil.spec_from_file_location(name, path)
    out = imputil.module_from_spec(spec)
    sys.modules[name] = out
    spec.loader.exec_module(out)
    return out


loghdlr = None
rootlog = None
tmpdir  = None
def setUpModule():
    global loghdlr
    global rootlog
    global tmpdir
    tmpdir = tempfile.TemporaryDirectory(prefix="_test_jobmgt.")
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_jobmgt.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    global tmpdir
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    if tmpdir:
        tmpdir.cleanup()

class TestJobmgtExec(test.TestCase):

    def setUp(self):
        self.jobdir = Path(tmpdir.name) / "queue"
        if not self.jobdir.is_dir():
            os.mkdir(self.jobdir)

    def tearDown(self):
        if self.jobdir.exists():
            shutil.rmtree(self.jobdir)

    def test_define_options(self):
        parser = jobexe.define_options("gurn")
        self.assertTrue(parser)

    def test_main(self):
        with self.assertRaises(jobexe.FatalError):
            # missing -I
            jobexe.main("-d goob".split())
        with self.assertRaises(jobexe.FatalError):
            # missing -d
            jobexe.main("-I goob".split())
        with self.assertRaises(jobexe.FatalError):
            # goob does not exist
            jobexe.main("-d goob -I goob".split())

#        with self.assertRaises(jobexe.FatalError):
#            jobexe.main("-h".split())

        job = jobmgt.Job("nistoar.jobmgt.testproc", "pdr0:ZZZZ")
        with self.assertRaises(jobexe.FatalError):
            # job file does not exist
            jobexe.main(f"-Q test -I {job.data_id} -d {str(self.jobdir)}".split())

        jobfile = self.jobdir/(job.data_id+".json")
        job.save_to(jobfile)
        with open(jobfile) as fd:
            jdata = json.load(fd)
        self.assertEqual(jdata.get('state'), jobmgt.PENDING)
        self.assertEqual(jdata.get('execmodule'), "nistoar.jobmgt.testproc")

#        with self.assertRaises(jobexe.FatalError):
#            # module name not loadable
#            jobexe.main(f"-Q test -I {job.data_id} -d {str(self.jobdir)}".split())

        testdir = Path(__file__).parents[0]
#        mod = import_file(testdir/"test_base.py", "testjob")
        import nistoar.jobmgt.testproc as mod
        self.assertTrue(mod)
        self.assertEqual(mod.__name__, "nistoar.jobmgt.testproc")
        self.assertTrue(hasattr(mod, "process"))

        jobexe.main(f"-Q test -I {job.data_id} -d {str(self.jobdir)}".split())
        
        with open(jobfile) as fd:
            jdata = json.load(fd)
        self.assertEqual(jdata.get('state'), jobmgt.EXITED)
        self.assertEqual(jdata.get('exitcode'), 0)
        self.assertGreater(jdata.get('runtime', -1), 0)

    def notest_logout(self):
        job = jobmgt.Job("nistoar.jobmgt.testproc", "pdr0:ZZZ1")
        jobfile = self.jobdir/(job.data_id+".json")
        job.save_to(jobfile)
        jobexe.main(f"-Q test -I {job.data_id} -L -d {str(self.jobdir)}".split())
        

if __name__ == "__main__":
    test.main()

