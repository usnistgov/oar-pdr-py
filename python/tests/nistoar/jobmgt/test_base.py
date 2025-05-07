import os, sys, json, pdb, logging, tempfile, re, time, tempfile, queue, asyncio, shutil, threading
from pathlib import Path
import unittest as test
import importlib

import nistoar.jobmgt as jobmgt

# def import_file(path, name=None):
#     if not name:
#         name = os.path.splitext(os.path.basename(path))[0]
#     import importlib.util as imputil
#     spec = imputil.spec_from_file_location(name, path)
#     out = imputil.module_from_spec(spec)
#     sys.modules[name] = out
#     spec.loader.exec_module(out)
#     return out

# if "goob" not in sys.modules:
#     import_file(__file__, "goob")

# def def_process(id, config, args, log=None):
#     if not log:
#         log = logging.getLogger("goober")
#     log.info("fake processing started")
# process = def_process

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
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(name)s: %(message)s"))
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

class TestJob(test.TestCase):

    def setUp(self):
        self.cfg = {"this": "that"}
        self.args = [1, "two"]
        self.job = jobmgt.Job("goob", "pdr0-XXXX", self.cfg, self.args)

    def tearDown(self):
        for jf in [f for f in os.listdir(tmpdir.name) if f.endswith(".json")]:
            os.unlink(os.path.join(tmpdir.name, jf))

    def test_ctor(self):
        self.assertEqual(self.job.info['execmodule'], "goob")
        self.assertEqual(self.job.info['dataid'], "pdr0-XXXX")
        self.assertEqual(self.job.info['config'], self.cfg)
        self.assertEqual(self.job.info['args'], self.args)
        self.assertTrue(isinstance(self.job.info['reqtime'], float))
        self.assertGreater(self.job.info['reqtime'], 0)
        self.assertIsNone(self.job.source)

        self.assertTrue(isinstance(self.job.request_time, float))
        self.assertGreater(self.job.request_time, 0)
        self.assertEqual(self.job.priority, 0)
        self.assertEqual(self.job.data_id, "pdr0-XXXX")

    def test_update_state(self):
        self.assertEqual(self.job.state, jobmgt.PENDING)
        self.job.update_state(jobmgt.RUNNING)
        self.assertEqual(self.job.state, jobmgt.RUNNING)

        with self.assertRaises(ValueError):
            self.job.update_state(50)

    def test_cmp(self):
        other = jobmgt.Job("goob", "pdr0-YYYY", self.cfg, self.args)
        self.assertEqual(self.job.priority, 0)
        self.assertEqual(other.priority, 0)
        self.assertEqual(self.job._cmp(other), -1)
        self.assertEqual(other._cmp(self.job),  1)
        self.assertEqual(other._cmp(other),     0)

        self.assertTrue(self.job < other)
        self.assertTrue(other > self.job)
        self.assertTrue(self.job == self.job)
        self.assertTrue(self.job <= self.job)
        self.assertTrue(self.job >= self.job)
        self.assertTrue(self.job != other)

        self.job.priority = -5
        self.assertEqual(self.job.priority, -5)
        self.assertEqual(self.job._cmp(other),  1)
        self.assertEqual(other._cmp(self.job), -1)
        self.assertEqual(self.job._cmp(self.job),    0)

        q = queue.PriorityQueue()
        q.put_nowait(self.job)
        q.put_nowait(other)
        self.assertIs(q.get(), other)
        self.assertIs(q.get(), self.job)
        
        

    def test_enable_relaunch(self):
        self.assertFalse(self.job.info.get('relaunch'))
        self.assertEqual(self.job.state, jobmgt.PENDING)
        self.assertIsNone(self.job.info.get('relaunch'))
        self.job.enable_relaunch(True)
        self.assertIsNone(self.job.info.get('relaunch'))
        self.job.update_state(jobmgt.RUNNING)
        self.job.enable_relaunch(True)
        self.assertTrue(self.job.info.get('relaunch'))
        
        self.job.enable_relaunch(False)
        self.assertTrue(not self.job.info.get('relaunch'))
        
        self.job.enable_relaunch()
        self.assertTrue(self.job.info.get('relaunch'))
        
        self.job.update_state(jobmgt.KILLED)
        self.job.enable_relaunch(False)
        self.assertTrue(not self.job.info.get('relaunch'))
        
        self.job.enable_relaunch()
        self.assertTrue(not self.job.info.get('relaunch'))
    
    def test_mark_running(self):
        self.assertEqual(self.job.state, jobmgt.PENDING)
        self.assertNotIn('pid', self.job.info)

        self.job.mark_running(345)
        self.assertEqual(self.job.state, jobmgt.RUNNING)
        self.assertEqual(self.job.info['pid'], 345)

    def test_mark_complete(self):
        self.assertEqual(self.job.state, jobmgt.PENDING)
        self.assertNotIn('exitcode', self.job.info)
        self.assertNotIn('runtime', self.job.info)

        self.job.mark_complete(1, time.time(), 2.0, "tired")
        self.assertEqual(self.job.state, jobmgt.EXITED)
        self.assertEqual(self.job.info['exitcode'], 1)
        self.assertGreater(self.job.info['comptime'], 0)
        self.assertEqual(self.job.info['runtime'], 2.0)
        self.assertEqual(self.job.info['errors'], ["tired"])

        self.job.mark_complete(0, time.time())
        self.assertEqual(self.job.state, jobmgt.EXITED)
        self.assertEqual(self.job.info['exitcode'], 0)
        self.assertGreater(self.job.info['comptime'], 0)
        self.assertNotIn('runtime', self.job.info)
        
        self.job.mark_running(1001)
        self.assertNotIn('runtime', self.job.info)
        self.assertNotIn('exitcode', self.job.info)
        self.assertNotIn('errors', self.job.info)
        self.assertEqual(self.job.state, jobmgt.RUNNING)

    def test_mark_killed(self):
        self.assertEqual(self.job.state, jobmgt.PENDING)
        self.assertNotIn('exitcode', self.job.info)
        self.assertNotIn('runtime', self.job.info)

        self.job.mark_killed(time.time(), 2.0, "killed")
        self.assertEqual(self.job.state, jobmgt.KILLED)
        self.assertEqual(self.job.info['exitcode'], -1)
        self.assertGreater(self.job.info['comptime'], 0)
        self.assertEqual(self.job.info['runtime'], 2.0)
        self.assertEqual(self.job.info['errors'], ["killed"])

    def test_file(self):
        self.job.mark_running(333)
        self.assertEqual(str(jobmgt.job_state_file(tmpdir.name, "goob")),
                         os.path.join(tmpdir.name, "goob.json"))
        self.job.save_to(jobmgt.job_state_file(tmpdir.name, self.job.data_id))
        self.assertIsNotNone(self.job.source)  # was saved above

        jobfile = os.path.join(tmpdir.name, self.job.data_id+".json")
        self.assertTrue(os.path.isfile(jobfile))
        with open(jobfile) as fd:
            jobdata = json.load(fd)
        self.assertEqual(jobdata['state'], jobmgt.RUNNING)    
        self.assertEqual(jobdata['pid'], 333)
        self.assertEqual(jobdata['execmodule'], "goob")
        self.assertEqual(jobdata['dataid'], "pdr0-XXXX")
        self.assertEqual(jobdata['config'], self.cfg)
        self.assertEqual(jobdata['args'], self.args)
        self.assertTrue(isinstance(jobdata['reqtime'], float))
        self.assertGreater(jobdata['reqtime'], 0)

        job = jobmgt.Job.from_state(jobdata)
        self.assertEqual(job.info['execmodule'], "goob")
        self.assertEqual(job.info['dataid'], "pdr0-XXXX")
        self.assertEqual(job.info['config'], self.cfg)
        self.assertEqual(job.info['args'], self.args)
        self.assertEqual(job.info['pid'], 333)
        self.assertEqual(job.info['state'], jobmgt.RUNNING)    
        self.assertTrue(isinstance(job.info['reqtime'], float))
        self.assertGreater(job.info['reqtime'], 0)
        self.assertIsNone(job.source)

        job = jobmgt.Job.from_state_file(jobfile)
        self.assertEqual(job.info['execmodule'], "goob")
        self.assertEqual(job.info['dataid'], "pdr0-XXXX")
        self.assertEqual(job.info['config'], self.cfg)
        self.assertEqual(job.info['args'], self.args)
        self.assertEqual(job.info['pid'], 333)
        self.assertEqual(job.info['state'], jobmgt.RUNNING)    
        self.assertTrue(isinstance(job.info['reqtime'], float))
        self.assertGreater(job.info['reqtime'], 0)
        self.assertEqual(str(job.source), jobfile)


class TestJobRunner(test.TestCase):

    def setUp(self):
        global tmpdir
        self.jobdir = Path(tmpdir.name) / "queue"
        os.mkdir(self.jobdir)
        self.job1 = jobmgt.Job("nistoar.jobmgt.testproc", "pdr0-XXXX")
        self.job2 = jobmgt.Job("nistoar.jobmgt.testproc", "pdr0-YYYY")
        self.queue = queue.PriorityQueue()
        self.queue.put_nowait(self.job1)
        self.queue.put_nowait(self.job2)
        self.runner = jobmgt.JobRunner("test", self.jobdir, self.queue)

    def tearDown(self):
        if self.jobdir.exists():
            shutil.rmtree(str(self.jobdir))

    def test_ctor(self):
        self.assertEqual(self.runner.qname, "test")
        self.assertEqual(self.runner.jdir, self.jobdir)
        self.assertTrue(self.runner.jq)
        self.assertIsNone(self.runner.runthread)
        self.assertEqual(self.runner.cfg, {})
        self.assertFalse(self.runner.is_running())

    def test_launch_job(self):
        jobfile = self.jobdir/(self.job1.data_id+".json")
        self.job1.save_to(jobfile)
        self.runner.cfg['capture_logging'] = True
        p = asyncio.run(self.runner._launch_job(self.job1), debug=True)

        self.assertTrue(p)
        self.assertGreater(p.pid, 0)
        self.assertEqual(p.returncode, 0)

        with open(jobfile) as fd:
            jdata = json.load(fd)
        self.assertEqual(jdata['pid'], p.pid)
        self.assertEqual(jdata['exitcode'], p.returncode)
        self.assertEqual(jdata['state'], jobmgt.EXITED)

    def test_drain_queue(self):
        self.job1.save_to(self.jobdir/(self.job1.data_id+".json"))
        self.job2.save_to(self.jobdir/(self.job2.data_id+".json"))
        self.assertEqual(self.queue.qsize(), 2)

        self.runner.cfg['capture_logging'] = True
        processed = asyncio.run(self.runner._drain_queue())

        self.assertEqual(processed, 2)
        self.assertEqual(self.queue.qsize(), 0)

        self.assertIn('workers', dir(self.runner._thdata))
        self.assertTrue(isinstance(self.runner._thdata.workers, list))
        self.assertEqual(len(self.runner._thdata.workers), 5)

    def test_thread_run(self):
        self.job1.save_to(self.jobdir/(self.job1.data_id+".json"))
        self.job2.save_to(self.jobdir/(self.job2.data_id+".json"))
        self.assertEqual(self.queue.qsize(), 2)
        self.assertEqual(self.runner.processed, 0)

        self.runner.cfg['capture_logging'] = True
        self.runner._run()
        self.assertIn('workers', dir(self.runner._thdata))
        self.assertTrue(isinstance(self.runner._thdata.workers, list))
        self.assertEqual(len(self.runner._thdata.workers), 5)
        self.assertEqual(self.runner.processed, 2)

    def test_trigger(self):
        self.job1.save_to(self.jobdir/(self.job1.data_id+".json"))
        self.job2.save_to(self.jobdir/(self.job2.data_id+".json"))
        self.assertEqual(self.queue.qsize(), 2)
        self.assertEqual(self.runner.processed, 0)

        self.runner.cfg['capture_logging'] = True
        self.runner.trigger()
        self.runner.runthread.join(5)
        self.assertEqual(self.queue.qsize(), 0)
        self.assertEqual(self.runner.processed, 2)
        
    def test_run_logfile(self):
        self.job1.save_to(self.jobdir/(self.job1.data_id+".json"))
        self.job2.save_to(self.jobdir/(self.job2.data_id+".json"))
        self.assertEqual(self.queue.qsize(), 2)

        self.runner.cfg['logdir'] = str(self.jobdir)
        self.runner._run()
        self.assertTrue((self.jobdir/(self.job1.data_id+".log")).is_file())
        self.assertTrue((self.jobdir/(self.job2.data_id+".log")).is_file())
        with open(self.jobdir/(self.job1.data_id+".log")) as fd:
            lines = fd.read()
        self.assertIn("fake processing started", lines)

class TestJobQueue(test.TestCase):

    def setUp(self):
        global tmpdir
        self.jobdir = Path(tmpdir.name) / "queue"
        os.mkdir(self.jobdir)

        self.jobq = jobmgt.JobQueue("test", self.jobdir, "nistoar.jobmgt.testproc")

    def tearDown(self):
        if self.jobdir.exists():
            shutil.rmtree(str(self.jobdir))

    def test_ctor(self):
        self.assertEqual(self.jobq.name, "test")
        self.assertEqual(self.jobq.qdir, self.jobdir)
        self.assertEqual(self.jobq.mod, "nistoar.jobmgt.testproc")
        self.assertEqual(self.jobq.pq.qsize(), 0)
        self.assertEqual(self.jobq.processed, 0)
        self.assertEqual(self.jobq.pending, 0)

    def test_submit(self):
        self.assertEqual(self.jobq.processed, 0)
        self.assertEqual(self.jobq.pending, 0)
        self.assertTrue(not (self.jobdir/"pdr0:XX01.json").exists())
        self.assertTrue(not (self.jobdir/"pdr0:XX02.json").exists())

        self.jobq.submit("pdr0:XX01", trigger=False)
        self.assertEqual(self.jobq.processed, 0)
        self.assertEqual(self.jobq.pending, 1)
        self.jobq.submit("pdr0:XX02", priority=1, trigger=False)
        self.assertEqual(self.jobq.processed, 0)
        self.assertEqual(self.jobq.pending, 2)

        self.assertTrue((self.jobdir/"pdr0:XX01.json").exists())
        self.assertTrue((self.jobdir/"pdr0:XX02.json").exists())

        job = self.jobq.get_job("pdr0:XX01")
        self.assertEqual(job.data_id, "pdr0:XX01")
        self.assertEqual(job.priority, 0)
        self.assertEqual(job.state, jobmgt.PENDING)
        self.assertEqual(job.info.get('execmodule'), "nistoar.jobmgt.testproc")

        # test not-relaunchability
        self.jobq.submit("pdr0:XX02", priority=1, trigger=False)
        self.assertEqual(self.jobq.processed, 0)
        self.assertEqual(self.jobq.pending, 2)
        job = jobmgt.Job.from_state_file(self.jobdir/"pdr0:XX02.json")
        self.assertTrue(not job.info.get('relaunch'))

        # will be relaunched if with different args
        self.jobq.submit("pdr0:XX02", args=["1"], priority=1, trigger=False)
        self.assertEqual(self.jobq.processed, 0)
        self.assertEqual(self.jobq.pending, 2)
        job = jobmgt.Job.from_state_file(self.jobdir/"pdr0:XX02.json")
        self.assertTrue(job.info.get('relaunch'))
        self.assertTrue(not job.info['relaunch'].get('relaunch'))

        self.jobq.run_queued()
        self.jobq.runner.runthread.join(5)
        # time.sleep(0.3)
        self.assertEqual(self.jobq.pending, 0)
        self.assertEqual(self.jobq.processed, 3)

        self.assertTrue((self.jobdir/"pdr0:XX01.json").exists())
        self.assertTrue((self.jobdir/"pdr0:XX02.json").exists())
        job = jobmgt.Job.from_state_file(self.jobdir/"pdr0:XX02.json")
        self.assertTrue(not job.info.get('relaunch'))

        self.jobq.clean(0)
        self.assertTrue(not (self.jobdir/"pdr0:XX01.json").exists())
        self.assertTrue(not (self.jobdir/"pdr0:XX02.json").exists())

    def test_restore_queue(self):
        self.assertEqual(self.jobq.processed, 0)
        self.assertEqual(self.jobq.pending, 0)

        self.jobq.runner.cfg['capture_logging'] = True
        
        job1 = jobmgt.Job("nistoar.jobmgt.testproc", "pdr0-XXXX")
        job1.save_to(jobmgt.job_state_file(self.jobdir, "pdr0-XXXX"))
        job2 = jobmgt.Job("nistoar.jobmgt.testproc", "pdr0-YYYY")
        job2.save_to(jobmgt.job_state_file(self.jobdir, "pdr0-YYYY"))
        self.jobq.submit("pdr0:XX02")

        self.jobq.runner.runthread.join()
        self.assertTrue((self.jobdir/"pdr0:XX02.json").is_file())
        self.assertEqual(self.jobq.processed, 1)
        self.assertEqual(self.jobq.pending, 0)

        self.jobq._restore_queue(False)
        self.assertEqual(self.jobq.processed, 1)
        self.assertEqual(self.jobq.pending, 2)

        self.jobq.run_queued()
        self.jobq.runner.runthread.join()
        self.assertEqual(self.jobq.processed, 3)
        self.assertEqual(self.jobq.pending, 0)

        

if __name__ == "__main__":
    test.main()
