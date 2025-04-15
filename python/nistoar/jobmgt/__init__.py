"""
a module for managing jobs that launch as separate, potentially long-running processes.  It provides 
the following features and capabilities:
  * Apart from the optional handling of logging messages, there are no communications between the job 
    launcher memory space and launched process
  * Job processes are readily identifiable from the process table (``ps``)
  * The management queue can prevent launching multiple jobs operating on the same data simultaneously
  * If a request comes in to run a job that is already running, it can be requeued to be relaunch after 
    the initial job completes.

This implementation is intended to manage only a handful of simultaneous jobs.  Scaling up to a large 
number is expected to require use of a scalable job management system.

## Job state data 

The state of each job is persisted as part of its queue as a dictionary with the following properties:
``queue``
    a name for the queue that the job was submitted to.
``execmodule``
    the name of the module that contains the process function that will be executed when the job is 
    launched. 
``dataid``
    a str identifier for the data that the job is to operate on.  Only one job with this ID can execute 
    at a time.
``pid``
    the process ID number that the job was launched as
``state``
    an int enumeration value indicating the current state of the job process (PENDING, RUNNING, EXITED, 
    KILLLED)
``config``
    the configuration data provided to the job (as a dictionary).  This dictionary must be JSON encodable.
``args``
    a list of job-specific arguments to pass (or was passed) to the job
``exitcode``
    if present, the status code that process, once completed, exited with.
``relaunch``
    if present and True, then the job will get requeued for execution after it completes.
``priority``
    an integer indicating the relative priority of job where 0 is normal priority.  Jobs with a higher 
    priority value will be run ahead of those with lower priorities.
``reqtime``
    the epoch time (in seconds) that the job request was queued; this is used (with priority) for requeuing 
    jobs after a system failure.
``runtime``
    present after completion of the job, it records the total runtime for the job (in seconds).  This time
    does not include the time the job was in the PENDING state.
``comptime``
    present after completion of the job, it records the epoch time (in seconds) of the job's completion.
``errors``
    a list of messages describing errors that led to a failed or killed execution
"""
import time, asyncio, threading, queue
from asyncio import subprocess as sp
from typing import List, Callable, Union
from types import ModuleType
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from logging import Logger
import logging, os, shutil, threading, json

from nistoar.pdr.utils import read_json, write_json, LockedFile

PENDING = 0
RUNNING = 1
EXITED  = 2
KILLED  = 3
_states = list(range(KILLED+1))

class Job:
    """
    a representation of a queued job.
    """

    def __init__(self, mod: Union[str|ModuleType], dataid: str, config: Mapping=None, args: List=None,
                 state_data: Mapping=None):
        """
        :param str|module mod:  the module (or name of module) that contains a ``process`` function
                                that should be run in the external process.  This ``process`` function
                                must take four arguments: the data ID, the configuration dictionary,
                                a list of arguments, and a Logger to use (can be None)
        :param str     dataid:  the identifier for the data that will be processed.  Two jobs with 
                                the same data ID cannot be run simultaneously.
        :param dict    config:  the configuration data that should be passed to the executing job.
        :param list      args:  the list of arguments that should be passed into the ``process`` 
                                function to control the behavior of the processing.
        :param dict state_data: the job state data to initialize with.  This is provided if the Job
                                instance is being reconstituted from its persistent record. 
        """
        if not state_data:
            state_data = { "state": PENDING, "priority": 0 }

        modname = mod if isinstance(mod, str) else mod.__name__
        state_data['execmodule'] = modname
        if dataid:
            state_data['dataid'] = dataid
        if config:
            state_data['config'] = config
        if args:
            state_data['args'] = args

        self.info = deepcopy(state_data)
        if not self.info.get('reqtime'):
            self.info['reqtime'] = time.time()

        self.source = None

    @classmethod
    def from_state(cls, state_data: Mapping):
        return cls(state_data.get('execmodule'), state_data.get('dataid'), state_data=state_data)

    @classmethod
    def from_state_file(cls, statefile: Union[str|Path]):
        if isinstance(statefile, str):
            statefile = Path(statefile)
        out = cls.from_state(read_json(statefile))
        out.source = statefile
        return out

    @property
    def request_time(self):
        """
        the time that this job was requested
        """
        return self.info.get('reqtime', 0)

    @property
    def priority(self):
        """
        the priority level assigned to the job
        """
        return self.info.get('priority', 0)

    @property
    def data_id(self):
        return self.info.get('dataid')

    @priority.setter
    def priority(self, p):
        self.info['priority'] = p

    @property
    def state(self):
        return self.info.get('state', PENDING)

    def update_state(self, st):
        if st not in _states:
            raise ValueError("Unrecognized state value: "+str(st))
        self.info['state'] = st

    def _cmp(self, other):
        if self.priority > other.priority:
            # higher priority go first
            return -1
        elif self.priority < other.priority:
            return +1
        elif self.request_time < other.request_time:
            return -1
        elif self.request_time > other.request_time:
            return +1
        else:
            return 0

    def __lt__(self, other):
        return self._cmp(other) < 0
    def __le__(self, other):
        return self._cmp(other) <= 0
    def __gt__(self, other):
        return self._cmp(other) > 0
    def __ge__(self, other):
        return self._cmp(other) >= 0
    def __eq__(self, other):
        return self._cmp(other) == 0
    def __ne__(self, other):
        return self._cmp(other) != 0

    def save_to(self, outfile):
        """
        persist the job state data to the given file
        """
        write_json(self.info, outfile)

    def enable_relaunch(self, onoff=True):
        """
        mark this job for relaunch if it is currently in a RUNNING state
        :param bool onoff:  if True (default), relaunching is enabled; otherwise, it is disabled
        """
        if not onoff or self.state == RUNNING:
            self.info['relaunch'] = onoff

    def mark_running(self, pid):
        self.info['pid'] = pid
        self.info['state'] = RUNNING
        for prop in ["runtime", "exitcode", "comptime", "errors"]:
            if prop in self.info:
                del self.info[prop]

    def mark_complete(self, exitcode: int, completetime: float, runtime: float=None,
                      errors: Union[str,List[str]]=None):
        """
        update the state indicating the completion of the job
        """
        self.info['exitcode'] = exitcode
        self.info['comptime'] = completetime
        if runtime is None:
            if 'runtime' in self.info:
                del self.info['runtime']
        else:
            self.info['runtime'] = runtime
        self.info['state'] = EXITED
        if errors is not None:
            if not isinstance(errors, (list, tuple,)):
                errors = (errors,)
            self.info['errors'] = list(errors)

    def mark_killed(self, completetime: float=None, runtime: float=None,
                    errors: Union[str,List[str]]=None):
        """
        update the state indicating that the job was prematurely killed before it could complete
        """
        self.mark_complete(-1, completetime, runtime, errors)
        self.info['state'] = KILLED

class FatalError(Exception):
    def __init__(self, msg, exitcode: int=10):
        super(FatalError, self).__init__(msg)
        self.exitcode = exitcode

def job_state_file(dir: Path, dataid: str):
    if isinstance(dir, str):
        dir = Path(dir)
    return dir/f"{dataid}.json"

class JobQueue:
    """
    an ordered view of a list of jobs
    """
    def __init__(self, queuename: str, queuedir: Union[Path,str], execmodule: Union[ModuleType,str],
                 config: Mapping=None, log: Logger=None):
        self.name = queuename
        if isinstance(queuedir, str):
            queuedir = Path(queuedir)
        self.qdir = queuedir
        self.mod = execmodule
        if not config:
            config = {}
        self.cfg = config
        self.relaunchable = True
        if not log:
            log = logging.getLogger(queuedir.name)
        self.log = log

        self.pq = queue.PriorityQueue()
        self._restore_queue()
        self.runner = JobRunner(self.name, self.qdir, self.pq,
                                self.log.getChild("runner"), self.cfg.get("runner"))

    def _restore_queue(self):
        lockfile = self.qdir/"_restorer.json"
        time.sleep(randint(0, 25) / 100.0)   # sleep a randome amount of time between 0 and 0.25 s
        if lockfile.is_file():
            lfdata = read_json(lockfile)
            if lfdata['pid'] != os.getpid() and self._restorer_is_running(lfdata):
                # consider this queue already recovered
                return

        with LockedFile(lockfile, 'w') as fd:
            json.dump({"pid": os.getpid(), "cmd": sys.argv[0], "args": sys.argv[1:]}, fd)
        self.log.info("Checking for zombie jobs...")
        
        for f in os.listdir(self.qdir):
            if not f.endswith(".json") or f.startswith(".") or f.startswith('_'):
                continue
            did = f[:-1*len(".json")]
            statefile = self.qdir/f
            try:
                job = Job.from_state_file(statefile)
                if job.info.get('state') == EXITED and not job.info['relaunch']:
                    statefile.unlink()
                    log.debug("Cleaned up exited job: %s", did)
                    continue
                if self.is_running(job):
                    continue

                self.submit(job.info['dataid'], job.info.get('args', []), job.priority)
                self.log.info("Resubmitting job for %s", job.info['dataid'])

            except (ValueError, KeyError):
                self.log.warning("Trouble reading job state file: %s", statefile.name)
                statefile.unlink()
                
        
    def submit(self, dataid: str, args: List[str]=None, priority: int=0, trigger=True) -> Job:
        """
        create and submit a job to process the data with a given ID
        """
        statefile = get_job_state_file(self.qdir, dataid)
        if statefile.is_file():
            job = Job.from_state_file(statefile)
            if job.info.get('state') == RUNNING:
                # don't add if it is already running
                if self.relaunchable and not job.info['relaunch']:
                    # mark it to be relaunched after it has completed
                    job.info['relaunch'] = True
                    job.save_to(statefile)
                return

        job = Job(self.mod, dataid, args)
        job.priority = priority
        job.save_to(statefile)

        # add to in-memory job queue
        self.pq.put_nowait(job)
        if trigger:
            self.run_queued()

        return job

    def get_job(self, dataid: str) -> Job:
        """
        return the job created to process the data with a given ID
        """
        statefile = get_job_state_file(self.qdir, dataid)
        if not statefile.is_file():
            return None
        return Job.from_state_file(statefile)

    def run_queued(self):
        """
        asynchronously process all jobs currently in the queue
        """
        self.runner.trigger()

    def queue_size(self):
        """
        return the number of jobs currently queued to be run
        """
        return self.pq.qsize()

    def clean(self, age=300):
        """
        remove records of jobs that have completed more than ``age`` seconds ago.  The default is 5 minutes.
        """
        pass

    def is_running(self, job: Job) -> bool:
        """
        return True if the given :py:class:`Job` is in a running state _and_ a matching, running 
        process is found.
        """
        if 'pid' not in self.info or self.state != RUNNING:
            return False
        try:
            proc = psutil.Process(job.info['pid'])
            cl = p.cmdline()
            if not any(__name__ in a for a in cl):
                return False
            idx = cl.index("-I")
            if idx+1 >= len(cl) or cl[idx+1] != job.info['dataid']:
                return False
            if "-Q" in cl and 'queue' in job.info:
                idx = cl.index("-Q")
                if idx+1 < len(cl) and cl[idx+1] != job.info['queue']:
                    return False
            return True
        except (psutil.NoSuchProcess, ValueError):
            return False

class JobRunner:
    """
    a class that executes Jobs in a queue via a dedicated thread.
    """

    def __init__(self, qname: str, jobdir: Path, jobq: queue.Queue, log: Logger=None, config: Mapping=None):
        self.qname = qname
        self.jdir = jobdir
        self.jq =jobq
        self.runthread = None
        if not log:
            log = logging.getLogger(f"JobRunner:{qname}")
        self.log = log
        if config is None:
            config = {}
        self.cfg = config
        self._thdata = threading.local()

    async def _launch_job(self, job: Job):

        pyexe = self.cfg.get("python_exe", "python")
        if not os.path.isabs(pyexe):
            which = shutil.which(pyexe)
            if which:
                pyexe = which
        cmd = f"{pyexe} -m nistoar.jobmgt.exec".split()
        cmd.extend(["-Q", self.qname, "-I", job.data_id])
        cmd.extend(["-d", str(self.jdir)])
        cmd.extend(job.info.get('args',[]))

        out = sp.DEVNULL
        if self.cfg.get('capture_logging'):
            cmd.append("-L")
            out = sp.PIPE
        if self.cfg.get('logdir'):
            logfile = os.path.join(self.cfg['logdir'], job.data_id+".log")
            cmd.extend(["-l", logfile])
        # cmd = " ".join(cmd)
        
        proc = await asyncio.create_subprocess_exec(*cmd, stdin=sp.DEVNULL, stderr=sp.STDOUT, stdout=out)

        if out is sp.PIPE:
            async def capture():
                buffer = []
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    line = line.decode('utf8')
                    if line.startswith('{'):
                        try:
                            logdata = json.loads(line)
                        except Exception:
                            buffer.append(line)
                        else:
                            if buffer:
                                self.log.warning("\n".join(buffer))
                                buffer = []

                            logrec = logging.LogRecord(logdata.get("name", self.log.name),
                                                       logdata.get("level", logging.INFO),
                                                       logdata.get("pathname", ""),
                                                       logdata.get("lineno", -1),
                                                       logdata.get("msg",""), [], None)
                            logging.getLogger(logrec.name).handle(logrec)
                    else:
                        buffer.append(line)

                if buffer:
                    self.log.warning("".join(buffer))

            await capture()
        else:
            await proc.communicate()

        return proc

    async def _drain_queue(self):
        class worker:
            def __init__(self, jq, runner):
                self.jq = jq
                self.runner = runner
                self.current_proc = None
                self.task = None
                self.processed = 0
            async def __call__(self):
                while not self.jq.empty():
                    job = self.jq.get()
                    proc = None
                    try:
                        proc = await self.runner._launch_job(job)
                        ec = await proc.wait()
                        self.processed += 1
                        if job.source and job.source.is_file():
                            job = Job.from_state_file(job.source)
                        if job.info.get('relaunch'):
                            job.enable_relaunch(False)
                            job.info['state'] == PENDING
                            job.save_to(job.source)
                            self.jq.put_nowait(job)
                    except asyncio.CancelledError:
                        self.runner.log.warning("Monitor %s task cancelled; shutting down job",
                                                self.runner.qname)
                        if proc:
                            proc.terminate()
                        raise
                    except Exception as ex:
                        self.runner.log.exception("Failed to launch %s job: %s", job.data_id, str(ex))
                    else:
                        if ec != 0:
                            self.runner.log.warning("%s job exited with status=%d", job.data_id, ec)
                            with open(job_state_file(self.runner.jdir, job.data_id)) as fd:
                                jd = json.load(fd)
                            self.runner.log.warning("\n".join(jd('errors', ['??'])))
                        else:
                            self.runner.log.debug("%s job exited successfully", job.data_id)
                return self.processed

        self._thdata.workers = []
        for i in range(self.cfg.get("maxsim", 5)):
            wrkr = worker(self.jq, self)
            self._thdata.workers.append(wrkr)
            wrkr.task = asyncio.create_task(wrkr())

        return sum(await asyncio.gather(*[w.task for w in self._thdata.workers]))

    def _run(self):
        if not self.jq.empty():
            try:
                self.log.debug("Starting queue processing with %d job%s",
                               self.jq.qsize(), "s" if self.jq.qsize() != 1 else "")
                processed = asyncio.run(self._drain_queue())
                self.log.debug("Finished processing %d job%s from queue",
                               processed, "s" if processed != 1 else "")
            except Exception as ex:
                self.log.error("Failure managing queue execution: "+str(ex))

    def trigger(self):
        """
        ensure the runner thread is running to execute jobs in the queue
        """
        if not self.jq.empty() and not self.runthread or not self.runthread.is_alive():
            t = threading.Thread(target=self._run, name=self.qname)
            t.start()
            self.runthread = t

    def is_running(self):
        """
        return True if the runner thread is running
        """
        if self.runthread:
            return self.runthread.is_alive()
        return False

    
    
