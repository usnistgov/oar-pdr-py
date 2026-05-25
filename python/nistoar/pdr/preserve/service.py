"""
a module for asynchronously launching preservation jobs and access their status.
"""
import os, logging, re, json, time, shutil
from typing import Mapping, Iterator, List, NewType
from abc import ABC, ABCMeta, abstractmethod
from logging import Logger
from collections import namedtuple
from typing import Iterator, Tuple
from pathlib import Path
from copy import deepcopy
from datetime import datetime

import filelock

from nistoar.base.config import ConfigurationException
from nistoar.pdr.exceptions import IDNotFound, StateException
from nistoar.pdr.utils import read_json
from nistoar.pdr.publish.service.status import SIPStatus
from nistoar import jobmgt
from .task.framework import PreservationStepsAware
from .task.state import JSONPreservationStateManager
from . import PreservationException, PreservationInProgress, PreservationSystem
from .bagit import NISTBag

_pdridre = re.compile(r"^ark:/\d+/")
def _pdrid2aipid(pdrid):
    if not pdrid:
        return None
    aipid = _pdridre.sub('', pdrid)
    if '/' in aipid:
        raise PreservationException(f"{pdrid}: not a legal PDR identifier")
    return aipid

PreservationStatus = NewType("PreservationStatus", object)

class PreservationStatus(PreservationStepsAware):
    """
    a read-only description of the state of the preservation of an AIP.  

    This class is a container for several attributes accessible via the :py:meth:`get` method.  The 
    status is guaranteed to include four attributes; for this reason, they are also accessible as 
    properties of an instance: 

    ``aipid``: 
        (str) the identifier for the AIP
    ``steps``:
        (int) the steps that have been completed, encoded as an int byte array, as defined by 
        :py:class:`~nistoar.pdr.preserve.framework.PreservationStepsAware`.
    ``laststep``:
        (str) the name of the state of the latest preservation effort; this corresponds to the 
        state labels defined in :py:class:`~nistoar.pdr.preserve.framework.PreservationStepsAware`.
    ``message``:
        (str) a displayable message indicating what is currently happening in the preservation process

    All other properties are only accessibel via :py:meth:`get`.  Additional parameters that may 
    be included if the presrevation effort has been started:

    ``reqtime``:
        (float) the epoch time when the last preservation effort was requested
    ``reqdate``:
        (str) a human-readable version of ``reqtime``, formatted as a date
    ``version``:
        (str) the version of the AIP that is being (or was) preserved

    These properties will not be set if preservation has not started or is currently underway, but 
    rather only after preservation has finished:

    ``comptime``:
        (float) the epoch time when the last preservation effort finished, successful or not.  
    ``compdate``:
        (str) a human-readable version of ``comptime``, formatted as a date
    ``runtime``:
        (float) the length of time that the last preservation effort ran, in seconds.  (This is 
        essentially ``comptime - reqtime``.)
    ``exitcode``:
        (int) the exit code that the preservation process exited with (where 0 indicates successful
        completion).  If not zero, the ``message`` should indicated what went wrong.
    ``headbag``:
        (str) the name of the head preservation bag produced by successful completion of the last 
        preservation effort.  
    """

    def __init__(self, aipid: str, message: str, state: int=0, info: Mapping=None):

        if not info:
            info = {}
        self._info = dict(info)
        self._info.update({ "aipid": aipid, "message": message,
                            "steps": state, "laststep": self._label_for_step(state) })

    @property
    def aipid(self) -> str:
        "the identifier of the AIP described by this status"
        return self._info.get('aipid')

    @property
    def steps(self) -> int:
        "a byte array integer code of the preservation steps that have been completed"
        return self._info.get('steps')

    @property
    def laststep(self) -> str:
        "a displayable label indicating the last preservation step that was completed"
        return self._info.get('laststep')

    @property
    def message(self):
        """
        a displayable message indicating what most recently happened in the preservaiton 
        process.  This can be more detailed that what is given by ``laststep``.  If process 
        ended unsuccessfully, this should indicate what went wrong.
        """
        return self._info.get('message')

    def get(self, prop, default=None):
        """
        return the value of the named property or None if it is not available.
        """
        return self._info.get(prop, default)

    @property
    def successful(self):
        "True if the last preservation effort completed successfully."
        return self.get('exitcode') == 0 and self.steps == self._all_steps

    @property
    def failed(self):
        "True if the last preservation effort failed with an error"
        return self.get('exitcode', 0) != 0

    @property
    def in_progress(self):
        "True if preservation appears to be underway"
        return self.steps > self.UNSTARTED and self.steps != self._all_steps

    def items(self) -> Iterator[Tuple]:
        return self._info.items()

    def to_json(self, pretty=False):
        kw = {}
        if pretty:
            kw['indent'] = 2
        return json.dumps(self._info, **kw)

    def append_to_history(self, histfile):
        """
        append this status object to a preservation history file.  

        A history file lists machine-readable status from each attempt to preserve a particular 
        AIP.  It has a format in which each line is JSON-parseable object. The lines are
        ordered in time, so that last line describes the most recent attempt to preserve the AIP.
        Thus, this function appends the status data contained in this object to the history file
        as a line of JSON.

        :param str|Path histfile:  the history file to append to; if the file does not exist, 
                                   it will be created.
        :raises OSError:  if unable to open the history file for appending
        """
        with open(histfile, 'a') as fd:
            fd.write(self.to_json())
            fd.write('\n')

    @classmethod
    def last_from_history(cls, histfile) -> PreservationStatus:
        """
        extract and instantiate the most recent recorded preservation state from the given 
        history file

        :param str|Path histfile:  the history file to extract the state from
        :return: a ``PreservationState`` instance or ``None`` if the file is empty
        :raises OSError:  if there is an error opening or reading the history file
        :raises JSONDecodeError:  if there is an error parsing the JSON data
        """
        entries = deque(maxlen=2)
        with open(histfile) as fd:
            for line in fd:
                entries.append(line.strip())
        if entries.count() == 0:
            return None

        return cls.from_json(entries.pop())

    @classmethod
    def from_json(cls, jsondata) -> PreservationStatus:
        """
        convert JSON export of a preservation state back into a ``PreservationStatus`` object.

        :param str|dict jsondata: the exported state data.  If it is a str, it will be taken as 
                                  raw JSON and decoded; if it is a dictionary, it will be assumed 
                                  to already be decoded.  
        :raises JSONDecodeError:  if there is an error parsing the JSON data
        """
        if isinstance(jsondata, str):
            status = json.loads(jsondata)
        elif isinstance(jsondata, Mapping):
            status = jsondata
        else:
            raise TypeError("Not a str or Mapping")
        return cls(status.get('aipid'), status.get('message'), status.get('steps'), status)

    @classmethod
    def load_history(cls, histfile) -> List[PreservationStatus]:
        """
        load the states from all preservation attempts recorded in the given history file and 
        return them as a list of ``PreservationStatus`` instances. 

        :param str|Path histfile:  the history file to extract the states from
        :raises OSError:  if there is an error opening or reading the history file
        :raises JSONDecodeError:  if there is an error parsing the JSON data
        """
        out = []
        with open(histfile) as fd:
            for line in fd:
                out.append(cls.from_json(line.strip()))
        return out
        

class PreservationService(ABC, PreservationSystem, PreservationStepsAware):
    """
    a service for requesting the aynchronous preservation of AIPs and monitoring the progress of 
    the requests.  
    """

    def __init__(self, config: Mapping, log: Logger=None):
        """
        initialize the service instance

        :param dict config:  the configuration for the service
        :param Logger  log:  the log to use for messages from this service.  If not 
                             provided, a default one will be used.  
        """
        super(PreservationService, self).__init__()
        self.cfg = config
        if not log:
            log = self.getSysLogger().getChild("service")
        self.log = log

    @abstractmethod
    def active_aip_ids(self) -> Iterator[str]:
        """
        return an iterator of the identifiers that are actively being tracked by this service.  
        This includes have been submitted to this service but whose presrevation state has not 
        yet been cleaned up.  
        """
        return iter([])

    @abstractmethod
    def status_of(self, aipid) -> PreservationStatus:
        """
        return the preservation status of the AIP with the given identifier
        """
        raise NotImplemented()

    @abstractmethod
    def preserve(self, aipid, pubstat: SIPStatus=None,
                 message: str=None, startover: bool=False) -> PreservationStatus:
        """
        submit the AIP with the given identifier for preservation.  The AIP should be accessible
        from a known location that was configured into this service at construction time.  If the 
        AIP is found and is in a preservable state, the preservation will be launched asynchronously.

        :param str         aipid:  the identifier for the AIP to preserve
        :param SIPStatus pubstat:  the SIPStatus instance tracking its publication status.  If 
                                   provided, the preservation process can update the status as 
                                   appropriate.
        :param str       message:  a message to record as part of its initial status.  If not provided,
                                   a default one will be set.
        :param bool    startover:  If False (default), preservation will attempt to start over where it
                                   left off; otherwise, state from any previous attempt will be wiped 
                                   clean, and preservation will start from the initial SIP.  
        :return:  the intials status preservation request
                  :rtype: PreservationStatus
        :raises PreservationException:  if the preservation process cannot be launched due to bad 
                  inputs or bad system state.
        :raises IDNotFound:  if an SIP associated with the given
                  inputs or bad system state.
        """
        raise NotImplemented()

    @abstractmethod
    def preserve_from(self, sipdir, pubstat: SIPStatus=None,
                      message: str=None, startover: bool=False) -> PreservationStatus:
        """
        submit the SIP located at the given directory path for preservation.  If the SIP is found and 
        is in a preservable state, the preservation will be launched asynchronously.

        An SIP takes the form of an NIST preservation bag.

        :param str|Path   sipdir:  the path to the root directory of the SIP bag
        :param SIPStatus pubstat:  the SIPStatus instance tracking its publication status.  If 
                                   provided, the preservation process can update the status as 
                                   appropriate.
        :param str       message:  a message to record as part of its initial status.  If not provided,
                                   a default one will be set.
        :param bool    startover:  If False (default), preservation will attempt to start over where it
                                   left off; otherwise, state from any previous attempt will be wiped 
                                   clean, and preservation will start from the initial SIP.  
        :return:  the intials status preservation request
                  :rtype: PreservationStatus
        :raises PreservationException:  if the preservation process cannot be launched due to bad 
                  inputs or bad system state--in particular, if ``sipdir`` does not point to a proper
                  preservation bag.
        """
        raise NotImplemented()

class AIP1PreservationService(PreservationService):
    """
    a :py:class:`PreservationService` that preserves AIPs according to the aip1 convention.

    The aip1 preservation convention assumes the following:
      *  accepts as input a bag compliant with the NIST Archive BagIt profile (but which will 
         not include a POD description file among its metadata)
      *  delivers multibag serializations compliant with the first generation NIST PDR archiving
         convention (aip0)
      *  asynchronous preservation processing is launched via a separate process using a 
         :py:class:`nistoar.jobmgt.JobQueue` framework.

    By default this service will engage an preservation processing implementation based on the 
    :py:mod:`preservation task framework <nistoar.pdr.preserve.task>` with the steps defined by 
    the :py:class:`nistoar.pdr.preserve.task.nist.pdr.PreservationTaskFactory`; however, this can 
    swapped out by providing a different job execution module to the constructor.  
    
    This implementation manages state via various files persisted on disk (with locations defined by 
    configuration but by default rooted under ``pdr/preserve``), including:
      *  preservation state -- (default: _sip_``/_state.json``) tracks progress of the preservation
         process through its steps (see the :py:mod:`preservation task 
         framework<nistoar.pdr.preserve.task.framework>`
      *  job execution state -- (default: ``_jobs/``_sip_``.json``) tracks the asynchronous execution
         of the process executing the preservation. 
      *  preservation history -- (default: ``_history/``_sip_``_history.json``) a history of 
         completed preservation processes.  

    This class supports the following configuration parameters:

    ``working_dir``
         (str) _required_.  The base directory where this service can store all its stage 
         information.  Other more specific locations not specified in the configuration will 
         default to a location under this directory.  
    ``sip_dir``
         (str) _required_.  The directory to look for SIPs submitted to this service by its AIP 
         identifier (i.e. via :py:meth:`preserve`).  
    ``in_progress_dir``
         (str) _optional_.  The directory where the states of "active" preservation requests are 
         stored (default: working_dir/``preserve``).
    ``job_dir``:
         (str) _optional_.  The directory where the state of the preservation 
         :py:class:`~nistoar.jobmgt.JobQueue` is stored (default: in_progress_dir/``_jobs``).
    ``history_dir``
         (str) _optional_.  The directory where histories of preservation requests for each of the 
         are requested AIPs are stored (default: in_progressdir/``_history``).  
    ``preserve_log_dir``
         (str) _recommended_. When a preservation task completes, the log messages will be appended 
         to a per-AIP historical log file (called _aipid_``.log``) located in the directory given by
         this parameter (default: history_dir).
    ``task``
         (dict) _required by default_.  The details configuring the 
         :py:class:`~nistoar.pdr.preserve.task.frameowrk.PreservationTask` that is created when
         preservation is requested.  This dictionary configures all the steps in the task.  This 
         may not be required if an alternate job execution module is provided at construction time.
         See :py:class:`nistoar.pdr.preserve.task.nist.pdr.PreservationTaskFactory` for details.
    ``headbag_cache``
         (str) _recommended_.  a directory where head bags are cached for easier future access.  
    """

    _state_file = "_state.json"

    def __init__(self, config: Mapping, log: Logger=None, execmod=None, working_dir=None):
        """
        initialize the service 

        :param dict config:  the configuration for the service
        :param Logger  log:  the log to use for messages from this service.  If not 
                             provided, a default one will be used.  
        :param module|str execmod:  the :py:mod:`~nistoar.jobmgt` execution module, containing the 
                             required ``process()`` funtion to use.  If not provided (typically),
                             the default preservation job module 
                             (:py:mod:`nistoar.pdr.preserve.task.jobexec) will be used.
                             Either a module (dot-delimited) name or an imported module object can 
                             be used as a value. 
        """
        super(AIP1PreservationService, self).__init__(config, log)
        if not execmod:
            from . import jobexec
            execmod = jobexec

        workdir = self.cfg.get('working_dir')   # typically the "pdr" directory
        if workdir:
            workdir = Path(workdir)

        self.sipdir = self.cfg.get('sip_dir')   
        if not self.sipdir:
            self.log.warning("JobQueuePreservationService: sip_dir not specified; "
                             "ID-based submission not enabled")
        else:
            self.sipdir = Path(self.sipdir)

        self.inprogdir = self.cfg.get('in_progress_dir')   # typically "pdr/preserve"
        if not self.inprogdir:
            self.inprogdir = workdir / "preserve"
        else:
            self.inprogdir = Path(self.inprogdir)
        if not self.inprogdir:
            raise ConfigurationException("missing required config param: in_progress_dir")

        self.jobdir = None
        qname = 'preservation'
        jqcfg = self.cfg.get('job_queue', {})
        if jqcfg:
            self.jobdir = jqcfg.get('job_dir')
            qname = jqcfg.get('name', qname)

        if not self.jobdir:
            self.jobdir = self.inprogdir / '_jobs'
        else:
            self.jobdir = Path(self.jobdir)

        self.historydir = self.cfg.get('history_dir')
        if not self.historydir:
            self.historydir = self.inprogdir / '_history'
        else:
            self.historydir = Path(self.historydir)

        self.preslogdir = self.cfg.get('history_dir', self.historydir)

        self.hbagdir = self.cfg.get('headbag_cache')
        # required?

        for dir in (self.inprogdir, self.jobdir, self.historydir, self.preslogdir):
            if not dir.exists():
                try:
                    dir.mkdir()
                except Exception as ex:
                    raise ConfigurationException("%s: does not exist and cannot be created: %s" %
                                                 (dir, str(ex)))

        self.presq = jobmgt.JobQueue("preservation", self.jobdir, execmod, jqcfg, None, True)


    def _state_file_for(self, aipid):
        return self.inprogdir/aipid/self._state_file

    def _history_file_for(self, aipid):
        return self.historydir/f"{aipid}_history.json"

    def _lock_file_for(self, aipid):
        return self.inprogdir/f".{aipid}.lock"

    def active_aip_ids(self) -> Iterator[str]:
        """
        return an iterator of the identifiers that are actively being tracked by this service.  
        This includes have been submitted to this service but whose presrevation state has not 
        yet been cleaned up.  
        """
        # each active AIP has a directory under the in-progress dir with a name matching its ID
        for did in os.listdir(self.inprogdir):
            dir = self.inprogdir/did
            if not dir.is_dir() or dir.name.startswith('_') or not (dir/self._state_file).is_file():
                continue
            yield str(did)

    def status_of(self, aipid) -> PreservationStatus:
        """
        return the preservation status of the AIP with the given identifier.  
        """
        info = {}
        pstatefile = self._state_file_for(aipid)
        job = self.presq.get_job(aipid)
        if pstatefile.exists():
            return self._status_from_current_job(pstatefile, job)
        
        histf = self._history_file_for(aipid)
        if histf.is_file():
            return PreservationStatus.last_from_history(histf)

        raise IDNotFound(aipid, "AIP ID preservation status not found")

    def _status_from_current_job(self, statefile, job):
        pstate = read_json(statefile)
        aipid = pstate.get('_aipid')
        if not aipid:
            raise StateException(f"{statefile}: preservation state file is missing aipid")

        info = {}
        if pstate.get('version'):
            info['version'] = pstate['version']

        job = self.presq.get_job(aipid)
        if job:
            info['reqtime'] = job.request_time
            info['reqdate'] = datetime.fromtimestamp(job.request_time).isoformat()
            if job.state > jobmgt.RUNNING:
                info.update({ 'comptime': job.info.get('comptime'),
                              'runtime':  job.info.get('runtime'),
                              'exitcode': job.info.get('exitcode') })
                if job.info.get('pid'):
                    info['jobpid'] = job.info['pid']
                if isinstance(job.info.get('result', {}), Mapping):
                    info['headbag'] = job.info.get('result', {}).get('headbag')

        return PreservationStatus(aipid, pstate.get('_message'), pstate.get('_completed'), info)
            
    def preserve(self, aipid, pubstat: SIPStatus=None,
                 message: str=None, startover: bool=False) -> PreservationStatus:
        """
        submit the AIP with the given identifier for preservation.  The AIP should be accessible
        from a known location that was configured into this service at construction time.  If the 
        AIP is found and is in a preservable state, the preservation will be launched asynchronously.

        :param str         aipid:  the identifier for the AIP to preserve
        :param SIPStatus pubstat:  the SIPStatus instance tracking its publication status.  If 
                                   provided, the preservation process can update the status as 
                                   appropriate.
        :param str       message:  a message to record as part of its initial status.  If not provided,
                                   a default one will be set.
        :param bool    startover:  If False (default), preservation will attempt to start over where it
                                   left off; otherwise, state from any previous attempt will be wiped 
                                   clean, and preservation will start from the initial SIP.  
        :return:  the intials status preservation request
                  :rtype: PreservationStatus
        """
        if not self.sipdir:
            raise ConfigurationException("sip_dir: parameter required for ID-based submission not set")

        aipdir = self.sipdir/aipid
        if not aip.is_dir():
            raise IDNotFound(aipid, f"{aipid}: staged SIP bag not found")

        return self.preserve_from(aipdir, pubstat, message)

    def preserve_from(self, aipdir, pubstat: SIPStatus=None,
                      message: str=None, startover: bool=False) -> PreservationStatus:
        """
        submit the AIP located at the given directory path for preservation.  If the AIP is found 
        and is in a preservable state, the preservation will be launched asynchronously.

        :param str|Path   aipdir:  the path to the root directory of the AIP bag
        :param SIPStatus pubstat:  the SIPStatus instance tracking its publication status.  If 
                                   provided, the preservation process can update the status as 
                                   appropriate.
        :param str       message:  a message to record as part of its initial status.  If not provided,
                                   a default one will be set.
        :param bool    startover:  If False (default), preservation will attempt to start over where it
                                   left off; otherwise, state from any previous attempt will be wiped 
                                   clean, and preservation will start from the initial SIP.  
        :return:  the intials status preservation request
                  :rtype: PreservationStatus
        """
        aipdir = Path(aipdir)
        if not aipdir.is_dir():
            raise PreservationException(f"{str(aipdir)}: not an existing directory")

        # open the bag and get key info
        bag = NISTBag(aipdir)
        if not os.path.isdir(bag.metadata_dir):
            raise PreservationException(f"{str(aipdir)}: does not appear to be a complete SIP bag "
                                        "(missing metadata)")
        md = bag.nerd_metadata_for('')
        aipid = _pdrid2aipid(md.get('@id'))
        if not aipid:
            raise PreservationException(f"{str(aipdir)}: incomplete metadata: missing @id")
        version = md.get('version')
        if not version:
            version = "1.0"
            self.log.warning("%s: version not set in SIP; defaulting to %s", aipid, version)

        # setup PreservationStateManager so that status is available
        smcfg = deepcopy(self.cfg.get('state_manager', {}))
        workparent = self.inprogdir/aipid
        smcfg['working_dir'] = str(workparent/"work")
        smcfg['stage_dir']   = str(workparent/"stage")
        smcfg['persist_in']  = self._state_file_for(aipid)
        smcfg['sip_dir']     = self.sipdir
        try:
            for dir in (workparent, smcfg['working_dir'], smcfg['stage_dir']):
                if not os.path.isdir(dir):
                    os.mkdir(dir)
        except Exception as ex:
            raise PreservationException(f"{str(dir)}: failed to create directory: {str(ex)}")

        with filelock.FileLock(self._lock_file_for(aipid)):
            job = self.presq.get_job(aipid)
            if job and job.state < jobmgt.EXITED:
                raise PreservationInProgress(aipid)

            # TODO: save history of previous failed attempt?
            
            pstate = JSONPreservationStateManager.for_aip(smcfg, aipid, aipdir, self.log, # need SIPStatus
                                                          clear_state=startover)
            pstate.set_state_property('version', version)

            # submit job
            jcfg = {
                "status_manager": smcfg,
                "task": self.cfg.get('task', {}),
                "logfile": str(workparent/"preservation.log"),
                "history_dir": self.historydir
            }
            self.presq.submit(aipid, [self._state_file_for(aipid)], jcfg)

        self.presq.clean()

        # return status
        if isinstance(self.cfg.get('wait_to_start'), (int, float)):
            time.sleep(self.cfg['wait_to_start'])
        return self.status_of(aipid)
            
    def _notify_job_exited(self, jobfile: str):
        """
        recieve a signal from the job management system that a preservation job has exited.

        This method is intended for use by the preservation job management system to give this 
        service a chance to tidy-up after an asynchronous job has exited (whether successfully 
        or with an error).  It should not be called by clients who submit preservation requests.

        :param str jobfile:  the path to the job state file corresponding to the job that exited. 
        """
        if not os.path.isfile(jobfile):
            self.log.error("Job file %s: does not exist as a file", jobfile)
            return

        try:
            job = jobmgt.Job.from_state_file(jobfile)
            aipid = job.data_id
            workdir = self.inprogdir/aipid
            if job.successful:
                self.log.info("Job %s reporting successful completion; filing results", aipid)
            else:
                self.log.info("Job %s reporting failure (see state under %s)", aipid, workdir)

            lockfile = self._lock_file_for(aipid)
            with filelock.FileLock(lockfile):
                if job.state != jobmgt.EXITED:
                    self.log.warning("It appears that Job %s is still running or was relaunched; "+
                                     "won't clean up", aipid)
                else:
                    self._cleanup_pres_job(aipid, job)

                # if the job was successful, the preservation state file and log will have been
                # deleted; it is safe, then, to get rid of the working dir
                if not self._state_file_for(aipid).exists() and \
                   len([f for f in os.listdir(self.inprogdir/aipid)
                          if f.startswith("preservation.log")]) == 0:
                    shutil.rmtree(self.inprogdir/aipid)

            lockfile.unlink()

        except Exception as ex:
            self.log.error("Unable able to clean-up preservation job (%s): %s", jobfile, str(ex))

    def _cleanup_pres_job(self, aipid, job):
        # this assumes that we've already checked that the job is not running
        # and we have a lock on the work dir

        workdir = self.inprogdir/aipid
        preslog = workdir/"preservation.log"

        if preslog.exists():
            # save the preservation log
            fulllog = self.preslogdir/(aipid+".log")
            if not preslog.is_file():
                self.log.error("%s: does not exist as a file", preslog)
            else:
                with open(fulllog, 'a') as dest:
                    dest.write(f"---------- {aipid}: {job.info.get('reqdate')} --------------\n")
                    with open(preslog) as src:
                        for line in src:
                            dest.write(line)

        statefile = self._state_file_for(aipid)
        if statefile.exists():
            # append pres status to history
            pstat = self._status_from_current_job(statefile, job)
            pstat.append_to_history(self._history_file_for(aipid))

            if pstat.successful:
                # we can really clean up
                statefile.unlink()
                if preslog.exists():
                    preslog.unlink()

                # clean out workdir
                for subdir in "work stage".split():
                    d = workdir/subdir
                    if d.is_dir():
                        shutil.rmtree(d)

            else:
                self.log.warning("Preservation job, %s, appears to have exited before completing; "+
                                 "keeping state available for restart")

                bkupre = re.compile(r"^preservation.log.(\d+)$")
                bkups = [f for f in os.listdir(workdir) if bkupre.match(f)]
                seq = [int(bkupre.match(f).group(1)) for f in bkups]
                seq.sort()
                if len(seq) > 0:
                    seq = seq[-1]
                else:
                    seq = 1
                preslog.rename(workdir/f"preservation.log.{str(seq)}")

        
