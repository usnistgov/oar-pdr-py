"""
a module for asynchronously launching preservation jobs and access their status.
"""
import logging
from typing import Mapping, Iterator
from abc import ABCMeta, abstractmethod
from logging import Logger
from collections import namedtuple

from nistoar.base.config import ConfigurationException
from nistoar.pdr.exceptions import IDNotFound
from nistoar.pdr.utils import read_json
from nistoar import jobmgt
from .task.framework import PreservationStepsAware
from . import PreservationException, PreservationSystem

_pdridre = re.compile(r"^ark:/\d+/")
def _pdrid2aipid(pdrid):
    if not pdrid:
        return None
    aipid = _pdridre.sub('', pdrid)
    if '/' in aipid:
        raise PreservationException(f"{pdrid}: not a legal PDR identifier")
    return aipid

class PreservationStatus(PreservationStepsAware):
    """
    a read-only description of the state of the preservation of an AIP.  

    The status is container for several attributes accessible via the :py:meth:`get` method.  The 
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
    rather only after preservatin has finished:

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

    def get(self, prop):
        """
        return the value of the named property or None if it is not available.
        """
        return self._info.get(prop)

    @property
    def successful(self):
        "True if the last preservation effort completed successfully."
        return self.get('exitcode') == 0 and self.steps == self._all_steps

    @property
    def failed(self):
        "True if the last preservation effort failed with an error"
        return self.get('exitcode') != 0

    @property
    def in_progress(self):
        "True if preservation appears to be underway"
        return self.steps > self.UNSTARTED

class PreservationService(PreservationSystem, PreservationStepsAware, meta=ABCMeta):
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
        PreservationSystem.__init__(self)
        self.cfg = config
        if not log:
            log = logging.getLogger("PreservationService")
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
    
    This class supports the following configuration parameters:

    """

    self._state_file = "_state.json"

    def __init__(self, config: Mapping, log: Logger=None, execmod=None, working_dir=None):
        """
        initialize the service 

        :param dict config:  the configuration for the service
        :param Logger  log:  the log to use for messages from this service.  If not 
                             provided, a default one will be used.  
        :param module|str execmod:  the :py:mod:`~nistoar.jobmgt` execution module, containing the 
                             required ``process()`` funtion to use.  If not provided (typically),
                             use the default preservation job module 
                             (:py:mod:`nistoar.pdr.preserve.framework.jobexec) will be used.
                             Either a module (dot-delimited) name or an imported module object can 
                             be used as a value. 
        """
        super(JobQueuePreservationService, self).__init__(config, log)
        if not execmod:
            from .framework import jobexec
            execmod = jobexec

        workdir = self.cfg.get('working_dir')   # typically the "pdr" parent directory
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
            self.inprogdir = self.workdir / "preserve"
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

        self.presq = JobQueue(name, self.jobdir, execmod, jqcfg, None, True)

        self.historydir = self.cfg.get('logdir')
        if not self.historydir:
            self.historydir = self.inprogdir / '_history'
        else:
            self.historydir = Path(self.historydir)

        self.hbagdir = self.cfg.get('headbag_cache')
        # required?

        for dir in (self.inprogdir, self.jobdir, self.historydir):
            if not dir.exists():
                try:
                    dir.mkdir()
                except Exception as ex:
                    raise ConfigurationException("%s: does not exist and cannot be created: %s" %
                                                 (dir, str(ex)))


    def _state_file_for(self, aipid):
        return self.inprogdir/aipid/self._state_file

    def _history_file_for(self, aipid):
        return self.historydir/f"{aipid}_history.json"

    def _open_presstatemgr(self, aipid, aipdir=None):
        iddir = self.inprogdir/aipid
        if not iddir.is_dir():
            return None
        return JSONPreservationStateManager(self.cfg.get('state_manager', {}), aipid, 
                                            persistin=iddir/self._state_file, log=self.log)

    def active_aip_ids(self) -> Iterator[str]:
        """
        return an iterator of the identifiers that are actively being tracked by this service.  
        This includes have been submitted to this service but whose presrevation state has not 
        yet been cleaned up.  
        """
        # each active AIP has a directory under the in-progress dir with a name matching its ID
        for did in os.listdir(self.inprogdir):
            dir = Path(did)
            if not dir.is_dir() or (dir/self._state_file).is_file():
                continue
            yield did

    def status_of(self, aipid) -> PreservationStatus:
        """
        return the preservation status of the AIP with the given identifier.  
        """
        info = {}
        pstatefile = self._state_file_for(aipid)
        if statefile.exists():
            pstate = read_json(statefile)
            job = self.jobq.get_job(aipid)
            if pstate.get('version'):
                info['version'] = pstate['version']
            if job:
                info['reqtime'] = job.request_time
                info['reqdate'] = datatime.fromtimestamp(job.request_time).isformat()
                if job.state > jobmgt.RUNNING:
                    info.update({'comptime', job.info.get('comptime'),
                                 'runtime',  job.info.get('runtime'),
                                 'exitcode',  job.info.get('exitcode')})
                # TODO: need headbag
            return PreservationState(aipid, pstate.get('message'), pstate.get('state'), info)

        histf = self._history_file_for(aipid)
        if histf.is_file():
            try:
                history = read_json(histf)
                if not isinstance(history, list):
                    raise ValueError("not a list")
            except ValueError as ex:
                self.log.error("%s: bad json format: %s", histf, str(ex))
            else:
                history = history[-1]
                msg = history.get('message', '')
                state = history.get('state', 0)
                job = history.get('job', {})
                info = {}
                if history.get('version'):
                    info['version'] = history['version']
                for prop in "reqtime comptime runtime exitcode".split():
                    if prop in job:
                        info[prop] = job[prop]
                # TODO: need version and headbag
                return PreservationState(aipid, msg, info)

        raise IDNotFound(aipid, "AIP ID preservation status not found")
            
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
        workparent = str(self.inprogdir/aipid)
        smcfg['working_dir'] = str(workparent/"work")
        smcfg['stage_dir']   = str(workparent/"stage")
        try:
            for dir in (workparent, smcfg['working_dir'], smcfg['stage_dir']):
                if not os.path.isdir(dir):
                    os.mkdir(dir)
        except Exception as ex:
            raise PreservationException(f"{str(dir)}: failed to create directory: {str(ex)}")

        pstate = JSONPreservationStateManager(smcfg, aipid, aipdir, self.log, # need SIPStatus
                                              persistin=workparent/"_state.json")
        pstate._data['version'] = version
        pstate._cache()

        # submit job
        jcfg = {
            "status_manager": smcfg,
            "process": self.cfg.get('process', {})
            "logfile": str(workparent/"preservation.log")
        }
        self.jobq.submit(aipid, [aipdir, self._state_file_for(aipid)], jcfg)

        # return status
        return self.status_of(aipid)
            
            
            

    

        
