"""
A module that provides monitoring the asynchronous publication of SIPs via a 
:py:class:`~nistoar.pdr.publish.service.base.PublishingService`.
"""
import logging, threading, csv, time
from logging import Logger
from abc import ABC, abstractmethod
from typing import Callable, Tuple, Mapping, Union, List, Sequence
from pathlib import Path

from . import status
from nistoar.pdr.utils import AtomicAccessFile

class PublishingMonitor(ABC):
    """
    A class that monitors the processing of SIPs submitted to a 
    :py:class:`~nistoar.pdr.publish.service.base.PublishingService`.

    Typically, SIP publishing is asynchronous (primarily due to the longer running preservation 
    part).  This class provides a way to monitor SIPs that have been submitted to a 
    :py:class:`~nistoar.pdr.publish.service.base.PublishingService`
    so that when publishing is complete, some follow-up activity can be accomplished.  

    This base class abstracts two implementation details: how the queue is stored and how 
    the publishing state is determined.
    """

    def __init__(self, onchange: Callable, cyclesecs: int, log: Logger=None):
        """
        initialize the base implementation.  

        This base constructor also the function that should be called when certain publishing 
        states are reached.  The function implementation should do nothing when states are reached
        that it doesn't care about.  Typically, the states that it should react to are the final
        ones:  "published", "failed", and "on-hold".  When an SIP reaches a final state montitoring
        of the SIP ceases.  

        :param Callable onchange:  a function to call when a submitted SIP changes state or 
                                   reaches a state indicating the completion of its 
                                   publication processing.  This function takes three arguments:
                                   the SIP-ID, the publication state, and a message about its 
                                   current state.
        :param int     cyclesecs:  the number of seconds to allow to pass before starting a new
                                   before updating the status of the SIPs in the queue.
        :param Logger        log:  the log to report activity to.
        """
        self._onchange = onchange
        self._cycletime = cyclesecs
        if not log:
            log = logging.getLogger("PublishingMonitor")
        self.log = log
        self._in_queue = []
        self._monthread = None

    def _init_state(self):
        """
        initialize the state of the monitor.  

        Concrete subclasses should call this method from their ``__init__()`` function.  This 
        implementation calls :py:meth:`_get_sips` to load the SIPs currently in the queue into 
        memory.
        """
        self._in_queue = self._get_sips()

    @abstractmethod
    def _load_queue(self) -> Mapping[str, Tuple]:
        """
        return contents of the queue from persistent storage.

        :return:  a dictionary mapping SIP IDs to tuples where the first tuple element 
                  is the publishing state label and the second is a descriptive message.  
        """
        raise NotImplemented()

    @abstractmethod
    def _update_queue(self, updates: Mapping[str, Tuple], deletes: List[str]):
        """
        update the contents of the queue with the given changes.  

        The implementation should pull the latest persisted queue contents, merge the given
        changes, and persist the updated queue.

        :param dict updates:  a dictionary with the same form as returned by 
                              :py:meth:`_load_queue` that contains only those SIPs that 
                              have changed.  
        :param list deletes:  the SIP identifiers that should be deleted from the queue
        """
        raise NotImplemented()

    @abstractmethod
    def state_of(self, sipid: str) -> Tuple[str, str]:
        """
        return the remember publishing state of the SIP as a tuple of the state label and
        its message.
        """
        raise NotImplemented()

    def _get_sips(self):
        """
        return a list of SIP identifiers currently in the queue
        """
        return list(self._load_queue().keys())

    def update_statuses_in_queue(self):
        """
        iterate (once) through the SIPs in the monitor queue file and update their statuses, 
        calling the "on-complete" function when a status changes.

        The updated status are written back to the queue file, removing any SIPs that have 
        reached any of the "not found", "submitted", and "failed" states.  
        """
        done_states = [ status.NOT_FOUND, status.PUBLISHED, status.FAILED ]
        stats = self._load_queue()
        if not stats:
            self.log.debug("No active SIP publishing in queue")
            return []
        updates = {}
        deletes = set()

        self.log.info("Checking on %i SIPs", len(stats))
        dbfmt = "Queue: %s"
        if len(stats) > 5:
            dbfmt += "..."
        self.log.debug(dbfmt, " ".join(list(stats.keys())[:5]))

        for sipid in stats:
            newstat = self.state_of(sipid)

            if newstat[0] in done_states or newstat[0] != stats[sipid][0]:
                # SIP has either changed state or reached a done state
                try:
                    self._onchange(sipid, newstat[0], newstat[1])
                except Exception as ex:
                    self.log.exception("Failed to process sipid=" + sipid + " that reached state=" +
                                       newstat[0] + ": " + str(ex))
                finally:
                    if newstat[0] in done_states:
                        deletes.add(sipid)
                    else:
                        updates[sipid] = newstat

        self._update_queue(updates, deletes)
        return list(stats.keys())

    def monitor(self, stop_after=Union[int|bool], timeout: int=0):
        """
        run the monitoring loop: periodically check and update that status of the SIPs 
        in the internal queue.

        :param int|bool stop_after:  If a positive number, stop cycling and exit after 
                                     this value number of cycles.  If negative or True,
                                     exit function when the queue is empty; if 0 or False
                                     (default), run forever (or until the timeout limit 
                                     is reached).
        :param int         timeout:  Exit the function if, after the completion of a 
                                     cycle through the queue, the total time monitoring
                                     since enter this function exceeds this value in 
                                     seconds.  If less than or equal to 0, no timeout 
                                     is enforced.  
        :return:  the number of SIPs still in the queue.  If the number is positive, the 
                  loop exited because the stop_after or timeout was exceeded
                  :rtype: int
        """
        opstart = time.time()
        cyclestart = opstart
        exittime = opstart + timeout if timeout > 0 else 0
        cycles = 0

        if stop_after is True:
            stop_after = -1
        elif stop_after is False:
            stop_after = 0

        while stop_after < 1 or cycles < stop_after:
            self._in_queue = self.update_statuses_in_queue()
            cycles += 1
            now = time.time()
            if ((stop_after < 0 and len(self._in_queue) <= 0) or  # queue is empty,
                (stop_after > 0 and cycles > stop_after) or       # max cycles exceeded, or
                (exittime > 0 and now > exittime)):               # timeout exceeded
                break
            rest = cyclestart + self._cycletime - now
            if rest < 0:
                rest = 0
            time.sleep(rest)
            cyclestart = time.time()
            
        return len(self._in_queue)

    def launch_monitoring(self, stop_after=Union[int|bool], timeout: int=0):
        """
        start monitoring in a separate thread.  If the thread is already running, do nothing.
        """
        if self._monthread and self._monthread.is_alive():
            return False

        self._monthread = threading.Thread(target=self.monitor, name="publishing-monitor",
                                           kwargs={"stop_after": stop_after, "timeout": timeout})
        self._monthread.start()

    def is_running(self):
        """
        return True if monitoring is running (asynchronously in a separate thread)
        """
        return self._monthread and self._monthread.is_alive()

    def __del__(self):
        if self._monthread and self._monthread.is_alive():
            self._monthread.join(5)
        

class FileBasedPublishingMonitor(PublishingMonitor):
    """
    a :py:meth:`PublishingMonitor` in which the monitoring queue is persisted in a local file.
    """
    def __init__(self, queue_file: str, onchange: Callable, cyclesecs: int, log: Logger=None):
        super(FileBasedPublishingMonitor, self).__init__(onchange, cyclesecs, log)
        if isinstance(queue_file, str):
            queue_file = Path(queue_file)
        self._qfile = queue_file

    def _load_queue(self) -> Mapping[str, Tuple]:
        """
        return contents of the queue from persistent storage.

        :return:  a dictionary mapping SIP IDs to tuples where the first tuple element 
                  is the publishing state label and the second is a descriptive message.  
        """
        if not self._qfile.exists():
            PublishingMonitorQueue.write(self._qfile, {})
        return PublishingMonitorQueue.read(self._qfile)

    def _update_queue(self, updates: Mapping[str, Tuple], deletes: List[str]):
        """
        save the queue to persistent storage.

        :param dict queue:  the queue contents to save in the same form as returned by 
                            :py:meth:`_load_queue`
        """
        with PublishingMonitorQueue(self._qfile, PublishingMonitorQueue.LOCK_WRITE) as q:
            stats = q.read_data()
            if updates:
                stats.update(updates)
            for finished in deletes:
                del stats[finished]
            q.write_data(stats)

class LocalPublishingMonitor(FileBasedPublishingMonitor):
    """
    A (concrete) :py:meth:`PublishingMonitor` that is can determine the publishing status of 
    an SIP by consulting the local filesystem.  

    When the publishing service and the MIDAS service share a filesystem, accessing the persisted
    state directly is cheaper than going through the publishing web service.  
    """
    def __init__(self, statusdir: str, queue_file: str, onchange: Callable,
                 cyclesecs: int=600, log: Logger=None):
        """
        initialize the monitor

        :param str|Path queue_file:  the path to the file for persisting the queue.  
        :param str|Path  statusdir:  the path to the directory containing publishing state files
        :param Callable   onchange:  a function to call when a submitted SIP changes state or 
                                     reaches a state indicating the completion of its 
                                     publication processing.  This function takes three arguments:
                                     the SIP-ID, the publication state, and a message about its 
                                     current state.
        :param int       cyclesecs:  the number of seconds to allow to pass before starting a new
                                     before updating the status of the SIPs in the queue
                                     (Default: 10 minutes).
        :param Logger          log:  the log to report activity to.
        """
        super(LocalPublishingMonitor, self).__init__(queue_file, onchange, cyclesecs, log)
        self._statusdir = statusdir
        self._init_state()
    
    def state_of(self, sipid: str) -> Tuple[str, str]:
        """
        return the remember publishing state of the SIP as a tuple of the state label and
        its message.
        """
        stat = status.SIPStatus(sipid, self._statusdir)
        return (stat.state, stat.message)

class PublishingMonitorQueue(AtomicAccessFile):
    """
    a class used to manage locked access to the publishing monitor's queue file.

    This class is the persistence interface used by :py:class:`LocalPublishingMonitor`.  It
    implements safe access to a file containing SIP publishing status information between 
    a dedicated process that is monitoring SIP publication processing and clients (e.g. MIDAS)
    that are interested in that status.  The queue of SIPs and their last known state are 
    persisted into a TSV-formatted file.
    """
    open_params = { 'newline': '' }

    def _parse_data(self, fd):
        out = {}
        rdr = csv.reader(fd, delimiter='\t')
        for row in rdr:
            if len(row) == 0:
                continue
            if len(row) < 2:
                out[row[0]] = ('', '')
            else:
                out[row[0]] = tuple(row[1:])
        return out

    def _format_data(self, data, fd):
        if not isinstance(data, Mapping):
            raise ValueError("Input data is not a dict")
        wrtr = csv.writer(fd, delimiter='\t')
        for id in data:
            row = [id]
            if isinstance(data[id], str) or not isinstance(data[id], Sequence):
                row += ['','']
            elif len(data[id]) < 2:
                row += list(data[id]) + ['' for i in range(2 - len(data[id]))]
            else:
                row += list(data[id])
            wrtr.writerow(row)

    def add_SIP(self, id: str, state: str=None, msg: str=None):
        """
        add an SIP to the persisted queue

        :param str    id:  the ID of the SIP to add
        :param str state:  the current state of the SIP, if known
        :param str   msg:  a status message for the current state of the SIP, if known
        """
        if not state:
            state = ''
        if not msg:
            msg = ''

        release = self.acquire(self.LOCK_WRITE)
        try:
            data = self.read_data() if self._file.exists() else {}
            if id not in data or state:
                data[id] = (state, msg)
            self.write_data(data)
        finally:
            if release:
                self.release()
        

class PublishingMonitorClient(ABC):
    """
    an abstract interface registering with a :py:class:`PublishingMonitor` SIPs that were 
    submitted for publishing
    """

    @abstractmethod
    def watch(self, sipid: str, initstate: str = None, initmsg: str = None):
        """
        register an SIP to monitor

        :param str sipid:      the identifier of the SIP to monitor
        :param str initstate:  the initial publishing state for set for the SIP; if not 
                               provided, the state will set from an immediate query to 
                               the publishing service.
        :param str initmsg     the initial message to set
        """
        raise NotImplemented()

class FileBasedPublishingMonitorClient(PublishingMonitorClient):
    """
    a :py:class:`PublishingMonitorClient` implementation for registering SIPs with a 
    :py:class:`FileBasedPublishingMonitor`.
    """

    def __init__(self, monitorqfile: Union[str,Path]):
        """
        initialize the client

        :param PublishingClient pubcli:  the PublishingClient that the SIP was submitted through
        """
        self.qfile = monitorqfile

    def watch(self, sipid: str, initstate: str = None, initmsg: str = None):
        """
        register an SIP to monitor

        :param str sipid:      the identifier of the SIP to monitor
        :param str initstate:  the initial publishing state for set for the SIP; if not 
                               provided, the state will set from an immediate query to 
                               the publishing service.
        :param str initmsg     the initial message to set
        """
        PublishingMonitorQueue(self.qfile).add_SIP(sipid, initstate, initmsg)

