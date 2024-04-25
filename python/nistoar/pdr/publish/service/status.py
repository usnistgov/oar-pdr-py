"""
This module provides tools for managing and retrieving the status of a 
preservation efforts across multiple processes.  
"""
import json, os, time, fcntl, re
from collections import OrderedDict
from collections.abc import Mapping
from typing import Iterable, Union, List
from copy import deepcopy

from ...exceptions import StateException
from .. import system as pubsys

NOT_FOUND  = "not found"     # SIP has not been created
AWAITING   = "awaiting"      # SIP requires an update before it can be published
PENDING    = "pending"       # SIP has been created/updated but not yet published
PROCESSING = "processing"    # The SIP contents are being processed; further actions are not possible
                             #  until processing completes.
FINALIZED  = "finalized"     # The SIP has been finalized and is ready to be published; additional
                             #  actions other than to publish may change the state to PENDING or AWAITING.
PUBLISHED  = "published"     # SIP was successfully published
FAILED     = "failed"        # an attempt to publish (or finalize) was made but failed due to an
                             #  unexpected state or condition; SIP must be updated (or rebuilt from
                             #  scratch) before it can be published 

states = [ NOT_FOUND, AWAITING, PENDING, PROCESSING, FINALIZED, PUBLISHED, FAILED ]

user_message = {
    NOT_FOUND:   "Submission not found or available",
    AWAITING:    "Submission is awaiting further update before being ready to publish",
    PENDING:     "Submission is available to be published",
    PROCESSING:  "Submission is being processed (please stand by)",
    FINALIZED:   "Submission is ready to be published",
    PUBLISHED:   "Submission was successfully published",
    FAILED:      "Submission cannot be published due to previous error"
}

LOCK_WRITE = fcntl.LOCK_EX
LOCK_READ  = fcntl.LOCK_SH

class SIPStatusFile(object):
    """
    a class used to manage locked access to the status data file
    """
    LOCK_WRITE = fcntl.LOCK_EX
    LOCK_READ  = fcntl.LOCK_SH
    
    def __init__(self, filepath, locktype=None):
        """
        create the file wrapper
        :param filepath  str: the path to the file
        :param locktype:      the type of lock to acquire.  The value should 
                              be either LOCK_READ or LOCK_WRITE.
                              If None, no lock is acquired.  
        """
        self._file = filepath
        self._fd = None
        self._type = None

        if locktype is not None:
            self.acquire(locktype)

    def __del__(self):
        self.release()

    @property
    def lock_type(self):
        """
        the current type of lock held, or None if no lock is held.
        """
        return self._type

    def acquire(self, locktype):
        """
        set a lock on the file
        """
        if self._fd:
            if self._type == locktype:
                return False
            elif locktype == self.LOCK_WRITE:
                raise RuntimeError("Release the read lock before "+
                                   "requesting write lock")

        if locktype == LOCK_READ:
            self._fd = open(self._file)
            fcntl.flock(self._fd, fcntl.LOCK_SH)
            self._type = LOCK_READ
        elif locktype == LOCK_WRITE:
            self._fd = open(self._file, 'w')
            fcntl.flock(self._fd, fcntl.LOCK_EX)
            self._type = LOCK_WRITE
        else:
            raise ValueError("Not a recognized lock type: "+ str(locktype))
        return True

    def release(self):
        if self._fd:
            self._fd.seek(0, os.SEEK_END)
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            self._fd.close()
            self._fd = None
            self._type = None

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_val, ex_tb):
        self.release()

    def read_data(self):
        """
        read the status data from the configured file.  If a lock is not 
        currently set, one is acquired and immediately released.  
        """
        release = self.acquire(LOCK_READ)
        self._fd.seek(0)
        out = json.load(self._fd, object_pairs_hook=OrderedDict)
        if release:
            self.release()
        return out
        
    def write_data(self, data):
        """
        write the status data to the configured file.  If a lock is not 
        currently set, one is acquired and immediately released.  
        """
        release = self.acquire(LOCK_WRITE)
        self._fd.seek(0)
        json.dump(data, self._fd, indent=2, separators=(',', ': '))
        if release:
            self.release()

    @classmethod
    def read(cls, filepath):
        return cls(filepath).read_data()

    @classmethod
    def write(cls, filepath, data):
        cls(filepath).write_data(data)

def _read_status(filepath):
    try:
        with open(filepath) as fd:
            try:
                fcntl.flock(fd, fcntl.LOCK_SH)
                return json.load(fd, object_pairs_hook=OrderedDict)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError as ex:
        raise StateException("Can't open preservation status file: "
                             +filepath+": "+str(ex), cause=ex,
                             sys=preservsys)


def _write_status(filepath, data):
    try:
        with open(filepath, 'w') as fd:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                json.dump(data, fd, indent=2, separators=(',', ': '))
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError as ex:
        raise StateException("Can't open preservation status file: "
                             +filepath+": "+str(ex), cause=ex,
                             sys=preservsys)

class SIPStatus(object):
    """
    a class that represents the status of an SIP processing effort (for publication).
    It encapsulates a dictionary of data that can get updated as the publication
    process progresses.  This data is cached to disk so that multiple processes can 
    access it.  
    """

    def __init__(self, id: str, config: dict=None, sysdata: dict=None, _data: dict=None):
        """
        open up the status for the given identifier.  Initial data can be 
        provided or, if no cached data exist, it can be initialized with 
        default data.  In either case, this constructor will not cache the
        data until next call to update() or cache().  

        :param str id:       the identifier for the SIP
        :param str config:   the configuration data to apply.  If not provided
                             defaults will be used; in particular, the status
                             data will be cached to /tmp (intended only for 
                             testing purposes).
        :param dict sysdata: if not None, include this data as system data
        :param dict   _data: initialize the status with this data.  This is 
                             not intended for public use.   
        """
        if not id:
            raise ValueError("SIPStatus(): id needs to be non-empty")
        if not config:
            config = {}
        cachedir = config.get('cachedir', '/tmp/sipstatus')
        fbase = re.sub(r'^ark:/\d+/', '', id)
        self._cachefile = os.path.join(cachedir, fbase + ".json")

        if _data:
            self._data = deepcopy(_data)
        elif os.path.exists(self._cachefile):
            self._data = SIPStatusFile.read(self._cachefile)
        else:
            self._data = OrderedDict([
                ('sys', {}),
                ('user', OrderedDict([
                    ('id', ''),
                    ('state', NOT_FOUND),
                    ('siptype', ''),
                    ('authorized', []),
                    ('message', user_message[NOT_FOUND]),
                ])),
                ('history', [])
            ])
        self._data['user']['id'] = id
        if sysdata:
            self._data['sys'].update(sysdata)

    @property
    def id(self) -> str:
        """
        the SIP's identifier
        """
        return self._data['user']['id']

    @property
    def message(self) -> str:
        """
        the SIP's current status message
        """
        return self._data['user']['message']

    @property
    def siptype(self) -> str:
        """
        return the label of the SIP convention that the SIP is/was being handled under or an 
        empty string if the SIP is yet to be processed through the publishing service (or 
        otherwise its processing having been forgotten by the system).
        """
        return self._data['user']['siptype']

    @property
    def state(self) -> str:
        """
        the SIP's status state.  

        :return str:  one of NOT_FOUND, AWAITING, PENDING, PROCESSING, FINALIZED, PUBLISHED, FAILED
        """
        return self._data['user']['state']

    @property
    def authorized_agents(self) -> [str]:
        """
        the agent permission groups that currently have control over the SIP.  An empty list indicates
        that it is currently unaffiliated.  See :py:class:`~nistoar.pdr.utils.Agent` 
        for details on agent groups.
        """
        return list(self._data['user']['authorized'])

    def __str__(self):
        return "{0} {1} status: {2}: {3}".format(self.id, self.siptype, self.state, self.message)

    @property
    def data(self) -> Mapping:
        """
        the current status data.  
        """
        return self._data

    def cache(self) -> None:
        """
        cache the data to a JSON file on disk
        """
        if not os.path.exists(self._cachefile):
            cachedir = os.path.dirname(self._cachefile)
            if not os.path.exists(cachedir):
                try:
                    os.mkdir(cachedir)
                except Exception as ex:
                    raise StateException("Can't create preservation status dir: "
                                         +cachedir+": "+str(ex), cause=ex,
                                         sys=preservsys)

        self._data['user']['update_time'] = time.time()
        self._data['user']['updated'] = time.asctime()
        SIPStatusFile.write(self._cachefile, self._data)
        
    def update(self, label: str, message: str=None, userdata: dict=None, sysdata: dict=None,
               cache: bool=True) -> None:
        """
        change the state of the processing.  In addition to updating the 
        data in-memory, the full, current set of status metadata will be 
        flushed to disk.

        :param str    label:  one of the recognized state labels defined in this
                              class's module (e.g. PENDING).  
        :param str  message:  an optional message for display to the end user
                              explaining this state.  If not provided, a default
                              explanation is set. 
        :param dict userdata: extra data properties that can be part of the 
                              user-exportable data; properties in this dictionary 
                              that overlap with the standard status properties will 
                              be ignored.
        :param dict sysdata:  extra internal data properties to update.  This will
                              not be included in the user-exported data, but it 
                              will get cached.
        :param bool   cache:  if True (default), persist the status information after 
                              update.
        """
        if label not in states:
            raise ValueError("Not a recognized state label: "+label)
        if not message:
            message = user_message[label]

        handsoff = set("id state siptype authorized start_time started updated update_time".split())
        if userdata and isinstance(userdata, Mapping):
            for key in userdata:
                if key not in handsoff:
                    self._data['user'][key] = userdata[key]
            
        self._data['user']['state'] = label
        self._data['user']['message'] = message
        if cache:
            self.cache()

    def add_authorized_agent(self, agentid: str, cache: bool=True) -> None:
        """
        Add an agent group as one of the groups authorized to access and update this SIP
        (see :py:class:`~nistoar.pdr.utils.prov.PubAgent` for details about agents).  
        In addition to updating the data in-memory, the full, current set of status metadata 
        will be flushed to disk.  
        :param str  group:  the name of the agent group to add 
        :param bool cache:  if True (default), persist the status information after 
                            update.
        """
        if '/' in agentid:
            agentid = agentid.lsplit('/', 1)[0]
        if agentid not in self._data['user']['authorized']:
            self._data['user']['authorized'].append(agentid)
        if cache:
            self.cache()

    def any_authorized(self, agents: Union[str,Iterable[str]]):
        """
        return True if any of the named agents is listed as an authorized agent for the SIP.
        :param str|list[str] groups: a name of a list of names of agent permis
        """
        if not agents:
            return False
        if isinstance(agents, str):
            agents = [agents]
        if not isinstance(agents, set):
            agents = set(agents)
        return bool(agents & set(self._data['user']['authorized']))

    def remember(self, message: str=None, reset: bool=False):
        """
        Save the current status information as part of its history and then 
        reset that status to PENDING,

        :param str message:  an optional message for display to the end user
                             explaining this state.  If not provided, a default
                             explanation is set. 
        :param bool reset:   if True, reset the current state to PENDING; False is default.
        """
        if 'update_time' not in self._data['user']:
            # save the current status only if it was previously cached to disk
            return
        
        oldstatus = deepcopy(self._data['user'])
        del oldstatus['id']
        if 'history' in self.data:
            self._data['history'].insert(0, oldstatus)
        else:
            self._data['history'] = [ oldstatus ]

        state = self.state
        if reset or message:
            self.update(state, message)

    def revert(self) -> None:
        """
        reset this status to the last state saved to the status history.  This is usually the state just 
        before the last call to start().  This should be called if SIP processing is canceled.
        """
        self.refresh()
        if 'history' in self.data and len(self.data['history']) > 0:
            id = self.id
            self._data['user'] = self._data['history'].pop(0)
            self._data['user']['id'] = id
            self.cache()
        else:
            self._data['sys'] = {}
            self._data['user'] = OrderedDict([
                ('id', self._data['user']['id']),
                ('state', NOT_FOUND),
                ('siptype', self._data['user']['siptype']),
                ('authorized', []),
                ('message', user_message[NOT_FOUND])
            ])
            self._data['history'] = []
            if os.path.exists(self._cachefile):
                os.remove(self._cachefile)
            

    def start(self, siptype: str, agroup: str=None, message: str=None) -> None:
        """
        Signal that the publishing process has started using the specified SIP convention.
        Set the starting time to now and change the state to PROCESSING.  

        :param str siptype:  the label for the SIP convention being applied
        :param str agroup:   the name of the agent group that is starting the SIP
        :param message str:  an optional message for display to the end user
                             explaining this state.  If not provided, a default
                             explanation is set. 
        """
        self.refresh()
        if self.state == PUBLISHED or self.state == FAILED:
            self.remember(False)
        self._data['user']['siptype'] = siptype;
        self._data['user']['start_time'] = time.time()
        self._data['user']['started'] = time.asctime()
        if self.state == FAILED and self._data['sys']:
            self._data['sys'] = {}
        if agroup:
            self.add_authorized_agent(agroup, False)
        self.update(PROCESSING, message)

    def record_progress(self, message: str) -> None:
        """
        Update the status with a user-oriented message.  The state will be 
        unchanged, but the data will be cached to disk.
        """
        self._data['user']['message'] = message
        self.cache()

    def refresh(self) -> None:
        """
        Read the cached status data and replace the data in memory.
        """
        if os.path.exists(self._cachefile):
            self._data = SIPStatusFile.read(self._cachefile)

    def user_export(self) -> dict:
        """
        return the portion of the status data intended for export through the
        preservation service interface.  
        """
        out = deepcopy(self._data['user'])
        out['history'] = self._data['history']
        if out['history'] or out['state'] == SUCCESSFUL:
            out['published'] = True
        return out

    @classmethod
    def requests(cls, config: Mapping, agents: Union[str,Iterable[str],None]=None) -> list:
        """
        return a list of SIP IDs for which there exist status information.  
        :param Mapping config:  the status configurtion (which should include the `cachedir` parameter)
        :param str|list agents: a name or a list of names of agent groups; if provided, the 
                                returned list will include only those SIPs whose authorized agent 
                                groups include at least one of these. 
        """
        cachedir = config.get('cachedir', '/tmp/sipstatus')
        all = [ os.path.splitext(id)[0] for id in os.listdir(cachedir)
                                        if not id.startswith('_') and
                                           not id.startswith('.')      ]
        if agents is None:
            return all

        if isinstance(agents, str):
            agents = [agents]
        if not isinstance(agents, set):
            agents = set(agents)
        out = [s for s in all if SIPStatus(s, config).any_authorized(agents)]
        return out
            
