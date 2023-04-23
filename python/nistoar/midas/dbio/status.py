"""
Module for tracking the status of a dbio record.

Note that this module is similar in intent and implementation to 
:py:mod:`nistoar.pdr.publish.service.status` but has implemented to different requirements.
"""
import math
from collections.abc import Mapping
from time import time
from datetime import datetime
from copy import deepcopy

from nistoar.pdr.publish.prov import Action

# Available record states:
#
EDIT       = "edit"        # Record is currently being edit for a new released version
PROCESSING = "processing"  # Record is being processed at the moment and cannot be updated
                           #   further until this processing is complete.
READY      = "ready"       # Record is ready for submission having finalized and passed all
                           #   validation tests.
SUBMITTED  = "submitted"   # Record has been submitted and is either processed or is under review
ACCEPTED   = "accepted"    # Record has been reviewed and is being processed for release
INPRESS    = "in press"    # Record was submitted to the publishing service and is still being processed
PUBLISHED  = "published"   # Record was successfully preserved and released
UNWELL     = "unwell"      # Record is in a state that does not allow it to be further processed or
                           #   updated and requires administrative care to restore it to a usable state
_state_p    = "state"
_since_p    = "since"
_action_p   = "action"
_modified_p = "modified"
_created_p  = "created"
_message_p  = "message"

# Common record actions
# 
ACTION_CREATE = "create"
ACTION_UPDATE = "update"

class RecordStatus:
    """
    a class that holds the current status of a record (particularly, a project record), aggregating 
    multiple pieces of information about the record's state and the last action applied to it.  
    """
    CREATE_ACTION = ACTION_CREATE
    UPDATE_ACTION = ACTION_UPDATE
    
    def __init__(self, id: str, status_data: Mapping):
        """
        wrap the status information for a particular record.  Note that this constructor
        may update the input data to add default values to standard properties.
        :param str id:  the record identifier that this status object belongs to
        :param Mapping status_data:  the dictionary containing the record's status data.  This 
                        data usually comes from the ``status`` the internal property of a 
                        :py:class:`~nistoar.midas.dbio.project.ProtectedRecord`.  This class will 
                        manipulate the given dictionary directly without making a copy.
        """
        self._id = id
        self._data = status_data
        if not self._data.get(_state_p):
            self._data[_state_p] = EDIT
        if not self._data.get(_action_p):
            self._data[_action_p] = self.CREATE_ACTION

        # try to keep created,since <= modified by default
        if _created_p not in self._data or not isinstance(self._data[_created_p], (int, float)):
            self._data[_created_p] = self._data.get(_modified_p) \
                if isinstance(self._data.get(_modified_p), float) else 0
        if _since_p not in self._data or not isinstance(self._data[_since_p], (int, float)):
            self._data[_since_p] = self._data.get(_modified_p) \
                if isinstance(self._data.get(_modified_p), float) else 0
        if _modified_p not in self._data or not isinstance(self._data[_modified_p], (int, float)):
            self._data[_modified_p] = -1 if self._data[_since_p] < 0 else 0

        now = time()
        if self._data[_modified_p] < 0:
            self._data[_modified_p] = now
        if self._data[_since_p] < 0 or (self._data[_modified_p] > 0 and self._data[_since_p] < 1):
            self._data[_since_p] = now
        if self._data[_created_p] < 0 or (self._data[_modified_p] > 0 and self._data[_created_p] < 1):
            self._data[_created_p] = now

        if _message_p not in self._data:
            self._data[_message_p] = ""
        elif not isinstance(self._data[_message_p], str):
            self._data[_message_p] = str(self._data[_message_p])

    @property
    def id(self) -> str:
        """
        the identifier for the record this object provides the status for
        """
        return self._id

    @property
    def state(self) -> str:
        """
        One of a set of enumerated values indicating a distinct stage of the record's evolution.
        """
        return self._data[_state_p]

    @property
    def created(self) -> float:
        """
        The epoch timestamp when the record entered the current state
        """
        return self._data[_created_p]

    @property
    def created_date(self) -> str:
        """
        the timestamp for when the record entered the current state, formatted as an ISO string
        """
        if self.created <= 0:
            return "pending"
        return datetime.fromtimestamp(math.floor(self.created)).isoformat()

    @property
    def since(self) -> float:
        """
        The epoch timestamp when the record entered the current state
        """
        return self._data[_since_p]

    @property
    def since_date(self) -> str:
        """
        the timestamp for when the record entered the current state, formatted as an ISO string
        """
        if self.since <= 0:
            return "pending"
        return datetime.fromtimestamp(math.floor(self.since)).isoformat()

    @property
    def action(self) -> str:
        """
        The name of the last action applied to the record.  In general the actions that can be applied 
        to a record are record-type-specific; however, there are a set of common actions.
        """
        return self._data[_action_p]

    @property
    def modified(self) -> float:
        """
        The epoch timestamp when the latest action was applied to the record.
        """
        return self._data[_modified_p]

    @property
    def modified_date(self) -> str:
        """
        The timestamp when the latest action was applied to the record, formatted as an ISO string
        """
        if self.modified <= 0:
            return "pending"
        return datetime.fromtimestamp(math.floor(self.modified)).isoformat()

    @property
    def message(self) -> str:
        """
        A statement providing further description of about the last action.  The message may be 
        provided by the requesting user (to record the intent of the action) or set by default 
        by the record service.
        """
        return self._data[_message_p]

    @message.setter
    def message(self, val):
        self._data[_message_p] = val

    def act(self, action: str, message: str="", when: float=0):
        """
        record the application of a particular action on the record
        :param str  action:  the name of the action being applied
        :param str message:  a statement indicating the reason or intent of the action
        :param float  when:  the epoch timestamp for when the action was applied.  A value of 
                             zero (default) indicates that the timestamp should be set when the 
                             record is saved.  A value less than zero will cause the current 
                             time to be set.  
        """
        if not action:
            raise ValueError("Action not specified")
        if message is None:
            message = ""
        if when < 0:
            when = time()

        self._data[_action_p]   = action
        self._data[_message_p]  = message
        self._data[_modified_p] = when
        if self._data[_created_p] < 1:
            self._data[_created_p] = when

    def set_state(self, state, when: float=-1):
        """
        record a new state that the record has entered.
        :param str state:  the name of the new state that the record has entered
        :param float when: the epoch timestamp for when the state changed.  A value of 
                             zero indicates that the timestamp should be set when the 
                             record is saved.  A value less than zero (default) will 
                             cause the current time to be set.  
        """
        if not state:
            raise ValueError("State not specified")

        if self._data[_state_p] != state:
            if when < 0:
                when = time()
            self._data[_state_p]  = state
            self._data[_since_p] = when
            if self._data[_created_p] < 1:
                self._data[_created_p] = when

    def set_times(self, set_modified=True):
        """
        update any dates that are waiting to be set.  This will be called when the record is 
        saved.  
        :param bool set_modified:  if True (default), the modified time will always be updated; 
                                   otherwise, it is only updated if it is non-positive.
        """
        now = time()
        if self._data[_created_p] < 1:
            self._data[_created_p] = now
        if self._data[_since_p] < 1:
            self._data[_since_p] = now
        if set_modified or self._data[_modified_p] < 1:
            self._data[_modified_p] = now
        
    def to_dict(self, with_id=True):
        """
        return a new dictionary instance containing the storable data from this RecordStatus instance
        """
        out = deepcopy(self._data)
        if with_id:
            out['@id'] = self.id
        return out

    def clone(self):
        """
        return a copy that will be detached from its respective ProjectRecord
        """
        return RecordStatus(self.id, self.to_dict(False))

    def __str__(self):
        return str(self.to_dict())

