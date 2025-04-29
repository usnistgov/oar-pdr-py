"""
Module for tracking the status of a dbio record.

Note that this module is similar in intent and implementation to 
:py:mod:`nistoar.pdr.publish.service.status` but has implemented to different requirements.
"""
import math
from collections.abc import Mapping
from typing import List
from time import time
from datetime import datetime
from copy import deepcopy

from nistoar.pdr.utils.prov import Action

# Available record states:
#
EDIT       = "edit"        # Record is currently being edited for a new released version
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

# these are keys that access values available as Status properties
_state_p    = "state"
_since_p    = "since"
_action_p   = "action"
_modified_p = "modified"
_created_p  = "created"
_message_p  = "message"
_creatby_p  = "created_by"
_bywho_p    = "byWho"
_submitted_p    = "submitted"
_published_p    = "published"
_last_version_p = "last_version"
_published_as_p = "published_as"
_archived_at_p  = "archived_at"

# Common record actions
# 
ACTION_CREATE = "create"
ACTION_UPDATE = "update"

class RecordStatus:
    """
    a class that holds the current status of a record (particularly, a project record), aggregating 
    multiple pieces of information about the record's state and the last action applied to it.  

    This class provides some key information as property values that help determine the status of 
    the record:

    :py:attr:`state`
        a controlled value indicating the stage of handling the record is in.  For example, the 
        ``EDIT`` state indicates that the record is currently being edited.

    :py:attr:`action`
        a label that indicates the last type of action that was applied to the record

    :py:attr:`published_as`
        the identifier under which this record was last published as.  This value can be used to 
        check if the record has been published previously as it will be None if it has never been 
        published.  

    :py:attr:`message`
        A brief, user-oriented string that describes what was last done to this record.  This 
        description may be more specific than what :py:attr:`action` may indicate.  

    See the property documentation for descriptions of other properties.  
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
    def created_by(self) -> str:
        """
        a label indicating who originally created the record.  This may be a user ID or an Agent ID.   
        """
        return self._data.get(_creatby_p, "unspecified")

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
    def by_who(self) -> str:
        """
        the identifier of the agent that committed the last action applied to the record.  If None,
        the agent is unknown.
        """
        return self._data.get(_bywho_p)

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

    @property
    def submitted(self) -> float:
        """
        The epoch timestamp when the record was last submitted for publication
        """
        return self._data.get(_submitted_p, -1)

    @property
    def submitted_date(self) -> str:
        """
        The timestamp when the record was last submitted for publication, formatted as an ISO string
        """
        if self.submitted <= 0:
            return "(not yet submitted)"
        return datetime.fromtimestamp(math.floor(self.submitted)).isoformat()

    def pubreview(self, revsys: str, phase: str, id: str=None, infourl: str=None, 
                  feedback: List[Mapping]=None, fbreplace: bool=True, **extra_info):
        """
        register a current phase of an external review for publication along with other information 
        as to its status, including feedback for the authors.  A publication review typically comes 
        after the project is submitted and before final publication, but this implementation does 
        not enforce this.  

        This method can be used to provide reviewer feed back to the record's authors/editors.  Each 
        piece of feedback (which could be a request, suggestion, or comment) is a dictionary with the 
        following properties:
        ``reviewer``
             (str) _recommended_.  a user identifier or full name of the reviewer or other origin of 
             this piece of feedback.
        ``type``
             (str) _optional_.  a label indicating the type of feedback.  Special values include, 
             ``req`` (required to be addressed for approval), ``warn`` (not required but of potentially
             serious concern or otherwise strongly recommended), ``rec`` (recommended to be addressed),
             and ``comment`` (just a comment with no explicit recommendation being made).  Other values
             are allowed (as defined by the external system) but will be interpreted by default as 
             comments.  
        ``description``
             (str) _required_.  text describing the request or comment

        Other properties are allowed as defined by the external review system.

        If previously saved feedback is replaced, it will be assumed that those items have been addressed 
        and no longer need attention by the authors.

        :param str  revsys:  a unique name for the external review system providing this information.
        :param str   phase:  a label indicating the phase of review that the project is currently in. 
                             The values are defined by the external review system.
        :param str      id:  an identifier used by the external review system to track the review.  If 
                             None, then there is none defined and can probably default to the current 
                             project identifier
        :param str infourl:  a URL that DBIO client user can access to get information on the status of 
                             the external review.  If None, such information is not (yet) available
        :param list feedback:  a list of reviewer feedback.  If None, the previously saved feedback will 
                             be retained.  If an empty list and ``fbreplace`` is True (default), the 
                             previously save feedback will be dropped and replaced with an empty list.
        :param bool fbreplace:  if True (default), this feedback should replace all previously registered 
                             feedback
        :param extra_info:   Other JSON-encodable properties that should be included in the registration.
        """
        pubrev = OrderedDict([('phase', phase)])
        if id:
            pubrev['@id'] = id
        if infourl:
            pubrev['info_at'] = infourl

        oldfb = self._data.get(_pubreview_p, {}).get('feedback', [])
        if feedback is not None:
            if isinstance(feedback, tuple):
                feedback = list(feedback)
            if not isinstance(feedback, list) or any([not isinstance(fb, Mapping) for fb in feedback]):
                raise ValueError("RecordStatus.pubreview(): feedback is not a list of dicts")
            try:
                json.dumps(feedback)
            except Exception as ex:
                raise ValueError("RecordStatus.pubreview(): feedback is not JSON-encodable")

            if not fbreplace and oldfb:
                feedback = oldfb + feedback
            pubrev['feedback'] = feedback
        elif oldfb:
            pubrev['feedback'] = oldfb

        if extra_info:
            for prop in extra_info:
                if prop in pubrev:
                    continue
                try: 
                    json.dumps(extra_info[prop])
                except Exception as ex:
                    raise ValueError(f"RecordStatus.pubreview(): {prop} is not JSON-encodable")
                pubrev[prop] = extra_info[prop]

        self._data.setdefault(_pubreview_p, {})
        self._data[_pubreview_p][revsys] = pubrev

        return pubrev

    @property
    def published(self) -> float:
        """
        The epoch timestamp when the record was last published 
        """
        return self._data.get(_published_p, -1)

    @property
    def published_date(self) -> str:
        """
        The timestamp when the record was last published, formatted as an ISO string
        """
        if self.published <= 0:
            return "(not yet published)"
        return datetime.fromtimestamp(math.floor(self.published)).isoformat()

    @property
    def published_as(self):
        """
        the identifier that this draft record was most recently published as.  If None,
        this record has never been published.
        """
        return self._data.get(_published_as_p)

    @property
    def last_version(self):
        """
        the version string assigned to the most recently published version of the record.
        This should be None if the record has never been published; however, clients should 
        rely on the value of :py:attr:`published_as` to determine publishing status.
        """
        return self._data.get(_last_version_p)

    @property
    def archived_at(self):
        """
        a URL indicating where the published version was archived; the URL should resolve to the 
        data content of the record.  This URL may feature a custom (non-standard) scheme to indicate 
        an internal protocol for accessing the archived artifact and resolving it into the published 
        content.  If None, clients should assume a default location based on the value of 
        :py:attr:`published_as`.
        """
        return self._data.get(_archived_at_p)

    def publish(self, pub_id: str, version: str, arch_loc: str = None, asof: float = -1):
        """
        set or update the publishing status properties
        :param str   pub_id: the identifier that the record has been published as
        :param str  version: the version that was assigned to that publication
        :param str arch_loc: a URL that indicates where the publication was archived at.
                             If None, the associated property is not set, incicating that 
                             a default location should be assumed.  
        :param float   asof: the date to record as the publication date, in epoch seconds.
                             If None or <= 0, the current time will be recorded.
        """
        if not pub_id or not isinstance(pub_id, str):
            raise ValueError("Status.publish(): pub_id must be a non-empty str")
        if not version or not isinstance(version, str):
            raise ValueError("Status.publish(): version must be a non-empty str")
        if arch_loc and not isinstance(arch_loc, str):
            raise ValueError("Status.publish(): arch_loc must be a str or None")
        self._data[_published_as_p] = pub_id
        self._data[_last_version_p] = version
        if arch_loc:
            self._data[_archived_at_p] = arch_loc
        if not asof or asof < 0:
            asof = time()
        self._data[_published_p] = asof

    def act(self, action: str, message: str="", who: str=None, when: float=0):
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
        self._data[_bywho_p] = who

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

