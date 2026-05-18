"""
module defining classes that help track data provenance.

Provenance--the history of changes that occur to a dataset--is recorded throughout the publishing 
process with the help of classes in this module.  This is handled (separately) in two parts of the 
system:  in MIDAS, while tracking draft DAPs (digital asset publications) and DMPs (data management 
plans), and in the PDR publishing service, tracking SIPs (submission information packages) going 
through preservation and publication.  Both processes are modeled as a a series of actions taken on 
the SIP data; each is captured as an :py:class:`Action`.  Actions are initiated by an agent, 
represented by a :py:class:`Agent` instance.  Actions are applied to a _subject_, which is either 
the SIP data as a whole or some portion of it; the subject is identified via an identifier.  

(This model is intended to correspond to that in the 
`W3C PROV model <https://www.w3.org/TR/2013/NOTE-prov-primer-20130430/>`_.  In particular, an 
:py:class:`Action` or a set of Actions can correspond to an Activity.  The subject of the action
corresponds to an Entity.)

Provenance then is a recording of a series of actions on an entity.  The default file format (used 
in the PDR publishing service) is in the form of a YAML file in which each YAML document encoded in 
it represents a single request by the client, which can be a compound Action.  MIDAS stores its 
provenance as JSON in its backend database.

While the :py:class:`Agent` class was built to support provenance recording to capture who-and-what 
made an action, it also provides features for assessing authorization.
"""
import time, datetime, json, re
from io import StringIO
from typing import List, Iterable
from collections import OrderedDict
from typing import Mapping, Sequence, Iterable, Iterator, Tuple, Callable, NewType
from copy import deepcopy

import yaml
from jsonpatch import JsonPatch

Agent = NewType("Agent", object)
PUBLIC_AGENT_CLASS = "public"
ADMIN_AGENT_CLASS = "admin"
INVALID_AGENT_CLASS = "invalid"
ANONYMOUS_USER = "anonymous"

class Agent(object):
    """
    a class describing an agent that can make some change on a data entity.  An agent's identifier has
    two main parts: a *vehicle*--the software that requests or effects the change, and an 
    *actor*--an authenticated identity, either human or functional--that authorizes the change.  
    An Agent can also provide other information about the agent such as its origins and properties
    that can be used to assess authorization rights or record provenance.

    Two properties in particular are provided for assessing authorization.  First is the 
    :py:attr:`groups` property, a list of named collections of users that can be assigned permissions 
    on particular data entities.  A group can be a static or persisted set of users stored in a 
    database (like a UNIX user group) or something dynamic whose membership is computed on the fly 
    based on current circumstances.  A group can be attached to an ``Agent``, signifying the agent's 
    membership, via :py:meth:`attach_group` (or "detached" via :py:meth:`detach_group`).  

    The second property intended for supporting authorization is the :py:attr:`agent_class`.  The 
    class represents a dynamic group that is typically assigned to the agent based on the origins of 
    the agent.  Like a group, it represents an identity enhancement that can alter the privileges afforded 
    to the agent.  For example, an ordinary user with the class set to ``ADMIN_ACTOR_CLASS`` can gain 
    administrative privileges.  This allows actions to be logged to show that an administrative 
    action was taken by a particular user.  Similarly, the ``INVALID_ACTOR_CLASS`` may restrict 
    the actor's privileges.  The :py:attr:`agent_class` name will always appear as 
    the first group in the :py:attr:`groups` property.  The PDR publishing service, whose clients 
    are typically other software agents, uses :py:attr:`agent_class` to control which clients can 
    publish which SIPs.

    This class is intended to represent an Agent from the 
    `W3C PROV model <https://www.w3.org/TR/2013/NOTE-prov-primer-20130430/>`_.
    """
    USER: str = "user"
    AUTO: str = "auto"  # for functional identities
    UNKN: str = ""
    PUBLIC: str = PUBLIC_AGENT_CLASS
    ADMIN: str = ADMIN_AGENT_CLASS
    INVALID: str = INVALID_AGENT_CLASS
    ANONYMOUS: str = ANONYMOUS_USER
    default_class = PUBLIC_AGENT_CLASS

    def __init__(self, vehicle: str, actortype: str, actorid: str = None, agclass: str = None,
                 agents: Iterable[str] = None, groups: Iterable[str] = None, **kwargs):
        """
        create an agent
        :param str   vehicle:  a name for the software component that this agent originates from.
        :param str actortype:  one of USER, AUTO, or UNKN, indicating the type of actor the identifier
                               represents
        :param str   actorid:  the unique identifier for the actor driving the software vehicle.  (This 
                               is often refered to as a "username" or "login name".)
        :param str   agclass:  an agent classification name (see :py:attr:`class`).  This is usually 
                               assigned based on the actor identity and/or the identity of the service
                               client.  
        :param list[str] agents:  the list of upstream agents that this agent is acting on behalf of
                               (optional).
        :param list[str] groups:  a list of names of permission groups that the actor should be 
                               considered part of (optional).
        :param kwargs:  arbitrary key-value pairs that will be saved as custom properties of the agent
        """
        self._vehicle = vehicle

        if actortype not in (self.USER, self.AUTO, self.UNKN):
            raise ValueError("Actor: actortype not one of "+str((self.USER, self.AUTO)))
        self._actor_type = actortype
        if not agclass:
            agclass = self.default_class
        self._agclass = agclass

        self._groups = []
        if groups:
            self._groups = set(groups)

        self._agents = []
        if agents:
            self._agents = list(agents)
        self._actor= actorid
        self._md = OrderedDict((k,v) for k,v in kwargs.items() if v is not None)

    @property
    def actor(self) -> str:
        """
        an identifier for the specific client actor making a request.  This should be unique 
        within the context of the application and blindly comparable to other identifiers within
        this scope; that is if self.id == other.id is True, self and other refer to the same actor.
        """
        return self._actor

    @property
    def actor_type(self) -> str:
        """
        the category of the actor behind this change.  The type can be known even if the 
        specific identity of the actor is not.  Possible values include USER, representing real
        people, and AUTO, representing functional identities.  
        """
        return self._actor_type

    @property
    def vehicle(self) -> str:
        """
        the name of the software compenent that established this Agent
        """
        return self._vehicle

    @property
    def agent_class(self) -> str:
        """
        a named category for the agent intended to confirm a set of permissions automatically.
        This class will be listed as the first group in the groups property
        """
        return self._agclass

    @property
    def id(self) -> str:
        """
        an identifier for this agent, of the form *vehicle*/*actor*.  
        """
        return f"{self.vehicle}/{self.actor}"

    @property
    def groups(self) -> Tuple[str]:
        """
        return the names of permission groups currently attached to this agent.  These can be used
        to make authorization decisions.
        """
        return tuple([self._agclass] + list(self._groups))

    def attach_group(self, group: str):
        """
        Attach the given group to this user to indicate that the user should be considered part of 
        this group.
        """
        groups.add(group)

    def detach_group(self, group: str):
        """
        Remove this user from the given group.  Nothing is done if the group is not already attached
        to this user.
        """
        groups.discard(group)

    def is_in_group(self, group: str):
        """
        return True if the given group is one of the groups currently attached to this user.
        """
        return group in groups

    @property
    def delegated(self) -> Tuple[str]:
        """
        a list representing a chain of delegated agents--tools or services--that led to this 
        request.  The first element is identifier for the original agent used to initiate the 
        change; this might be a front-end web application or end-user client tool.  Subsequent 
        entries should list the chain of services delegated to to make the request.  

        By convention, an agent is identified by a name representing a tool or service, optionally 
        followed by a forward slash and the identifier of the user using the tool or service.  

        This information is intended only for tracking the provenance of data objects.  It should 
        _not_ be used to make authorization decisions as the information is typically provided 
        through unauthenticated means.  
        """
        return tuple(self._agents)

    def agent_vehicles(self, current_first=True) -> Iterator[str]:
        """
        return an iterator to the vehicles in the delegated agent chain.
        :param bool current_first:  if True (default), order the list starting with the most current 
                                    agent vehicle (i.e. the value of ``self.vehicle``).  
        """
        vehicles = [a.rsplit('/', 1)[0] for a in self._agents]
        vehicles.append(self.vehicle)
        if current_first:
            vehicles.reverse()
        return iter(vehicles)

    def new_vehicle(self, vehicle: str) -> Agent:
        """
        create a clone this Agent but with a new software component vehicle name.  The agent id
        for this agent will be added to the agent list of the new Agent.
        :param str vehicle:  the vehicle name to attach to the cloned Agent
        """
        out = Agent(vehicle, self.actor_type, self.actor, self.agent_class,
                    self._agents + [self.id], self.groups)
        out._md = deepcopy(self._md)
        return out

    def get_prop(self, propname: str, defval=None):
        """
        return an actor property with the given name.  These arbitrary properties are typically 
        set at construction time from authentication credentials, but others can be set via 
        :py:meth:`set_prop`.  
        :param str propname: the name of the actor property to return
        :param Any    deval: the value to return if the property is not set (defaults to None)
        :return:  the property value
                  :rtype: Any
        """
        return self._md.get(propname, defval)

    def set_prop(self, propname: str, val):
        """
        set an actor property with the given name to a given value.  To unset a property, provide 
        None as the value.  
        """
        if val is None and propname in self._md:
            del self._md[propname]
        else:
            self._md[propname] = val

    def iter_props(self) -> Iterable[tuple]:
        """
        iterate through the attached actor properties, returning them a (property, value) tuples
        """
        return self._md.items()

    def to_dict(self, withmd=False) -> Mapping:
        """
        return a dictionary describing this agent that can be converted to JSON directly via the 
        json module.  This implementation returns an OrderedDict which provides a preferred ordering 
        of keys for serializing.
        """
        out = OrderedDict([
            ("vehicle", self.vehicle),
            ("actor", self.actor),
            ("type", self.actor_type),
            ("class", self.agent_class)
        ])
        if self._groups:
            out['groups'] = list(self._groups)
        if self._agents:
            out['delegated'] = list(self._agents)
        if withmd and self._md:
            out['actor_md'] = deepcopy(self._md)
        return out
    
    def serialize(self, indent=None, withmd=False):
        """
        serialize this agent to a JSON string
        :param int indent:  use the given value as the desired indentation.  If None, the output will 
                            include no newline characters (and thus no indentation)
        :param bool withmd: if True, include all extra user properties stored in this instance; 
                            default: True
        """
        kw = {}
        if indent:
            kw['indent'] = indent
        return json.dumps(self.to_dict(withmd), **kw)

    def __str__(self):
        return self.id

    def __repr__(self):
        return "Agent(%s)" % self.id

    @classmethod
    def from_dict(self, data: Mapping) -> Agent:
        """
        convert a dictionary like that created by to_dict() back into an Agent instance
        """
        missing = [p for p in "vehicle type".split() if p not in data]
        if missing:
            raise ValueError("Agent.from_dict(): data is missing required properties: "+str(missing))
        out = Agent(data.get('vehicle'), data.get('type'), data.get('actor'), data.get('class'),
                    data.get('delegated'), data.get('groups'))
        mdprops = [k for k in data.keys() if k not in "vehicle type actor agents groups".split()]
        for key in mdprops:
            out.set_prop(key, data[key])
        return out


class Action(object):
    """
    a description of an action that was taken on a dataset or some identifiable part of it.  Actions
    are intended to be recorded as part of the provenence history of the dataset.  

    An ``Action`` has a :py:attr:`subject`, an identifier for the entity (or entity part) that the 
    action was applied to, and it is classified as of a particular :py:attr:`type`.  Some Actions
    may also have an :py:attr:`object`, depending on the type and the subject, which provides the
    data that was applied to the subject as part of the action.  While an ``Action`` type may 
    semantically supports having an object, an object is not required.  An ``Action`` can also list 
    :py:attr:`subactions` that comprised the action; action object data may be attached to subactions
    in lieu of the Action.  Every ``Action`` may also have a brief message attached to it which 
    indicates a human-oriented statement as to the goal or intent of the action.  

    The following ``Action`` types are supported:

    ``CREATE``
        the subject was created and initialized with some data (the object)
    ``PUT``
        the content of the subject was replaced with some data (the object)
    ``PATCH``
        the content of the subject was updated with some data (the object)
    ``MOVE``
        the content formally given by subject identifier was moved to a new identifier.  If the 
        new identifier had content already, it was deleted.  The subject identifier is also 
        considered deleted.  The object of this action is the identifier the content was moved to.
    ``DELETE``
        the content of the subject was deleted, and the subject identifer is no longer accessible.
        The ``Action`` should not have object data.  
    ``PROCESS``
        the subject was submitted for some type of processing.  The Action's object field will be an
        object that will contain a ``name`` property giving the name of the process or operation 
        that was applied (e.g. "finalize", "submit", etc.); the other object properties may represent
        parameters that were used to control the processing.  Some types of processing--most notably, 
        submitting for publishing--may result in the subject becoming no longer accessible or actionable.
    ``COMMENT``
        This action serves to provide via its message extra information about an action (e.g. as a 
        subaction) or otherwise describe an action that is not strictly one of the above types.  
        This action should not include an object.

    This class is intended to correspond to the concept of Activity from the W3C PROV model 
    (https://www.w3.org/TR/2013/NOTE-prov-primer-20130430/):  an Action or a group of Actions could be 
    mapped into an Activity.
    """
    CREATE:  str = "CREATE"
    PUT:     str = "PUT"
    PATCH:   str = "PATCH"
    MOVE:    str = "MOVE"
    DELETE:  str = "DELETE"
    PROCESS: str = "PROCESS"
    COMMENT: str = "COMMENT"
    types = "CREATE PUT PATCH MOVE DELETE PROCESS COMMENT".split()
    TZ = datetime.timezone.utc

    def __init__(self, acttype: str, subj: str, agent: Agent, msg: str = None, obj = None,
                 timestamp: float = 0.0, subacts: List["Action"] = None):
        """
        intialize the action
        :param str    acttype:  the type of action taken; one of CREATE, PUT, PATCH, MOVE, DELETE, 
                                PROCESS, COMMENT.
        :param str       subj:  the identifier for the part of the dataset/entity that was updated
        :param Agent    agent:  the agent (person or system) that intiated the action
        :param msg        str:  a description of the change (and possibly why).
        :param obj:             an indicator or description of what was changed; the value, if applicable,
                                is specific to the action and subject.
        :param timestamp float: the nominal time whan the action was applied (either the start or just after 
                                completion).  If <= 0, the time will be set to the current time (default).
                                If None, there is no unique time to be associated with this action (perhaps
                                because this action is a subaction of compound action to be considered atomic.
        :param List[Action] subacts:  a list of sub-actions that make up this action; an order is indicated 
                                by the timestamps associated with each sub-action.  If None, no subactions
                                are recorded.  
        """
        if acttype not in self.types:
            raise ValueError("Action: Not a recognized action type: "+acttype)
        self._type: str = acttype
        self._subj: str = subj
        self._agent: Agent = agent
        self._msg: str = msg
        self._obj = obj
        self._time: float = timestamp
        if timestamp is not None and timestamp <= 0:
            self.timestamp_now()
        
        self._subacts = []
        if subacts is None:
            subacts = []
        for act in subacts:
            if not isinstance(act, Action):
                raise TypeError("Action: subacts element is not an Action: "+str(act))
            self._subacts.append(act)

    @property
    def type(self) -> str:
        """
        the type of action, one of CREATE, PUT, PATCH, MOVE, DELETE, PROCESS, COMMENT
        """
        return self._type

    @property
    def subject(self) -> str:
        """
        the identifier for the dataset (entity) or portion of the dataset that the action was applied to
        """
        return self._subj

    @subject.setter
    def subject(self, subj: str) -> None:
        self._subj = subj

    @property
    def agent(self) -> Agent:
        """
        return the Agent describing who directed the action
        """
        return self._agent

    @agent.setter
    def agent(self, agnt: Agent) -> None:
        if not isinstance(agnt, Agent):
            raise TypeError("Agent.agent setter: input is not a Agent: "+str(agnt))
        self._agent = agnt

    @property
    def message(self) -> str:
        """
        return the message describing what was being accomplished by this action (and perhaps why)
        """
        return self._msg

    @message.setter
    def message(self, message: str) -> None:
        self._msg = message

    @property
    def timestamp(self) -> float:
        """
        the nominal time when this action was applied.  
        """
        return self._time

    _tzre = re.compile(r"[+\-]\d\d:\d\d$")

    @property
    def date(self) -> str:
        """
        the current timestamp formatted as an ISO9660 string
        """
        if not self._time:
            return ""
        out = datetime.datetime.fromtimestamp(self._time, self.TZ).isoformat(sep=" ")
        if out.endswith("+00:00"):
            out = out[:-1*len("+00:00")] + "Z"
        return self._tzre.sub('', out)

    def timestamp_now(self) -> None:
        """
        update the timestamp attached to this action to the current time.  This will be a local system 
        time if Action.TIME_IS_LOCAL is ``True``; otherwise (the default), it will be UTC.
        """
        self._time = time.time()

    @property
    def object(self):
        """
        the data that effectively was applied to the subject.  The type and content of this data
        generally depends on the action type, and for some types (DELETE, COMMENT), this will be 
        None.
        """
        return self._obj

    @property
    def subactions(self) -> List["Action"]:
        """
        a list of sub-actions that can contstitute this action.  
        """
        return list(self._subacts)

    def subactions_count(self):
        """
        return the number of subactions attached to this action
        """
        return len(self._subacts)

    def add_subaction(self, action: "Action") -> None:
        """
        append a subaction to this action.  A subaction is one in a series of changes that were made 
        which should be considered part of this overall action.
        """
        if not isinstance(action, Action):
            raise TypeError("add_subaction(): input is not an Action: "+str(action))
        self._subacts.append(action)

    def clear_subactions(self) -> None:
        """
        remove all subactions attached to this action
        """
        self._subacts = []

    def to_dict(self) -> Mapping:
        """
        convert this Action into JSON-serializable dictionary.  This implementation returns an 
        OrderedDict which provides a preferred ordering of keys for serializing.
        """
        out = self._simple_parts_to_dict()
        if self.object:
            out['object'] = self._object_to_dict()
        if self._subacts:
            subacts = []
            for act in self._subacts:
                subacts.append(act.to_dict())
            out['subactions'] = subacts
        return out

    def _simple_parts_to_dict(self) -> Mapping:
        out = OrderedDict([
            ("type", self.type),
            ("subject", self.subject)
        ])
        if self.agent:
            out['agent'] = self.agent.to_dict()
        if self.message is not None:
            out['message'] = self.message
        if self.timestamp:
            out['date'] = self.date
            out['timestamp'] = self.timestamp
        return out

    def _object_to_dict(self):
        if isinstance(self.object, JsonPatch):
            return json.loads(self.object.to_string())
        if hasattr(self.object, 'to_dict'):
            return self.object.to_dict()
        if isinstance(self.object, Mapping):
            out = OrderedDict()
            for k,v in self.object.items():
                if isinstance(v, JsonPatch):
                    out[k] = json.loads(v.to_string())
                else:
                    out[k] = v
            return out
        return self.object

    def _serialize_subactions(self, indent=4) -> str:
        acts = []
        for act in self._subacts:
            acts.append(" "*indent + act.serialize())
        return '[\n%s\n]' % ",\n".join(acts)

    def serialize(self, *args, **kwargs) -> str:
        """
        """
        return self.serialize_as_yaml(*args, **kwargs)

    def serialize_as_yaml(self, indent=None) -> str:
        """
        serialize this agent to a JSON string
        :param int indent:  use the given value as the desired indentation.  If None, the output will 
                            not be "pretty-printed"; but each subaction will be on a separate line.
        """
        out = StringIO()
        kw = { "explicit_start": True, "sort_keys": False }
        if indent is not None:
            kw['indent'] = indent
        else:
            kw.update({ 'default_flow_style': True, 'width': float("inf") })
        yaml.dump(self, out, _ActionYAMLDumper, **kw)
        return out.getvalue()

    def serialize_as_json(self, indent=None) -> str:
        """
        serialize this agent to a JSON string
        :param int indent:  use the given value as the desired indentation.  If None, the output will 
                            not be "pretty-printed"; but each subaction will be on a separate line.
        """
        if indent is not None:
            return json.dumps(self.to_dict(), indent=indent, skipkeys=True)

        return _ActionJSONEncoder().encode(self)

    @classmethod
    def from_dict(self, data: Mapping) -> "Action":
        """
        convert a dictionary like that created by to_dict() back into an Action instance
        """
        missing = [p for p in "type subject agent".split() if p not in data]
        if missing:
            raise ValueError("Action.from_dict(): data is missing required properties: "+str(missing))

        agent = None
        if 'agent' in data:
            agent = Agent.from_dict(data['agent'])
        subacts = []
        if 'subactions' in data:
            for act in data['subactions']:
                subacts.append(Action.from_dict(act))
        obj = data.get('object')
        return Action(data['type'], data['subject'], agent, data.get('message'), obj,
                      data.get('timestamp'), subacts)

class _ActionJSONEncoder(json.JSONEncoder):

    def __init__(self, indent=None):
        super(_ActionJSONEncoder, self).__init__(indent=indent, skipkeys=True)
        self.__indent = indent
        self._stp = 4
        self.__spind = 0

    def _encode_Action(self, action):
        if self.__indent:
            return super(_ActionJSONEncoder, self).encode(action.to_dict())
        
        sp = " "*self.__spind
        out = sp + self.encode(action._simple_parts_to_dict())
        if action.object or action._subacts:
            self.__spind += self._stp
            out = out.rstrip('}').rstrip()
            if action.object:
                out += ', "object": ' + self.encode(action.object)
            if action._subacts:
                out += ', "subactions": [\n%s%s\n]' % (
                    ",\n".join([self.encode(sa) for sa in action._subacts]), sp
                )
            out += '}'
            self.__spind -= self._stp
        return out

    def _encode_JsonPatch(self, patch):
        ops = json.loads(patch.to_string())  # an array
        if self.__indent:
            return super(_ActionJSONEncoder, self).encode(ops)
        if not ops:
            return "[]"

        sp = " "*(self.__spind)
        return "[\n%s\n]" % ",\n".join([sp+self.encode(c) for c in ops])

    def _encode_array(self, ary):
        if self.__indent:
            return self.encode(ary)
        return "[%s]" % ", ".join([self.encode(e) for e in ary])

    def _encode_dict(self, d):
        if self.__indent:
            return self.encode(d)
        return "{%s}" % ", ".join(['"%s": %s' % (k, self.encode(v)) for k,v in d.items()])

    def encode(self, o):
        if isinstance(o, Action):
            return self._encode_Action(o)
        if isinstance(o, JsonPatch):
            return self._encode_JsonPatch(o)
        if isinstance(o, Mapping):
            return self._encode_dict(o)
        if isinstance(o, str):
            return super(_ActionJSONEncoder, self).encode(o)
        if isinstance(o, Sequence):
            return self._encode_array(o)
        return super(_ActionJSONEncoder, self).encode(o)

# A specialized YAML Dumper
class _ActionYAMLDumper(yaml.Dumper):
    @classmethod
    def _action_mapping_representer(cls, dumper, data):
        return dumper.represent_dict(data.items())
    @classmethod
    def _agent_representer(cls, dumper, agent):
        return dumper.represent_mapping("tag:yaml.org,2002:map",
                                        agent.to_dict().items(), flow_style=True)
    @classmethod
    def _action_representer(cls, dumper, act):
        data = act.to_dict()
        if 'agent' in data:
            data['agent'] = act.agent
        if 'subactions' in data:
            data['subactions'] = act.subactions
        return dumper.represent_dict(data.items())
_ActionYAMLDumper.add_multi_representer(Mapping, _ActionYAMLDumper._action_mapping_representer)
_ActionYAMLDumper.add_representer(OrderedDict, _ActionYAMLDumper._action_mapping_representer)
_ActionYAMLDumper.add_representer(Action, _ActionYAMLDumper._action_representer)
_ActionYAMLDumper.add_representer(Agent, _ActionYAMLDumper._agent_representer)

class _ActionYAMLLoader(yaml.Loader):
    @classmethod
    def _action_mapping_constructor(cls, dumper, data):
        return OrderedDict(loader.construct_pairs(node))
_ActionYAMLLoader.add_multi_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                                        _ActionYAMLLoader._action_mapping_constructor)

def dump_to_json_history(action: Action, histfp) -> None:
    """
    record a serialized action to a history file in JSON format.  The history file is formatted as a 
    sequence of JSON objects.  While an object may span multiple lines (for readability), each object 
    starts on a new line.  The whole file, however, is not a legal JSON document.  
    """
    histfp.write(action.serialize())
    histfp.write("\n")

def dump_to_yaml_history(action: Action, histfp) -> None:
    """
    record a serialized action to a history file in YAML format.  Each action is rendered as 
    separate YAML document within the output stream.  
    """
    # histfp.write("---\n")
    histfp.write(action.serialize(2))
    # histfp.write("\n")

def dump_to_history(action: Action, histfp) -> None:
    """
    an alias for :py:func:`dump_to_yaml_history`.
    """
    dump_to_yaml_history(action, histfp)

def load_from_history(histfp) -> List[Action]:
    return load_from_yaml_history(histfp)

def load_from_yaml_history(histfp) -> List[Action]:
    out = []
    for data in yaml.load_all(histfp, Loader=_ActionYAMLLoader):
        out.append(Action.from_dict(data))
    return out

def load_from_json_history(histfp) -> List[Action]:
    out = []
    current = ""
    for line in histfp:
        line = line.rstrip()
        current += line
        if not line.startswith(" ") and current.endswith("}"):
            out.append(Action.from_dict(json.loads(current)))
            current = ""
    if current != "":
        raise ValueError("Last line contains incomplete record: "+current)
    return out


            
    

            
        
