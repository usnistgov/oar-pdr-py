"""
module defining classes that help track publishing provenance.

The publishing provenance for an SIP going through the publishing process is modeled as a series of 
actions taken on the SIP data; each is captured as an :py:class:`Action`.  Actions are initiated by 
an agent, represented by a :py:class:`PubAgent` instance.  Actions are applied to a _subject_, which 
is either the SIP data as a whole or some portion of it; the subject is identified via an identifier.  

Typically, an SIPBagger will record these records to a file within the bag.  The default format is in 
the form of a YAML file in which each YAML document encoded in it represents a single request by the 
client, which can be a compound Action.  
"""
import time, datetime, json
from io import StringIO
from typing import List, Iterable
from collections import OrderedDict
from collections.abc import Mapping, Sequence

import yaml
from jsonpatch import JsonPatch

class PubAgent(object):
    """
    a class describing an actor motivating a change to an SIP or other publishing artifact
    """
    USER: str = "user"
    AUTO: str = "auto"
    UNKN: str = ""

    def __init__(self, group: str, actortype: str, actor: str = None, agents: Iterable[str] = None):
        self._group = group
        if actortype not in [self.USER, self.AUTO, self.UNKN]:
            raise ValueError("PubAgent: actortype not one of "+str((self.USER, self.AUTO)))
        self._actor_type = actortype
        self._agents = []
        if agents:
            self._agents = list(agents)
        self._actor= actor

    @property
    def group(self) -> str:
        """
        the name of the permission group whose permissions are invoked in the context of this agent.
        A permission group is a group of users that have a common set of permisisons to make changes.  
        This value can used to determine if the agent is authorized to make a particular change, or 
        record the group whose permissions allowed a certain action.  
        """
        return self._group

    @property
    def actor(self) -> str:
        """
        an identifier for the specific actor--either a person or some automated agent--that is 
        initiating a change.  If the change is ultimately initiated by an action by a real person,
        the identifier should be for that person if possible.  If None, the actor is not known.
        """
        return self._actor

    @property
    def actor_type(self) -> str:
        """
        return the category of the actor behind this change.  The type can be known even if the 
        specific identity of the actor is not.  Possible values include USER and AUTO.  
        """
        return self._actor_type

    @property
    def agents(self) -> List[str]:
        """
        a chain of agents--tools or services--used to request a change.  The first element is the 
        original agent used to initiate the change; this might either be a web clients HTTP user 
        agent description or the name of a software daemon that first detected a triggering event.
        Subsequent entries should list the chain of services delegated to to effect the change.
        """
        return list(self._agents)

    def add_agent(self, agent: str) -> None:
        """
        append an agent name or description to the list of agents that delegated to this agent 
        to effect a change.  This allows a chain of delegation to be recorded with a change to 
        show where ultimately the request came from.  
        """
        if agent:
            self._agents.append(agent)

    def to_dict(self) -> Mapping:
        """
        return a dictionary describing this agent that can be converted to JSON directly via the 
        json module.  This implementation returns an OrderedDict which provides a preferred ordering 
        of keys for serializing.
        """
        out = OrderedDict([
            ("group", self.group),
            ("actor", self.actor),
            ("type", self.actor_type)
        ])
        if self._agents:
            out['agents'] = self.agents
        return out
    
    def serialize(self, indent=None):
        """
        serialize this agent to a JSON string
        :param int indent:  use the given value as the desired indentation.  If None, the output will 
                            include no newline characters (and thus no indentation)
        """
        kw = {}
        if indent:
            kw['indent'] = indent
        return json.dumps(self.to_dict(), **kw)

    def __str__(self):
        return "PubAgent(%s:%s)" % (self.group, self.actor)

    @classmethod
    def from_dict(self, data: Mapping) -> "PubAgent":
        """
        convert a dictionary like that created by to_dict() back into a PubAgent instance
        """
        missing = [p for p in "group type".split() if p not in data]
        if missing:
            raise ValueError("PubAgent.from_dict(): data is missing required properties: "+str(missing))
        return PubAgent(data.get('group'), data.get('type'), data.get('actor'), data.get('agents'))
        

class Action(object):
    """
    a description of an action that was taken on a dataset or some identifiable part of it.  Actions
    are intended to be recorded as part of the provenence history of the dataset.  

    An ``Action`` has a :py:attr:`subject`, an identifier for the dataset or dataset part that the 
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
    ``COMMENT``
        This action serves to provide via its message extra information about an action (e.g. as a 
        subaction) or otherwise describe an action that is not strictly one of the above types.  
        This action should not include an object.
    """
    CREATE:  str = "CREATE"
    PUT:     str = "PUT"
    PATCH:   str = "PATCH"
    MOVE:    str = "MOVE"
    DELETE:  str = "DELETE"
    COMMENT: str = "COMMENT"
    types = "CREATE PUT PATCH MOVE DELETE COMMENT".split()

    def __init__(self, acttype: str, subj: str, agent: PubAgent, msg: str = None, obj = None,
                 timestamp: float = 0.0, subacts: List["Action"] = None):
        """
        intialize the action
        :param str    acttype:  the type of action taken; one of CREATE, PUT, PATCH, MOVE, DELETE, COMMENT.
        :param str       subj:  the identifier for the part of the dataset that was updated
        :param PubAgent agent:  the agent (person or system) that intiated the action
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
        self._agent: PubAgent = agent
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
        the type of action, one of CREATE, PUT, PATCH, MOVE, DELETE, COMMENT
        """
        return self._type

    @property
    def subject(self) -> str:
        """
        the identifier for the dataset or portion of the dataset that the action was applied to
        """
        return self._subj

    @subject.setter
    def subject(self, subj: str) -> None:
        self._subj = subj

    @property
    def agent(self) -> PubAgent:
        """
        return the PubAgent describing who directed the action
        """
        return self._agent

    @agent.setter
    def agent(self, agnt: PubAgent) -> None:
        if not isinstance(agnt, PubAgent):
            raise TypeError("Agent.agent setter: input is not a PubAgent: "+str(agnt))
        self._agent = agnt

    @property
    def message(self) -> str:
        """
        return the message describing what was being accomplished by this action (and perhaps why)
        """
        return self._msg

    @message.setter
    def message(self, msg: str) -> None:
        self._msg = message

    @property
    def timestamp(self) -> float:
        """
        the nominal time when this action was applied.  
        """
        return self._time

    @property
    def date(self) -> str:
        """
        the current timestamp formatted as an ISO9660 string
        """
        if not self._time:
            return ""
        return datetime.datetime.fromtimestamp(self._time).isoformat(sep=" ")

    def timestamp_now(self) -> None:
        """
        update the timestamp attached to this action to the current time
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
            agent = PubAgent.from_dict(data['agent'])
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
    def _pubagent_representer(cls, dumper, agent):
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
_ActionYAMLDumper.add_representer(PubAgent, _ActionYAMLDumper._pubagent_representer)

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


            
    

            
        
