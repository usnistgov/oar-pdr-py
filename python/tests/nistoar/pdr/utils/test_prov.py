import os, json, pdb, logging, time, yaml
import unittest as test
from io import StringIO

from nistoar.pdr.utils import prov
from nistoar.testing import *

class TestAgent(test.TestCase):

    def test_ctor(self):
        agent = prov.Agent("pdp0", prov.Agent.AUTO)
        self.assertEqual(agent.vehicle, "pdp0")
        self.assertEqual(agent.actor_type, prov.Agent.AUTO)
        self.assertEqual(agent.actor_type, "auto")
        self.assertIsNone(agent.actor)
        self.assertEqual(agent.agents, ())
        self.assertEqual(agent.groups, ())

        agent = prov.Agent("pdp0", prov.Agent.USER, "Ray")
        self.assertEqual(agent.vehicle, "pdp0")
        self.assertEqual(agent.actor_type, prov.Agent.USER)
        self.assertEqual(agent.actor_type, "user")
        self.assertEqual(agent.actor, "Ray")
        self.assertEqual(agent.agents, ())
        self.assertEqual(agent.groups, ())

        agent = prov.Agent("pdp0", prov.Agent.AUTO, "pdp", ("thing1/Ray", "thing2/auto"))
        self.assertEqual(agent.vehicle, "pdp0")
        self.assertEqual(agent.actor_type, prov.Agent.AUTO)
        self.assertEqual(agent.actor_type, "auto")
        self.assertEqual(agent.actor, "pdp")
        self.assertEqual(agent.agents, ("thing1/Ray", "thing2/auto"))
        self.assertEqual(dict(agent.iter_props()), {})

        agent = prov.Agent("pdp0", prov.Agent.AUTO, "pdp", ("thing1/Ray", "thing2/auto"), ("ncnr",))
        self.assertEqual(agent.vehicle, "pdp0")
        self.assertEqual(agent.actor_type, prov.Agent.AUTO)
        self.assertEqual(agent.actor_type, "auto")
        self.assertEqual(agent.actor, "pdp")
        self.assertEqual(agent.agents, ("thing1/Ray", "thing2/auto"))
        self.assertEqual(agent.groups, ("ncnr",))
        self.assertEqual(dict(agent.iter_props()), {})

    def test_props(self):
        agent = prov.Agent("pdp0", prov.Agent.AUTO, "pdp", ("thing1/Ray", "thing2/pdp"),
                           email="a@b.com", lunch="tacos", likes=["mom"])
        self.assertEqual(agent.agents, ("thing1/Ray", "thing2/pdp"))
        self.assertEqual(dict(agent.iter_props()),
                         {"email": "a@b.com", "lunch": "tacos", "likes": ["mom"]})
        self.assertEqual(agent.get_prop("email"), "a@b.com")
        self.assertEqual(agent.get_prop("lunch"), "tacos")
        self.assertEqual(agent.get_prop("likes", "beer"), ["mom"])
        self.assertIsNone(agent.get_prop("hairdo"))
        self.assertEqual(agent.get_prop("hairdo", "cueball"), "cueball")
        agent.set_prop("hairdo", "piece")
        self.assertEqual(agent.get_prop("hairdo", "cueball"), "piece")
        agent.set_prop("likes", "beer")
        self.assertEqual(agent.get_prop("likes"), "beer")

    def test_new_vehicle(self):
        oldagent = prov.Agent("pdp0", prov.Agent.AUTO, "pdp", ("thing1/Ray", "thing2/pdp"),
                              email="a@b.com", lunch="tacos", likes=["mom"])
        agent = oldagent.new_vehicle("midas")

        self.assertEqual(agent.vehicle, "midas")
        self.assertEqual(agent.actor_type, prov.Agent.AUTO)
        self.assertEqual(agent.actor_type, "auto")
        self.assertEqual(agent.actor, "pdp")
        self.assertEqual(agent.id, "midas/pdp")
        self.assertEqual(agent.agents, ("thing1/Ray", "thing2/pdp", "pdp0/pdp"))
        self.assertEqual(dict(agent.iter_props()),
                         {"email": "a@b.com", "lunch": "tacos", "likes": ["mom"]})
        self.assertEqual(agent.get_prop("email"), "a@b.com")
        self.assertEqual(agent.get_prop("lunch"), "tacos")
        self.assertEqual(agent.get_prop("likes", "beer"), ["mom"])
        self.assertIsNone(agent.get_prop("hairdo"))
        self.assertEqual(agent.get_prop("hairdo", "cueball"), "cueball")

    def test_agent_vehicles(self):
        agent = prov.Agent("pdp0", prov.Agent.AUTO, "pdp", ("thing1", "thing2/pdp"),
                           email="a@b.com", lunch="tacos", likes=["mom"])
        self.assertEqual(list(agent.agent_vehicles()), "pdp0 thing2 thing1".split())
        self.assertEqual(list(agent.agent_vehicles(False)), "thing1 thing2 pdp0".split())
        
        
    def test_to_dict(self):
        agent = prov.Agent("pdp0", prov.Agent.AUTO)
        data = agent.to_dict()
        self.assertEqual(data.get('vehicle'), "pdp0")
        self.assertEqual(data.get('type'), "auto")
        self.assertIn('actor', data)
        self.assertIsNone(data.get('user'))
        self.assertNotIn('agents', data)
        self.assertEqual(set(data.keys()), set("vehicle actor type".split()))

        agent = prov.Agent("pdp0", prov.Agent.USER, "Ray")
        data = agent.to_dict()
        self.assertEqual(data.get('vehicle'), "pdp0")
        self.assertEqual(data.get('type'), "user")
        self.assertEqual(data.get('actor'), "Ray")
        self.assertNotIn('agents', data)
        self.assertEqual(set(data.keys()), set("vehicle actor type".split()))

        agent = prov.Agent("pdp0", prov.Agent.AUTO, "pdp", ("thing1", "thing2"))
        data = agent.to_dict()
        self.assertEqual(data.get('vehicle'), "pdp0")
        self.assertEqual(data.get('type'), "auto")
        self.assertEqual(data.get('actor'), "pdp")
        self.assertEqual(data.get('agents'), ["thing1", "thing2"])
        self.assertEqual(set(data.keys()), set("vehicle actor type agents".split()))

    def test_serialize(self):
        agent = prov.Agent("pdp0", prov.Agent.AUTO)
        data = json.loads(agent.serialize())
        self.assertEqual(data.get('vehicle'), "pdp0")
        self.assertEqual(data.get('type'), "auto")
        self.assertIn('actor', data)
        self.assertIsNone(data.get('actor'))
        self.assertNotIn('agents', data)

        agent = prov.Agent("pdp0", prov.Agent.USER, "Ray")
        data = json.loads(agent.serialize())
        self.assertEqual(data.get('vehicle'), "pdp0")
        self.assertEqual(data.get('type'), "user")
        self.assertEqual(data.get('actor'), "Ray")
        self.assertNotIn('agents', data)

        agent = prov.Agent("pdp0", prov.Agent.AUTO, "pdp", ("thing1", "thing2"))
        data = json.loads(agent.serialize())
        self.assertEqual(data.get('vehicle'), "pdp0")
        self.assertEqual(data.get('type'), "auto")
        self.assertEqual(data.get('actor'), "pdp")
        self.assertEqual(data.get('agents'), ["thing1", "thing2"])

    def test_from_dict(self):
        src = prov.Agent("pdp0", prov.Agent.AUTO)
        agent = prov.Agent.from_dict(json.loads(src.serialize()))  # prove round-trip
        self.assertEqual(agent.vehicle, "pdp0")
        self.assertEqual(agent.actor_type, prov.Agent.AUTO)
        self.assertEqual(agent.actor_type, "auto")
        self.assertIsNone(agent.actor)
        self.assertEqual(agent.agents, ())

        src = prov.Agent("pdp0", prov.Agent.USER, "Ray")
        agent = prov.Agent.from_dict(json.loads(src.serialize()))
        self.assertEqual(agent.vehicle, "pdp0")
        self.assertEqual(agent.actor_type, prov.Agent.USER)
        self.assertEqual(agent.actor_type, "user")
        self.assertEqual(agent.actor, "Ray")
        self.assertEqual(agent.agents, ())

        src = prov.Agent("pdp0", prov.Agent.AUTO, "pdp", ("thing1", "thing2"))
        agent = prov.Agent.from_dict(json.loads(src.serialize()))
        self.assertEqual(agent.vehicle, "pdp0")
        self.assertEqual(agent.actor_type, prov.Agent.AUTO)
        self.assertEqual(agent.actor_type, "auto")
        self.assertEqual(agent.actor, "pdp")
        self.assertEqual(agent.agents, ("thing1", "thing2"))


class TestAction(test.TestCase):
    who = prov.Agent("pdp0", prov.Agent.AUTO, "ray", ["brian"])

    def test_ctor(self):
        with self.assertRaises(ValueError):
            prov.Action("goober", "me", self.who)
            
        act = prov.Action(prov.Action.CREATE, "me", self.who)
        self.assertEqual(act.type, "CREATE")
        self.assertEqual(act.subject, "me")
        self.assertEqual(act.agent.actor, "ray")
        self.assertIsNone(act.message)
        self.assertIsNone(act.object)
        self.assertGreater(act.timestamp, 1634501962)
        self.assertTrue(act.date.startswith("20"))
        self.assertEqual(act.subactions, [])
        self.assertEqual(act.subactions_count(), 0)

        act = prov.Action(prov.Action.PUT, "me", self.who, "Tak it, mon, tak it")
        self.assertEqual(act.type, "PUT")
        self.assertEqual(act.subject, "me")
        self.assertEqual(act.agent.actor, "ray")
        self.assertEqual(act.message, "Tak it, mon, tak it")
        self.assertIsNone(act.object)
        self.assertGreater(act.timestamp, 1634501962)
        self.assertTrue(act.date.startswith("20"))
        self.assertEqual(act.subactions, [])
        self.assertEqual(act.subactions_count(), 0)

        act = prov.Action(prov.Action.MOVE, "me", self.who, "Tak it, mon, tak it", "yours")
        self.assertEqual(act.type, "MOVE")
        self.assertEqual(act.subject, "me")
        self.assertEqual(act.agent.actor, "ray")
        self.assertEqual(act.message, "Tak it, mon, tak it")
        self.assertEqual(act.object, "yours")
        self.assertGreater(act.timestamp, 1634501962)
        self.assertTrue(act.date.startswith("20"))
        self.assertEqual(act.subactions, [])
        self.assertEqual(act.subactions_count(), 0)

        act = prov.Action(prov.Action.PATCH, "me", self.who, "Tak it, mon, tak it", {"a": 2}, 1634500000)
        self.assertEqual(act.type, "PATCH")
        self.assertEqual(act.subject, "me")
        self.assertEqual(act.agent.actor, "ray")
        self.assertEqual(act.message, "Tak it, mon, tak it")
        self.assertEqual(act.object, {"a": 2})
        self.assertEqual(act.timestamp, 1634500000)
        self.assertEqual(act.date, "2021-10-17 19:46:40Z")
        self.assertEqual(act.subactions, [])
        self.assertEqual(act.subactions_count(), 0)

        act = prov.Action(prov.Action.COMMENT, "me", self.who, "Tak it, mon, tak it", subacts=[act])
        self.assertEqual(act.type, "COMMENT")
        self.assertEqual(act.subject, "me")
        self.assertEqual(act.agent.actor, "ray")
        self.assertEqual(act.message, "Tak it, mon, tak it")
        self.assertIsNone(act.object)
        self.assertGreater(act.timestamp, 1634501962)
        self.assertTrue(act.date.startswith("20"))
        self.assertEqual(act.subactions[0].object, {"a": 2})
        self.assertEqual(len(act.subactions), 1)
        self.assertEqual(act.subactions_count(), 1)

        act = prov.Action(prov.Action.DELETE, "me", self.who, "Tak it, mon, tak it")
                         
    def test_add_subaction(self):
        act = prov.Action(prov.Action.CREATE, "me", self.who)
        self.assertEqual(act.type, "CREATE")
        self.assertEqual(act.subject, "me")
        self.assertEqual(act.agent.actor, "ray")
        self.assertEqual(act.subactions_count(), 0)

        with self.assertRaises(TypeError):
            act.add_subaction({"type": "DELETE", "agent": self.who, "subject": "me"})

        act.add_subaction(prov.Action(prov.Action.CREATE, "me/arm", self.who))
        self.assertEqual(act.subactions_count(), 1)
        self.assertEqual(act.subactions[0].subject, "me/arm")

        act.add_subaction(prov.Action(prov.Action.CREATE, "me/leg", self.who))
        self.assertEqual(act.subactions_count(), 2)
        self.assertEqual(act.subactions[0].subject, "me/arm")
        self.assertEqual(act.subactions[1].subject, "me/leg")

    def test_to_dict(self):
        act = prov.Action(prov.Action.CREATE, "me", self.who)
        data = act.to_dict()
        self.assertEqual(data.get('type'), "CREATE")
        self.assertEqual(data.get('subject'), "me")
        self.assertEqual(data.get('agent', {}).get('vehicle'), 'pdp0')
        self.assertEqual(list(data.keys()), "type subject agent date timestamp".split())

        act = prov.Action(prov.Action.COMMENT, "me", None, "Tak it, mon, tak it")
        data = act.to_dict()
        self.assertEqual(data.get('type'), "COMMENT")
        self.assertEqual(data.get('subject'), 'me')
        self.assertIsNone(data.get('agent'))
        self.assertEqual(data.get('message'), "Tak it, mon, tak it")
        self.assertEqual(list(data.keys()), "type subject message date timestamp".split())
        
        act = prov.Action(prov.Action.COMMENT, "me", self.who, "", None, None)
        data = act.to_dict()
        self.assertEqual(data.get('type'), "COMMENT")
        self.assertEqual(data.get('subject'), 'me')
        self.assertEqual(data.get('agent', {}).get('vehicle'), 'pdp0')
        self.assertEqual(data.get('message'), "")
        self.assertEqual(list(data.keys()), "type subject agent message".split())
        
        act.add_subaction(prov.Action(prov.Action.CREATE, "me/arm", self.who))
        act.add_subaction(prov.Action(prov.Action.CREATE, "me/leg", self.who))
        data = act.to_dict()
        self.assertEqual(data.get('type'), "COMMENT")
        self.assertEqual(data.get('subject'), 'me')
        self.assertEqual(data.get('agent', {}).get('vehicle'), 'pdp0')
        self.assertEqual(data.get('message'), "")
        self.assertEqual(len(data['subactions']), 2)
        self.assertEqual(list(data.keys()), "type subject agent message subactions".split())
        self.assertEqual(data['subactions'][0].get('type'), "CREATE")
        self.assertEqual(data['subactions'][0].get('subject'), "me/arm")
        self.assertNotIn('subactions', data['subactions'][0])
        self.assertEqual(data['subactions'][1].get('type'), "CREATE")
        self.assertEqual(data['subactions'][1].get('subject'), "me/leg")
        self.assertNotIn('subactions', data['subactions'][1])

    def test_serialize_as_json(self):
        act = prov.Action(prov.Action.COMMENT, "me", None, "Tak it, mon, tak it")
        jstr = act.serialize_as_json(2)
        self.assertGreater(len(jstr.split("\n")), 4)
        jstr = act.serialize_as_json()
        self.assertEqual(len(jstr.split("\n")), 1)

        data = json.loads(jstr)
        self.assertEqual(data.get('type'), "COMMENT")
        self.assertEqual(data.get('subject'), 'me')
        self.assertIsNone(data.get('agent'))
        self.assertEqual(data.get('message'), "Tak it, mon, tak it")
        self.assertEqual(list(data.keys()), "type subject message date timestamp".split())
        self.assertGreater(data.get('timestamp', -1), 1634501962)
        self.assertTrue(data.get('date','goob').startswith("20"))

        act = prov.Action(prov.Action.COMMENT, "me", self.who, "", None, None)
        act.add_subaction(prov.Action(prov.Action.CREATE, "me/arm", self.who))
        act.add_subaction(prov.Action(prov.Action.CREATE, "me/leg", self.who))
        jstr = act.serialize_as_json()
        self.assertEqual(len(jstr.split("\n")), 4)
        data = json.loads(jstr)
        self.assertEqual(data.get('type'), "COMMENT")
        self.assertEqual(data.get('subject'), 'me')
        self.assertEqual(data.get('agent', {}).get('vehicle'), 'pdp0')
        self.assertEqual(data.get('message'), "")
        self.assertEqual(len(data['subactions']), 2)
        self.assertEqual(list(data.keys()), "type subject agent message subactions".split())
        self.assertEqual(data['subactions'][0].get('type'), "CREATE")
        self.assertEqual(data['subactions'][0].get('subject'), "me/arm")
        self.assertNotIn('subactions', data['subactions'][0])
        self.assertEqual(data['subactions'][1].get('type'), "CREATE")
        self.assertEqual(data['subactions'][1].get('subject'), "me/leg")
        self.assertNotIn('subactions', data['subactions'][1])
        

    def test_serialize_as_yaml(self):
        act = prov.Action(prov.Action.COMMENT, "me", None, "Tak it, mon, tak it")
        jstr = act.serialize_as_yaml(2)
        self.assertGreater(len(jstr.split("\n")), 4)
        jstr = act.serialize_as_yaml()
        self.assertEqual(len(jstr.split("\n")), 2)

        data = yaml.safe_load(StringIO(jstr))
        self.assertEqual(data.get('type'), "COMMENT")
        self.assertEqual(data.get('subject'), 'me')
        self.assertIsNone(data.get('agent'))
        self.assertEqual(data.get('message'), "Tak it, mon, tak it")
        self.assertEqual(list(data.keys()), "type subject message date timestamp".split())
        self.assertGreater(data.get('timestamp', -1), 1634501962)
        self.assertTrue(data.get('date','goob').startswith("20"))

        act = prov.Action(prov.Action.COMMENT, "me", self.who, "", None, None)
        act.add_subaction(prov.Action(prov.Action.CREATE, "me/arm", self.who))
        act.add_subaction(prov.Action(prov.Action.CREATE, "me/leg", self.who))
        jstr = act.serialize_as_yaml()
        self.assertEqual(len(jstr.split("\n")), 2)
        data = yaml.safe_load(StringIO(jstr))
        self.assertEqual(data.get('type'), "COMMENT")
        self.assertEqual(data.get('subject'), 'me')
        self.assertEqual(data.get('agent', {}).get('vehicle'), 'pdp0')
        self.assertEqual(data.get('message'), "")
        self.assertEqual(len(data['subactions']), 2)
        self.assertEqual(list(data.keys()), "type subject agent message subactions".split())
        self.assertEqual(data['subactions'][0].get('type'), "CREATE")
        self.assertEqual(data['subactions'][0].get('subject'), "me/arm")
        self.assertNotIn('subactions', data['subactions'][0])
        self.assertEqual(data['subactions'][1].get('type'), "CREATE")
        self.assertEqual(data['subactions'][1].get('subject'), "me/leg")
        self.assertNotIn('subactions', data['subactions'][1])

    def test_serialize_IS_yaml(self):
        act = prov.Action(prov.Action.COMMENT, "me", None, "Tak it, mon, tak it")
        jstr = act.serialize(2)
        data = yaml.safe_load(StringIO(jstr))
        self.assertEqual(data.get('type'), "COMMENT")
        
        
    def test_from_dict(self):
        src = prov.Action(prov.Action.PATCH, "me", self.who, "Tak it, mon, tak it", {"a": 2}, 1634500000)
        act = prov.Action.from_dict(src.to_dict())
        self.assertEqual(act.type, "PATCH")
        self.assertEqual(act.subject, "me")
        self.assertEqual(act.agent.actor, "ray")
        self.assertEqual(act.message, "Tak it, mon, tak it")
        self.assertEqual(act.object, {"a": 2})
        self.assertEqual(act.timestamp, 1634500000)
        self.assertEqual(act.date, "2021-10-17 19:46:40Z")
        self.assertEqual(act.subactions, [])
        self.assertEqual(act.subactions_count(), 0)

        src = prov.Action(prov.Action.COMMENT, "me", self.who, "Tak it, mon, tak it")
        src.add_subaction(prov.Action(prov.Action.CREATE, "me/arm", self.who))
        src.add_subaction(prov.Action(prov.Action.CREATE, "me/leg", self.who))
        act = prov.Action.from_dict(src.to_dict())
        self.assertEqual(act.type, "COMMENT")
        self.assertEqual(act.subject, "me")
        self.assertEqual(act.agent.actor, "ray")
        self.assertEqual(act.message, "Tak it, mon, tak it")
        self.assertIsNone(act.object)
        self.assertGreater(act.timestamp, 1634501962)
        self.assertTrue(act.date.startswith("20"))
        self.assertEqual(act.subactions_count(), 2)
        self.assertEqual(act.subactions[0].subject, "me/arm")
        self.assertEqual(act.subactions[1].subject, "me/leg")
        
    def test_readwrite(self):
        fp = StringIO()
        prov.dump_to_history(prov.Action(prov.Action.CREATE, "me", self.who), fp)
        prov.dump_to_history(prov.Action(prov.Action.MOVE, "me", self.who, "Tak it, mon, tak it", "yours"), fp)
        act = prov.Action(prov.Action.COMMENT, "me", self.who, "", None, None)
        act.add_subaction(prov.Action(prov.Action.CREATE, "me/arm", self.who))
        act.add_subaction(prov.Action(prov.Action.CREATE, "me/leg", self.who))
        prov.dump_to_history(act, fp)
        prov.dump_to_history(prov.Action(prov.Action.PATCH, "me", self.who, "Tak it, mon, tak it",
                                         {"a": 2}, 1634500000), fp)

        fp = StringIO(fp.getvalue())
        acts = prov.load_from_history(fp)

        self.assertEqual(len(acts), 4)
        self.assertEqual(acts[0].type, "CREATE")
        self.assertIsNone(acts[0].message)
        self.assertEqual(acts[1].type, "MOVE")
        self.assertEqual(acts[2].type, "COMMENT")
        self.assertEqual(acts[2].subactions_count(), 2)
        self.assertEqual(acts[3].type, "PATCH")
        
        
        
        
        
        
                         
if __name__ == '__main__':
    test.main()
        
