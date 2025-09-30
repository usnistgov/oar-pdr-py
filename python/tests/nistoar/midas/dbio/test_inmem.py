import os, json, pdb, logging,asyncio 
from pathlib import Path
import unittest as test
import websockets

from nistoar.midas.dbio import inmem, base
from nistoar.pdr.utils.prov import Action, Agent

testuser = Agent("dbio", Agent.AUTO, "tester", "test")
testdir = Path(__file__).parents[0]
datadir = testdir / "data"

asc_andor = datadir / 'asc_andor.json'
asc_and   = datadir / 'asc_and.json'
asc_or    = datadir / 'asc_or.json'
dmp_path  = datadir / 'dmp.json'

with open(asc_or, 'r') as file:
    constraint_or = json.load(file)

with open(asc_and, 'r') as file:
    constraint_and = json.load(file)

with open(asc_andor, 'r') as file:
    constraint_andor = json.load(file)

with open(dmp_path, 'r') as file:
    dmp = json.load(file)

class TestInMemoryDBClientFactory(test.TestCase):

    def setUp(self):
        self.cfg = {
            "goob": "gurn",
            "client_notifier": { "service_endpoint": "ws://localhost/" }
        }
        self.fact = inmem.InMemoryDBClientFactory(
            self.cfg, {"nextnum": {"hank": 2}})

    def test_ctor(self):
        self.assertEqual(self.fact._cfg, self.cfg)
        self.assertTrue(self.fact._db)
        self.assertEqual(self.fact._db.get(base.DAP_PROJECTS), {})
        self.assertEqual(self.fact._db.get(base.DMP_PROJECTS), {})
        self.assertEqual(self.fact._db.get(base.GROUPS_COLL), {})
        self.assertEqual(self.fact._db.get(base.PEOPLE_COLL), {})
        self.assertEqual(self.fact._db.get("nextnum"), {"hank": 2})
        self.assertIsNone(self.fact._peopsvc)
        self.assertIsNone(self.fact._notifier)

    def test_create_client(self):
        avauser = Agent("dbio", Agent.AUTO, "ava1", "test")
        cli = self.fact.create_client(base.DMP_PROJECTS, {}, avauser)
        self.assertEqual(cli._db, self.fact._db)
        self.assertEqual(cli._cfg, self.fact._cfg)
        self.assertEqual(cli._projcoll, base.DMP_PROJECTS)
        self.assertEqual(cli.user_id, "ava1")
        self.assertTrue(isinstance(cli._who, Agent))
        self.assertIsNone(cli._whogrps)
        self.assertIs(cli._native, self.fact._db)
        self.assertIsNotNone(cli._dbgroups)
        self.assertIsNone(cli._peopsvc)

        self.assertIsNotNone(cli.notifier)
        self.assertEqual(cli.notifier.api_key, "")


class TestInMemoryDBClient(test.TestCase):

    def setUp(self):
        self.cfg = {
            "project_id_minting": {
                "default_shoulder": {
                    "public": "pdr0"
                }
            }
        }
        self.user = "nist0:ava1"
        self.agent = Agent("dbio", Agent.AUTO, self.user, "test")
        self.cli = inmem.InMemoryDBClientFactory({}).create_client(
            base.DMP_PROJECTS, self.cfg, self.agent)
        self.received_messages = []

    async def mock_websocket_server(self, websocket):
        """
        Mock WebSocket server to capture messages sent by the client.
        """
        async for message in websocket:
            self.received_messages.append(message)
        

    def test_next_recnum(self):
        self.assertEqual(self.cli._next_recnum("goob"), 1)
        self.assertEqual(self.cli._next_recnum("goob"), 2)
        self.assertEqual(self.cli._next_recnum("goob"), 3)
        self.assertEqual(self.cli._next_recnum("gary"), 1)
        self.assertEqual(self.cli._next_recnum("goober"), 1)
        self.assertEqual(self.cli._next_recnum("gary"), 2)

        self.assertEqual(self.cli._db["nextnum"]["goob"], 3)
        self.cli._try_push_recnum("goob", 2)
        self.assertEqual(self.cli._db["nextnum"]["goob"], 3)
        self.assertNotIn("hank", self.cli._db["nextnum"])
        self.cli._try_push_recnum("hank", 2)
        self.assertNotIn("hank", self.cli._db["nextnum"])
        self.cli._try_push_recnum("goob", 3)
        self.assertEqual(self.cli._db["nextnum"]["goob"], 2)

    def test_get_from_coll(self):
        # test query on non-existent collection
        self.assertIsNone(self.cli._get_from_coll("alice", "p:bob"))

        # test query on existing but empty collection
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"))

        # put in some test data into the underlying database
        self.cli._db[base.GROUPS_COLL]["p:bob"] = {
            "id": "p:bob", "owner": "alice"}
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"))
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:bob"),
                         {"id": "p:bob", "owner": "alice"})

        self.cli._db[base.GROUPS_COLL]["p:mine"] = {
            "id": "p:mine", "owner": "p:bob"}
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"),
                         {"id": "p:mine", "owner": "p:bob"})
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:bob"),
                         {"id": "p:bob", "owner": "alice"})

        self.assertIsNone(self.cli._get_from_coll("nextnum", "goob"))
        self.cli._db["nextnum"] = {"goob": 0}
        self.assertEqual(self.cli._get_from_coll("nextnum", "goob"), 0)

    def test_select_from_coll(self):
        # test query on non-existent collection
        it = self.cli._select_from_coll(
            "alice", owner="p:bob", hobby="knitting")
        self.assertTrue(hasattr(it, "__next__"),
                        "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # test query on existing but empty collection
        recs = list(self.cli._select_from_coll(
            base.GROUPS_COLL, owner="p:bob", hobby="knitting"))
        self.assertEqual(len(recs), 0)

        # put in some test data into the underlying database
        self.cli._db[base.GROUPS_COLL]["p:bob"] = {
            "id": "p:bob", "owner": "alice", "hobby": "whittling"}
        it = self.cli._select_from_coll(
            base.GROUPS_COLL, owner="p:bob", hobby="knitting")
        self.assertTrue(hasattr(it, "__next__"),
                        "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)
        recs = list(self.cli._select_from_coll(
            base.GROUPS_COLL, owner="alice", hobby="whittling"))
        self.assertEqual(len(recs), 1)
        self.assertEqual(
            recs[0], {"id": "p:bob", "owner": "alice", "hobby": "whittling"})

        self.cli._db[base.GROUPS_COLL]["p:mine"] = {
            "id": "p:mine", "owner": "p:bob", "hobby": "whittling"}
        recs = list(self.cli._select_from_coll(
            base.GROUPS_COLL, owner="alice", hobby="whittling"))
        self.assertEqual(len(recs), 1)
        self.assertEqual(
            recs[0], {"id": "p:bob", "owner": "alice", "hobby": "whittling"})
        recs = list(self.cli._select_from_coll(
            base.GROUPS_COLL, hobby="whittling"))
        self.assertEqual(len(recs), 2)

        # test deactivated filter
        self.cli._db[base.GROUPS_COLL]["p:gang"] = {
            "id": "p:gang", "owner": "p:bob", "deactivated": 1.2}
        recs = list(self.cli._select_from_coll(
            base.GROUPS_COLL, owner="p:bob"))
        self.assertEqual(len(recs), 1)
        recs = list(self.cli._select_from_coll(
            base.GROUPS_COLL, incl_deact=True, owner="p:bob"))
        self.assertEqual(len(recs), 2)
        self.cli._db[base.GROUPS_COLL]["p:gang"]["deactivated"] = None
        recs = list(self.cli._select_from_coll(
            base.GROUPS_COLL, owner="p:bob"))
        self.assertEqual(len(recs), 2)

    def test_select_prop_contains(self):
        # test query on non-existent collection
        it = self.cli._select_prop_contains("alice", "hobbies", "whittling")
        self.assertTrue(hasattr(it, "__next__"),
                        "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # test query on existing but empty collection
        recs = list(self.cli._select_prop_contains(
            base.GROUPS_COLL, "members", "alice"))
        self.assertEqual(len(recs), 0)

        # put in some test data into the underlying database
        self.cli._db[base.GROUPS_COLL]["p:bob"] = {
            "id": "p:bob", "members": ["p:bob"]}
        self.cli._db[base.GROUPS_COLL]["stars"] = {
            "id": "stars", "members": ["p:tom", "p:bob"]}
        recs = list(self.cli._select_prop_contains(
            base.GROUPS_COLL, "members", "alice"))
        self.assertEqual(len(recs), 0)

        self.cli._db[base.GROUPS_COLL]["p:bob"]["members"].append("alice")
        it = self.cli._select_prop_contains(
            base.GROUPS_COLL, "members", "alice")
        self.assertTrue(hasattr(it, "__next__"),
                        "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 1)
        self.assertEqual(
            recs[0], {"id": "p:bob", "members": ["p:bob", "alice"]})

        recs = list(self.cli._select_prop_contains(
            base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 2)
        self.assertEqual(set([r.get('id') for r in recs]),
                         set("p:bob stars".split()))

        # test deactivated filter
        self.cli._db[base.GROUPS_COLL]["p:gang"] = {
            "id": "p:gang", "members": ["p:bob"], "deactivated": 1.2}
        recs = list(self.cli._select_prop_contains(
            base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 2)
        recs = list(self.cli._select_prop_contains(
            base.GROUPS_COLL, "members", "p:bob", incl_deact=True))
        self.assertEqual(len(recs), 3)
        self.cli._db[base.GROUPS_COLL]["p:gang"]["deactivated"] = None
        recs = list(self.cli._select_prop_contains(
            base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 3)

    def test_delete_from(self):
        # test query on non-existent collection
        self.assertFalse(self.cli._delete_from("alice", "p:bob"))

        # test query on existing but empty collection
        self.assertFalse(self.cli._delete_from(base.GROUPS_COLL, "p:bob"))

        self.cli._db[base.GROUPS_COLL]["p:bob"] = {
            "id": "p:bob", "members": ["p:bob"]}
        self.cli._db[base.GROUPS_COLL]["stars"] = {
            "id": "stars", "members": ["p:tom", "p:bob"]}
        self.assertTrue(self.cli._delete_from(base.GROUPS_COLL, "p:bob"))
        self.assertNotIn("p:bob", self.cli._db[base.GROUPS_COLL])
        self.assertIn("stars", self.cli._db[base.GROUPS_COLL])

    def test_upsert(self):
        # test on a non-existent collection
        self.assertIsNone(self.cli._get_from_coll("about", "p:bob"))
        self.assertIsNone(self.cli._get_from_coll("about", "alice"))

        self.assertTrue(self.cli._upsert("about",
                                         {"id": "p:bob", "owner": "alice", "hobby": "whittling"}))   # insert
        self.assertEqual(self.cli._get_from_coll("about", "p:bob"),
                         {"id": "p:bob", "owner": "alice", "hobby": "whittling"})
        self.assertIsNone(self.cli._get_from_coll("about", "alice"))

        self.assertFalse(self.cli._upsert("about",
                                          {"id": "p:bob", "owner": "alice", "hobby": "knitting"}))   # update
        self.assertEqual(self.cli._get_from_coll("about", "p:bob"),
                         {"id": "p:bob", "owner": "alice", "hobby": "knitting"})
        self.assertIsNone(self.cli._get_from_coll("about", "alice"))

        # test on an existing collection
        self.assertIsNone(self.cli._get_from_coll(
            base.GROUPS_COLL, "g:friends"))
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "stars"))

        self.assertTrue(self.cli._upsert(base.GROUPS_COLL, {
                        "id": "p:bob", "members": ["p:bob"]}))
        rec = self.cli._get_from_coll(base.GROUPS_COLL, "p:bob")
        self.assertEqual(rec, {"id": "p:bob", "members": ["p:bob"]})
        rec['members'].append("alice")
        self.cli._upsert(base.GROUPS_COLL, rec)
        rec2 = self.cli._get_from_coll(base.GROUPS_COLL, "p:bob")
        self.assertEqual(rec2, {"id": "p:bob", "members": ["p:bob", "alice"]})

    def test_adv_select_records(self):
        # inject some data into the database
        id = "pdr0:0002"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "test 1", "deactivated": "null", "status": {
                "created": 1689021185.5037804,
                "state": "create",
                "action": "create",
                "since": 1689021185.5038593,
                "modified": 1689021185.5050585,
                "message": "draft created"
            }}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id] = rec.to_dict()


        id = "pdr0:0006"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "test 2", "status": {
                "created": 1689021185.5037804,
                "state": "edit",
                "action": "create",
                "since": 1689021185.5038593,
                "modified": 1689021185.5050585,
                "message": "draft created"
            }}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id] = rec.to_dict()

        id = "pdr0:0003"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "test3","title":"Gretchen", "status": {
                "created": 1689021185.5037804,
                "state": "edit",
                "action": "create",
                "since": 1689021185.5038593,
                "modified": 1689021185.5050585,
                "message": "draft created"
            },
            "data": {
                "keyword": ["Ray", "Bob"],
                "theme": ["Gretchen", "Deo"]

            }}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id] = rec.to_dict()

        id = "pdr0:0005"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "test 2", "deactivated": "null","owner":"not_self", "status": {
                "created": 1689021185.5037804,
                "state": "create",
                "action": "create",
                "since": 1689021185.5038593,
                "modified": 1689021185.5050585,
                "message": "draft created"
            }}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id] = rec.to_dict()

        constraint_wrong = {'$a,nkd': [
            {'$okn,r': [{'name': 'test 2'}, {'name': 'test3'}]}]}
        with self.assertRaises(SyntaxError) as context:
            recs = list(self.cli.adv_select_records(constraint_wrong,base.ACLs.READ))
        self.assertEqual(str(context.exception), "Wrong query format")
        recs = list(self.cli.adv_select_records(constraint_or))
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].id, "pdr0:0006")
        self.assertEqual(recs[1].id, "pdr0:0003")
        self.assertEqual(recs[1].data["keyword"][0], "Ray")
        self.assertEqual(recs[1].data["theme"][0], "Gretchen")

        recs = list(self.cli.adv_select_records(constraint_and))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].id, "pdr0:0003")
        recs = list(self.cli.adv_select_records(constraint_andor,base.ACLs.READ))
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].id, "pdr0:0006")
        self.assertEqual(recs[1].id, "pdr0:0003")

    def test_select_records_by_ids(self):
        # Inject some data into the database
        id1 = "pdr0:0002"
        rec1 = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id1, "name": "test 1"}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id1] = rec1.to_dict()

        id2 = "pdr0:0006"
        rec2 = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id2, "name": "test 2"}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id2] = rec2.to_dict()

        id3 = "pdr0:0003"
        rec3 = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id3, "name": "test3"}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id3] = rec3.to_dict()

        # Add a record with a different owner (should not be returned for this user)
        rec4 = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": "pdr0:0014", "name": "test5", "owner": "not_self"}, self.cli)
        self.cli._db[base.DMP_PROJECTS]["pdr0:0014"] = rec4.to_dict()

        # Test selecting by a subset of IDs
        ids = [id1, id3, "pdr0:0014"]
        recs = list(self.cli.select_records_by_ids(ids, base.ACLs.READ))
        rec_ids = [r.id for r in recs]
        self.assertIn(id1, rec_ids)
        self.assertIn(id3, rec_ids)
        self.assertNotIn("pdr0:0014", rec_ids)  # not owned by self.user

        # Test selecting with a single ID
        recs = list(self.cli.select_records_by_ids([id2], base.ACLs.READ))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].id, id2)

        # Test selecting with no matching IDs
        recs = list(self.cli.select_records_by_ids(["nonexistent"], base.ACLs.READ))
        self.assertEqual(len(recs), 0)
    
    def test_notify(self):
        """
        Test that a WebSocket message is sent when a record is created.
        """
        self.assertIsNone(self.cli.notifier)

        notifcfg = {
            "service_endpoint": "ws://localhost:8765",
            "broadcast_key": "secret_key"
        }
        fact = inmem.InMemoryDBClientFactory({ "client_notifier": notifcfg })
        self.cli = fact.create_client(base.DMP_PROJECTS, self.cfg, self.user)
        self.assertIsNotNone(self.cli.notifier)
        
        async def run_test():
            server = await websockets.serve(self.mock_websocket_server, "localhost", 8765)

            try:
                self.cli.create_record("test_record")
                await asyncio.sleep(1)
                self.assertEqual(len(self.received_messages), 1)
                self.assertIn("secret_key,proj-create,dmp,test_record", self.received_messages[0])

            finally:
                server.close()
                await server.wait_closed()
        asyncio.run(run_test())


    def test_select_records(self):
        # test query on existing but empty collection
        it = self.cli.select_records(base.ACLs.READ)

        self.assertTrue(hasattr(it, "__next__"),
                        "selection not in the form of an iterator")
        recs = list(it)

        self.assertEqual(len(recs), 0)

        # inject some data into the database
        id = "pdr0:0002"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "test 1"}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id] = rec.to_dict()

        id = "pdr0:0006"
        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": id, "name": "test 2"}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id] = rec.to_dict()

        id = "pdr0:0003"
        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": id, "name": "test3"}, self.cli)
        self.cli._db[base.DMP_PROJECTS][id] = rec.to_dict()

        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": "pdr0:0004", "name": "test4"}, self.cli)
        self.cli._db[base.DMP_PROJECTS]["pdr0:0004"] = rec.to_dict()

        # not working because owner is not self
        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": "pdr0:0014", "name": "test5","owner":"not_self"},
                                 self.cli)
        self.cli._db[base.DMP_PROJECTS]["pdr0:0014"] = rec.to_dict()

        recs = list(self.cli.select_records(base.ACLs.READ))
        self.assertEqual(len(recs), 4)
        self.assertTrue(isinstance(recs[0], base.ProjectRecord))
        self.assertEqual(recs[0].id, "pdr0:0002")

    def test_action_log_io(self):
        with self.assertRaises(ValueError):
            self.cli._save_action_data({'goob': 'gurn'})

        self.cli._save_action_data({'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertTrue('prov_action_log' in self.cli._db)
        self.assertTrue('goob:gurn' in self.cli._db['prov_action_log'])
        self.assertEqual(len(self.cli._db['prov_action_log']['goob:gurn']), 1)
        self.assertEqual(self.cli._db['prov_action_log']['goob:gurn'][0],
                         {'subject': 'goob:gurn', 'foo': 'bar'})

        self.cli._save_action_data({'subject': 'goob:gurn', 'bob': 'alice'})
        self.assertEqual(len(self.cli._db['prov_action_log']['goob:gurn']), 2)
        self.assertEqual(self.cli._db['prov_action_log']['goob:gurn'][0],
                         {'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(self.cli._db['prov_action_log']['goob:gurn'][1],
                         {'subject': 'goob:gurn', 'bob': 'alice'})

        self.cli._save_action_data({'subject': 'grp0001', 'dylan': 'bob'})
        self.assertTrue('prov_action_log' in self.cli._db)
        self.assertTrue('grp0001' in self.cli._db['prov_action_log'])
        self.assertEqual(len(self.cli._db['prov_action_log']['grp0001']), 1)
        self.assertEqual(self.cli._db['prov_action_log']['grp0001'][0],
                         {'subject': 'grp0001', 'dylan': 'bob'})

        acts = self.cli._select_actions_for("goob:gurn")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0], {'subject': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(acts[1], {'subject': 'goob:gurn', 'bob': 'alice'})
        acts = self.cli._select_actions_for("grp0001")
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts[0], {'subject': 'grp0001', 'dylan': 'bob'})

        self.cli._delete_actions_for("goob:gurn")
        self.assertTrue('prov_action_log' in self.cli._db)
        self.assertTrue('goob:gurn' not in self.cli._db['prov_action_log'])
        self.assertEqual(len(self.cli._db['prov_action_log']['grp0001']), 1)
        self.assertEqual(self.cli._db['prov_action_log']['grp0001'][0],
                         {'subject': 'grp0001', 'dylan': 'bob'})

        self.cli._delete_actions_for("grp0001")
        self.assertTrue('prov_action_log' in self.cli._db)
        self.assertTrue('goob:gurn' not in self.cli._db['prov_action_log'])
        self.assertTrue('grp0001' not in self.cli._db['prov_action_log'])

        self.assertEqual(self.cli._select_actions_for("goob:gurn"), [])
        self.assertEqual(self.cli._select_actions_for("grp0001"), [])

    def test_save_history(self):
        with self.assertRaises(ValueError):
            self.cli._save_history({'goob': 'gurn'})

        self.cli._save_history({'recid': 'goob:gurn', 'foo': 'bar'})
        self.cli._save_history({'recid': 'goob:gurn', 'alice': 'bob'})

        self.assertTrue('history' in self.cli._db)
        self.assertTrue('goob:gurn' in self.cli._db['history'])
        self.assertEqual(len(self.cli._db['history']['goob:gurn']), 2)
        self.assertEqual(self.cli._db['history']['goob:gurn'][0],
                         {'recid': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(self.cli._db['history']['goob:gurn'][1],
                         {'recid': 'goob:gurn', 'alice': 'bob'})

    def test_record_action(self):
        rec = self.cli.create_record("mine1")
        self.cli.record_action(Action(Action.CREATE, "pdr0:0001", testuser, "created"))
        self.cli.record_action(Action(Action.COMMENT, "pdr0:0001", testuser, "i'm hungry"))
        acts = self.cli._select_actions_for("pdr0:0001")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0]['type'], Action.CREATE)
        self.assertEqual(acts[1]['type'], Action.COMMENT)


if __name__ == '__main__':
    test.main()
