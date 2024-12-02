import os, json, pdb, logging,asyncio
from pathlib import Path
import unittest as test
import websockets

from nistoar.midas.dbio import inmem, base
from nistoar.pdr.utils.prov import Action, Agent
from nistoar.midas.dbio.notifier import Notifier

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

    @classmethod
    def initialize_notification_server(cls):
        notification_server = Notifier()
        try:
            cls.loop = asyncio.get_event_loop()
            if cls.loop.is_closed():
                raise RuntimeError
        except RuntimeError:
            cls.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(cls.loop)
        cls.loop.run_until_complete(notification_server.start())
        return notification_server
    
    @classmethod
    def setUpClass(cls):
        cls.notification_server = cls.initialize_notification_server()

    @classmethod
    def tearDownClass(cls):
        # Ensure the WebSocket server is properly closed
        cls.loop.run_until_complete(cls.notification_server.stop())
        cls.loop.run_until_complete(cls.notification_server.wait_closed())

        # Cancel all lingering tasks
        asyncio.set_event_loop(cls.loop)  # Set the event loop as the current event loop
        tasks = asyncio.all_tasks(loop=cls.loop)
        for task in tasks:
            task.cancel()
        cls.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

        # Close the event loop
        cls.loop.close()
    
    

    def setUp(self):
        self.cfg = {"goob": "gurn"}
        self.fact = inmem.InMemoryDBClientFactory(
            self.cfg,self.notification_server, {"nextnum": {"hank": 2}})

    def test_ctor(self):
        self.assertEqual(self.fact._cfg, self.cfg)
        self.assertTrue(self.fact._db)
        self.assertEqual(self.fact._db.get(base.DAP_PROJECTS), {})
        self.assertEqual(self.fact._db.get(base.DMP_PROJECTS), {})
        self.assertEqual(self.fact._db.get(base.GROUPS_COLL), {})
        self.assertEqual(self.fact._db.get(base.PEOPLE_COLL), {})
        self.assertEqual(self.fact._db.get("nextnum"), {"hank": 2})

    def test_create_client(self):
        cli = self.fact.create_client(base.DMP_PROJECTS, {}, "ava1")
        self.assertEqual(cli._db, self.fact._db)
        self.assertEqual(cli._cfg, self.fact._cfg)
        self.assertEqual(cli._projcoll, base.DMP_PROJECTS)
        self.assertEqual(cli._who, "ava1")
        self.assertIsNone(cli._whogrps)
        self.assertIs(cli._native, self.fact._db)
        self.assertIsNotNone(cli._dbgroups)


class TestInMemoryDBClient(test.TestCase):

    @classmethod
    def initialize_notification_server(cls):
        notification_server = Notifier()
        try:
            cls.loop = asyncio.get_event_loop()
        except RuntimeError:
            cls.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(cls.loop)
        cls.loop.run_until_complete(notification_server.start())
        return notification_server
    
    @classmethod
    def setUpClass(cls):
        cls.notification_server = cls.initialize_notification_server()

    @classmethod
    def tearDownClass(cls):
        # Ensure the WebSocket server is properly closed
        cls.loop.run_until_complete(cls.notification_server.stop())
        cls.loop.run_until_complete(cls.notification_server.wait_closed())

        # Cancel all lingering tasks
        asyncio.set_event_loop(cls.loop) 
        tasks = asyncio.all_tasks(loop=cls.loop)
        for task in tasks:
            task.cancel()
        cls.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))

        # Close the event loop
        cls.loop.close()

    def setUp(self):
        self.cfg = {"default_shoulder": "mds3"}
        self.user = "nist0:ava1"
        self.cli = inmem.InMemoryDBClientFactory({},self.notification_server).create_client(
            base.DMP_PROJECTS, self.cfg, self.user)
    

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
        self.cli.record_action(Action(Action.CREATE, "mds3:0001", testuser, "created"))
        self.cli.record_action(Action(Action.COMMENT, "mds3:0001", testuser, "i'm hungry"))
        acts = self.cli._select_actions_for("mds3:0001")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0]['type'], Action.CREATE)
        self.assertEqual(acts[1]['type'], Action.COMMENT)


class TestNotifier(test.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.notification_server = Notifier()
        self.loop = asyncio.get_event_loop()
        await self.notification_server.start()

        # Initialize the InMemoryDBClientFactory with the notification_server
        self.cfg = {"default_shoulder": "mds3"}
        self.user = "nist0:ava1"
        self.cli = inmem.InMemoryDBClientFactory({},self.notification_server).create_client(
            base.DMP_PROJECTS, self.cfg, self.user)

    async def asyncTearDown(self):
        await self.notification_server.stop()
        await self.notification_server.wait_closed()
        

    async def test_create_records_with_notifier(self):
        messages = []

        async def receive_messages(uri):
            try:
                async with websockets.connect(uri) as websocket:
                    while True:
                        message = await websocket.recv()
                        #print(f"Received message: {message}")
                        messages.append(message)
                        #print(f"Messages: {messages}")
                        # Break the loop after receiving the first message for this test
            except Exception as e:
                print(f"Failed to connect to WebSocket server: {e}")

        # Start the WebSocket client to receive messages
        uri = 'ws://localhost:8765'
        receive_task = asyncio.create_task(receive_messages(uri))
        await asyncio.sleep(2)

        # Inject some data into the database
        rec = self.cli.create_record("mine1")
        await asyncio.sleep(2)

        #print(f"Messages: {messages}")
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0], "New record created : mine1")

    

if __name__ == '__main__':
    test.main()
