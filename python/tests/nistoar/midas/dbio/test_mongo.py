import os, json, pdb, logging, tempfile,asyncio
from pathlib import Path
import unittest as test
import websockets

from pymongo import MongoClient

from nistoar.midas.dbio import mongo, base
from nistoar.midas import dbio
from nistoar.base.config import ConfigurationException

dburl = None
if os.environ.get('MONGO_TESTDB_URL'):
    dburl = os.environ.get('MONGO_TESTDB_URL')

testdir = Path(__file__).parents[0]
datadir = testdir / "data"

asc_andor = datadir / 'asc_andor.json'
asc_and   = datadir / 'asc_and.json'
asc_or    = datadir / 'asc_or.json'
dmp_path  = datadir / 'dmp.json'
asc_dates = datadir / 'asc_dates.json'
asc_text  = datadir / 'asc_text.json'
asc_keyandtheme = datadir / 'asc_keyandtheme.json'
asc_orkeywords = datadir / 'asc_orkeywords.json'

with open(asc_or, 'r') as file:
    constraint_or = json.load(file)

with open(asc_and, 'r') as file:
    constraint_and = json.load(file)

with open(asc_andor, 'r') as file:
    constraint_andor = json.load(file)

with open(dmp_path, 'r') as file:
    dmp = json.load(file)

with open(asc_dates, 'r') as file:
    constraint_dates = json.load(file)

with open(asc_text, 'r') as file:
    constraint_text = json.load(file)

with open(asc_keyandtheme, 'r') as file:
    constraint_keyandtheme = json.load(file)

with open(asc_orkeywords, 'r') as file:
    constraint_orkeywords = json.load(file)

@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestInMemoryDBClientFactory(test.TestCase):

    def setUp(self):
        self.cfg = {"goob": "gurn"}
        self.fact = mongo.MongoDBClientFactory(self.cfg, dburl)

    def tearDown(self):
        client = MongoClient(dburl)
        if not hasattr(client, 'get_database'):
            client.get_database = client.get_default_database
        db = client.get_database()
        if base.GROUPS_COLL in db.list_collection_names():
            db.drop_collection(base.GROUPS_COLL)
        if base.PEOPLE_COLL in db.list_collection_names():
            db.drop_collection(base.PEOPLE_COLL)
        if base.DMP_PROJECTS in db.list_collection_names():
            db.drop_collection(base.DMP_PROJECTS)
        if base.DRAFT_PROJECTS in db.list_collection_names():
            db.drop_collection(base.DRAFT_PROJECTS)
        if "nextnum" in db.list_collection_names():
            db.drop_collection("nextnum")

    def test_ctor(self):
        self.assertEqual(self.fact._cfg, self.cfg)
        self.assertEqual(self.fact._dburl, dburl)

        with self.assertRaises(ConfigurationException):
            mongo.MongoDBClientFactory(self.cfg)

    def test_create_client(self):
        cli = self.fact.create_client(base.DMP_PROJECTS, {}, "bob")
        self.assertEqual(cli._cfg, self.fact._cfg)
        self.assertEqual(cli._projcoll, base.DMP_PROJECTS)
        self.assertEqual(cli._who, "bob")
        self.assertIsNone(cli._whogrps)
        self.assertIsNone(cli._native)
        self.assertIsNotNone(cli._dbgroups)


@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestMongoDBClient(test.TestCase):

    def setUp(self):
        self.cfg = {}
        self.user = "nist0:ava1"
        self.cli = mongo.MongoDBClient(dburl, self.cfg, base.DMP_PROJECTS, self.user)
        self.received_messages = [] 
    
    async def mock_websocket_server(self, websocket):
        """
        Mock WebSocket server to capture messages sent by the client.
        """
        async for message in websocket:
            self.received_messages.append(message)

    def tearDown(self):
        client = MongoClient(dburl)
        if not hasattr(client, 'get_database'):
            client.get_database = client.get_default_database
        db = client.get_database()
        for coll in [base.GROUPS_COLL, base.PEOPLE_COLL, base.DMP_PROJECTS, base.DRAFT_PROJECTS,
                     "nextnum", "about", "prov_action_log", "history"]:
            if coll in db.list_collection_names():
                db.drop_collection(coll)

    def test_connect(self):
        self.assertEqual(self.cli._dburl, dburl)
        self.assertIsNone(self.cli._native)
        self.cli.connect()
        self.assertIsNotNone(self.cli._native)
        self.assertIsNotNone(self.cli.native)
        self.cli.disconnect()
        self.assertIsNone(self.cli._native)

    def test_auto_connect(self):
        self.assertEqual(self.cli._dburl, dburl)
        self.assertIsNone(self.cli._native)
        self.assertIsNotNone(self.cli.native)
        self.assertIsNotNone(self.cli._native)
        self.cli.disconnect()
        self.assertIsNone(self.cli._native)

    def test_next_recnum(self):
        self.assertEqual(self.cli._next_recnum("goob"), 1)
        self.assertEqual(self.cli._next_recnum("goob"), 2)
        self.assertEqual(self.cli._next_recnum("goob"), 3)
        self.assertEqual(self.cli._next_recnum("gary"), 1)
        self.assertEqual(self.cli._next_recnum("goober"), 1)
        self.assertEqual(self.cli._next_recnum("gary"), 2)

        slot = self.cli.native.nextnum.find_one({"slot": "goob"})
        self.assertEqual(slot["next"], 4)
        self.cli._try_push_recnum("goob", 2)
        slot = self.cli.native.nextnum.find_one({"slot": "goob"})
        self.assertEqual(slot["next"], 4)

        self.assertEqual(self.cli.native.nextnum.count_documents({"slot": "hank"}), 0)
        self.cli._try_push_recnum("hank", 2)
        self.assertEqual(self.cli.native.nextnum.count_documents({"slot": "hank"}), 0)

        self.cli._try_push_recnum("goob", 3)
        slot = self.cli.native.nextnum.find_one({"slot": "goob"})
        self.assertEqual(slot["next"], 3)

    def test_get_from_coll(self):
        # test query on unrecognized collection
        self.assertIsNone(self.cli._get_from_coll("alice", "p:bob"))

        # test query on a recognized but non-existent collection
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"))

        # put in some test data into the underlying database
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "p:bob", "owner": "alice"})
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"))
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:bob"),
                         {"id": "p:bob", "owner": "alice"})

        self.cli.native[base.GROUPS_COLL].insert_one({"id": "p:mine", "owner": "p:bob"})
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:mine"),
                         {"id": "p:mine", "owner": "p:bob"})
        self.assertEqual(self.cli._get_from_coll(base.GROUPS_COLL, "p:bob"),
                         {"id": "p:bob", "owner": "alice"})

#        self.assertIsNone(self.cli._get_from_coll("nextnum", "goob"))
#        self.cli.native["nextnum"].insert_one({"slot": "goob", "next": 0})
#        self.assertEqual(self.cli._get_from_coll("nextnum", "goob"), 0)


    def test_select_from_coll(self):
        # test query on unrecognized collection
        it = self.cli._select_from_coll("alice", owner="p:bob", hobby="knitting")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # test query on a recognized but existing collection
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "blah", "owner": "meh"})
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob", hobby="knitting"))
        self.assertEqual(len(recs), 0)

        # put in some test data into the underlying database
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "p:bob", "owner": "alice", "hobby": "whittling"})
        it = self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob", hobby="knitting")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="alice", hobby="whittling"))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0], {"id": "p:bob", "owner": "alice", "hobby": "whittling"})

        self.cli.native[base.GROUPS_COLL].insert_one({"id": "p:mine", "owner": "p:bob", "hobby": "whittling"})
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="alice", hobby="whittling"))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0], {"id": "p:bob", "owner": "alice", "hobby": "whittling"})
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, hobby="whittling"))
        self.assertEqual(len(recs), 2)

        # test deactivated filter
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "p:gang",
                                                      "owner": "p:bob", "deactivated": 1.2})
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob"))
        self.assertEqual(len(recs), 1)
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, incl_deact=True, owner="p:bob"))
        self.assertEqual(len(recs), 2)
        self.cli.native[base.GROUPS_COLL].find_one_and_update({"id": "p:gang"},
                                                              { "$set": { "deactivated": None } })
        recs = list(self.cli._select_from_coll(base.GROUPS_COLL, owner="p:bob"))
        self.assertEqual(len(recs), 2)

    def test_select_prop_contains(self):
        # test query on unrecognized collection
        it = self.cli._select_prop_contains("alice", "hobbies", "whittling")
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # test query on a recognized and existing collection
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "blah", "owner": "meh"})
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "alice"))
        self.assertEqual(len(recs), 0)

        # put in some test data into the underlying database
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "p:bob", "members": ["p:bob"]})
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "stars", "members": ["p:tom", "p:bob"]})
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "alice"))
        self.assertEqual(len(recs), 0)
        ids = [r["id"] for r in self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob")]
        self.assertIn("p:bob", ids)
        self.assertIn("stars", ids)
        self.assertEqual(len(ids), 2)

        self.cli.native[base.GROUPS_COLL].find_one_and_update({"id": "p:bob"},
                                                              {"$push": {"members": "alice"}})
        it = self.cli._select_prop_contains(base.GROUPS_COLL, "members", "alice")
            
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0], {"id": "p:bob", "members": ["p:bob", "alice"]})

        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 2)
        self.assertEqual(set([r.get('id') for r in recs]), set("p:bob stars".split()))

        # test deactivated filter
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "p:gang",
                                                      "members": ["p:bob"], "deactivated": 1.2})
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 2)
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob", incl_deact=True))
        self.assertEqual(len(recs), 3)
        self.cli.native[base.GROUPS_COLL].find_one_and_update({"id": "p:gang"},
                                                              { "$set": { "deactivated": None } })
        recs = list(self.cli._select_prop_contains(base.GROUPS_COLL, "members", "p:bob"))
        self.assertEqual(len(recs), 3)

    def test_delete_from(self):
        # test delete on unrecognized, non-existent collection
        self.assertFalse(self.cli._delete_from("alice", "p:bob"))

        # test query on existing but empty collection
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "blah", "owner": "meh"})
        self.assertFalse(self.cli._delete_from(base.GROUPS_COLL, "p:bob"))

        self.cli.native[base.GROUPS_COLL].insert_one({"id": "p:bob", "members": ["p:bob"]})
        self.cli.native[base.GROUPS_COLL].insert_one({"id": "stars", "members": ["p:tom", "p:bob"]})
        self.assertTrue(self.cli.native[base.GROUPS_COLL].find_one({"id": "p:bob"}))
        self.assertTrue(self.cli.native[base.GROUPS_COLL].find_one({"id": "stars"}))

        self.assertTrue(self.cli._delete_from(base.GROUPS_COLL, "p:bob"))
        self.assertTrue(not self.cli.native[base.GROUPS_COLL].find_one({"id": "p:bob"}))
        self.assertTrue(self.cli.native[base.GROUPS_COLL].find_one({"id": "stars"}))
            
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

        # test on a recognized collection
#        self.cli.native[base.GROUPS_COLL].insert_one({"id": "blah", "owner": "meh"})
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "g:friends"))
        self.assertIsNone(self.cli._get_from_coll(base.GROUPS_COLL, "stars"))

        self.assertTrue(self.cli._upsert(base.GROUPS_COLL, {"id": "p:bob", "members": ["p:bob"]}))
        rec = self.cli._get_from_coll(base.GROUPS_COLL, "p:bob")
        self.assertEqual(rec, {"id": "p:bob", "members": ["p:bob"]})
        rec['members'].append("alice")
        self.cli._upsert(base.GROUPS_COLL, rec)
        rec2 = self.cli._get_from_coll(base.GROUPS_COLL, "p:bob")
        self.assertEqual(rec2, {"id": "p:bob", "members": ["p:bob", "alice"]})

    def test_notify(self):
        """
        Test that a WebSocket message is sent when a record is created.
        """
        async def run_test():
            server = await websockets.serve(self.mock_websocket_server, "localhost", 8765)

            try:
                self.cli.create_record("test_record")
                await asyncio.sleep(1)
                self.assertEqual(len(self.received_messages), 1)
                self.assertIn("New dmp record created: test_record", self.received_messages[0])

            finally:
                server.close()
                await server.wait_closed()
        asyncio.run(run_test())

    def test_select_records(self):
        # test query on a recognized but empty collection
        it = self.cli.select_records()
        self.assertTrue(hasattr(it, "__next__"), "selection not in the form of an iterator")
        recs = list(it)
        self.assertEqual(len(recs), 0)

        # inject some data into the database
        id = "pdr0:0002"
        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": id}, self.cli)
        self.cli.native[base.DMP_PROJECTS].insert_one(rec.to_dict())

        id = "pdr0:0003"
        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": id}, self.cli)
        self.cli.native[base.DMP_PROJECTS].insert_one(rec.to_dict())

        rec = base.ProjectRecord(base.DMP_PROJECTS, {"id": "goob", "owner": "alice"}, self.cli)
        self.cli.native[base.DMP_PROJECTS].insert_one(rec.to_dict())

        recs = list(self.cli.select_records())

        self.assertEqual(len(recs), 2)
        self.assertTrue(isinstance(recs[0], base.ProjectRecord))
        self.assertEqual(recs[1].id, id)


    def test_adv_select_records(self):

        self.cli.native[base.DMP_PROJECTS].create_index([("$**", "text")], weights={"name": 2})

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
            },
            "data": {
                "keywords": ["Chemistry", "Bob"],
                "theme": ["Physics", "Deo"]

            }
            }, self.cli)
        self.cli.native[base.DMP_PROJECTS].insert_one(rec.to_dict())

        id = "pdr0:0006"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "test 2", "status": {
                "created": 1689021185.5037804,
                "state": "edit",
                "action": "create",
                "since": 1689021185.5038593,
                "modified": 1689021180.5050585,
                "message": "draft created"
            },
            "data": {
                "keywords": ["Ray", "Bob"],
                "theme": ["Gretchen", "Deo"]

            }
            }, self.cli)
        self.cli.native[base.DMP_PROJECTS].insert_one(rec.to_dict())

        id = "pdr0:0003"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "test3", "status": {
                "created": 1689021185.5037804,
                "state": "edit",
                "action": "create",
                "since": 1689021185.5038593,
                "modified": 1689021185.5050585,
                "message": "draft created"
            }}, self.cli)
        self.cli.native[base.DMP_PROJECTS].insert_one(rec.to_dict())

        id = "pdr0:0008"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "qwerty", "status": {
                "created": 1689021185.5037804,
                "state": "edit",
                "action": "create",
                "since": 1689021185.5038593,
                "modified": 1689021183.5050585,
                "message": "test"
            }}, self.cli)
        self.cli.native[base.DMP_PROJECTS].insert_one(rec.to_dict())

        id = "pdr0:0007"
        rec = base.ProjectRecord(
            base.DMP_PROJECTS, {"id": id, "name": "test3", "owner": "alice", "status": {
                "created": 1689021185.5037804,
                "state": "edit",
                "action": "create",
                "since": 1689021185.5038593,
                "modified": 1689021189.5050585,
                "message": "draft created"
            }}, self.cli)
        self.cli.native[base.DMP_PROJECTS].insert_one(rec.to_dict())


        constraint_wrong = {'$a,nkd': [
            {'$okn,r': [{'name': 'test 2'}, {'name': 'test3'}]}]}
        with self.assertRaises(SyntaxError) as context:
            recs = list(self.cli.adv_select_records(constraint_wrong))
        self.assertEqual(str(context.exception), "Wrong query format")
        recs = list(self.cli.adv_select_records(constraint_or, base.ACLs.READ))
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].id, "pdr0:0006")
        self.assertEqual(recs[1].id, "pdr0:0003")

        recs = list(self.cli.adv_select_records(constraint_and))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].id, "pdr0:0003")
        recs = list(self.cli.adv_select_records(constraint_andor, base.ACLs.READ))
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].id, "pdr0:0006")
        self.assertEqual(recs[1].id, "pdr0:0003")

        recs = list(self.cli.adv_select_records(constraint_dates, base.ACLs.READ))
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].id, "pdr0:0002")
        self.assertEqual(recs[1].id, "pdr0:0003")

        recs = list(self.cli.adv_select_records(constraint_text, base.ACLs.READ))
        self.assertEqual(len(recs), 3)
        self.assertEqual(recs[0].id, "pdr0:0006")
        self.assertEqual(recs[1].id, "pdr0:0002")
        self.assertEqual(recs[2].id, "pdr0:0008")

        recs = list(self.cli.adv_select_records(constraint_keyandtheme, base.ACLs.READ))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0].id, "pdr0:0006")

        recs = list(self.cli.adv_select_records(constraint_orkeywords, base.ACLs.READ))
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0].id, "pdr0:0002")
        self.assertEqual(recs[1].id, "pdr0:0006")

        

    def test_action_log_io(self):
        self.assertEqual(self.cli.native['prov_action_log'].count_documents({}), 0)
        self.cli._save_action_data({'subject': 'goob:gurn', 'foo': 'bar', 'timestamp': 8})
        acts = [r for r in self.cli.native['prov_action_log'].find({}, {'_id': False})]
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts[0], {'subject': 'goob:gurn', 'foo': 'bar', 'timestamp': 8})

        self.cli._save_action_data({'subject': 'goob:gurn', 'bob': 'alice', 'timestamp': 5})
        acts = [r for r in self.cli.native['prov_action_log'].find({}, {'_id': False})]
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0], {'subject': 'goob:gurn', 'foo': 'bar', 'timestamp': 8})
        self.assertEqual(acts[1], {'subject': 'goob:gurn', 'bob': 'alice', 'timestamp': 5})

        self.assertEqual(self.cli.native['prov_action_log'].count_documents({'subject': 'grp0001'}), 0)
        self.cli._save_action_data({'subject': 'grp0001', 'dylan': 'bob'})
        self.assertEqual(self.cli.native['prov_action_log'].count_documents({}), 3)
        acts = [r for r in self.cli.native['prov_action_log'].find({'subject': 'grp0001'}, {'_id': False})]
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts[0], {'subject': 'grp0001', 'dylan': 'bob'})

        # _select_actions_for() will return the actions sorted by timestamp
        acts = self.cli._select_actions_for("goob:gurn")
        self.assertEqual(len(acts), 2)
        self.assertEqual(acts[0], {'subject': 'goob:gurn', 'bob': 'alice', 'timestamp': 5})
        self.assertEqual(acts[1], {'subject': 'goob:gurn', 'foo': 'bar', 'timestamp': 8})
        acts = self.cli._select_actions_for("grp0001")
        self.assertEqual(len(acts), 1)
        self.assertEqual(acts[0], {'subject': 'grp0001', 'dylan': 'bob'})

        self.cli._delete_actions_for("grp0001")
        self.assertEqual(self.cli.native['prov_action_log'].count_documents({}), 2)
        self.assertEqual(self.cli.native['prov_action_log'].count_documents({'subject': 'grp0001'}), 0)
        self.cli._delete_actions_for("goob:gurn")
        self.assertEqual(self.cli.native['prov_action_log'].count_documents({}), 0)

        self.assertEqual(self.cli._select_actions_for("goob:gurn"), [])
        self.assertEqual(self.cli._select_actions_for("grp0001"), [])

    def test_save_history(self):
        self.assertEqual(self.cli.native['history'].count_documents({}), 0)
        self.cli._save_history({'recid': 'goob:gurn', 'foo': 'bar'})
        self.cli._save_history({'recid': 'pdr0:0001', 'alice': 'bob'})

        data = [r for r in self.cli.native['history'].find({}, {'_id': False})]
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0], {'recid': 'goob:gurn', 'foo': 'bar'})
        self.assertEqual(data[1], {'recid': 'pdr0:0001', 'alice': 'bob'})


@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestMongoProjectRecord(test.TestCase):

    def setUp(self):
        self.fact = mongo.MongoDBClientFactory({}, dburl)
        self.user = "nist0:ava1"
        self.cli = self.fact.create_client(base.DRAFT_PROJECTS, {}, self.user)
        self.rec = base.ProjectRecord(base.DRAFT_PROJECTS,
                                      {"id": "pdr0:2222", "name": "brains", "owner": self.user}, self.cli)

    def tearDown(self):
        client = MongoClient(dburl)
        if not hasattr(client, 'get_database'):
            client.get_database = client.get_default_database
        db = client.get_database()
        for coll in [base.GROUPS_COLL, base.PEOPLE_COLL, base.DMP_PROJECTS, base.DRAFT_PROJECTS,
                     "nextnum", "about"]:
            if coll in db.list_collection_names():
                db.drop_collection(coll)

    def test_save(self):
        self.assertEqual(self.rec.data, {})
        self.assertEqual(self.rec.meta, {})
        self.assertIsNone(self.cli.native[base.DRAFT_PROJECTS].find_one({"id": "pdr0:2222"}))

        self.rec.save()
        data = self.cli.native[base.DRAFT_PROJECTS].find_one({"id": "pdr0:2222"})
        self.assertIsNotNone(data)
        self.assertEqual(data['name'], "brains")
        self.assertEqual(data['data'], {})
        self.assertEqual(data['meta'], {})
        self.assertEqual(data['acls'][base.ACLs.READ], [self.user])

        self.rec.meta['type'] = 'software'
        self.rec.acls.grant_perm_to(base.ACLs.READ, "alice")
        self.rec.save()
        data = self.cli.native[base.DRAFT_PROJECTS].find_one({"id": "pdr0:2222"})
        self.assertEqual(data['meta'], {"type": "software"})
        self.assertEqual(data['acls'][base.ACLs.READ], [self.user, "alice"])

        rec = self.cli.get_record_for("pdr0:2222")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.meta,  {"type": "software"})
        self.assertTrue(rec.authorized(base.ACLs.READ, "alice"))

    def test_authorized(self):
        self.assertTrue(self.rec.authorized(base.ACLs.READ))
        self.assertTrue(self.rec.authorized(base.ACLs.WRITE))
        self.assertTrue(self.rec.authorized(base.ACLs.ADMIN))
        self.assertTrue(self.rec.authorized(base.ACLs.DELETE))
        self.assertTrue(self.rec.authorized([base.ACLs.READ, base.ACLs.WRITE]))
        self.assertTrue(self.rec.authorized(base.ACLs.OWN))
        self.assertFalse(self.rec.authorized(base.ACLs.ADMIN, "gary"))
        self.assertFalse(self.rec.authorized(base.ACLs.DELETE, "gary"))
        self.assertFalse(self.rec.authorized([base.ACLs.READ, base.ACLs.WRITE], "gary"))


@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestMongoDBGroups(test.TestCase):

    def setUp(self):
        self.cfg = { "default_shoulder": "pdr0" }
        self.fact = mongo.MongoDBClientFactory(self.cfg, dburl)
        self.user = "nist0:ava1"
        self.cli = self.fact.create_client(base.DMP_PROJECTS, {}, self.user)
        self.dbg = self.cli.groups

    def tearDown(self):
        client = MongoClient(dburl)
        if not hasattr(client, 'get_database'):
            client.get_database = client.get_default_database
        db = client.get_database()
        for coll in [base.GROUPS_COLL, base.PEOPLE_COLL, base.DMP_PROJECTS, base.DRAFT_PROJECTS,
                     "nextnum", "about"]:
            if coll in db.list_collection_names():
                db.drop_collection(coll)

    def test_create_group(self):
        # group does not exist yet
        id = "grp0:nist0:ava1:enemies"
        self.assertIsNone(self.cli.native[base.GROUPS_COLL].find_one({"id": id}))

        grp = self.dbg.create_group("enemies")
        self.assertEqual(grp.name, "enemies")
        self.assertEqual(grp.owner, self.user)
        self.assertEqual(grp.id, id)
        self.assertTrue(grp.is_member(self.user))

        self.assertTrue(grp.authorized(base.ACLs.OWN))

        # group record was saved to db
        self.assertIsNotNone(self.cli.native[base.GROUPS_COLL].find_one({"id": id}))

        with self.assertRaises(base.AlreadyExists):
            grp = self.dbg.create_group("enemies")

        with self.assertRaises(base.NotAuthorized):
            grp = self.dbg.create_group("friends", "alice")

        self.cli._cfg['superusers'] = [self.user]
        grp = self.dbg.create_group("friends", "alice")
        self.assertEqual(grp.name, "friends")
        self.assertEqual(grp.owner, "alice")
        self.assertEqual(grp.id, "grp0:alice:friends")
        self.assertTrue(grp.is_member("alice"))
        self.assertTrue(not grp.is_member(self.user))

    def test_get(self):
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:friends"))
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:enemies"))
        with self.assertRaises(KeyError):
            self.dbg["grp0:nist0:ava1:friends"]
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:enemies"))

        self.dbg.create_group("friends")
        self.dbg.create_group("enemies")
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:enemies"))

        grp = self.dbg.get("grp0:nist0:ava1:friends")
        self.assertEqual(grp.id, "grp0:nist0:ava1:friends")
        self.assertEqual(grp.name, "friends")
        self.assertEqual(grp.owner, "nist0:ava1")
        grp = self.dbg.get("grp0:nist0:ava1:enemies")
        self.assertEqual(grp.id, "grp0:nist0:ava1:enemies")
        self.assertEqual(grp.name, "enemies")
        self.assertEqual(grp.owner, "nist0:ava1")
        grp = self.dbg["grp0:nist0:ava1:enemies"]
        self.assertEqual(grp.id, "grp0:nist0:ava1:enemies")
        self.assertEqual(grp.name, "enemies")
        self.assertEqual(grp.owner, "nist0:ava1")

    def test_get_by_name(self):
        self.assertIsNone(self.dbg.get_by_name("friends"))
        self.assertIsNone(self.dbg.get_by_name("enemies"))

        self.dbg.create_group("friends")
        self.dbg.create_group("enemies")

        grp = self.dbg.get_by_name("friends")
        self.assertEqual(grp.id, "grp0:nist0:ava1:friends")
        self.assertEqual(grp.name, "friends")
        self.assertEqual(grp.owner, "nist0:ava1")
        grp = self.dbg.get_by_name("enemies", self.user)
        self.assertEqual(grp.id, "grp0:nist0:ava1:enemies")
        self.assertEqual(grp.name, "enemies")
        self.assertEqual(grp.owner, "nist0:ava1")

        self.assertIsNone(self.dbg.get_by_name("friends", "alice"))

        self.cli._cfg['superusers'] = [self.user]
        grp = self.dbg.create_group("friends", "alice")
        grp = self.dbg.get_by_name("friends", "alice")
        self.assertEqual(grp.id, "grp0:alice:friends")
        self.assertEqual(grp.name, "friends")
        self.assertEqual(grp.owner, "alice")

    def test_delete(self):
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:friends"))
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:enemies"))
        with self.assertRaises(KeyError):
            self.dbg["grp0:nist0:ava1:friends"]
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:enemies"))

        self.dbg.create_group("friends")
        self.dbg.create_group("enemies")

        self.assertIsNotNone(self.dbg.get("grp0:nist0:ava1:friends"))
        grp = self.dbg.get("grp0:nist0:ava1:enemies")
        self.assertIsNotNone(grp)
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:friends"))
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:enemies"))

        self.dbg.delete_group(grp.id)
        self.assertIsNotNone(self.dbg.get("grp0:nist0:ava1:friends"))
        self.assertIsNone(self.dbg.get("grp0:nist0:ava1:enemies"))
        self.assertTrue(not self.dbg.exists("grp0:nist0:ava1:enemies"))
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:friends"))

        grp.save()
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:enemies"))
        self.assertTrue(self.dbg.exists("grp0:nist0:ava1:friends"))

    def test_select_ids_for_user(self):
        for s in "abcdefghijklmnopqrstuvwxyz":
            self.dbg.create_group("group-"+s)

        ns = "grp0:nist0:ava1:group-"
        for s in "rlp":
            grp = self.dbg.get(ns+s)
            grp.add_member("ray")
            grp.save()
        for s in "abtp":
            grp = self.dbg.get(ns+s)
            grp.add_member("alice")
            grp.save()

        self.assertTrue(self.dbg.get(ns+"r").is_member("ray"))
        self.assertTrue(self.dbg.get(ns+"b").is_member("alice"))
        self.assertTrue(self.dbg.get(ns+"p").is_member("ray"))
        self.assertTrue(self.dbg.get(ns+"p").is_member("alice"))

        # no recursive resolving needed
        matches = list(self.dbg.select_ids_for_user("ray"))
        self.assertIn(ns+"r", matches)
        self.assertIn(ns+"l", matches)
        self.assertIn(ns+"p", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 4)

        matches = list(self.dbg.select_ids_for_user("alice"))
        self.assertIn(ns+"a", matches)
        self.assertIn(ns+"b", matches)
        self.assertIn(ns+"t", matches)
        self.assertIn(ns+"p", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 5)

        # one-level recursive resolving
        grp = self.dbg.get(ns+"b")
        grp.add_member(ns+"l")
        grp.save()
        matches = list(self.dbg.select_ids_for_user("ray"))
        self.assertIn(ns+"r", matches)
        self.assertIn(ns+"l", matches)
        self.assertIn(ns+"p", matches)
        self.assertIn(ns+"b", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 5)

        matches = list(self.dbg.select_ids_for_user("alice"))
        self.assertIn(ns+"a", matches)
        self.assertIn(ns+"b", matches)
        self.assertIn(ns+"t", matches)
        self.assertIn(ns+"p", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 5)

        # two-level recursive resolving
        grp = self.dbg.get(ns+"l")
        grp.add_member(ns+"t")
        grp.save()
        grp = self.dbg.get(ns+"t")
        grp.add_member("gary")
        grp.save()
        matches = list(self.dbg.select_ids_for_user("gary"))
        self.assertIn(ns+"b", matches)
        self.assertIn(ns+"l", matches)
        self.assertIn(ns+"t", matches)
        self.assertIn(base.PUBLIC_GROUP, matches)
        self.assertEqual(len(matches), 4)


if __name__ == '__main__':
    test.main()
