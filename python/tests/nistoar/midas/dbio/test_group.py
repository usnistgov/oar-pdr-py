"""
Unit tests for the group.py module in midas/dbio/wsgi.

It tests:
  - Creating a group (POST /midas/group/grp0)
  - Getting a group (GET /midas/group/grp0/<group_id>)
  - Adding members (POST /midas/group/grp0/<group_id>)
  - Replacing membership (PUT /midas/group/grp0/<group_id>)
  - Deleting group entirely (DELETE /midas/group/grp0/<group_id>)
  - Deleting one member from group (DELETE /midas/group/grp0/<group_id>/<member>)
"""

import os
import json
import logging
import unittest as test
import tempfile
from io import StringIO
from collections import OrderedDict

from nistoar.midas.dbio.inmem import InMemoryDBClientFactory
from nistoar.midas.dbio.mongo import MongoDBClientFactory
from nistoar.midas.dbio.wsgi import group
from nistoar.pdr.utils.prov import Agent

tmpdir = tempfile.TemporaryDirectory(prefix="_test_group.")
rootlog = logging.getLogger()
loghdlr = None

def setUpModule():
    global loghdlr
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name, "test_group.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        rootlog.removeHandler(loghdlr)
        loghdlr.flush()
        loghdlr.close()
    tmpdir.cleanup()

test_agent = Agent("midas", Agent.USER, "tester1", "midas")


class TestMIDASGroupApp(test.TestCase):
    """
    Test the group endpoints using an in-memory DB.
    """

    def start(self, status, headers=None, exc_info=None):
        self.resp.append(status)
        if headers:
            for h in headers:
                self.resp.append(f"{h[0]}: {h[1]}")

    def body2dict(self, body):
        # Convert the list of byte strings into a single JSON, parse with OrderedDict
        text = "\n".join([b.decode("utf-8") for b in body])
        try:
            return json.loads(text, object_pairs_hook=OrderedDict)
        except json.JSONDecodeError:
            return text  # If it's not JSON, just return the raw text

    def setUp(self):
        self.cfg = {
            # Could contain DBIO config like superusers, etc.
            "dbio": {
                "superusers": ["adminuser"],
                # e.g. "allowed_group_shoulders": ["grp0"], etc.
            }
        }
        self.dbfact = InMemoryDBClientFactory({}, {})
        # Create the GroupServiceFactory
        self.svcfactory = group.GroupServiceFactory(self.dbfact, self.cfg, rootlog)
        # Create the main WSGI app
        self.app = group.MIDASGroupApp(self.svcfactory, rootlog, self.cfg)

        self.rootpath = "/midas/group/"
        self.resp = []

    def test_no_shoulder_provided(self):
        """
        If we call /midas/group/ with no <shoulder>, we expect a 500 or 400
        because group.py expects a shoulder.
        """
        path = ""
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        # This should fail because group.py checks for a shoulder
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("405 ", self.resp[0])

    def test_create_group(self):
        """
        POST /midas/group/grp0 -> create new group
        """
        path = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path,
        }
        # Provide JSON input: { "name": "my-test-group" }
        req['wsgi.input'] = StringIO(json.dumps({"name": "my-test-group"}))

        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()

        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        self.assertEqual(resp['name'], "my-test-group")
        self.assertEqual(resp['owner'], "tester1")  # The default is the user making the request
        # The ID can be "grp0:tester1:my-test-group" or similar
        self.assertIn("grp0:tester1:", resp['id'])
        self.assertEqual(resp['members'], ["tester1"])  # By default, the group is owned by user

    def test_create_group_missing_name(self):
        """
        POST /midas/group/grp0 with no 'name' property -> 400
        """
        path = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path,
        }
        req['wsgi.input'] = StringIO(json.dumps({"foruser": "someone"}))
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()

        self.assertIn("400 ", self.resp[0])
        # Check the error message
        self.assertIn("Missing name property", "".join(self.resp))

    def test_get_group_notfound(self):
        """
        GET /midas/group/grp0/<nonexistentID> => 404
        """
        path = "grp0/doesnotexist"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])
        self.assertIn("Group not found", "".join(self.resp))

    def test_get_group_found(self):
        """
        GET /midas/group/grp0/<group_id> => 200 + group record
        """
        # Create the group
        create_path = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + create_path,
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "found-group"}))
        hdlr = self.app.create_handler(req, self.start, create_path, test_agent)
        body = hdlr.handle()
        resp = self.body2dict(body)
        gid = resp['id']

        # GET the group
        self.resp = []
        path = f"grp0/{gid}"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        retrieved = self.body2dict(body)
        self.assertEqual(retrieved['name'], "found-group")
        self.assertEqual(retrieved['owner'], "tester1")
        self.assertIn("grp0:tester1:", retrieved['id'])

    def test_add_members(self):
        """
        POST /midas/group/grp0/<group_id> => add members
        Body can be single string or list
        """
        # create the group
        cpath = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + cpath
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "coolteam"}))
        hdlr = self.app.create_handler(req, self.start, cpath, test_agent)
        body = hdlr.handle()
        resp = self.body2dict(body)
        gid = resp['id']

        self.resp = []
        # Add members => single string
        path = f"grp0/{gid}"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path,
        }
        req['wsgi.input'] = StringIO(json.dumps("alice"))
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        updated = self.body2dict(body)
        self.assertEqual(updated, ["tester1", "alice"])

        # Add more => a list
        self.resp = []
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path,
        }
        req['wsgi.input'] = StringIO(json.dumps(["bob", "charlie"]))
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        updated = self.body2dict(body)
        self.assertEqual(updated, ["tester1", "alice", "bob", "charlie"])

    def test_replace_members(self):
        """
        PUT /midas/group/grp0/<group_id> => replace membership entirely
        """
        # create the group
        cpath = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + cpath
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "myteam"}))
        hdlr = self.app.create_handler(req, self.start, cpath, test_agent)
        body = hdlr.handle()
        resp = self.body2dict(body)
        gid = resp['id']

        # Now replace membership
        self.resp = []
        path = f"grp0/{gid}"
        req = {
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': self.rootpath + path,
        }
        req['wsgi.input'] = StringIO(json.dumps(["zach", "yolanda"]))
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        replaced = self.body2dict(body)
        self.assertEqual(replaced, ["zach", "yolanda"])

        # confirm membership actually changed
        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        groupinfo = self.body2dict(body)
        self.assertIn("zach", groupinfo['members'])
        self.assertNotIn("tester1", groupinfo['members'])

    def test_delete_group(self):
        """
        DELETE /midas/group/grp0/<group_id> => delete entire group
        """
        # create group
        cpath = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + cpath
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "delteam"}))
        hdlr = self.app.create_handler(req, self.start, cpath, test_agent)
        body = hdlr.handle()
        resp = self.body2dict(body)
        gid = resp['id']

        # check get
        self.resp = []
        path = f"grp0/{gid}"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])

        # delete
        self.resp = []
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        body_text = "".join(b.decode("utf-8") for b in body)
        self.assertIn("Group deleted", body_text)

        # confirm it's gone
        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

    def test_delete_single_member(self):
        """
        DELETE /midas/group/grp0/<group_id>/<member_id> => remove that user from membership
        """
        # create group
        cpath = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + cpath
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "unitsquad"}))
        hdlr = self.app.create_handler(req, self.start, cpath, test_agent)
        body = hdlr.handle()
        resp = self.body2dict(body)
        gid = resp['id']

        # add some members
        self.resp = []
        path = f"grp0/{gid}"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path
        }
        req['wsgi.input'] = StringIO(json.dumps(["alpha", "beta"]))
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        updated = self.body2dict(body)
        self.assertEqual(updated, ["tester1", "alpha", "beta"])

        # remove "alpha"
        self.resp = []
        path = f"grp0/{gid}/alpha"
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        body_text = "".join(b.decode("utf-8") for b in body)
        self.assertIn("Removed alpha from group", body_text)

        # confirm "alpha" is gone
        self.resp = []
        path = f"grp0/{gid}"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        record = self.body2dict(body)
        self.assertNotIn("alpha", record['members'])
        self.assertIn("beta", record['members'])

        # try removing "alice" who is not a member
        self.resp = []
        path = f"grp0/{gid}/alice"
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        # "No change; alice was not a member."
        self.assertIn("200 ", self.resp[0])
        body_text = "".join(b.decode("utf-8") for b in body)
        self.assertIn("No change; alice was not a member", body_text)


@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "Mongo DB not available for testing")
class TestMIDASGroupAppMongo(test.TestCase):
    """
    Repeat the same style of tests as above, but with a MongoDB-based factory,
    so we confirm the group code works with a real DB as well.
    """

    def start(self, status, headers=None, exc_info=None):
        self.resp.append(status)
        if headers:
            for h in headers:
                self.resp.append(f"{h[0]}: {h[1]}")

    def body2dict(self, body):
        text = "\n".join([b.decode("utf-8") for b in body])
        try:
            return json.loads(text, object_pairs_hook=OrderedDict)
        except json.JSONDecodeError:
            return text

    def setUp(self):
        self.cfg = {
            "dbio": {
                "superusers": ["adminuser"],
            }
        }
        self.dburl = os.environ['MONGO_TESTDB_URL']
        self.dbfact = MongoDBClientFactory({}, self.dburl)
        self.svcfactory = group.GroupServiceFactory(self.dbfact, self.cfg, rootlog)
        self.app = group.MIDASGroupApp(self.svcfactory, rootlog, self.cfg)
        self.rootpath = "/midas/group/"
        self.resp = []

        # Clean up test collections
        cli = self.dbfact.create_client("groups", self.cfg.get("dbio", {}), "tester1")
        cli.native.drop_collection("groups")

    def tearDown(self):
        # Clean up after tests
        cli = self.dbfact.create_client("groups", self.cfg.get("dbio", {}), "tester1")
        cli.native.drop_collection("groups")

    def test_create_get_delete_group(self):
        """
        Basic create, retrieve, delete cycle
        """
        # create
        path = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path,
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "mongo-team"}))
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("201 ", self.resp[0])
        resp = self.body2dict(body)
        gid = resp['id']
        self.assertIn("grp0:tester1:", gid)
        self.assertEqual(resp['name'], "mongo-team")

        # get
        self.resp = []
        path = f"grp0/{gid}"
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path,
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        got = self.body2dict(body)
        self.assertEqual(got['name'], "mongo-team")
        self.assertEqual(got['members'], ["tester1"])

        # delete
        self.resp = []
        req = {
            'REQUEST_METHOD': 'DELETE',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])

        # confirm not found
        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + path
        }
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("404 ", self.resp[0])

    def test_add_members_mongo(self):
        """
        POST to add members with real mongo
        """
        # create group
        path = "grp0"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path,
        }
        req['wsgi.input'] = StringIO(json.dumps({"name": "devops-team"}))
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        created = self.body2dict(body)
        gid = created['id']

        self.resp = []
        path = f"grp0/{gid}"
        req = {
            'REQUEST_METHOD': 'POST',
            'PATH_INFO': self.rootpath + path,
        }
        req['wsgi.input'] = StringIO(json.dumps(["eve", "frank"]))
        hdlr = self.app.create_handler(req, self.start, path, test_agent)
        body = hdlr.handle()
        self.assertIn("200 ", self.resp[0])
        updated = self.body2dict(body)
        self.assertEqual(updated, ["tester1", "eve", "frank"])

        # retrieve and confirm
        self.resp = []
        req = {
            'REQUEST_METHOD': 'GET',
            'PATH_INFO': self.rootpath + f"grp0/{gid}",
        }
        hdlr = self.app.create_handler(req, self.start, f"grp0/{gid}", test_agent)
        body = hdlr.handle()
        groupinfo = self.body2dict(body)
        self.assertEqual(groupinfo['members'], ["tester1", "eve", "frank"])


if __name__ == '__main__':
    test.main()
