import os, json, pdb, logging, tempfile, re
import unittest as test
from collections.abc import Mapping

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio import project, status
from nistoar.pdr.utils import prov

tmpdir = tempfile.TemporaryDirectory(prefix="_test_project.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_pdp.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

nistr = prov.Agent("midas", prov.Agent.USER, "nstr1", "midas")

class TestProjectService(test.TestCase):

    def setUp(self):
        self.cfg = {
            "default_perms": {
                "read":  ["grp0:public"],
                "edit":  ["grp0:overlord"]
            },
            "dbio": {
                "project_id_minting": {
                    "allowed_shoulders": {
                        "public": [],
                        "midas":  ["mdm0", "mdm1", "spc1"]
                    },
                    "default_shoulder": { "public": "mdm1" },
                    "localid_providers": { "midas": ["mdm1"] }
                }
            }
        }
        self.fact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdm1": 2 }})

    def create_service(self, request=None):
        self.project = project.ProjectService(base.DMP_PROJECTS, self.fact, self.cfg, nistr,
                                              rootlog.getChild("project"))
        return self.project

    def last_action_for(self, recid):
        acts = self.project.dbcli._db.get(base.PROV_ACT_LOG, {}).get(recid,[])
        if not acts:
            return None
        return acts[-1]

    def assertActionCount(self, recid, count):
        self.assertEqual(len(self.project.dbcli._db.get(base.PROV_ACT_LOG, {}).get(recid,[])), count)

    def test_ctor(self):
        self.create_service()
        self.assertTrue(self.project.dbcli)
        self.assertEqual(self.project.cfg, self.cfg)
        self.assertEqual(self.project.who.actor, "nstr1")
        self.assertEqual(self.project.who.agent_class, "midas")
        self.assertTrue(self.project.log)

    def test_get_id_shoulder(self):
        self.create_service()
        self.assertIsNone(self.project._get_id_shoulder(nistr, {}))

        ## _get_id_shoulder() now returns None by default
        #
        # usr = prov.Agent("midas", prov.Agent.USER, "nstr1", "malware")
        # self.assertEqual(self.project._get_id_shoulder(usr), "mdm0")

        # del self.cfg['clients']['default']['default_shoulder']
        # self.create_service()
        # with self.assertRaises(project.NotAuthorized):
        #     self.project._get_id_shoulder(usr)
        # del self.cfg['clients']['default']
        # self.create_service()
        # with self.assertRaises(project.NotAuthorized):
        #     self.project._get_id_shoulder(usr)
        
        # self.assertEqual(self.project._get_id_shoulder(nistr), "mdm1")

    def test_extract_data_part(self):
        data = {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A", "vec": [22, 11, 0], "desc": {"a": 1}}}
        self.create_service()
        self.assertEqual(self.project._extract_data_part(data, "color"), "red")
        self.assertEqual(self.project._extract_data_part(data, "pos"),
                         {"x": 23, "y": 12, "grid": "A", "vec": [22, 11, 0], "desc": {"a": 1}})
        self.assertEqual(self.project._extract_data_part(data, "pos/vec"), [22, 11, 0])
        self.assertEqual(self.project._extract_data_part(data, "pos/y"), 12)
        self.assertEqual(self.project._extract_data_part(data, "pos/desc/a"), 1)
        with self.assertRaises(project.ObjectNotFound):
            self.project._extract_data_part(data, "pos/desc/b")
        

    def test_create_record(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("goob"))
        
        prec = self.project.create_record("goob")
        self.assertEqual(prec.name, "goob")
        self.assertEqual(prec.id, "mdm1:0003")
        self.assertEqual(prec.data, {})
        self.assertEqual(prec.meta, {})
        self.assertEqual(prec.owner, "nstr1")
        self.assertEqual(prec.status.action, "create")
        self.assertEqual(prec.status.message, "draft created")
        self.assertEqual(prec.status.state, "edit")
        self.assertEqual(prec.status.created_by, "midas/nstr1")

        self.assertEqual(prec.acls._perms.get("read"), [ self.project.who.actor, "grp0:public"])
        self.assertEqual(prec.acls._perms.get("write"), [ self.project.who.actor ])
        self.assertEqual(prec.acls._perms.get("edit"), [ "grp0:overlord"])

        self.assertTrue(self.project.dbcli.name_exists("goob"))
        prec2 = self.project.get_record(prec.id)
        self.assertEqual(prec2.name, "goob")
        self.assertEqual(prec2.id, "mdm1:0003")
        self.assertEqual(prec2.data, {})
        self.assertEqual(prec2.meta, {})
        self.assertEqual(prec2.owner, "nstr1")

        lastact = self.last_action_for(prec.id)
        self.assertEqual(lastact['subject'], prec.id)
        self.assertEqual(lastact['type'], prov.Action.CREATE)
        self.assertNotIn('subactions', lastact)

        with self.assertRaises(project.AlreadyExists):
            self.project.create_record("goob")

    def test_create_record_withdata(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("gurn"))
        
        prec = self.project.create_record("gurn", {"color": "red"}, {"temper": "dark"})
        self.assertEqual(prec.name, "gurn")
        self.assertEqual(prec.id, "mdm1:0003")
        self.assertEqual(prec.data, {"color": "red"})
        self.assertEqual(prec.meta, {"temper": "dark", "agent_vehicle": 'midas'})

    def test_create_record_withlocalid(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("gurn"))

        with self.assertRaises(project.NotAuthorized):
            self.project.create_record("gurn", {"color": "red"}, {"temper": "dark"}, "mdm0:goob")

        self.project.dbcli._cfg["project_id_minting"]["localid_providers"] = {
            self.project.dbcli._who.agent_class: ["mdm0"]
        }
        self.assertEqual(self.project.dbcli._cfg["project_id_minting"]["localid_providers"].get("midas"),
                         ["mdm0"])

        prec = self.project.create_record("gurn", {"color": "red"}, {"temper": "dark"}, "mdm0:goob")
        self.assertEqual(prec.name, "gurn")
        self.assertEqual(prec.id, "mdm0:goob")
        self.assertEqual(prec.data, {"color": "red"})
        self.assertEqual(prec.meta, {"temper": "dark", "agent_vehicle": 'midas'})

    def test_create_record_reassign(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("gurn"))
        
        prec = self.project.create_record("gurn", {"color": "red"}, {"foruser": "harry"})
        self.assertEqual(prec.name, "gurn")
        self.assertEqual(prec.id, "mdm1:0003")
        self.assertEqual(prec.data, {"color": "red"})
        self.assertEqual(prec.owner, "harry")
        self.assertEqual(prec.meta, {"foruser": "harry", "agent_vehicle": 'midas'})
        self.assertEqual(prec.status.created_by, "midas/nstr1")

    def test_delete_new_record(self):
        self.create_service()
        prec = self.project.create_record("gurn", {"color": "red"}, {"temper": "dark"})
        prec = self.project.get_record(prec.id)
        self.assertIsNone(prec.status.published_as)  # never been published

        dprec = self.project.delete_record(prec.id)
        self.assertIsNone(dprec)
        with self.assertRaises(project.ObjectNotFound):
            self.project.get_record(prec.id)

    def test_restore_last_published_data(self):
        self.create_service()
        prec = self.project.create_record("gurn", {"color": "red"}, {"temper": "dark"})
        with self.assertRaises(ValueError):
            self.project._restore_last_published_data(prec)

        recd = prec.to_dict()
        recd['id'] = "ark:/88434/" + re.sub(r':', '-', prec.id)
        recd['name'] = recd['id']
        prec.status.publish(recd['id'], "1.0.0")
        prec.save()

        pubcli = self.project.dbcli.client_for(self.project.dbcli.project + "_latest")
        pubrec = project.ProjectRecord(pubcli.project, recd, pubcli)
        pubrec.status.set_state(status.SUBMITTED)
        pubrec.save()
        pubrec = pubcli.get_record_for(recd['id'])
        self.assertEqual(pubrec.id, recd['id'])
        self.assertTrue(pubrec.id.startswith("ark:/"))

        self.project.update_data(prec.id, {"title": "Now."})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data.get('color'), "red")
        self.assertEqual(prec.data.get('title'), "Now.")
        self.assertEqual(prec.status.state, status.EDIT)

        self.project._restore_last_published_data(prec)
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data.get('color'), "red")
        self.assertIsNone(prec.data.get('title'))
        self.assertEqual(prec.status.state, status.SUBMITTED)

    def test_delete_revision(self):
        self.create_service()
        prec = self.project.create_record("gurn", {"color": "red"}, {"temper": "dark"})
        with self.assertRaises(ValueError):
            self.project._restore_last_published_data(prec)

        recd = prec.to_dict()
        recd['id'] = "ark:/88434/" + re.sub(r':', '-', prec.id)
        recd['name'] = recd['id']
        prec.status.publish(recd['id'], "1.0.0")
        prec.save()

        pubcli = self.project.dbcli.client_for(self.project.dbcli.project + "_latest")
        pubrec = project.ProjectRecord(pubcli.project, recd, pubcli)
        pubrec.status.set_state(status.PUBLISHED)
        pubrec.save()
        pubrec = pubcli.get_record_for(recd['id'])
        self.assertEqual(pubrec.id, recd['id'])
        self.assertTrue(pubrec.id.startswith("ark:/"))

        self.project.update_data(prec.id, {"title": "Now."})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data.get('color'), "red")
        self.assertEqual(prec.data.get('title'), "Now.")
        self.assertEqual(prec.status.state, status.EDIT)

        self.project.delete_record(prec.id)
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data.get('color'), "red")
        self.assertIsNone(prec.data.get('title'))
        self.assertEqual(prec.status.state, status.PUBLISHED)

    def test_get_data(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("gurn"))
        prec = self.project.create_record("gurn", {"color": "red", "pos": {"x": 23, "y": 12, "desc": {"a": 1}}})
        self.assertTrue(self.project.dbcli.name_exists("gurn"))

        self.assertEqual(self.project.get_data(prec.id),
                         {"color": "red", "pos": {"x": 23, "y": 12, "desc": {"a": 1}}})
        self.assertEqual(self.project.get_data(prec.id, "color"), "red")
        self.assertEqual(self.project.get_data(prec.id, "pos"), {"x": 23, "y": 12, "desc": {"a": 1}})
        self.assertEqual(self.project.get_data(prec.id, "pos/desc"), {"a": 1})
        self.assertEqual(self.project.get_data(prec.id, "pos/desc/a"), 1)
        
        lastact = self.last_action_for(prec.id)
        self.assertEqual(lastact['subject'], prec.id)
        self.assertEqual(lastact['type'], prov.Action.CREATE)
        self.assertNotIn('subactions', lastact)

        with self.assertRaises(project.ObjectNotFound):
            self.project.get_data(prec.id, "pos/desc/b")
        with self.assertRaises(project.ObjectNotFound):
            self.project.get_data("goober")


    def test_update_replace_data(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("goob"))
        
        prec = self.project.create_record("goob")
        self.assertEqual(prec.name, "goob")
        self.assertEqual(prec.id, "mdm1:0003")
        self.assertEqual(prec.data, {})
        self.assertEqual(prec.meta, {})
        lastact = self.last_action_for(prec.id)
        self.assertEqual(lastact['subject'], prec.id)
        self.assertEqual(lastact['type'], prov.Action.CREATE)
        self.assertNotIn('subactions', lastact)
#        self.assertEqual(len(lastact['subactions']), 1)

        data = self.project.update_data(prec.id, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        self.assertEqual(data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})

        lastact = self.last_action_for(prec.id)
        self.assertEqual(lastact['subject'], prec.id)
        self.assertEqual(lastact['type'], prov.Action.PATCH)
        self.assertNotIn('subactions', lastact)
        self.assertActionCount(prec.id, 2)

        data = self.project.update_data(prec.id, {"y": 1, "z": 10, "grid": "B"}, "pos")
        self.assertEqual(data, {"x": 23, "y": 1, "z": 10, "grid": "B"})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 1, "z": 10, "grid": "B"}})
        
        lastact = self.last_action_for(prec.id)
        self.assertEqual(lastact['subject'], prec.id)
        self.assertEqual(lastact['type'], prov.Action.PATCH)
        self.assertEqual(len(lastact['subactions']), 1)
        self.assertEqual(lastact['subactions'][0]['type'], prov.Action.PATCH)
        self.assertEqual(lastact['subactions'][0]['subject'], prec.id+"#data.pos")

        data = self.project.update_data(prec.id, "C", "pos/grid")
        self.assertEqual(data, "C")
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 1, "z": 10, "grid": "C"}})

        # replace
        data = self.project.replace_data(prec.id, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})
        self.assertEqual(data, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})

        lastact = self.last_action_for(prec.id)
        self.assertEqual(lastact['subject'], prec.id)
        self.assertEqual(lastact['type'], prov.Action.PUT)
        self.assertNotIn('subactions', lastact)

        # update again
        data = self.project.update_data(prec.id, "blue", "color")
        self.assertEqual(data, "blue")
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "blue", "pos": {"vec": [15, 22, 1], "grid": "Z"}})

        with self.assertRaises(project.PartNotAccessible):
            self.project.update_data(prec.id, 2, "pos/vec/x")

        self.assertEqual(len(self.project.dbcli._db.get(base.PROV_ACT_LOG, {}).get(prec.id,[])), 6)

    def test_prep_for_update(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("goob"))
        
        prec = self.project.create_record("goob")
        prec.status.set_state(status.PUBLISHED)

        self.project._prep_for_update(prec, "Boom!", False)
        self.assertEqual(prec.status.state, status.PUBLISHED)
        self.assertEqual(prec.status.action, "update-prep")
        self.assertEqual(prec.status.message, "Boom!")

        # status was saved
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.status.state, status.PUBLISHED)
        self.assertEqual(prec.status.action, "update-prep")
        self.assertEqual(prec.status.message, "Boom!")

        self.project._prep_for_update(prec)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(prec.status.action, "update-prep")
        self.assertNotEqual(prec.status.message, "Boom!")
        
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(prec.status.action, "update-prep")
        self.assertNotEqual(prec.status.message, "Boom!")


    def test_revise(self):
        # tests call to preparation after publication via update_data() or replace_data().

        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("goob"))
        
        prec = self.project.create_record("goob")
        prec.status.set_state(status.PUBLISHED)
        prec.save()
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.status.state, status.PUBLISHED)

        self.project.update_data(prec.id, {"title": "Hello"})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(prec.status.action, "update")

        prec.status.set_state(status.PUBLISHED)
        prec.save()
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.status.state, status.PUBLISHED)

        self.project.replace_data(prec.id, {"title": "Goodbye"})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.status.state, status.EDIT)
        self.assertEqual(prec.status.action, "update")


    def test_clear_data(self):
        self.create_service()
        prec = self.project.create_record("goob")
        self.assertEqual(prec.data, {})
        
        data = self.project.update_data(prec.id, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        self.assertEqual(data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})

        self.assertIs(self.project.clear_data(prec.id, "color"), True)
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"pos": {"x": 23, "y": 12, "grid": "A"}})

        self.assertIs(self.project.clear_data(prec.id, "color"), False)
        self.assertIs(self.project.clear_data(prec.id, "gurn/goob/gomer"), False)

        self.assertIs(self.project.clear_data(prec.id, "pos/y"), True)
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"pos": {"x": 23, "grid": "A"}})

        self.assertIs(self.project.clear_data(prec.id), True)
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {})
        

    def test_finalize(self):
        self.create_service()
        prec = self.project.create_record("goob")
        self.assertEqual(prec.status.state, "edit")
        self.assertIn("created", prec.status.message)
        self.assertNotIn("@version", prec.data)
        self.assertNotIn("@id", prec.data)
        
        data = self.project.update_data(prec.id, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        self.project.finalize(prec.id)
        stat = self.project.get_status(prec.id)
        self.assertEqual(stat.state, "ready")
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data.get("@version"), "1.0.0")
        self.assertEqual(prec.data.get("@id"), "ark:/88434/mdm1-0003")
        self.assertTrue(stat.message.startswith("draft is ready for submission as "))

        prec = self.project.get_record(prec.id)
        prec._data['status']['state'] = "ennui"
        prec.save()
        with self.assertRaises(project.NotEditable):
            self.project.finalize(prec.id)
        
    def test_submit(self):
        self.create_service()
        prec = self.project.create_record("goob")
        self.assertEqual(prec.status.state, "edit")
        self.assertIn("created", prec.status.message)
        self.assertNotIn("@version", prec.data)
        self.assertNotIn("@id", prec.data)
        
        self.project.submit(prec.id)
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data.get("@version"), "1.0.0")
        self.assertEqual(prec.data.get("@id"), "ark:/88434/mdm1-0003")
        self.assertEqual(prec.status.state, "published")

        pubcli = self.project.dbcli.client_for(self.project.dbcli.project+"_latest")
        pubrec = pubcli.get_record_for(prec.data["@id"])
        self.assertEqual(pubrec.id, prec.data["@id"])
        self.assertEqual(pubrec.data.get('@version'), "1.0.0")
        self.assertEqual(pubrec.acls._perms['delete'], [])
        self.assertEqual(pubrec.acls._perms['write'], [])
        self.assertEqual(pubrec.acls._perms['admin'], [])
        self.assertEqual(pubrec.acls._perms['read'], ["grp0:public"])

        pubcli = self.project.dbcli.client_for(self.project.dbcli.project+"_version")
        vid = prec.data["@id"] + "/pdr:v/" + prec.data["@version"]
        pubrec = pubcli.get_record_for(vid)
        self.assertEqual(pubrec.id, vid)
        self.assertEqual(pubrec.data.get('@version'), "1.0.0")
        self.assertEqual(pubrec.acls._perms['delete'], [])
        self.assertEqual(pubrec.acls._perms['write'], [])
        self.assertEqual(pubrec.acls._perms['admin'], [])
        self.assertEqual(pubrec.acls._perms['read'], ["grp0:public"])

    def test_default_review(self):
        self.create_service()
        prec = self.project.create_record("goob")
        self.assertIsNone(self.project.review(prec.id))
        
        with self.assertRaises(project.ObjectNotFound):
            self.project.review("goober")
        
    def test_apply_external_review(self):
        self.create_service()
        prec = self.project.create_record("goob")
        self.assertEqual(prec.status.state, "edit")
        self.assertIn("created", prec.status.message)
        self.assertNotIn("@version", prec.data)
        self.assertNotIn("@id", prec.data)

        id = prec.id
        with self.assertRaises(project.NotAuthorized):
            self.project.apply_external_review(id, "nps", "group-leader-review", id, "/od/id/"+id)

        self.project.dbcli._cfg['superusers'] = ['nstr1']
        prec = self.project.get_record(id)
        prec.acls.grant_perm_to(base.ACLs.PUBLISH, 'nstr1')
        prec.save()
        self.project.dbcli._cfg['superusers'] = []
        
        with self.assertRaises(project.NotSubmitable):
            self.project.publish(id)  # not submitted yet

        self.project.apply_external_review(id, "nps", "group-leader-review", id, "/od/id/"+id)
        prec = self.project.get_record(id)
        stat = prec.status
        self.assertEqual(stat.state, "edit")
        sdata = stat.to_dict()
        self.assertIn("nps", sdata.get("external_review", {}))
        self.assertNotIn("elrs", sdata.get("external_review", {}))
        rdata = sdata.get("external_review", {}).get("nps")
        self.assertTrue(isinstance(rdata, Mapping))
        self.assertEqual(rdata['phase'], "group-leader-review")
        self.assertEqual(rdata['@id'], id)
        self.assertEqual(rdata['info_at'], "/od/id/"+id)
        self.assertNotIn('feedback', rdata)
        self.assertNotIn('gurn', rdata)

        fb = {
            "reviewer": "jerry",
            "type": "warn",
            "description": "this looks like dangerous gnostic data"
        }
        self.project.apply_external_review(id, "elrs", "tech", feedback=[fb], gurn="burn")
        prec = self.project.get_record(id)
        stat = prec.status
        self.assertEqual(stat.state, "edit")
        sdata = stat.to_dict()
        self.assertIn("nps", sdata.get("external_review", {}))
        self.assertIn("elrs", sdata.get("external_review", {}))
        rdata = sdata.get("external_review", {}).get("elrs")
        self.assertTrue(isinstance(rdata, Mapping))
        self.assertEqual(rdata['phase'], "tech")
        self.assertNotIn('@id', rdata)
        self.assertNotIn('info_at', rdata)
        self.assertIn('feedback', rdata)
        self.assertEqual(len(rdata['feedback']), 1)
        self.assertEqual(rdata['feedback'][0], fb)
        self.assertIn('gurn', rdata)

        self.assertLess(prec.status.submitted, 0)
        prec.status.set_state(status.SUBMITTED)
        self.assertGreater(prec.status.submitted, 0)
        prec.save()

        self.project.apply_external_review(id, "elrs", "tech", id, feedback=[])
        prec = self.project.get_record(id)
        stat = prec.status
        self.assertEqual(stat.state, "submitted")
        sdata = stat.to_dict()
        self.assertIn("nps", sdata.get("external_review", {}))
        self.assertIn("elrs", sdata.get("external_review", {}))
        rdata = sdata.get("external_review", {}).get("elrs")
        self.assertTrue(isinstance(rdata, Mapping))
        self.assertEqual(rdata['phase'], "tech")
        self.assertEqual(rdata['@id'], id)
        self.assertNotIn('info_at', rdata)
        self.assertIn('feedback', rdata)
        self.assertEqual(len(rdata['feedback']), 0)
        self.assertIn('gurn', rdata)

        with self.assertRaises(project.NotSubmitable):
            self.project.publish(id)

        self.project.approve(id, "elrs", publish=False)
        prec = self.project.get_record(id)
        stat = prec.status
        self.assertEqual(stat.state, "submitted")
        sdata = stat.to_dict()
        self.assertIn("nps", sdata.get("external_review", {}))
        self.assertIn("elrs", sdata.get("external_review", {}))
        rdata = sdata.get("external_review", {}).get("elrs")
        self.assertEqual(rdata.get('phase'), "approved")
        rdata = sdata.get("external_review", {}).get("nps")
        self.assertNotEqual(rdata.get('phase'), "approved")
        
        with self.assertRaises(project.NotSubmitable):
            self.project.publish(id)

        self.project.approve(id, "nps", publish=False)
        prec = self.project.get_record(id)
        stat = prec.status
        self.assertEqual(stat.state, "accepted")
        sdata = stat.to_dict()
        self.assertIn("nps", sdata.get("external_review", {}))
        self.assertIn("elrs", sdata.get("external_review", {}))
        rdata = sdata.get("external_review", {}).get("elrs")
        self.assertEqual(rdata.get('phase'), "approved")
        rdata = sdata.get("external_review", {}).get("nps")
        self.assertEqual(rdata.get('phase'), "approved")

        self.project.publish(id)
        prec = self.project.get_record(id)
        stat = prec.status
        self.assertEqual(stat.state, "published")

    def test_publish(self):
        self.create_service()
        prec = self.project.create_record("goob")
        self.assertEqual(prec.status.state, "edit")
        self.assertIn("created", prec.status.message)
        self.assertNotIn("@version", prec.data)
        self.assertNotIn("@id", prec.data)
        id = prec.id
        
        with self.assertRaises(project.NotAuthorized):
            self.project.publish(id)  

        self.project.dbcli._cfg['superusers'] = ['nstr1']
        prec = self.project.get_record(id)
        prec.acls.grant_perm_to(base.ACLs.PUBLISH, 'nstr1')
        prec.save()
        self.project.dbcli._cfg['superusers'] = []
        
        with self.assertRaises(project.NotSubmitable):
            self.project.publish(id)  # not submitted yet
        prec = self.project.get_record(id)
        self.assertLess(prec.status.submitted, 0)

        prec.status.set_state(status.INPRESS)
        prec.save()
        with self.assertRaises(project.NotSubmitable):
            self.project.publish(id)  # in press already

        prec.status.set_state(status.SUBMITTED)
        prec.save()
        prec = self.project.get_record(id)
        self.assertGreater(prec.status.submitted, 0)
        self.assertLess(prec.status.published, 0)

        self.project.publish(id)
        prec = self.project.get_record(id)
        self.assertGreater(prec.status.submitted, 0)
        self.assertGreater(prec.status.published, 0)
        
        
        
        
class TestProjectServiceFactory(test.TestCase):

    def setUp(self):
        self.cfg = {
            "clients": {
                "midas": {
                    "default_shoulder": "mdm1"
                },
                "default": {
                    "default_shoulder": "mdm0"
                }
            },
            "dbio": {
                "project_id_minting": {
                    "allowed_shoulders": {
                        "public": [],
                        "midas":  ["mdm1", "spc1"]
                    },
                    "default_shoulder": { "public": "mdm0" },
                    "localid_providers": { "midas": ["mdm0"] }
                }
            }
        }

        self.dbfact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdm1": 2 }})
        self.fact = project.ProjectServiceFactory("dmp", self.dbfact, self.cfg)

    def test_ctor(self):
        self.assertEqual(self.fact._prjtype, "dmp")
        self.assertTrue(self.fact._dbclifact)
        self.assertIn("dbio", self.fact._cfg)
        self.assertIsNone(self.fact._log)

    def test_create_service_for(self):
        svc = self.fact.create_service_for(nistr)

        self.assertEqual(svc.cfg, self.cfg)
        self.assertTrue(svc.dbcli)
        self.assertEqual(svc.dbcli._cfg, self.cfg["dbio"])
        self.assertEqual(svc.who.actor, "nstr1")
        self.assertEqual(svc.who.agent_class, "midas")
        self.assertTrue(svc.log)

        prec = svc.create_record("goob")
        self.assertEqual(prec._coll, "dmp")
    
    


                         
if __name__ == '__main__':
    test.main()
        
        

        
