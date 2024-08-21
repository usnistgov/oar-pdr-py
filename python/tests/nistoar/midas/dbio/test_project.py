import os, json, pdb, logging, tempfile
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio import project
from nistoar.pdr.utils import prov

tmpdir = tempfile.TemporaryDirectory(prefix="_test_project.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_pdp.log"))
    loghdlr.setLevel(logging.DEBUG)
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
            "clients": {
                "midas": {
                    "default_shoulder": "mdm1"
                },
                "default": {
                    "default_shoulder": "mdm0"
                }
            },
            "default_perms": {
                "read":  ["grp0:public"],
                "edit":  ["grp0:overlord"]
            },
            "dbio": {
                "allowed_project_shoulders": ["mdm1", "spc1"],
                "default_shoulder": "mdm0"
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
        self.assertEqual(self.project._get_id_shoulder(nistr), "mdm1")
        
        usr = prov.Agent("midas", prov.Agent.USER, "nstr1", "malware")
        self.assertEqual(self.project._get_id_shoulder(usr), "mdm0")

        del self.cfg['clients']['default']['default_shoulder']
        self.create_service()
        with self.assertRaises(project.NotAuthorized):
            self.project._get_id_shoulder(usr)
        del self.cfg['clients']['default']
        self.create_service()
        with self.assertRaises(project.NotAuthorized):
            self.project._get_id_shoulder(usr)
        
        self.assertEqual(self.project._get_id_shoulder(nistr), "mdm1")

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
        data = self.project.update_data(prec.id, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        self.project.finalize(prec.id)
        stat = self.project.get_status(prec.id)
        self.assertEqual(stat.state, "ready")
        self.assertTrue(stat.message.startswith("draft is ready for submission as "))

        prec = self.project.get_record(prec.id)
        prec._data['status']['state'] = "ennui"
        prec.save()
        with self.assertRaises(project.NotEditable):
            self.project.finalize(prec.id)
        
        


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
                "allowed_project_shoulders": ["mdm1", "spc1"],
                "default_shoulder": "mdm0"
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
        
        

        
