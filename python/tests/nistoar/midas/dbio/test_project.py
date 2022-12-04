import os, json, pdb, logging, tempfile
import unittest as test

from nistoar.midas.dbio import inmem, base
from nistoar.midas.dbio import project
from nistoar.pdr.publish import prov

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

nistr = prov.PubAgent("midas", prov.PubAgent.USER, "nstr1")

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

    def test_ctor(self):
        self.create_service()
        self.assertTrue(self.project.dbcli)
        self.assertEqual(self.project.cfg, self.cfg)
        self.assertEqual(self.project.who.actor, "nstr1")
        self.assertEqual(self.project.who.group, "midas")
        self.assertTrue(self.project.log)

    def test_get_id_shoulder(self):
        self.create_service()
        self.assertEqual(self.project._get_id_shoulder(nistr), "mdm1")
        
        usr = prov.PubAgent("malware", prov.PubAgent.USER, "nstr1")
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

        self.assertTrue(self.project.dbcli.name_exists("goob"))
        prec2 = self.project.get_record(prec.id)
        self.assertEqual(prec2.name, "goob")
        self.assertEqual(prec2.id, "mdm1:0003")
        self.assertEqual(prec2.data, {})
        self.assertEqual(prec2.meta, {})
        self.assertEqual(prec2.owner, "nstr1")

        with self.assertRaises(project.AlreadyExists):
            self.project.create_record("goob")

    def test_create_record_withdata(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("gurn"))
        
        prec = self.project.create_record("gurn", {"color": "red"}, {"temper": "dark"})
        self.assertEqual(prec.name, "gurn")
        self.assertEqual(prec.id, "mdm1:0003")
        self.assertEqual(prec.data, {"color": "red"})
        self.assertEqual(prec.meta, {"temper": "dark"})

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

        data = self.project.update_data(prec.id, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        self.assertEqual(data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})

        data = self.project.update_data(prec.id, {"y": 1, "z": 10, "grid": "B"}, "pos")
        self.assertEqual(data, {"x": 23, "y": 1, "z": 10, "grid": "B"})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 1, "z": 10, "grid": "B"}})
        
        data = self.project.update_data(prec.id, "C", "pos/grid")
        self.assertEqual(data, "C")
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 1, "z": 10, "grid": "C"}})

        # replace
        data = self.project.replace_data(prec.id, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})
        self.assertEqual(data, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"pos": {"vec": [15, 22, 1], "grid": "Z"}})

        # update again
        data = self.project.update_data(prec.id, "blue", "color")
        self.assertEqual(data, "blue")
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "blue", "pos": {"vec": [15, 22, 1], "grid": "Z"}})

        with self.assertRaises(project.PartNotAccessible):
            self.project.update_data(prec.id, 2, "pos/vec/x")


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
        self.assertEqual(svc.who.group, "midas")
        self.assertTrue(svc.log)

        prec = svc.create_record("goob")
        self.assertEqual(prec._coll, "dmp")
    

    


                         
if __name__ == '__main__':
    test.main()
        
        

        
