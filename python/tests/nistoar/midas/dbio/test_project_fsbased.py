import os, json, pdb, logging, tempfile
from pathlib import Path
import unittest as test

from nistoar.midas.dbio import fsbased, base
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

class TestInMemoryDBClientFactory(test.TestCase):

    def setUp(self):
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_dbclient.", dir=".")
        self.cfg = { "goob": "gurn" }
        self.fact = fsbased.FSBasedDBClientFactory(self.cfg, self.outdir.name)

    def tearDown(self):
        self.outdir.cleanup()

class TestProjectService(test.TestCase):

    def setUp(self):
        self.cfg = {
            "dbio": {
                "project_id_minting": {
                    "allowed_shoulders": {
                        "public": [],
                        "midas":  ["mdm0", "mdm1", "spc1"]
                    },
                    "default_shoulder": { "public": "mdm1" },
                    "localid_providers": { "midas": ["mdm0"] }
                }
            }
        }
        self.outdir = tempfile.TemporaryDirectory(prefix="_test_dbclient.", dir=".")
        self.fact = fsbased.FSBasedDBClientFactory(self.cfg["dbio"], self.outdir.name)

    def tearDown(self):
        self.outdir.cleanup()

    def create_service(self, request=None):
        self.project = project.ProjectService(base.DMP_PROJECTS, self.fact, self.cfg, nistr,
                                              rootlog.getChild("project"))
        return self.project

    def last_action_for(self, recid):
        recpath = os.path.join(self.fact._dbroot, base.PROV_ACT_LOG, (recid+".lis"))
        line = None
        with open(recpath) as fd:
            for line in fd:
                pass
        return json.loads(line)

    def assertActionCount(self, recid, count):
        recpath = os.path.join(self.fact._dbroot, base.PROV_ACT_LOG, (recid+".lis"))
        with open(recpath) as fd:
            i = 0
            for line in fd:
                i += 1
        self.assertEqual(i, count)

    def test_ctor(self):
        self.create_service()
        self.assertTrue(self.project.dbcli)
        self.assertEqual(self.project.cfg, self.cfg)
        self.assertEqual(self.project.who.actor, "nstr1")
        self.assertEqual(self.project.who.agent_class, "midas")
        self.assertTrue(self.project.log)

    def test_update_replace_data(self):
        self.create_service()
        self.assertTrue(not self.project.dbcli.name_exists("goob"))
        
        prec = self.project.create_record("goob")
        self.assertEqual(prec.name, "goob")
        self.assertEqual(prec.id, "mdm1:0001")
        self.assertEqual(prec.data, {})
        self.assertEqual(prec.meta, {})
        self.assertEqual(prec.status.state, "edit")
        self.assertEqual(prec.status.action, "create")
        lastact = self.last_action_for(prec.id)
        self.assertEqual(lastact['subject'], prec.id)
        self.assertEqual(lastact['type'], prov.Action.CREATE)
        self.assertNotIn('subactions', lastact)
#        self.assertEqual(len(lastact['subactions']), 1)

        data = self.project.update_data(prec.id, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        self.assertEqual(data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        prec = self.project.get_record(prec.id)
        self.assertEqual(prec.data, {"color": "red", "pos": {"x": 23, "y": 12, "grid": "A"}})
        self.assertEqual(prec.status.state, "edit")
        self.assertEqual(prec.status.action, "update")

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

        self.assertActionCount(prec.id, 6)


    

    


                         
if __name__ == '__main__':
    test.main()
