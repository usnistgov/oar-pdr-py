import os, json, pdb, logging, tempfile, re
from pathlib import Path
import unittest as test

from pymongo import MongoClient

from nistoar.nsd import service as serv
from nistoar.base.config import ConfigurationException

tmpdir = tempfile.TemporaryDirectory(prefix="_test_project.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_nsd.log"))
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

testdir = Path(__file__).parents[0]
datadir = testdir / 'data'

dburl = None
dbname = None
if os.environ.get('MONGO_TESTDB_URL'):
    dburl = os.environ.get('MONGO_TESTDB_URL')
    dbname = re.sub(r'\?.*$', '', dburl).split('/')[-1]
    assert dbname

@test.skipIf(not os.environ.get('MONGO_TESTDB_URL'), "test mongodb not available")
class TestMongoPeopleService(test.TestCase):

    def setUp(self):
        self.cfg = {
            "dir": datadir
        }
        self.svc = serv.MongoPeopleService(dburl)

    def tearDown(self):
        self.svc._cli.drop_database(dbname)

    def test_load(self):
        self.assertEqual(self.svc._cli[dbname]['OUs'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['Divisions'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['Groups'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['Orgs'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['People'].count_documents({}), 0)

        self.svc.load(self.cfg, rootlog)
        self.assertEqual(self.svc._cli[dbname]['OUs'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['Divisions'].count_documents({}), 0)
        self.assertEqual(self.svc._cli[dbname]['Groups'].count_documents({}), 0)

        self.assertEqual(self.svc._cli[dbname]['Orgs'].count_documents({}), 8)
        self.assertEqual(self.svc._cli[dbname]['Orgs'].count_documents({"orG_LVL_ID": self.svc.OU_LVL_ID}), 4)
        self.assertEqual(self.svc._cli[dbname]['Orgs'].count_documents({"orG_LVL_ID": self.svc.DIV_LVL_ID}), 2)
        self.assertEqual(self.svc._cli[dbname]['Orgs'].count_documents({"orG_LVL_ID": self.svc.GRP_LVL_ID}), 2)
        self.assertEqual(self.svc._cli[dbname]['People'].count_documents({}), 4)
        
    def test_OUs(self):
        self.svc.load(self.cfg, rootlog, False)
        ous = self.svc.OUs()
        self.assertEqual(len(ous), 4)
        self.assertEqual(set([u['orG_ACRNM'] for u in ous]), set("DOL DOC DOF DOS".split()))
    
    def test_divs(self):
        self.svc.load(self.cfg, rootlog, False)
        divs = self.svc.divs()
        self.assertEqual(len(divs), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in divs]), set("LERA SAA".split()))
    
    def test_groups(self):
        self.svc.load(self.cfg, rootlog, False)
        grps = self.svc.groups()
        self.assertEqual(len(grps), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in grps]), set("BWM VTA".split()))

    def test_select_people(self):
        self.svc.load(self.cfg, rootlog)

        peops = list(self.svc.select_people({"firstName": ["Phillip"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))
    
        peops = list(self.svc.select_people({"firstName": ["Phillip"], "lastName": ["Austin"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))
    
        peops = list(self.svc.select_people({"firstName": ["Phillip"], "lastName": ["Austin"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))

        peops = list(self.svc.select_people({"firstName": ["Peter", "David"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Bergman Ossman".split()))
    
        peops = list(self.svc.select_people({"firstName": ["phil", "hank"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))
    
        peops = list(self.svc.select_people({"groupNumber": ["10001"]}))
        self.assertEqual(len(peops), 3)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Ossman Bergman".split()))

        peops = list(self.svc.select_people({"firstName": ["i"]}))
        self.assertEqual(len(peops), 3)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor Ossman".split()))


    def test_select_people_exact(self):
        self.svc = serv.MongoPeopleService(dburl, True)
        self.svc.load(self.cfg, rootlog)

        peops = list(self.svc.select_people({"firstName": ["Phillip"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))
    
        peops = list(self.svc.select_people({"firstName": ["Phillip"], "lastName": ["Austin"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))
    
        peops = list(self.svc.select_people({"firstName": ["Phillip"], "lastName": ["Austin"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))

        peops = list(self.svc.select_people({"firstName": ["Peter", "David"]}))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Bergman Ossman".split()))
    
        peops = list(self.svc.select_people({"firstName": ["phil", "hank"]}))
        self.assertEqual(len(peops), 0)
    
        peops = list(self.svc.select_people({"groupNumber": ["10001"]}))
        self.assertEqual(len(peops), 3)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Ossman Bergman".split()))

        peops = list(self.svc.select_people({"firstName": ["i"]}))
        self.assertEqual(len(peops), 0)

    def test_select_people_like(self):
        self.svc.load(self.cfg, rootlog)

        peops = list(self.svc.select_people({}, like="phi"))
        self.assertEqual(len(peops), 2)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Proctor".split()))

        peops = list(self.svc.select_people({}, like="au"))
        self.assertEqual(len(peops), 1)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin".split()))

        peops = list(self.svc.select_people({"lastName": ["st"]}, like="phi"))
        self.assertEqual(len(peops), 1)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin".split()))

        peops = list(self.svc.select_people({"lastName": ["ss"]}, like="phi"))
        self.assertEqual(len(peops), 0)

    def test_get_person(self):
        self.svc.load(self.cfg, rootlog, False)

        who = self.svc.get_person(11)
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Austin")

        who = self.svc.get_person(10)
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Proctor")

    def test_select_orgs(self):
        self.svc.load(self.cfg, rootlog, False)

        orgs = list(self.svc.select_orgs({}, orgtype=self.svc.OU_ORG_TYPE))
        self.assertEqual(len(orgs), 4)
        self.assertEqual(set([u['orG_ACRNM'] for u in orgs]), set("DOL DOC DOF DOS".split()))

        orgs = list(self.svc.select_orgs({"orG_ACRNM": "F"}, orgtype=self.svc.OU_ORG_TYPE))
        self.assertEqual(len(orgs), 1)
        self.assertEqual(set([u['orG_ACRNM'] for u in orgs]), set("DOF".split()))

        orgs = list(self.svc.select_orgs({}))
        self.assertEqual(len(orgs), 8)
        self.assertEqual(set([u['orG_ACRNM'] for u in orgs]), set("DOL DOC DOF DOS LERA SAA BWM VTA".split()))

        orgs = list(self.svc.select_orgs({"orG_ACRNM": "DO"}))
        self.assertEqual(len(orgs), 4)
        self.assertEqual(set([u['orG_ACRNM'] for u in orgs]), set("DOL DOC DOF DOS".split()))

        orgs = list(self.svc.select_orgs({"orG_Name": "s"}))
        self.assertEqual(len(orgs), 7)
        self.assertEqual(set([u['orG_ACRNM'] for u in orgs]), set("DOL DOC DOS LERA SAA BWM VTA".split()))

        orgs = list(self.svc.select_orgs({"orG_Name": "s"}, ["DO", "VTA"]))
        self.assertEqual(len(orgs), 4)
        self.assertEqual(set([u['orG_ACRNM'] for u in orgs]), set("DOL DOC DOS VTA".split()))

        orgs = list(self.svc.select_orgs({"orG_Name": "s"}, ["DO", "VTA"], self.svc.GRP_ORG_TYPE))
        self.assertEqual(len(orgs), 1)
        self.assertEqual(set([u['orG_ACRNM'] for u in orgs]), set("VTA".split()))

    def test_get_OU(self):
        self.svc.load(self.cfg, rootlog, False)

        what = self.svc.get_OU(3)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "DOF")

        what = self.svc.get_OU(1)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "DOL")

    def test_get_div(self):
        self.svc.load(self.cfg, rootlog, False)

        what = self.svc.get_div(5)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "LERA")

        what = self.svc.get_div(6)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "SAA")

    def test_get_group(self):
        self.svc.load(self.cfg, rootlog, False)

        what = self.svc.get_group(7)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "BWM")

        what = self.svc.get_group(8)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "VTA")

    def test_status(self):
        stat = self.svc.status()
        self.assertEqual(stat['status'], "not ready")
        self.assertEqual(stat['person_count'], 0)
        self.assertEqual(stat['org_count'], 0)
        self.assertTrue(stat['message'].startswith("Not Ready"))
        
        self.svc.load(self.cfg, rootlog, False)

        stat = self.svc.status()
        self.assertEqual(stat['status'], "ready")
        self.assertEqual(stat['person_count'], 4)
        self.assertEqual(stat['org_count'], 8)
        self.assertTrue(stat['message'].startswith("Ready"))

    def test_get_person_by(self):
        self.svc.load(self.cfg, rootlog, False)

        who = self.svc.get_person_by_eid("pgp1")
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Proctor")

        who = self.svc.get_person_by_email("phillip.austin@nist.gov")
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Austin")

        
        
        
                         
if __name__ == '__main__':
    test.main()
