import os, json, pdb, logging, tempfile, re
from pathlib import Path
import unittest as test

from nistoar.nsd.service import files as serv
from nistoar.base.config import ConfigurationException

testdir = Path(__file__).parents[0]
datadir = testdir.parents[0] / 'data'

class TestMongoPeopleService(test.TestCase):

    def setUp(self):
        self.cfg = {
            "dir": datadir
        }
        self.svc = serv.FilesBasedPeopleService(self.cfg)

    def test_orgs(self):
        orgs = self.svc.orgs()
        self.assertEqual(len(orgs), 8)
        self.assertEqual(set([u['orG_ACRNM'] for u in orgs]), set("DOL DOC DOF DOS LERA SAA BWM VTA".split()))

    def test_people(self):
        peops = self.svc.people()
        self.assertEqual(len(peops), 4)
        self.assertEqual(set([u['lastName'] for u in peops]), set("Austin Bergman Ossman Proctor".split()))

    def test_OUs(self):
        ous = self.svc.OUs()
        self.assertEqual(len(ous), 4)
        self.assertEqual(set([u['orG_ACRNM'] for u in ous]), set("DOL DOC DOF DOS".split()))
    
    def test_divs(self):
        divs = self.svc.divs()
        self.assertEqual(len(divs), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in divs]), set("LERA SAA".split()))
    
    def test_groups(self):
        grps = self.svc.groups()
        self.assertEqual(len(grps), 2)
        self.assertEqual(set([u['orG_ACRNM'] for u in grps]), set("BWM VTA".split()))

    def test_select_people(self):
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


    def test_select_people_like(self):
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
        who = self.svc.get_person(11)
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Austin")

        who = self.svc.get_person(10)
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Proctor")

    def test_select_orgs(self):
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
        what = self.svc.get_OU(3)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "DOF")

        what = self.svc.get_OU(1)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "DOL")

    def test_get_div(self):
        what = self.svc.get_div(5)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "LERA")

        what = self.svc.get_div(6)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "SAA")

    def test_get_group(self):
        what = self.svc.get_group(7)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "BWM")

        what = self.svc.get_group(8)
        self.assertIsNotNone(what)
        self.assertEqual(what["orG_ACRNM"], "VTA")

    def test_get_person_by(self):
        who = self.svc.get_person_by_eid("pgp1")
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Proctor")

        who = self.svc.get_person_by_email("phillip.austin@nist.gov")
        self.assertIsNotNone(who)
        self.assertEqual(who["lastName"], "Austin")

    def test_select_best_person_matches(self):
        peops = self.svc.select_best_person_matches("Proctor, Phil")
        self.assertEqual(len(peops), 1)
        self.assertEqual(peops[0]['lastName'], "Proctor")

        peops = self.svc.select_best_person_matches("Austin, Phillip")
        self.assertEqual(len(peops), 1)
        self.assertEqual(peops[0]['lastName'], "Austin")

        peops = self.svc.select_best_person_matches("Bergman, Peter")
        self.assertEqual(len(peops), 1)
        self.assertEqual(peops[0]['lastName'], "Bergman")
        peops = self.svc.select_best_person_matches("Berg, Peter")
        self.assertEqual(len(peops), 1)
        self.assertEqual(peops[0]['lastName'], "Bergman")

        peops = self.svc.select_best_person_matches("Phillip Austin")
        self.assertEqual(len(peops), 1)
        self.assertEqual(peops[0]['lastName'], "Austin")
        
        peops = self.svc.select_best_person_matches("Austin Phil")
        self.assertEqual(len(peops), 1)
        self.assertEqual(peops[0]['lastName'], "Austin")
        
        peops = self.svc.select_best_person_matches("Phillip")
        self.assertEqual(len(peops), 2)
        peops = self.svc.select_best_person_matches("Phillip, ")
        self.assertEqual(len(peops), 0)
        peops = self.svc.select_best_person_matches(", Phillip")
        self.assertEqual(len(peops), 2)






        
        
        
                         
if __name__ == '__main__':
    test.main()
