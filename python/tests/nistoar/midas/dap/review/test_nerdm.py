import os, sys, pdb, json
import unittest as test
from pathlib import Path
from copy import deepcopy

import nistoar.midas.dap.review.nerdm as rev
import nistoar.pdr.utils.validate as base
from nistoar.pdr.utils.io import read_nerd
from nistoar.nerdm.utils import is_type

testdir = Path(__file__).parent
datadir = testdir.parent / "data"
sipdir = datadir / "mdssip"/"mdst:1491"

class TestDAPNERDmReviewValidator(test.TestCase):

    def setUp(self):
        self.nerd = read_nerd(sipdir/"nerdm.json")
        self.val = rev.DAPNERDmReviewValidator()

    def test_test_title(self):
        res = self.val.test_title(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 2)
        self.assertEqual(res.passed()[0].label, "1.2.1 title")
        self.assertEqual(res.passed()[1].label, "1.2.2 title")

        self.nerd["title"] = "John Doe"
        res = self.val.test_title(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "1.2.1 title")
        self.assertEqual(res.failed()[0].label, "1.2.2 title")
        self.assertEqual(len(res.failed()[0].comments), 2)
        
        res = self.val.test_title(self.nerd, want=rev.REC)
        self.assertEqual(res.count_applied(), 0)

    def test_test_description(self):
        # test file contains description with only one sentence
        res = self.val.test_description(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "2.2.1 description")
        self.assertEqual(res.failed()[0].label, "2.2.2 description")
        self.assertEqual(len(res.failed()[0].comments), 2)

        # make it good
        self.nerd["description"][0] += ".  John Doe is unknown."
        self.nerd["description"].append("Heavan can wait.")
        res = self.val.test_description(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 2)
        self.assertEqual(res.passed()[0].label, "2.2.1 description")
        self.assertEqual(res.passed()[1].label, "2.2.2 description")

        self.nerd["description"] = ["John Doe is unknown. drive.google.com"]
        res = self.val.test_description(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "2.2.1 description")
        self.assertEqual(res.failed()[0].label, "2.2.2 description")
        self.assertEqual(len(res.failed()[0].comments), 2)

        # no description
        del self.nerd["description"]
        res = self.val.test_description(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)
        self.assertEqual(res.failed()[0].label, "2.2.1 description")
        self.assertEqual(len(res.failed()[0].comments), 1)

        res = self.val.test_description(self.nerd, want=rev.REC)
        self.assertEqual(res.count_applied(), 0)

    def test_test_keywords(self):
        res = self.val.test_keywords(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 2)
        self.assertEqual(res.passed()[0].label, "2.4.1 keyword")
        self.assertEqual(res.passed()[1].label, "2.4.2 keyword")

        self.nerd["keyword"] += ["testing; simulation"]
        res = self.val.test_keywords(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "2.4.1 keyword")
        self.assertEqual(res.failed()[0].label, "2.4.2 keyword")

        self.nerd["keyword"] = []
        res = self.val.test_keywords(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)
        self.assertEqual(res.failed()[0].label, "2.4.1 keyword")
        
    def test_test_topics(self):
        res = self.val.test_topics(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 2)
        self.assertEqual(res.passed()[0].label, "2.3.1 topic")
        self.assertEqual(res.passed()[1].label, "2.3.2 topic")

        self.nerd["topic"][0]['scheme'] = "goob"
        res = self.val.test_topics(self.nerd)
        self.assertEqual(res.count_applied(), 2)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "2.3.1 topic")
        self.assertEqual(res.failed()[0].label, "2.3.2 topic")

        self.nerd["topic"] = []
        res = self.val.test_topics(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)
        self.assertEqual(res.failed()[0].label, "2.3.1 topic")

    def test_test_has_software(self):
        res = self.val.test_has_software(self.nerd, want=rev.REQ)
        self.assertEqual(res.count_applied(), 0)

        res = self.val.test_has_software(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "1.1.1 links")

        self.nerd['@type'].insert(0, "nrds:SoftwarePublication")
        res = self.val.test_has_software(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_failed(), 1)
        self.assertEqual(res.failed()[0].label, "1.1.1 links")

        self.nerd['components'].append({
            "@type": [ "nrd:AccessPage"],
            "accessURL": "https://github.com/usnistgov/oar-pdr-py"
        })
        res = self.val.test_has_software(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "1.1.1 links")

        self.nerd['components'].pop(-1)
        self.nerd['components'].append({
            "@type": [ "nrds:SoftwareDistribution"],
            "downloadURL": "https://github.com/usnistgov/oar-pdr-py/archive/refs/2.1.3.zip"
        })
        res = self.val.test_has_software(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "1.1.1 links")

        self.nerd['components'].pop(-1)
        self.nerd['landingPage'] = "https://github.com/usnistgov/oar-pdr-py"
        res = self.val.test_has_software(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "1.1.1 links")

    def test_test_has_data(self):
        res = self.val.test_has_data(self.nerd, want=rev.WARN, willUpload=True)
        self.assertEqual(res.count_applied(), 0)

        res = self.val.test_has_data(self.nerd, willUpload=True)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "3.0.1 files")

        res = self.val.test_has_data(self.nerd, willUpload=False)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "3.0.1 links")

        self.nerd['components'] = [c for c in self.nerd['components'] if not is_type(c, "DataFile")]
        res = self.val.test_has_data(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)

        self.nerd['landingPage'] = "https://data.nist.gov/od/id/mds3-1111"
        res = self.val.test_has_data(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)

        self.nerd['landingPage'] = "https://srddata.nist.gov/srd1111"
        res = self.val.test_has_data(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)
        
        self.nerd['landingPage'] = "https://data.nist.gov/od/id/mds3-1111"
        self.nerd['components'].append({
            "@type": "nrd:AccessPage",
            "accessURL": "http://nvdb.nist.gov/db"
        })
        res = self.val.test_has_data(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)

    def test_test_author(self):
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)
        self.assertEqual(res.failed()[0].label, "1.3.1 authors")

        res = self.val.test_author(self.nerd, want=rev.REQ&rev.WARN)
        self.assertEqual(res.count_applied(), 0)
        self.assertEqual(res.count_passed(), 0)

        self.nerd['authors'] = []
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)

        self.nerd['authors'].append({"familyName": "Sade"})
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "1.3.1 authors")
        self.assertEqual(res.failed()[0].label, "1.3.5 authors")
        self.assertEqual(res.failed()[1].label, "1.3.2 authors")
        self.assertEqual(res.failed()[2].label, "1.3.6 authors")
        self.assertTrue(res.failed()[0].comments[0].endswith("1 author"))
        
        self.nerd['authors'].append({"givenName": "Madonna"})
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 1)
        self.assertTrue(res.failed()[0].comments[0].endswith("2 authors"))
        
        res = self.val.test_author(self.nerd, want=rev.WARN)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)
        self.assertEqual(res.failed()[0].label, "1.3.5 authors")

        self.nerd['authors'] = [{"familyName": "Doe", "givenName": "John"}]
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 2)
        self.assertEqual(res.passed()[0].label, "1.3.5 authors")
        self.assertEqual(res.passed()[1].label, "1.3.1 authors")
        self.assertEqual(res.failed()[0].label, "1.3.2 authors")
        self.assertEqual(res.failed()[1].label, "1.3.6 authors")
        
        self.nerd['authors'][0]['affiliation'] = [{ "title": "the National" }]
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 3)
        self.assertEqual(res.passed()[0].label, "1.3.5 authors")
        self.assertEqual(res.passed()[1].label, "1.3.1 authors")
        self.assertEqual(res.passed()[2].label, "1.3.6 authors")
        self.assertEqual(res.failed()[0].label, "1.3.2 authors")

        self.nerd['authors'].append({"familyName": "Sade"})
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "1.3.1 authors")
        self.assertEqual(res.failed()[0].label, "1.3.5 authors")
        self.assertEqual(res.failed()[1].label, "1.3.2 authors")
        self.assertEqual(res.failed()[2].label, "1.3.6 authors")

        del self.nerd['authors'][-1]
        self.nerd['authors'][0]['orcid'] = "https://orcid.org/0000-0000-0000-0000"
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 6)
        self.assertEqual(res.count_passed(), 5)
        self.assertEqual(res.passed()[0].label, "1.3.4 authors")
        self.assertEqual(res.passed()[1].label, "1.3.5 authors")
        self.assertEqual(res.passed()[2].label, "1.3.1 authors")
        self.assertEqual(res.passed()[3].label, "1.3.2 authors")
        self.assertEqual(res.passed()[4].label, "1.3.6 authors")
        self.assertEqual(res.failed()[0].label, "1.3.3 authors")
        
        self.nerd['authors'][0]['orcid'] = "0000-0000-0000-0000"
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 6)
        self.assertEqual(res.count_passed(), 6)
        self.assertEqual(res.passed()[0].label, "1.3.3 authors")
        self.assertEqual(res.passed()[1].label, "1.3.4 authors")
        self.assertEqual(res.passed()[2].label, "1.3.5 authors")
        self.assertEqual(res.passed()[3].label, "1.3.1 authors")
        self.assertEqual(res.passed()[4].label, "1.3.2 authors")
        self.assertEqual(res.passed()[5].label, "1.3.6 authors")

        self.nerd['authors'].append(deepcopy(self.nerd['authors'][0]))
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 6)
        self.assertEqual(res.count_passed(), 5)
        self.assertEqual(res.passed()[0].label, "1.3.3 authors")
        self.assertEqual(res.passed()[1].label, "1.3.5 authors")
        self.assertEqual(res.passed()[2].label, "1.3.1 authors")
        self.assertEqual(res.passed()[3].label, "1.3.2 authors")
        self.assertEqual(res.passed()[4].label, "1.3.6 authors")
        self.assertEqual(res.failed()[0].label, "1.3.4 authors")
        
        self.nerd['authors'][0]['orcid'] = "0000-0000-0000-000X"
        res = self.val.test_author(self.nerd)
        self.assertEqual(res.count_applied(), 6)
        self.assertEqual(res.count_passed(), 6)
        self.assertEqual(res.passed()[0].label, "1.3.3 authors")
        self.assertEqual(res.passed()[1].label, "1.3.4 authors")
        self.assertEqual(res.passed()[2].label, "1.3.5 authors")
        self.assertEqual(res.passed()[3].label, "1.3.1 authors")
        self.assertEqual(res.passed()[4].label, "1.3.2 authors")
        self.assertEqual(res.passed()[5].label, "1.3.6 authors")

    def test_test_files(self):
        res = self.val.test_files(self.nerd)
        self.assertEqual(res.count_applied(), 0)

        res = self.val.test_files(self.nerd, willUpload=True)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "3.4.1 files")
        
        self.nerd['components'] = [c for c in self.nerd['components'] if not is_type(c, "DataFile")]
        res = self.val.test_files(self.nerd, willUpload=True)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)
        self.assertEqual(res.failed()[0].label, "3.4.1 files")
        
        res = self.val.test_files(self.nerd, want=rev.REQ, willUpload=True)
        self.assertEqual(res.count_applied(), 0)

    def test_test_links(self):
        res = self.val.test_links(self.nerd)
        self.assertEqual(res.count_applied(), 0)

        res = self.val.test_links(self.nerd, resourceType="portal")
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 0)
        self.assertEqual(res.failed()[0].label, "1.1.2 links")

        self.nerd['components'].append({
            "@type": "nrd:AccessPage"
        })
        res = self.val.test_links(self.nerd, resourceType="portal")
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 1)
        self.assertEqual(res.passed()[0].label, "1.1.2 links")
        self.assertEqual(res.failed()[0].label, "3.3.1 links")
        self.assertEqual(res.failed()[1].label, "3.3.2 links")
        self.assertEqual(res.failed()[2].label, "3.3.3 links")

        self.nerd['components'][-1]["accessURL"] = "https://mysite.nist.gov/"
        res = self.val.test_links(self.nerd, resourceType="portal")
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 2)
        self.assertEqual(res.passed()[0].label, "3.3.1 links")
        self.assertEqual(res.passed()[1].label, "1.1.2 links")
        self.assertEqual(res.failed()[0].label, "3.3.2 links")
        self.assertEqual(res.failed()[1].label, "3.3.3 links")

        self.nerd['components'][-1]["title"] = "My site"
        res = self.val.test_links(self.nerd, resourceType="portal")
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 3)
        self.assertEqual(res.passed()[0].label, "3.3.1 links")
        self.assertEqual(res.passed()[1].label, "1.1.2 links")
        self.assertEqual(res.passed()[2].label, "3.3.2 links")
        self.assertEqual(res.failed()[0].label, "3.3.3 links")

        self.nerd['components'][-1]["description"] = "It's my site"
        res = self.val.test_links(self.nerd, resourceType="portal")
        self.assertEqual(res.count_applied(), 4)
        self.assertEqual(res.count_passed(), 4)
        self.assertEqual(res.passed()[0].label, "3.3.1 links")
        self.assertEqual(res.passed()[1].label, "1.1.2 links")
        self.assertEqual(res.passed()[2].label, "3.3.2 links")
        self.assertEqual(res.passed()[3].label, "3.3.3 links")

        res = self.val.test_links(self.nerd)
        self.assertEqual(res.count_applied(), 3)
        self.assertEqual(res.count_passed(), 3)
        self.assertEqual(res.passed()[0].label, "3.3.1 links")
        self.assertEqual(res.passed()[1].label, "3.3.2 links")
        self.assertEqual(res.passed()[2].label, "3.3.3 links")

        res = self.val.test_links(self.nerd, want=rev.WARN)
        self.assertEqual(res.count_applied(), 1)
        self.assertEqual(res.count_passed(), 1)



        

        

    def test_validate(self):
        res = self.val.validate(self.nerd)
        self.assertEqual(res.count_applied(), 11)
        self.assertEqual(res.count_passed(), 9)

        self.nerd["description"][0] += ".  John Doe is unknown. Heaven can wait"
        self.nerd['authors'] = [{ "givenName": "John", "familyName": "Doe",
                                  "orcid": "1111-1111-1111-1111",
                                  "affiliation": [{"title": "NIST"}] }]
        self.nerd['components'].append({
            "@type": "nrd:AccessPage",
            "accessURL": "https://mysite.nist.gov/",
            "title": "my site",
            "description": "This is my site"
        })
        res = self.val.validate(self.nerd, willUpload=True, resourceType="portal")
        self.assertEqual(res.count_applied(), 21)
        self.assertEqual(res.count_passed(), 21)
        





if __name__ == '__main__':
    test.main()



