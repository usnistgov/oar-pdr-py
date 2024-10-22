import os, json, pdb, logging, tempfile, re
from pathlib import Path
import unittest as test
from copy import deepcopy

from nistoar.midas.dbio import index
import nistoar.nsd.client as nsd

testdir = Path(__file__).parents[0]
datadir = testdir.parents[1] / 'nsd' / 'data'
peopledata = datadir / "person.json"
oudata = datadir / "ou.json"
divdata = datadir / "div.json"

with open(peopledata) as fd:
    peops = json.load(fd)
with open(oudata) as fd:
    ous = json.load(fd)
with open(divdata) as fd:
    divs = json.load(fd)

class TestIndex(test.TestCase):

    def setUp(self):
        self.idx = index.Index()

    def test_ctor(self):
        self.assertEqual(self.idx._data, {})

    def test_register(self):
        self.assertEqual(self.idx.key_labels_for("Bergman"), {})

        self.idx.register("Bergman", 13, "Peter Bergman")
        self.assertEqual(len(self.idx._data), 1)
        self.assertEqual(self.idx._data["bergman"], {13: "Peter Bergman"})
        klm = self.idx.key_labels_for("Bergman")
        self.assertEqual(klm, {13: "Peter Bergman"})
        klm[13] = "Goofy"
        self.assertEqual(self.idx._data["bergman"], {13: "Peter Bergman"})

        self.idx.register("Peter", 13, "Peter Bergman")
        self.assertEqual(self.idx.key_labels_for("Bergman"), {13: "Peter Bergman"})
        self.assertEqual(self.idx.key_labels_for("Peter"), {13: "Peter Bergman"})

        self.idx.register("Phillip", 10, "Phillip Proctor")
        self.idx.register("Phillip", 11, "Phillip Austin")
        self.assertEqual(self.idx.key_labels_for("Phillip"),
                         {10: "Phillip Proctor", 11: "Phillip Austin"})

    def test_export(self):
        self.idx.register("Bergman", 13, "Peter Bergman")
        self.idx.register("Peter", 13, "Peter Bergman")
        self.idx.register("Phillip", 10, "Phillip Proctor")
        self.idx.register("Phillip", 11, "Phillip Austin")

        data = json.loads(self.idx.export_as_json())
        self.assertEqual(set(data.keys()), set("peter bergman phillip".split()))
        self.assertEqual(data["phillip"], {'10': "Phillip Proctor", '11': "Phillip Austin"})

        tbl = self.idx.export_as_csv(';')
        data = {}
        for row in tbl.split("\n"):
            cols = row.split(',')
            if len(cols) > 1:
                maps = dict([tuple(c.split(';')) for c in cols[1:]])
                data[cols[0]] = maps
        self.assertEqual(set(data.keys()), set("peter bergman phillip".split()))
        self.assertEqual(data["phillip"], {'10': "Phillip Proctor", '11': "Phillip Austin"})

    def test_startswith(self):
        self.idx.register("Bergman", 13, "Peter Bergman")
        self.idx.register("Peter", 13, "Peter Bergman")
        self.idx.register("Phillip", 10, "Phillip Proctor")
        self.idx.register("Phillip", 11, "Phillip Austin")

        subidx = self.idx.select_startswith("P")
        self.assertEqual(set(subidx._data.keys()), set("peter phillip".split()))
        self.assertEqual(subidx.key_labels_for("Phillip"),
                         {10: "Phillip Proctor", 11: "Phillip Austin"})

        subidx = self.idx.select_startswith("Berg")
        self.assertEqual(set(subidx._data.keys()), set("bergman".split()))
        self.assertEqual(subidx.key_labels_for("Phillip"), {})
        self.assertEqual(subidx.key_labels_for("Bergman"), {13: "Peter Bergman"})

        subidx = self.idx.select_startswith("Pampoon")
        self.assertEqual(set(subidx._data.keys()), set())
        self.assertEqual(subidx.key_labels_for("Phillip"), {})
        self.assertEqual(subidx.key_labels_for("Bergman"), {})

    def test_clone(self):
        self.idx.register("Bergman", 13, "Peter Bergman")
        self.idx.register("Peter", 13, "Peter Bergman")
        self.idx.register("Phillip", 10, "Phillip Proctor")
        self.idx.register("Phillip", 11, "Phillip Austin")

        clone = self.idx.clone()
        self.assertEqual(self.idx._data, clone._data)
        self.assertIs(self.idx._mkt, self.idx._mkt)

        self.idx = index.Index(False)
        self.idx.register("Bergman", 13, "Peter Bergman")
        self.idx.register("Peter", 13, "Peter Bergman")
        self.idx.register("Phillip", 10, "Phillip Proctor")
        self.idx.register("Phillip", 11, "Phillip Austin")
        self.assertEqual(set(self.idx._data.keys()), set("Peter Bergman Phillip".split()))

        clone = self.idx.clone()
        self.assertEqual(self.idx._data, clone._data)
        self.assertIs(self.idx._mkt, self.idx._mkt)

class TestIndexerOnProperty(test.TestCase):

    def test_default_dispval(self):
        idxr = index.IndexerOnProperty("firstName", "nistUsername")
        idx = idxr.make_index(peops)

        self.assertEqual(set([v for v in idx._data.keys()]),
                         set("phillip david peter".split()))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set("pgp1 pba1 do1 ppb1".split()))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set("Phillip David Peter".split()))
        self.assertEqual(idx._data["phillip"], {'pgp1': "Phillip", 'pba1': "Phillip"})
        self.assertEqual(idx._data["david"], {'do1': "David"})
        self.assertEqual(idx._data["peter"], {'ppb1': "Peter"})

        idx = idxr.make_index(peops, False)
        
        self.assertEqual(set([v for v in idx._data.keys()]),
                         set("Phillip David Peter".split()))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set("pgp1 pba1 do1 ppb1".split()))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set("Phillip David Peter".split()))
        self.assertNotIn("phillip", idx._data)
        self.assertNotIn("david", idx._data)
        self.assertNotIn("peter", idx._data)
        self.assertEqual(idx._data["Phillip"], {'pgp1': "Phillip", 'pba1': "Phillip"})
        self.assertEqual(idx._data["David"], {'do1': "David"})
        self.assertEqual(idx._data["Peter"], {'ppb1': "Peter"})

    def test_custom_dispval(self):
        idxr = index.IndexerOnProperty("firstName", "nistUsername", ["lastName", "firstName"])
        idx = idxr.make_index(peops)

        self.assertEqual(set([v for v in idx._data.keys()]),
                         set("phillip david peter".split()))
        keylabs = dict(idx.iter_key_labels())
        self.assertEqual(set(keylabs.keys()), set("pgp1 pba1 do1 ppb1".split()))
                         
        self.assertEqual(keylabs["pgp1"], "Proctor, Phillip")
        self.assertEqual(keylabs["pba1"], "Austin, Phillip")
        self.assertEqual(keylabs["do1"], "Ossman, David")
        self.assertEqual(keylabs["ppb1"], "Bergman, Peter")
        self.assertEqual(idx._data["phillip"], {'pgp1': "Proctor, Phillip", 'pba1': "Austin, Phillip"})
        self.assertEqual(idx._data["david"], {'do1': "Ossman, David"})
        self.assertEqual(idx._data["peter"], {'ppb1': "Bergman, Peter"})

    def test_custom_dispfmt(self):
        idxr = index.IndexerOnProperty("firstName", "nistUsername", ["lastName", "firstName"], "{1} {0}")
        idx = idxr.make_index(peops)

        self.assertEqual(set(v for v in idx._data.keys()),
                         set("phillip david peter".split()))
        keylabs = dict(idx.iter_key_labels())
        self.assertEqual(set(keylabs.keys()), set("pgp1 pba1 do1 ppb1".split()))
                         
        self.assertEqual(keylabs["pgp1"], "Phillip Proctor")
        self.assertEqual(keylabs["pba1"], "Phillip Austin")
        self.assertEqual(keylabs["do1"], "David Ossman")
        self.assertEqual(keylabs["ppb1"], "Peter Bergman")
        self.assertEqual(idx._data["phillip"], {'pgp1': "Phillip Proctor", 'pba1': "Phillip Austin"})
        self.assertEqual(idx._data["david"], {'do1': "David Ossman"})
        self.assertEqual(idx._data["peter"], {'ppb1': "Peter Bergman"})

    def test_custom_dispfunc(self):
        idxr = index.IndexerOnProperty("firstName", "nistUsername", ["lastName", "firstName"],
                                       lambda s,f: "-".join([f, s]))
        idx = idxr.make_index(peops)

        self.assertEqual(set(v for v in idx._data.keys()),
                         set("phillip david peter".split()))
        keylabs = dict(idx.iter_key_labels())
        self.assertEqual(set(keylabs.keys()), set("pgp1 pba1 do1 ppb1".split()))
                         
        self.assertEqual(keylabs["pgp1"], "Phillip-Proctor")
        self.assertEqual(keylabs["pba1"], "Phillip-Austin")
        self.assertEqual(keylabs["do1"], "David-Ossman")
        self.assertEqual(keylabs["ppb1"], "Peter-Bergman")
        self.assertEqual(idx._data["phillip"], {'pgp1': "Phillip-Proctor", 'pba1': "Phillip-Austin"})
        self.assertEqual(idx._data["david"], {'do1': "David-Ossman"})
        self.assertEqual(idx._data["peter"], {'ppb1': "Peter-Bergman"})

        
class TestDAPAuthorIndexer(test.TestCase):

    def make_data(self):
        auths = {}
        for who in peops:
            auths[who["nistUsername"]] = {
                "familyName": who['lastName'],
                "givenName": who['firstName'],
                "fn": f"{who['firstName']} {who['lastName']}",
                "orcid": f"0000-0000-0000-00{str(who['peopleID'])}",
                "affiliation": [
                    {
                        "title": who["ouName"],
                        "subunits": [ who["divisionName"], who["groupName"] ]
                    }
                ]
            }
        return [
            {
                'id': "mds3:0001",
                'data': {
                    "authors": [
                        deepcopy(auths['pgp1']), deepcopy(auths['pba1'])
                    ]
                }
            },
            {
                'id': "mds3:0002",
                'data': {
                    "authors": [
                        deepcopy(auths['do1']), deepcopy(auths['ppb1'])
                    ]
                }
            },
            {
                'id': "mds3:0003",
                'data': {
                    "authors": [
                        deepcopy(auths['pba1']), deepcopy(auths['do1'])
                    ]
                }
            },
            {
                'id': "mds3:0003",
                'data': {
                    "authors": [
                        deepcopy(auths['pgp1'])
                    ]
                }
            }
        ]

    def test_make_index(self):
        data = self.make_data()
        del data[2]['data']['authors'][1]['orcid']
        idxr = index.DAPAuthorIndexer()
        idx = idxr.make_index(data)

        self.assertEqual(set(idx._data.keys()),
                         set("peter bergman david ossman phillip proctor austin".split()))
        
        keylabs = idx.key_labels_for("ossman")
        self.assertEqual(set(keylabs.keys()), set(["mds3:0002/authors#orcid:0000-0000-0000-0012",
                                                   "mds3:0003/authors#familyName:Ossman"]))
        self.assertEqual(idx.key_labels_for("ossman"), idx.key_labels_for("david"))


class TestNSDOrgResponseIndexer(test.TestCase):

    def test_make_def_index(self):
        idxr = index.NSDOrgResponseIndexer()

        idx = idxr.make_index(ous)

        self.assertEqual(set([v for v in idx._data.keys()]),
                         set(["department of leftovers", "department of covers", "department of failure",
                              "department of spies", "dos", "dof", "doc", "dol", "01", "02", "03", "04"]))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set(range(1,5)))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Department of Leftovers (01)", "Department of Covers (02)",
                              "Department of Failure (03)", "Department of Spies (04)"]))
        
    
class TestNSDPeopleResponseIndexer(test.TestCase):

    def test_make_def_index(self):
        idxr = index.NSDPeopleResponseIndexer()

        idx = idxr.make_index(peops)
    
        self.assertEqual(set([v for v in idx._data.keys()]),
                         set("phillip austin proctor david ossman peter bergman".split()))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set(range(10,14)))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Austin, Phillip", "Ossman, David", "Bergman, Peter", "Proctor, Phillip"]))
        self.assertEqual(idx._data["phillip"], {10: "Proctor, Phillip", 11: "Austin, Phillip"})
        self.assertEqual(idx._data["david"], {12: "Ossman, David"})
        self.assertEqual(idx._data["bergman"], {13: "Bergman, Peter"})

    def test_make_index_on_first(self):
        idxr = index.NSDPeopleResponseIndexer(["firstName"])

        idx = idxr.make_index(peops)
    
        self.assertEqual(set([v for v in idx._data.keys()]),
                         set("phillip david peter".split()))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set(range(10,14)))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Austin, Phillip", "Ossman, David", "Bergman, Peter", "Proctor, Phillip"]))
        self.assertNotIn("ossman", idx._data)
        self.assertNotIn("bergman", idx._data)
        self.assertNotIn("austin", idx._data)
        self.assertEqual(idx._data["phillip"], {10: "Proctor, Phillip", 11: "Austin, Phillip"})
        self.assertEqual(idx._data["david"], {12: "Ossman, David"})
        self.assertEqual(idx._data["peter"], {13: "Bergman, Peter"})

    def test_make_index_on_extra(self):
        idxr = index.NSDPeopleResponseIndexer(["lastName", "firstName", "groupName"])
        idx = idxr.make_index(peops[0:2])

        self.assertEqual(set([v for v in idx._data.keys()]),
                         set(["phillip", "austin", "proctor",
                              "verterans' tapdance administration", "bureau of western mythology"]))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set(range(10,12)))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Austin, Phillip", "Proctor, Phillip"]))
        self.assertNotIn("ossman", idx._data)
        self.assertNotIn("bergman", idx._data)
        self.assertEqual(idx._data["phillip"], {10: "Proctor, Phillip", 11: "Austin, Phillip"})
        self.assertEqual(idx._data["bureau of western mythology"], {10: "Proctor, Phillip"})

serviceurl = None
if os.environ.get('PEOPLE_TEST_URL'):
    serviceurl = os.environ.get('PEOPLE_TEST_URL')

@test.skipIf(not os.environ.get('PEOPLE_TEST_URL'), "test people service not available")
class TestNSDOrgIndexClient(test.TestCase):

    def setUp(self):
        self.nsdcli = nsd.NSDClient(serviceurl)

    def test_make_ou_index(self):
        idxcli = index.NSDOrgIndexClient(self.nsdcli)

        idx = idxcli.get_index_for("ou", "dep")
    
        self.assertEqual(set([v for v in idx._data.keys()]),
                         set(["department of leftovers", "department of covers", "department of failure",
                              "department of spies"]))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set(range(1,5)))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Department of Leftovers (01)", "Department of Covers (02)",
                              "Department of Failure (03)", "Department of Spies (04)"]))

    def test_make_any_index(self):
        idxcli = index.NSDOrgIndexClient(self.nsdcli)

        idx = idxcli.get_index_for("division group ou group".split(), "ve")
    
        self.assertEqual(set([v for v in idx._data.keys()]),
                         set(["verterans' tapdance administration"]))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]), set([8]))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Verterans' Tapdance Administration (10001)"]))

    


@test.skipIf(not os.environ.get('PEOPLE_TEST_URL'), "test people service not available")
class TestNSDPeopleIndexClient(test.TestCase):

    def setUp(self):
        self.nsdcli = nsd.NSDClient(serviceurl)

    def test_make_def_index(self):
        idxcli = index.NSDPeopleIndexClient(self.nsdcli)

        idx = idxcli.get_index_for("p")
    
        self.assertEqual(set([v for v in idx._data.keys()]),
                         set("phillip proctor peter".split()))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set([10, 11, 13]))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Austin, Phillip", "Bergman, Peter", "Proctor, Phillip"]))
        self.assertEqual(idx._data["phillip"], {10: "Proctor, Phillip", 11: "Austin, Phillip"})
        self.assertEqual(idx._data["proctor"], {10: "Proctor, Phillip"})
        self.assertEqual(idx._data["peter"], {13: "Bergman, Peter"})

    def test_make_def_index_muststart(self):
        idxcli = index.NSDPeopleIndexClient(self.nsdcli, enforce_start=True)  # this should have no bearing

        idx = idxcli.get_index_for("p")
    
        self.assertEqual(set([v for v in idx._data.keys()]),
                         set("phillip proctor peter".split()))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set([10, 11, 13]))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Austin, Phillip", "Bergman, Peter", "Proctor, Phillip"]))
        self.assertEqual(idx._data["phillip"], {10: "Proctor, Phillip", 11: "Austin, Phillip"})
        self.assertEqual(idx._data["proctor"], {10: "Proctor, Phillip"})
        self.assertEqual(idx._data["peter"], {13: "Bergman, Peter"})

    def test_make_def_index_indexprops(self):
        idxcli = index.NSDPeopleIndexClient(self.nsdcli, ["groupName", "ouName"])

        idx = idxcli.get_index_for("dEp")
    
        self.assertEqual(set([v for v in idx._data.keys()]), set(["department of failure"]))
        self.assertEqual(set([k for k,d in idx.iter_key_labels()]),
                         set([10, 11, 12, 13]))
        self.assertEqual(set([d for k,d in idx.iter_key_labels()]),
                         set(["Austin, Phillip", "Bergman, Peter", "Proctor, Phillip", "Ossman, David"]))

        idx = idxcli.get_index_for("Bu")
    
        self.assertEqual(set([v for v in idx._data.keys()]), set(["bureau of western mythology"]))
        self.assertEqual([k for k,d in idx.iter_key_labels()], [10])
        self.assertEqual([d for k,d in idx.iter_key_labels()], ["Proctor, Phillip"])

        
    


        
    
        
                         
if __name__ == '__main__':
    test.main()

        
