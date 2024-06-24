import os, json, pdb, logging, tempfile, pathlib
import unittest as test

from nistoar.midas.dbio import inmem, base, AlreadyExists, InvalidUpdate, ObjectNotFound, PartNotAccessible
from nistoar.midas.dbio import project as prj
from nistoar.midas.dap.service import mds3
from nistoar.pdr.utils import read_nerd, prov
from nistoar.nerdm.constants import CORE_SCHEMA_URI

tmpdir = tempfile.TemporaryDirectory(prefix="_test_mds3.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_mds3.log"))
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

# test records
testdir = pathlib.Path(__file__).parents[0]
pdr2210 = testdir.parents[2] / 'pdr' / 'describe' / 'data' / 'pdr2210.json'
ncnrexp0 = testdir.parents[2] / 'pdr' / 'publish' / 'data' / 'ncnrexp0.json'

class TestMDS3DAPService(test.TestCase):

    def setUp(self):
        self.cfg = {
            "clients": {
                "midas": {
                    "default_shoulder": "mdsy"
                },
                "default": {
                    "default_shoulder": "mdsy"
                }
            },
            "dbio": {
                "allowed_project_shoulders": ["mdsy", "spc1"],
                "default_shoulder": "mdsy",
            },
            "assign_doi": "always",
            "doi_naan": "10.88888",
            "nerdstorage": {
#                "type": "fsbased",
#                "store_dir": os.path.join(tmpdir.name)
                "type": "inmem",
            }
        }
        self.dbfact = inmem.InMemoryDBClientFactory({}, { "nextnum": { "mdsy": 2 }})

    def create_service(self):
        self.svc = mds3.DAPService(self.dbfact, self.cfg, nistr, rootlog.getChild("mds3"))
        self.nerds = self.svc._store
        return self.svc

    def test_ctor(self):
        self.create_service()
        self.assertTrue(self.svc.dbcli)
        self.assertEqual(self.svc.cfg, self.cfg)
        self.assertEqual(self.svc.who.actor, "nstr1")
        self.assertEqual(self.svc.who.agent_class, "midas")
        self.assertTrue(self.svc.log)
        self.assertTrue(self.svc._store)
        self.assertTrue(self.svc._valid8r)
        self.assertEqual(self.svc._minnerdmver, (0, 6))

    def test_ids_for(self):
        self.create_service()
        self.assertEqual(self.svc._aipid_for("ncnr0:goob"), "ncnr0-goob")
        self.assertEqual(self.svc._arkid_for("ncnr0:goob"), "ark:/88434/ncnr0-goob")
        self.assertEqual(self.svc._doi_for("ncnr0:goob"), "doi:10.88888/ncnr0-goob")

    def test_create_record(self):
        self.create_service()
        self.assertTrue(not self.svc.dbcli.name_exists("goob"))
        
        prec = self.svc.create_record("goob")
        self.assertEqual(prec.name, "goob")
        self.assertEqual(prec.id, "mdsy:0003")
        self.assertEqual(prec.meta, {"creatorisContact": True, "resourceType": "data"})
        self.assertEqual(prec.owner, "nstr1")
        self.assertIn("_schema", prec.data)
        self.assertNotIn("_extensionSchemas", prec.data)  # contains only data summary
        self.assertEqual(prec.data['doi'], "doi:10.88888/mdsy-0003")
        self.assertEqual(prec.data['@id'], "ark:/88434/mdsy-0003")

        self.assertTrue(self.svc.dbcli.name_exists("goob"))
        prec2 = self.svc.get_record(prec.id)
        self.assertEqual(prec2.name, "goob")
        self.assertEqual(prec2.id, "mdsy:0003")
        self.assertEqual(prec2.data['@id'], "ark:/88434/mdsy-0003")
        self.assertEqual(prec2.data['doi'], "doi:10.88888/mdsy-0003")
        self.assertEqual(prec2.meta, {"creatorisContact": True, "resourceType": "data"})
        self.assertEqual(prec2.owner, "nstr1")

        with self.assertRaises(AlreadyExists):
            self.svc.create_record("goob")

    def test_create_record_withdata(self):
        self.create_service()
        self.assertTrue(not self.svc.dbcli.name_exists("gurn"))

        # goofy data
        prec = self.svc.create_record("gurn", {"color": "red"},
                                      {"temper": "dark", "creatorisContact": "goob",
                                       "softwarelink": "http://..." })  # misspelled key
        self.assertEqual(prec.name, "gurn")
        self.assertEqual(prec.id, "mdsy:0003")
        self.assertEqual(prec.meta, {"creatorisContact": False, "resourceType": "data",
                                     "agent_vehicle": "midas" })
        for key in "_schema @type author_count file_count reference_count".split():
            self.assertIn(key, prec.data)
        self.assertNotIn('color', prec.data)
        self.assertNotIn('contactPoint', prec.data)
        self.assertEqual(prec.data['doi'], "doi:10.88888/mdsy-0003")
        self.assertEqual(prec.data['@id'], "ark:/88434/mdsy-0003")
        self.assertEqual(prec.data['nonfile_count'], 0)

        # some legit metadata but no legit identity info
        prec = self.svc.create_record("goob", {"title": "test"},
                                      {"creatorIsContact": "TRUE",
                                       "softwareLink": "https://github.com/usnistgov/goob" })
        self.assertEqual(prec.name, "goob")
        self.assertEqual(prec.id, "mdsy:0004")
        self.assertEqual(prec.meta, {"creatorisContact": True, "resourceType": "data",
                                     "softwareLink": "https://github.com/usnistgov/goob",
                                     "agent_vehicle": "midas" })
        self.assertEqual(prec.data['doi'], "doi:10.88888/mdsy-0004")
        self.assertEqual(prec.data['@id'], "ark:/88434/mdsy-0004")
        self.assertEqual(prec.data['nonfile_count'], 1)
        self.assertNotIn('contactPoint', prec.data)
        nerd = self.svc._store.open(prec.id)
        links = nerd.nonfiles
        self.assertEqual(len(links), 1)
        self.assertEqual(links.get(0)['accessURL'], prec.meta['softwareLink'])
        

        # inject some identity info into service and try again
        self.svc.who._md.update({"userName": "Gurn", "userLastName": "Cranston", "email": "gurn@thejerk.org"})
        prec = self.svc.create_record("cranston", {"title": "test"},
                                      {"creatorIsContact": True,
                                       "softwareLink": "https://github.com/usnistgov/goob" })
        self.assertEqual(prec.name, "cranston")
        self.assertEqual(prec.id, "mdsy:0005")
        self.assertEqual(prec.meta, {"creatorisContact": True, "resourceType": "data",
                                     "softwareLink": "https://github.com/usnistgov/goob",
                                     "agent_vehicle": "midas" })
        self.assertEqual(prec.data['doi'], "doi:10.88888/mdsy-0005")
        self.assertEqual(prec.data['@id'], "ark:/88434/mdsy-0005")
        self.assertEqual(prec.data['nonfile_count'], 1)
        self.assertIn('contactPoint', prec.data)
        self.assertEqual(prec.data['contactPoint'], {"@type": "vcard:Contact", "fn": "Gurn Cranston",
                                                     "hasEmail": "mailto:gurn@thejerk.org"})
        
    
    def test_moderate_restype(self):
        self.create_service()

        try: 
            resmd = self.svc._moderate_restype([], {"@id": "nrd0:goob", "_schema": CORE_SCHEMA_URI },
                                               self.svc._store.open("nrd0:goob"), True)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(resmd['@type'], ["nrdp:PublicDataResource"])
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/PublicDataResource")]), 1)
        self.assertEqual(len(resmd), 4)

        with self.assertRaises(mds3.InvalidUpdate):
            resmd = self.svc._moderate_restype([], {"@id": "goob", "_schema": CORE_SCHEMA_URI },
                                               self.svc._store.open("goob"), True, doval=True)
        try:
            resmd = self.svc._moderate_restype([], {"@id": "goob", "_schema": CORE_SCHEMA_URI },
                                               self.svc._store.open("goob"), True, doval=False)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(resmd['@type'], ["nrdp:PublicDataResource"])
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/PublicDataResource")]), 1)
        self.assertEqual(len(resmd), 4)

        try: 
            resmd = self.svc._moderate_restype("nrdp:PublicDataResource",
                                               {"@id": "nrd0:goob", "_schema": CORE_SCHEMA_URI },
                                               self.svc._store.open("nrd0:goob"), True)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(resmd['@type'], ["nrdp:PublicDataResource"])
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/PublicDataResource")]), 1)
        self.assertEqual(len(resmd['_extensionSchemas']), 1)
        self.assertEqual(len(resmd), 4)

        basemd = {"@id": "nrd0:goob", "_schema": CORE_SCHEMA_URI }
        nerd = self.svc._store.open("nrd0:goob")
        nerd.replace_res_data(basemd)
        nerd.authors.append({"fn": "Enya"})
        try:
            resmd = self.svc._moderate_restype("PublicDataResource", basemd, nerd, True)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(resmd['@type'], ["nrdp:DataPublication", "nrdp:PublicDataResource"])
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/PublicDataResource")]), 0)
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/DataPublication")]), 1)
        self.assertEqual(len(resmd['_extensionSchemas']), 1)
        self.assertEqual(len(resmd), 4)

        basemd = {"@id": "nrd0:goob", "_schema": CORE_SCHEMA_URI }
        nerd = self.svc._store.open("nrd0:goob")
        nerd.replace_res_data(basemd)
        nerd.authors.append({"fn": "Enya"})
        try:
            resmd = self.svc._moderate_restype("nrdx:SoftwarePublication", basemd, nerd, True, False)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())

        self.assertEqual(resmd['@type'], ["nrdw:SoftwarePublication"])
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/PublicDataResource")]), 0)
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/SoftwarePublication")]), 1)
        self.assertEqual(len(resmd['_extensionSchemas']), 1)
        self.assertEqual(len(resmd), 4)

        basemd = {"@id": "nrd0:goob", "_schema": CORE_SCHEMA_URI, "instrumentsUsed": [] }
        nerd = self.svc._store.open("nrd0:goob")
        nerd.replace_res_data(basemd)
        nerd.authors.append({"fn": "Enya"})
        try:
            resmd = self.svc._moderate_restype("nrd:PublicDataResource", basemd, nerd, True)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(resmd['@type'], ["nrde:ExperimentalData",  "nrdp:DataPublication", 
                                          "nrdp:PublicDataResource"])
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/ExperimentalData")]), 1)
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/DataPublication")]), 1)
        self.assertEqual(len(resmd['_extensionSchemas']), 2)
        self.assertEqual(len(resmd), 5)

        basemd = {"@id": "nrd0:goob", "_schema": CORE_SCHEMA_URI, "instrumentsUsed": [] }
        nerd = self.svc._store.open("nrd0:goob")
        nerd.replace_res_data(basemd)
        nerd.authors.append({"fn": "Enya"})
        try:
            resmd = self.svc._moderate_restype(["ScienceTheme", "dcat:Collection"], basemd, nerd, True)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(resmd['@type'], ["nrda:ScienceTheme", "nrde:ExperimentalData", 
                                          "nrdp:DataPublication", "dcat:Collection"])
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/ExperimentalData")]), 1)
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/DataPublication")]), 1)
        self.assertEqual(len([t for t in resmd['_extensionSchemas']
                                if t.endswith("/ScienceTheme")]), 1)
        self.assertEqual(len(resmd['_extensionSchemas']), 3)
        self.assertEqual(len(resmd), 5)

    def test_moderate_text(self):
        self.create_service()
        self.assertEqual(self.svc._moderate_text("goober"), "goober")
        self.assertEqual(self.svc._moderate_text("goober", {}, False), "goober")

        with self.assertRaises(mds3.InvalidUpdate):
            self.svc._moderate_text(5)
        self.assertEqual(self.svc._moderate_text(5, doval=False), 5)

    def test_moderate_description(self):
        self.create_service()
        self.assertEqual(self.svc._moderate_description("goober"), ["goober"])
        self.assertEqual(self.svc._moderate_description(["goober", "Gurn"], {}, False), ["goober", "Gurn"])

        with self.assertRaises(mds3.InvalidUpdate):
            self.svc._moderate_description(["goober", 5])
        self.assertEqual(self.svc._moderate_description(["goober", 5], doval=False), ["goober", 5])
        self.assertEqual(self.svc._moderate_description(["goober", "", "gurn"]), ["goober", "gurn"])

    def test_moderate_contact(self):
        self.create_service()

        try: 
            contact = self.svc._moderate_contactPoint({"fn": "Gurn Cranston",
                                                       "hasEmail": "gurn.cranston@gmail.com",
                                                       "foo": "bar", "phoneNumber": "Penn6-5000"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(contact['fn'], "Gurn Cranston")
        self.assertEqual(contact['hasEmail'], "mailto:gurn.cranston@gmail.com")
        self.assertEqual(contact['phoneNumber'], "Penn6-5000")
        self.assertNotIn("foo", contact)
        self.assertEqual(contact["@type"], "vcard:Contact")
        self.assertEqual(len(contact), 4)

        try: 
            contact = self.svc._moderate_contactPoint({"fn": "Gurn J. Cranston", "goob": "gurn"},
                                                      {"contactPoint": contact})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(contact['fn'], "Gurn J. Cranston")
        self.assertEqual(contact['hasEmail'], "mailto:gurn.cranston@gmail.com")
        self.assertEqual(contact['phoneNumber'], "Penn6-5000")
        self.assertNotIn("foo", contact)
        self.assertEqual(contact["@type"], "vcard:Contact")
        self.assertEqual(len(contact), 4)

#        with self.assertRaises(mds3.InvalidUpdate):
#            contact = self.svc._moderate_contact({"fn": "Gurn J. Cranston", "goob": "gurn"},
#                                                 {"contactInfo": contact}, True)


        try:
            contact = self.svc._moderate_contactPoint({"fn": "Gurn Cranston", "goob": "gurn"},
                                                      {"contactPoint": contact}, True)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(contact['fn'], "Gurn Cranston")
#        self.assertEqual(contact['hasEmail'], "gurn.cranston@gmail.com")
        self.assertNotIn("hasEmail", contact)
        self.assertNotIn("phoneNumber", contact)
        self.assertNotIn("foo", contact)
        self.assertEqual(contact["@type"], "vcard:Contact")
        self.assertEqual(len(contact), 2)

    def test_moderate_res_data(self):
        self.create_service()
        nerd = self.svc._store.open("nrd0:goob")

        try:
            res = self.svc._moderate_res_data({}, {}, nerd)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(res.get("_schema"), mds3.NERDM_SCH_ID)
        self.assertEqual(res.get("@type"), ["nrdp:PublicDataResource"])
        self.assertEqual(res.get("_extensionSchemas"), [ mds3.NERDMPUB_DEF+"PublicDataResource" ])
        self.assertEqual(len(res), 3)

        with self.assertRaises(InvalidUpdate):
            self.svc._moderate_res_data({"description": 3}, {}, nerd)

        upd = {
            "description": ["This is it."],
            "contactPoint": {
                "fn": "Edgar Allen Poe",
                "hasEmail": "eap@dead.com"
            },
        }
        try:
            res = self.svc._moderate_res_data(upd, res, nerd)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(res.get("_schema"), mds3.NERDM_SCH_ID)
        self.assertEqual(res.get("@type"), ["nrdp:PublicDataResource"])
        self.assertEqual(res.get("_extensionSchemas"), [ mds3.NERDMPUB_DEF+"PublicDataResource" ])
        self.assertEqual(res.get("description"), ["This is it."])
        self.assertIn("contactPoint", res)
        self.assertEqual(res.get("contactPoint",{}).get("hasEmail"), "mailto:eap@dead.com")
        self.assertEqual(res.get("contactPoint",{}).get("@type"), "vcard:Contact")
            

    def test_moderate_author(self):
        self.create_service()

        try:
            auth = self.svc._moderate_author({"familyName": "Cranston", "firstName": "Gurn",
                                              "middleName": "J."})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
            
        self.assertEqual(auth['familyName'], "Cranston")
        self.assertEqual(auth['middleName'], "J.")
        self.assertNotIn("firstName", auth)
        self.assertNotIn("fn", auth)
        self.assertEqual(auth['@type'], "foaf:Person")

        auth['affiliation'] = "NIST"
        try:
            auth = self.svc._moderate_author(auth)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(auth['familyName'], "Cranston")
        self.assertEqual(auth['middleName'], "J.")
        self.assertEqual(auth['@type'], "foaf:Person")
        self.assertEqual(len(auth['affiliation']), 1)
        self.assertEqual(auth['affiliation'][0]['title'], "National Institute of Standards and Technology")
        self.assertEqual(auth['affiliation'][0]['abbrev'], ["NIST"])
        self.assertTrue(auth['affiliation'][0]['@id'].startswith("ror:"))

    def test_replace_update_authors(self):
        self.create_service()
        prec = self.svc.create_record("goob")
        id = prec.id
        nerd = self.svc._store.open(id)
        self.assertEqual(len(nerd.authors), 0)

        with self.assertRaises(InvalidUpdate):
            self.svc.replace_authors(id, {"fn": "Edgar Allen Poe"})

        self.svc.replace_authors(id, [
            { "familyName": "Cranston", "givenName": "Gurn", "middleName": "J." },
            { "fn": "Edgar Allen Poe", "affiliation": "NIST" }
        ])
        self.assertEqual(len(nerd.authors), 2)
        self.assertEqual(nerd.authors.get(0)["givenName"], "Gurn")
        self.assertEqual(nerd.authors.get(0)["@type"], "foaf:Person")
        self.assertEqual(nerd.authors.get(0)["@id"], "auth_0")
        self.assertEqual(len(nerd.authors.get(0)), 5)
        self.assertEqual(nerd.authors.get(1)["fn"], "Edgar Allen Poe")
        self.assertEqual(nerd.authors.get(1)["affiliation"][0]["abbrev"], ["NIST"])
        self.assertEqual(nerd.authors.get(1)["affiliation"][0]["title"],
                         "National Institute of Standards and Technology")
        self.assertEqual(nerd.authors.get(0)["@type"], "foaf:Person")
        self.assertEqual(nerd.authors.get(1)["@id"], "auth_1")
        self.assertEqual(len(nerd.authors.get(0)), 5)

        self.svc.update_author(id, { "fn": "Joe Don Baker" }, 1)
        self.assertEqual(nerd.authors.get(0)["@id"], "auth_0")
        self.assertEqual(nerd.authors.get(0)["givenName"], "Gurn")
        self.assertEqual(len(nerd.authors.get(0)), 5)
        self.assertEqual(nerd.authors.get(1)["@id"], "auth_1")
        self.assertEqual(nerd.authors.get(1)["fn"], "Joe Don Baker")
        self.assertEqual(nerd.authors.get(1)["affiliation"][0]["abbrev"], ["NIST"])
        self.assertEqual(nerd.authors.get(1)["affiliation"][0]["title"],
                         "National Institute of Standards and Technology")
        self.assertEqual(nerd.authors.get(0)["@type"], "foaf:Person")
        self.assertEqual(len(nerd.authors.get(0)), 5)

        self.svc.update_author(id, { "fn": "Joe Don Baker" }, 1, True)
        self.assertEqual(nerd.authors.get(0)["@id"], "auth_0")
        self.assertEqual(nerd.authors.get(0)["givenName"], "Gurn")
        self.assertEqual(len(nerd.authors.get(0)), 5)
        self.assertEqual(nerd.authors.get(1)["@id"], "auth_1")
        self.assertEqual(nerd.authors.get(1)["fn"], "Joe Don Baker")
        self.assertNotIn("affiliation", nerd.authors.get(1))
        self.assertEqual(nerd.authors.get(0)["@type"], "foaf:Person")
        self.assertEqual(len(nerd.authors.get(1)), 3)
        
        # self.svc._update_objlist(nerd.authors, self.svc._moderate_author,
        #                          [{"@id": "auth_1", "fn": "Edgar Allen Poe", "affiliation": "NIST"}])
        self.svc.update_data(id, [{"@id": "auth_1", "fn": "Edgar Allen Poe", "affiliation": "NIST"}],
                             "authors")
        self.assertEqual(nerd.authors.get(0)["@id"], "auth_0")
        self.assertEqual(nerd.authors.get(0)["givenName"], "Gurn")
        self.assertNotIn("fn", nerd.authors.get(0))
        self.assertEqual(len(nerd.authors.get(0)), 5)
        self.assertEqual(nerd.authors.get(1)["@id"], "auth_1")
        self.assertEqual(nerd.authors.get(1)["fn"], "Edgar Allen Poe")
        self.assertEqual(nerd.authors.get(1)["affiliation"][0]["abbrev"], ["NIST"])
        self.assertEqual(nerd.authors.get(1)["affiliation"][0]["title"],
                         "National Institute of Standards and Technology")
        self.assertEqual(nerd.authors.get(0)["@type"], "foaf:Person")
        self.assertEqual(len(nerd.authors.get(1)), 4)

        self.svc.replace_authors(id, [nerd.authors.get(1), nerd.authors.get(0)])
        self.assertEqual(len(nerd.authors), 2)
        self.assertEqual(nerd.authors.get(0)["@id"], "auth_1")
        self.assertEqual(nerd.authors.get(0)["fn"], "Edgar Allen Poe")
        self.assertEqual(nerd.authors.get(0)["affiliation"][0]["abbrev"], ["NIST"])
        self.assertEqual(nerd.authors.get(0)["affiliation"][0]["title"],
                         "National Institute of Standards and Technology")
        self.assertEqual(nerd.authors.get(0)["@type"], "foaf:Person")
        self.assertEqual(len(nerd.authors.get(0)), 4)
        self.assertEqual(nerd.authors.get(1)["givenName"], "Gurn")
        self.assertEqual(nerd.authors.get(1)["@type"], "foaf:Person")
        self.assertEqual(nerd.authors.get(1)["@id"], "auth_0")
        self.assertEqual(len(nerd.authors.get(1)), 5)

        self.svc.add_author(id, {"fn": "Madonna"})
        self.assertEqual(len(nerd.authors), 3)
        self.assertEqual(nerd.authors.get(0)["fn"], "Edgar Allen Poe")
        self.assertEqual(nerd.authors.get(1)["givenName"], "Gurn")
        self.assertEqual(nerd.authors.get(2)["fn"], "Madonna")

    def test_moderate_reference(self):
        self.create_service()

        try:
            ref = self.svc._moderate_reference({"location": "https://doi.org/10.18434/example",
                                                "goob": "gurn"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(ref['location'], "https://doi.org/10.18434/example")
        self.assertNotIn("goob", ref)
        self.assertEqual(ref['refType'], "References")
        self.assertIn('_extensionSchemas', ref)
        self.assertEqual(len(ref['_extensionSchemas']), 1)
        self.assertEqual(ref['_extensionSchemas'][0], mds3.NERDMBIB_DEF+"DCiteReference")
        self.assertEqual(ref["proxyFor"], "doi:10.18434/example")
        self.assertEqual(len(ref), 5)

        try:
            ref = self.svc._moderate_reference({"proxyFor": "doi:10.18434/example",
                                                "_extensionSchemas":
                           ["https://data.nist.gov/od/dm/nerdm-schema/bib/v0.6#/definitions/DCiteReference"],
                                                "goob": "gurn"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(ref['location'], "https://doi.org/10.18434/example")
        self.assertNotIn("goob", ref)
        self.assertEqual(ref['refType'], "References")
        self.assertIn('_extensionSchemas', ref)
        self.assertEqual(len(ref['_extensionSchemas']), 1)
        self.assertEqual(ref['_extensionSchemas'][0], mds3.NERDMBIB_DEF+"DCiteReference")
        self.assertEqual(ref["proxyFor"], "doi:10.18434/example")
        self.assertEqual(len(ref), 5)

        try:
            ref = self.svc._moderate_reference({"location": "doi:10.18434/example", "refType": "myown",
                                                "title": "A Resource", "@id": "#doi:ex",
                                                "abbrev": ["SRB-400"], "citation": "C",
                                                "label": "drink me", "inPreparation": False, 
                                                "goob": "gurn"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(ref, {"location": "doi:10.18434/example", "refType": "myown",
                               "title": "A Resource", "@id": "#doi:ex",
                               "@type": ['deo:BibliographicReference'],
                               "abbrev": ["SRB-400"], "citation": "C",
                               "label": "drink me", "inPreparation": False})

    def test_replace_update_references(self):
        self.create_service()
        prec = self.svc.create_record("goob")
        id = prec.id
        nerd = self.svc._store.open(id)

        with self.assertRaises(InvalidUpdate):
            self.svc.replace_references(id, {"location": "https://doi.org/10.1/blah"})

        self.svc.replace_references(id, [
            {"location": "https://doi.org/10.1/blah"},
            {"proxyFor": "doi:10.18434/example", "goob": "gurn"}
        ])
        self.assertEqual(len(nerd.references), 2)
        self.assertEqual(nerd.references.get(0)["location"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.references.get(0)["refType"], "References")
        self.assertEqual(nerd.references.get(0)["proxyFor"], "doi:10.1/blah")
        self.assertEqual(nerd.references.get(0)["@id"], "ref_0")
        self.assertEqual(nerd.references.get(0)["_extensionSchemas"],
                         [mds3.NERDMBIB_DEF+"DCiteReference"])
        self.assertNotIn("title", nerd.references.get(0))
        self.assertEqual(nerd.references.get(1)["location"], "https://doi.org/10.18434/example")
        self.assertEqual(nerd.references.get(1)["refType"], "References")
        self.assertEqual(nerd.references.get(1)["proxyFor"], "doi:10.18434/example")
        self.assertEqual(nerd.references.get(1)["@id"], "ref_1")
        self.assertEqual(nerd.references.get(1)["_extensionSchemas"],
                         [mds3.NERDMBIB_DEF+"DCiteReference"])
        self.assertNotIn("title", nerd.references.get(1))
        
        self.svc.update_reference(id, {"title": "The End of Film"}, 0)
        self.assertEqual(nerd.references.get(0)["@id"], "ref_0")
        self.assertEqual(nerd.references.get(0)["location"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.references.get(0)["refType"], "References")
        self.assertEqual(nerd.references.get(0)["proxyFor"], "doi:10.1/blah")
        self.assertEqual(nerd.references.get(0)["title"], "The End of Film")
        self.assertEqual(nerd.references.get(1)["@id"], "ref_1")
        self.assertEqual(nerd.references.get(1)["location"], "https://doi.org/10.18434/example")
        self.assertNotIn("title", nerd.references.get(1))
        
        self.svc.replace_references(id, [nerd.references.get(1),
                                         {"proxyFor": "doi:10.2/another"},
                                         nerd.references.get(0)])
        self.assertEqual(len(nerd.references), 3)
        self.assertEqual(nerd.references.get(0)["location"], "https://doi.org/10.18434/example")
        self.assertEqual(nerd.references.get(0)["refType"], "References")
        self.assertEqual(nerd.references.get(0)["proxyFor"], "doi:10.18434/example")
        self.assertEqual(nerd.references.get(0)["@id"], "ref_1")
        self.assertEqual(nerd.references.get(0)["_extensionSchemas"],
                         [mds3.NERDMBIB_DEF+"DCiteReference"])
        self.assertNotIn("title", nerd.references.get(0))
        self.assertEqual(nerd.references.get(1)["location"], "https://doi.org/10.2/another")
        self.assertEqual(nerd.references.get(1)["refType"], "References")
        self.assertEqual(nerd.references.get(1)["proxyFor"], "doi:10.2/another")
        self.assertEqual(nerd.references.get(1)["@id"], "ref_2")
        self.assertEqual(nerd.references.get(1)["_extensionSchemas"],
                         [mds3.NERDMBIB_DEF+"DCiteReference"])
        self.assertNotIn("title", nerd.references.get(1))
        self.assertEqual(nerd.references.get(2)["@id"], "ref_0")
        self.assertEqual(nerd.references.get(2)["location"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.references.get(2)["refType"], "References")
        self.assertEqual(nerd.references.get(2)["proxyFor"], "doi:10.1/blah")
        self.assertEqual(nerd.references.get(2)["title"], "The End of Film")

        self.svc.add_reference(id, {"location": "https://example.com/doc"})
        self.assertEqual(len(nerd.references), 4)
        self.assertEqual(nerd.references.get(0)["location"], "https://doi.org/10.18434/example")
        self.assertEqual(nerd.references.get(1)["location"], "https://doi.org/10.2/another")
        self.assertEqual(nerd.references.get(2)["location"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.references.get(3)["location"], "https://example.com/doc")

    def test_replace_update_nonfiles(self):
        self.create_service()
        prec = self.svc.create_record("goob")
        id = prec.id
        nerd = self.svc._store.open(id)

        with self.assertRaises(InvalidUpdate):
            self.svc.replace_nonfile_components(id, {"filepath": "top.zip"})

        self.svc.replace_nonfile_components(id, [
            {"accessURL": "https://doi.org/10.1/blah"},
        ])
        self.assertEqual(len(nerd.nonfiles), 1)
        self.assertEqual(len(nerd.files), 0)
        self.assertEqual(nerd.nonfiles.get(0)["accessURL"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.nonfiles.get(0)["@id"], "cmp_0")
        self.assertEqual(nerd.nonfiles.get(0)["@type"], ["nrdp:AccessPage"])
        self.assertEqual(nerd.nonfiles.get(0)["_extensionSchemas"], [mds3.NERDMPUB_DEF+"AccessPage"])
        self.assertNotIn("title", nerd.nonfiles.get(0))

        self.svc.add_nonfile_component(id, {"accessURL": "https://doi.org/10.1/blue"})
        self.assertEqual(len(nerd.nonfiles), 2)
        self.assertEqual(len(nerd.files), 0)
        self.assertEqual(nerd.nonfiles.get(0)["accessURL"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.nonfiles.get(0)["@id"], "cmp_0")
        self.assertEqual(nerd.nonfiles.get(0)["@type"], ["nrdp:AccessPage"])
        self.assertEqual(nerd.nonfiles.get(0)["_extensionSchemas"], [mds3.NERDMPUB_DEF+"AccessPage"])
        self.assertEqual(nerd.nonfiles.get(1)["accessURL"], "https://doi.org/10.1/blue")
        self.assertEqual(nerd.nonfiles.get(1)["@id"], "cmp_1")
        self.assertEqual(nerd.nonfiles.get(1)["@type"], ["nrdp:AccessPage"])
        self.assertEqual(nerd.nonfiles.get(1)["_extensionSchemas"], [mds3.NERDMPUB_DEF+"AccessPage"])

        with self.assertRaises(ObjectNotFound):
            self.svc.update_nonfile_component(id, {"title": "The End of Film"}, 2)

        self.svc.update_nonfile_component(id, {"title": "The End of Film"}, 0)
        self.assertEqual(nerd.nonfiles.get(0)["accessURL"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.nonfiles.get(0)["@id"], "cmp_0")
        self.assertEqual(nerd.nonfiles.get(0)["title"], "The End of Film")
        self.assertEqual(nerd.nonfiles.get(0)["@type"], ["nrdp:AccessPage"])
        self.assertEqual(nerd.nonfiles.get(0)["_extensionSchemas"], [mds3.NERDMPUB_DEF+"AccessPage"])
        self.assertNotIn("title", nerd.nonfiles.get(1))

        self.svc.replace_nonfile_components(id, [nerd.nonfiles.get(1), nerd.nonfiles.get(0)])
        self.assertEqual(nerd.nonfiles.get(1)["accessURL"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.nonfiles.get(1)["@id"], "cmp_0")
        self.assertEqual(nerd.nonfiles.get(1)["title"], "The End of Film")
        self.assertEqual(nerd.nonfiles.get(1)["@type"], ["nrdp:AccessPage"])
        self.assertEqual(nerd.nonfiles.get(1)["_extensionSchemas"], [mds3.NERDMPUB_DEF+"AccessPage"])
        self.assertEqual(nerd.nonfiles.get(0)["accessURL"], "https://doi.org/10.1/blue")
        self.assertEqual(nerd.nonfiles.get(0)["@id"], "cmp_1")
        self.assertEqual(nerd.nonfiles.get(0)["@type"], ["nrdp:AccessPage"])
        self.assertEqual(nerd.nonfiles.get(0)["_extensionSchemas"], [mds3.NERDMPUB_DEF+"AccessPage"])
        
        
    def test_replace_update_components(self):
        self.create_service()
        prec = self.svc.create_record("goob")
        id = prec.id
        nerd = self.svc._store.open(id)

        with self.assertRaises(InvalidUpdate):
            self.svc.replace_data(id, {"filepath": "top.zip"}, part="components")

        self.svc.replace_data(id, [
            {"accessURL": "https://doi.org/10.1/blah"},
            {"filepath": "raw", "description": "raw data"},
            {"downloadURL": "pdr:file", "filepath": "raw/data.csv"}
        ], "components")
        self.assertEqual(len(nerd.nonfiles), 1)
        self.assertEqual(nerd.nonfiles.get(0)["accessURL"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.nonfiles.get(0)["@id"], "cmp_0")
        self.assertEqual(nerd.nonfiles.get(0)["@type"], ["nrdp:AccessPage"])
        self.assertEqual(nerd.nonfiles.get(0)["_extensionSchemas"], [mds3.NERDMPUB_DEF+"AccessPage"])
        self.assertEqual(len(nerd.files), 2)
        ids = nerd.files.ids
        self.assertEqual(nerd.files.get(ids[0])["filepath"], "raw")
        self.assertEqual(nerd.files.get(ids[0])["description"], "raw data")
        self.assertEqual(nerd.files.get(ids[0])["@id"], "coll_0")
        self.assertEqual(nerd.files.get(ids[0])["@type"], ["nrdp:Subcollection"])
        self.assertEqual(nerd.files.get(ids[0])["_extensionSchemas"], [mds3.NERDMPUB_DEF+"Subcollection"])
        self.assertEqual(nerd.files.get(ids[1])["filepath"], "raw/data.csv")
        self.assertEqual(nerd.files.get(ids[1])["downloadURL"], "pdr:file")
        self.assertEqual(nerd.files.get(ids[1])["mediaType"], "text/csv")
        self.assertEqual(nerd.files.get(ids[1])["format"], {"description": "data table"})
        self.assertEqual(nerd.files.get(ids[1])["@id"], "file_1")
        self.assertEqual(nerd.files.get(ids[1])["@type"], ["nrdp:DataFile", "nrdp:DownloadableFile"])
        self.assertEqual(nerd.files.get(ids[1])["_extensionSchemas"], [mds3.NERDMPUB_DEF+"DataFile"])
                         
        self.svc.update_data(id, [{"title": "All data", "@id": "file_1"}], "components")
        self.assertEqual(len(nerd.nonfiles), 1)
        self.assertEqual(nerd.nonfiles.get(0)["accessURL"], "https://doi.org/10.1/blah")
        self.assertEqual(nerd.nonfiles.get(0)["@id"], "cmp_0")
        self.assertEqual(nerd.nonfiles.get(0)["@type"], ["nrdp:AccessPage"])
        self.assertEqual(nerd.nonfiles.get(0)["_extensionSchemas"], [mds3.NERDMPUB_DEF+"AccessPage"])
        self.assertNotIn('title', nerd.nonfiles.get(0))
        self.assertEqual(len(nerd.files), 2)
        self.assertEqual(nerd.files.get(ids[0])["filepath"], "raw")
        self.assertEqual(nerd.files.get(ids[0])["description"], "raw data")
        self.assertNotIn('title', nerd.files.get(ids[0]))
        self.assertEqual(nerd.files.get(ids[0])["@id"], "coll_0")
        self.assertEqual(nerd.files.get(ids[0])["@type"], ["nrdp:Subcollection"])
        self.assertEqual(nerd.files.get(ids[0])["_extensionSchemas"], [mds3.NERDMPUB_DEF+"Subcollection"])
        self.assertEqual(nerd.files.get(ids[1])["filepath"], "raw/data.csv")
        self.assertEqual(nerd.files.get(ids[1])["downloadURL"], "pdr:file")
        self.assertEqual(nerd.files.get(ids[1])["mediaType"], "text/csv")
        self.assertEqual(nerd.files.get(ids[1])["format"], {"description": "data table"})
        self.assertEqual(nerd.files.get(ids[1])["@id"], "file_1")
        self.assertEqual(nerd.files.get(ids[1])["title"], "All data")
        self.assertEqual(nerd.files.get(ids[1])["@type"], ["nrdp:DataFile", "nrdp:DownloadableFile"])
        self.assertEqual(nerd.files.get(ids[1])["_extensionSchemas"], [mds3.NERDMPUB_DEF+"DataFile"])
        


    def test_moderate_file(self):
        self.create_service()

        with self.assertRaises(InvalidUpdate):
            self.svc._moderate_file({"_extensionSchemas": ["s", None]})

        try:
            cmp = self.svc._moderate_file({"filepath": "top", "goob": "gurn"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"filepath": "top", "goob": "gurn", "@type": ["nrdp:Subcollection"],
                               "_extensionSchemas": [ mds3.NERDMPUB_DEF+"Subcollection" ]})

        try:
            cmp = self.svc._moderate_file({"filepath": "data.zip", "downloadURL": "pdr:file"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"filepath": "data.zip", "downloadURL": "pdr:file",
                               "@type": ["nrdp:DataFile", "nrdp:DownloadableFile"],
                               "mediaType": "application/zip",
                               "format": {"description": "compressed file archive"},
                               "_extensionSchemas": [ mds3.NERDMPUB_DEF+"DataFile" ]})

        try:
            cmp = self.svc._moderate_file({"filepath": "data.zip", "downloadURL": "pdr:file",
                                           "@type": ["dcat:Distribution"]})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"filepath": "data.zip", "downloadURL": "pdr:file",
                               "@type": ["nrdp:DataFile", "nrdp:DownloadableFile", "dcat:Distribution"],
                               "mediaType": "application/zip",
                               "format": {"description": "compressed file archive"},
                               "_extensionSchemas": [ mds3.NERDMPUB_DEF+"DataFile" ]})

        try:
            cmp = self.svc._moderate_file({"filepath": "data.zip.md5", "downloadURL": "pdr:file",
                                           "@type": ["nrdp:ChecksumFile"]})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"filepath": "data.zip.md5", "downloadURL": "pdr:file",
                               "mediaType": "text/plain",
                               "@type": ["nrdp:ChecksumFile", "nrdp:DownloadableFile"],
                               "format": {"description": "MD5 hash"},
                               "_extensionSchemas": [ mds3.NERDMPUB_DEF+"ChecksumFile" ]})

        try:
            cmp = self.svc._moderate_file({"filepath": "data.zip.md5", "downloadURL": "pdr:file",
                                           "@type": ["MagicFile"]})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"filepath": "data.zip.md5", "downloadURL": "pdr:file",
                               "@type": ["MagicFile", "nrdp:DataFile", "nrdp:DownloadableFile"],
                               "mediaType": "text/plain", 
                               "format": {"description": "MD5 hash"},
                               "_extensionSchemas": [ mds3.NERDMPUB_DEF+"DataFile" ]})

        try:
            cmp = self.svc._moderate_file({"filepath": "data.zip.md5", "downloadURL": "pdr:file",
                                           "@type": ["MagicFile"],
                                           "instrumentsUsed": [{"title": "flugalhorn"},
                                                               {"title": "knife"}]})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp["@type"],
                         ["MagicFile", "nrdp:DataFile", "nrdp:DownloadableFile", "nrde:AcquisitionActivity"])
        self.assertEqual(cmp["_extensionSchemas"],
                         [ mds3.NERDMPUB_DEF+"DataFile", mds3.NERDMEXP_DEF+"AcquisitionActivity" ])
        self.assertEqual(cmp["mediaType"], "text/plain")

        try:
            cmp = self.svc._moderate_file({"filepath": "data", "@type": ["MagicFile"],
                                           "instrumentsUsed": [{"title": "flugalhorn"},
                                                               {"title": "knife"}]})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp["@type"],
                         ["MagicFile", "nrdp:Subcollection", "nrde:AcquisitionActivity"])
        self.assertEqual(cmp["_extensionSchemas"],
                         [ mds3.NERDMPUB_DEF+"Subcollection", mds3.NERDMEXP_DEF+"AcquisitionActivity" ])

    def test_moderate_nonfile(self):
        self.create_service()

        with self.assertRaises(InvalidUpdate):
            self.svc._moderate_nonfile({"filepath": "foo/bar"})
        with self.assertRaises(InvalidUpdate):
            self.svc._moderate_nonfile({})
        with self.assertRaises(InvalidUpdate):
            self.svc._moderate_nonfile({"@type": ["nrdp:Subcollection"], "filepath": ""})

        try:
            cmp = self.svc._moderate_nonfile({"accessURL": "https://is.up/", "filepath": None})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"accessURL": "https://is.up/", "@type": ["nrdp:AccessPage"],
                               "_extensionSchemas": [ mds3.NERDMPUB_DEF+"AccessPage" ]})
        try:
            cmp = self.svc._moderate_nonfile({"accessURL": "https://is.up/", "@type": ["nrdp:SearchPage"]})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"accessURL": "https://is.up/", "@type": ["nrdp:SearchPage"],
                               "_extensionSchemas": [ mds3.NERDMPUB_DEF+"SearchPage" ]})

        try:
            cmp = self.svc._moderate_nonfile({"searchURL": "https://is.up/", "filepath": []})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"searchURL": "https://is.up/", "@type": ["nrdg:DynamicResourceSet"],
                               "_extensionSchemas": [ mds3.NERDMAGG_DEF+"DynamicResourceSet" ]})

        try:
            cmp = self.svc._moderate_nonfile({"resourceType": ["nrdp:DataPublication"], "description": "wow",
                                              "proxyFor": "ark:/88434/bob", "title": "Bob the Blob"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"@type": ["nrd:IncludedResource"], "resourceType": ['nrdp:DataPublication'],
                               "proxyFor": "ark:/88434/bob", "title": "Bob the Blob",
                               "description": "wow", "_extensionSchemas": [mds3.NERDM_DEF+"IncludedResource"]})

    def test_get_sw_desc_for(self):
        self.create_service()
        cmp = self.svc._get_sw_desc_for("https://github.com/foo/bar")
        self.assertEqual(cmp, {
            "@type": ["nrdp:AccessPage"],
            "title": "Software Repository in GitHub",
            "accessURL": "https://github.com/foo/bar"
        })

        cmp = self.svc._get_sw_desc_for("https://bitbucket.com/foo/bar")
        self.assertEqual(cmp, {
            "@type": ["nrdp:AccessPage"],
            "title": "Software Repository",
            "accessURL": "https://bitbucket.com/foo/bar"
        })

    def test_clear_data(self):
        self.create_service()
        prec = self.svc.create_record("goob")
        pdrid = "ark:/88434/%s-%s" % tuple(prec.id.split(":"))
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(set(nerd.keys()),
                         {"_schema", "@id", "doi", "_extensionSchemas", "@context", "@type"})

        nerd = self.svc.update_data(prec.id,
                             {"landingPage": "https://example.com",
                              "contactPoint": { "fn": "Gurn Cranston", "hasEmail": "mailto:gjc1@nist.gov"}})
        self.assertEqual(set(nerd.keys()),
                         {"_schema", "@id", "doi", "_extensionSchemas", "@context", "@type",
                          "contactPoint", "landingPage"})
        self.assertEqual(set(nerd["contactPoint"].keys()), {"@type", "fn", "hasEmail"})

        with self.assertRaises(PartNotAccessible):
            self.assertIs(self.svc.clear_data(prec.id, "goober"), False)
        with self.assertRaises(PartNotAccessible):
            self.assertIs(self.svc.clear_data(prec.id, "contactPoint/hasEmail"), True)
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(set(nerd.keys()),
                         {"_schema", "@id", "doi", "_extensionSchemas", "@context", "@type",
                          "contactPoint", "landingPage"})
        self.assertEqual(set(nerd["contactPoint"].keys()), {"@type", "fn", "hasEmail"})

        self.assertIs(self.svc.clear_data(prec.id, "landingPage"), True)
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(set(nerd.keys()),
                         {"_schema", "@id", "doi", "_extensionSchemas", "@context", "@type",
                          "contactPoint"})
        self.assertIs(self.svc.clear_data(prec.id, "references"), False)

        self.assertIs(self.svc.clear_data(prec.id), True)
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(set(nerd.keys()),
                         {"_schema", "@id", "doi", "_extensionSchemas", "@context", "@type"})
        

    def test_update(self):
        rec = read_nerd(pdr2210)
        self.create_service()

        prec = self.svc.create_record("goob")
        pdrid = "ark:/88434/%s-%s" % tuple(prec.id.split(":"))
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(nerd["@id"], pdrid)
        self.assertEqual(nerd["doi"], "doi:10.88888/mdsy-0003")
        self.assertNotIn("title", nerd)
        self.assertNotIn("authors", nerd)
        self.assertNotIn("references", nerd)
        self.assertNotIn("components", nerd)

        try:
            result = self.svc.replace_data(prec.id, rec)
        except InvalidUpdate as ex:
            self.fail(str(ex) + ":\n" + "\n".join([str(e) for e in ex.errors]))
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(result, nerd)

        self.assertEqual(nerd["@id"], pdrid)
        self.assertEqual(nerd["doi"], "doi:10.88888/mdsy-0003")
        self.assertTrue(nerd["title"].startswith("OptSortSph: "))
        self.assertEqual(nerd["contactPoint"]["fn"], "Zachary Levine")
        self.assertNotIn("bureauCode", nerd)
        self.assertNotIn("ediid", nerd)
        self.assertEqual(len(nerd["references"]), 1)
        self.assertEqual(len(nerd["components"]), 5)
        self.assertNotIn("authors", nerd)

        result = self.svc.update_data(prec.id, {"title": "The End of Food"})
        self.assertEqual(result["@id"], pdrid)
        self.assertEqual(result["title"], "The End of Food")
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(nerd["@id"], pdrid)
        self.assertEqual(nerd["title"], "The End of Food")
        self.assertEqual(nerd["contactPoint"]["fn"], "Zachary Levine")
        self.assertEqual(len(nerd["references"]), 1)
        self.assertEqual(len(nerd["components"]), 5)
        self.assertNotIn("authors", nerd)

        result = self.svc.update_data(prec.id, "The End of Film", "title")
        self.assertEqual(result, "The End of Film")
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(nerd["@id"], pdrid)
        self.assertEqual(nerd["title"], "The End of Film")
        self.assertEqual(nerd["contactPoint"]["fn"], "Zachary Levine")
        self.assertEqual(len(nerd["references"]), 1)
        self.assertEqual(len(nerd["components"]), 5)
        self.assertNotIn("authors", nerd)

        self.assertEqual(nerd["references"][0]["refType"], "IsReferencedBy")
        result = self.svc.update_data(prec.id, {"refType": "References"}, "references/[0]")
        self.assertEqual(result["location"], "https://doi.org/10.1364/OE.24.014100")
        self.assertEqual(result["refType"], "References")
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(nerd["@id"], pdrid)
        self.assertEqual(nerd["title"], "The End of Film")
        self.assertEqual(len(nerd["references"]), 1)
        self.assertEqual(len(nerd["components"]), 5)
        self.assertEqual(nerd["references"][0]["location"], "https://doi.org/10.1364/OE.24.014100")
        self.assertEqual(nerd["references"][0]["refType"], "References")

        with self.assertRaises(ObjectNotFound):
            self.svc.update_data(prec.id, {"refType": "References"}, "references/[1]")
        with self.assertRaises(ObjectNotFound):
            self.svc.update_data(prec.id, {"refType": "References"}, "references/[-2]")
        with self.assertRaises(ObjectNotFound):
            self.svc.update_data(prec.id, {"refType": "References"}, "references/goober")
        with self.assertRaises(InvalidUpdate):
            self.svc.update_data(prec.id, {"refType": "IsGurnTo"}, "references/[0]")

        try:
            result = self.svc.update_data(prec.id, {"refType": "IsSourceOf"}, "references/ref_0")
        except InvalidUpdate as ex:
            self.fail(str(ex) + ":\n" + "\n".join([str(e) for e in ex.errors]))
        self.assertEqual(result["location"], "https://doi.org/10.1364/OE.24.014100")
        self.assertEqual(result["refType"], "IsSourceOf")
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(nerd["@id"], pdrid)
        self.assertEqual(nerd["references"][0]["location"], "https://doi.org/10.1364/OE.24.014100")
        self.assertEqual(nerd["references"][0]["refType"], "IsSourceOf")

        # update a file by its filepath 
        filemd = nerd["components"][1]
        self.assertEqual(filemd["filepath"], "trial1.json")
        self.assertEqual(filemd["size"], 69)
        filemd["size"] = 70
        try:
            result = self.svc.update_data(prec.id, filemd, "components/trial1.json")
        except InvalidUpdate as ex:
            self.fail(str(ex) + ":\n" + "\n".join([str(e) for e in ex.errors]))
        self.assertEqual(result["filepath"], "trial1.json")
        self.assertEqual(result["size"], 70)
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(nerd["@id"], pdrid)
        self.assertEqual(nerd["components"][1]["filepath"], "trial1.json")
        self.assertEqual(nerd["components"][1]["size"], 70)

        rec = read_nerd(ncnrexp0)
        self.assertNotIn("references", rec)
        try:
            result = self.svc.replace_data(prec.id, rec)
        except InvalidUpdate as ex:
            self.fail(str(ex) + ":\n" + "\n".join([str(e) for e in ex.errors]))
        nerd = self.svc.get_nerdm_data(prec.id)
        self.assertEqual(result, nerd)
        self.assertEqual(nerd["@id"], pdrid)
        self.assertEqual(nerd["doi"], "doi:10.88888/mdsy-0003")
        self.assertTrue(nerd["title"].startswith("Neutron "))
        self.assertEqual(len(nerd["authors"]), 2)
        self.assertNotIn("references", nerd)
        self.assertEqual(len(nerd["components"]), 2)

    def test_set_landingpage(self):
        self.create_service()
        prec = self.svc.create_record("goob")
        id = prec.id
        nerd = self.svc._store.open(id)

        self.svc.replace_data(id, "https://example.com/", part="landingPage")
        res = nerd.get_res_data()
        self.assertEqual(res.get('landingPage'), "https://example.com/")
        
                
        

                         
if __name__ == '__main__':
    test.main()
        
        
