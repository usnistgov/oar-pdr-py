import os, json, pdb, logging, tempfile
import unittest as test

from nistoar.midas.dbio import inmem, base, AlreadyExists, InvalidUpdate
from nistoar.midas.dbio import project as prj
from nistoar.midas.dap.service import mds3
from nistoar.pdr.publish import prov
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

nistr = prov.PubAgent("midas", prov.PubAgent.USER, "nstr1")


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
            "doi_naan": "88888",
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
        self.assertEqual(self.svc.who.group, "midas")
        self.assertTrue(self.svc.log)
        self.assertTrue(self.svc._store)
        self.assertTrue(self.svc._valid8r)
        self.assertEqual(self.svc._minnerdmver, (0, 6))

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
        self.assertEqual(prec.data['doi'], "doi:88888/mdsy-0003")
        self.assertEqual(prec.data['@id'], "ark:/88434/mdsy-0003")

        self.assertTrue(self.svc.dbcli.name_exists("goob"))
        prec2 = self.svc.get_record(prec.id)
        self.assertEqual(prec2.name, "goob")
        self.assertEqual(prec2.id, "mdsy:0003")
        self.assertEqual(prec2.data['@id'], "ark:/88434/mdsy-0003")
        self.assertEqual(prec2.data['doi'], "doi:88888/mdsy-0003")
        self.assertEqual(prec2.meta, {"creatorisContact": True, "resourceType": "data"})
        self.assertEqual(prec2.owner, "nstr1")

        with self.assertRaises(AlreadyExists):
            self.svc.create_record("goob")

    def hold_test_create_record_withdata(self):
        self.create_service()
        self.assertTrue(not self.svc.dbcli.name_exists("gurn"))
        
        prec = self.svc.create_record("gurn", {"color": "red"},
                                      {"temper": "dark", "creatorisContact": "goob",
                                       "softwarelink": "http://..." })  # misspelled key
        self.assertEqual(prec.name, "gurn")
        self.assertEqual(prec.id, "mdsx:0003")
        self.assertEqual(prec.meta, {"creatorisContact": False, "resourceType": "data"})
        for key in "_schema @context _extensionSchemas".split():
            self.assertIn(key, prec.data)
        self.assertEqual(prec.data['color'], "red")
        self.assertEqual(prec.data['doi'], "doi:88888/mdsx-0003")
        self.assertEqual(prec.data['@id'], "ark:/88434/mdsx-0003")
    
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
            contact = self.svc._moderate_contact({"fn": "Gurn Cranston", "hasEmail": "gurn.cranston@gmail.com",
                                                  "foo": "bar", "phoneNumber": "Penn6-5000"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(contact['fn'], "Gurn Cranston")
        self.assertEqual(contact['hasEmail'], "gurn.cranston@gmail.com")
        self.assertEqual(contact['phoneNumber'], "Penn6-5000")
        self.assertNotIn("foo", contact)
        self.assertEqual(contact["@type"], "vcard:Contact")
        self.assertEqual(len(contact), 4)

        try: 
            contact = self.svc._moderate_contact({"fn": "Gurn J. Cranston", "goob": "gurn"},
                                                 {"contactInfo": contact})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(contact['fn'], "Gurn J. Cranston")
        self.assertEqual(contact['hasEmail'], "gurn.cranston@gmail.com")
        self.assertEqual(contact['phoneNumber'], "Penn6-5000")
        self.assertNotIn("foo", contact)
        self.assertEqual(contact["@type"], "vcard:Contact")
        self.assertEqual(len(contact), 4)

#        with self.assertRaises(mds3.InvalidUpdate):
#            contact = self.svc._moderate_contact({"fn": "Gurn J. Cranston", "goob": "gurn"},
#                                                 {"contactInfo": contact}, True)


        try:
            contact = self.svc._moderate_contact({"fn": "Gurn Cranston", "goob": "gurn"},
                                                 {"contactInfo": contact}, True)
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(contact['fn'], "Gurn Cranston")
#        self.assertEqual(contact['hasEmail'], "gurn.cranston@gmail.com")
        self.assertNotIn("hasEmail", contact)
        self.assertNotIn("phoneNumber", contact)
        self.assertNotIn("foo", contact)
        self.assertEqual(contact["@type"], "vcard:Contact")
        self.assertEqual(len(contact), 2)

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
        self.assertEqual(ref['_extensionSchemas'][0], mds3.NERDMBIB_SCH_ID)
        self.assertEqual(ref["proxyFor"], "doi:10.18434/example")
        self.assertEqual(len(ref), 5)

        try:
            ref = self.svc._moderate_reference({"proxyFor": "doi:10.18434/example",
                                                "goob": "gurn"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(ref['location'], "https://doi.org/10.18434/example")
        self.assertNotIn("goob", ref)
        self.assertEqual(ref['refType'], "References")
        self.assertIn('_extensionSchemas', ref)
        self.assertEqual(len(ref['_extensionSchemas']), 1)
        self.assertEqual(ref['_extensionSchemas'][0], mds3.NERDMBIB_SCH_ID)
        self.assertEqual(ref["proxyFor"], "doi:10.18434/example")
        self.assertEqual(len(ref), 5)
        

        try:
            ref = self.svc._moderate_reference({"location": "doi:10.18434/example", "refType": "myown",
                                                "title": "A Resource", "@id": "#doi:ex",
                                                "abbrev": ["SRB-400"], "citation": "C",
                                                "label": "drink me", "inprep": False, 
                                                "goob": "gurn"})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(ref, {"location": "doi:10.18434/example", "refType": "myown",
                               "title": "A Resource", "@id": "#doi:ex",
                               "@type": ['deo:BibliographicReference'],
                               "abbrev": ["SRB-400"], "citation": "C",
                               "label": "drink me", "inprep": False})

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
                               "mediaType": "application/octet-stream",
                               "format": {"description": "compressed file archive"},
                               "_extensionSchemas": [ mds3.NERDMPUB_DEF+"DataFile" ]})

        try:
            cmp = self.svc._moderate_file({"filepath": "data.zip", "downloadURL": "pdr:file",
                                           "@type": ["dcat:Distribution"]})
        except InvalidUpdate as ex:
            self.fail("Validation Error: "+ex.format_errors())
        self.assertEqual(cmp, {"filepath": "data.zip", "downloadURL": "pdr:file",
                               "@type": ["nrdp:DataFile", "nrdp:DownloadableFile", "dcat:Distribution"],
                               "mediaType": "application/octet-stream",
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


        


                         
if __name__ == '__main__':
    test.main()
        
        
