#! /usr/bin/env python3
#
import sys, os, csv, json, re
import importlib.util as imputil
import unittest as test
from pathlib import Path
from collections import OrderedDict

from nistoar.testing import *
from nistoar.base.config import hget

testdir = Path(__file__).resolve().parents[0]
scrpdir = testdir.parents[0]
basedir = scrpdir.parents[0]
nerdmdir = basedir / "metadata" / "model"

scriptfile = str(scrpdir / "loosen_nerdm.py")

def import_file(path, name=None):
    if not name:
        name = os.path.splitext(os.path.basename(path))[0]
    import importlib.util as imputil
    spec = imputil.spec_from_file_location(name, path)
    out = imputil.module_from_spec(spec)
    sys.modules["loosen"] = out
    spec.loader.exec_module(out)
    return out

loosen = None  # set at end of this file

def setUpModule():
    ensure_tmpdir()

def tearDownModule():
    rmtmpdir()


class TestLoosenNerdm(test.TestCase):

    def test_import(self):
        self.assertIsNotNone(loosen)
        self.assertTrue(hasattr(loosen, 'main'))
        self.assertIsNotNone(loosen.directives_by_file)

    def setUp(self):
        self.tf = Tempfiles()
        self.destdir = self.tf.mkdir("loosen_nerdm")

    def tearDown(self):
        self.tf.clean()

    def test_set_options(self):
        try:
            opts = loosen.set_options(loosen.def_progname, ["-D", "goob", "gurn"])
            self.assertFalse(opts.dedoc)
            self.assertFalse(opts.post2020)
            self.assertEqual(opts.srcdir, "goob")
            self.assertEqual(opts.destdir, "gurn")

            opts = loosen.set_options(loosen.def_progname, ["-J", "goob", "gurn"])
            self.assertTrue(opts.dedoc)
            self.assertTrue(opts.post2020)
            self.assertEqual(opts.srcdir, "goob")
            self.assertEqual(opts.destdir, "gurn")

            opts = loosen.set_options(loosen.def_progname, ["harry", "david"])
            self.assertTrue(opts.dedoc)
            self.assertFalse(opts.post2020)
            self.assertEqual(opts.srcdir, "harry")
            self.assertEqual(opts.destdir, "david")
        except SystemExit as ex:
            self.fail("error processing args")
        
    def test_find_nistoar_code(self):
        self.assertEqual(loosen.find_nistoar_code().parts[-2:], ("metadata", "python"))

    def test_loosen_schema(self):
        with open(nerdmdir/"nerdm-schema.json") as fd:
            schema = json.load(fd, object_pairs_hook=OrderedDict)

        self.assertTrue(hget(schema, "title"))
        self.assertTrue(hget(schema, "description"))
        self.assertTrue(hget(schema, "definitions.Resource.required"))
        self.assertTrue(hget(schema, "definitions.Resource.description"))
        self.assertTrue(hget(schema, "definitions.Organization.required"))
        self.assertTrue(hget(schema, "definitions.Organization.description"))

        loosen.loosen_schema(schema, {"derequire": ["Resource"], "dedocument": True})
        
        self.assertTrue(not hget(schema, "title"))
        self.assertTrue(not hget(schema, "description"))
        self.assertTrue(not hget(schema, "definitions.Resource.required"))
        self.assertTrue(not hget(schema, "definitions.Resource.description"))
        self.assertTrue(hget(schema, "definitions.Organization.required"))
        self.assertTrue(not hget(schema, "definitions.Organization.description"))

    def test_loosen_schema_no_dedoc(self):
        with open(nerdmdir/"nerdm-schema.json") as fd:
            schema = json.load(fd, object_pairs_hook=OrderedDict)

        self.assertTrue(hget(schema, "title"))
        self.assertTrue(hget(schema, "description"))
        self.assertTrue(hget(schema, "definitions.Resource.required"))
        self.assertTrue(hget(schema, "definitions.Resource.description"))
        self.assertTrue(hget(schema, "definitions.Organization.required"))
        self.assertTrue(hget(schema, "definitions.Organization.description"))

        loosen.loosen_schema(schema, {"derequire": ["Resource"], "dedocument": False})
        
        self.assertTrue(hget(schema, "title"))
        self.assertTrue(hget(schema, "description"))
        self.assertTrue(not hget(schema, "definitions.Resource.required"))
        self.assertTrue(hget(schema, "definitions.Resource.description"))
        self.assertTrue(hget(schema, "definitions.Organization.required"))
        self.assertTrue(hget(schema, "definitions.Organization.description"))

    def test_loosen_schema_with_opts(self):
        with open(nerdmdir/"nerdm-schema.json") as fd:
            schema = json.load(fd, object_pairs_hook=OrderedDict)
        opts = loosen.set_options(loosen.def_progname, ["goob", "gurn"])

        self.assertTrue(hget(schema, "title"))
        self.assertTrue(hget(schema, "description"))
        self.assertTrue(hget(schema, "definitions.Resource.required"))
        self.assertTrue(hget(schema, "definitions.Resource.description"))
        self.assertTrue(hget(schema, "definitions.Organization.required"))
        self.assertTrue(hget(schema, "definitions.Organization.description"))

        loosen.loosen_schema(schema, {"derequire": ["Resource"]}, opts)
        
        self.assertTrue(not hget(schema, "title"))
        self.assertTrue(not hget(schema, "description"))
        self.assertTrue(not hget(schema, "definitions.Resource.required"))
        self.assertTrue(not hget(schema, "definitions.Resource.description"))
        self.assertTrue(hget(schema, "definitions.Organization.required"))
        self.assertTrue(not hget(schema, "definitions.Organization.description"))

    def test_loosen_schema_with_opts_D(self):
        with open(nerdmdir/"nerdm-schema.json") as fd:
            schema = json.load(fd, object_pairs_hook=OrderedDict)
        opts = loosen.set_options(loosen.def_progname, ["-D", "goob", "gurn"])

        self.assertTrue(hget(schema, "title"))
        self.assertTrue(hget(schema, "description"))
        self.assertTrue(hget(schema, "definitions.Resource.required"))
        self.assertTrue(hget(schema, "definitions.Resource.description"))
        self.assertTrue(hget(schema, "definitions.Organization.required"))
        self.assertTrue(hget(schema, "definitions.Organization.description"))

        loosen.loosen_schema(schema, {"derequire": ["Resource"]}, opts)
        
        self.assertTrue(hget(schema, "title"))
        self.assertTrue(hget(schema, "description"))
        self.assertTrue(not hget(schema, "definitions.Resource.required"))
        self.assertTrue(hget(schema, "definitions.Resource.description"))
        self.assertTrue(hget(schema, "definitions.Organization.required"))
        self.assertTrue(hget(schema, "definitions.Organization.description"))
        
    def test_process_nerdm_schemas(self):
        schfre = re.compile(r"^nerdm-([a-zA-Z][^\-]*\-)?schema.json$")
        srcfiles = [f for f in os.listdir(nerdmdir) if schfre.match(f)]
        self.assertGreater(len(srcfiles), 6)

        destfiles = [f for f in os.listdir(self.destdir) if not f.startswith('.')]
        self.assertEqual(destfiles, [])
        self.assertEqual(loosen.process_nerdm_schemas(nerdmdir, self.destdir), {})

        destfiles = [f for f in os.listdir(self.destdir) if not f.startswith('.')]
        self.assertIn("nerdm-schema.json", destfiles)
        self.assertIn("nerdm-pub-schema.json", destfiles)
        for schfile in srcfiles:
            self.assertIn(schfile, destfiles)
        self.assertEqual(len(destfiles), len(srcfiles))



    

        
        
if __name__ == '__main__':
    if len(sys.argv) > 1:
        scriptfile = sys.argv[1]
    loosen = import_file(scriptfile)
    test.main()
