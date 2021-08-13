import os, sys, logging, argparse, pdb, imp, time, json, shutil, tempfile
import unittest as test

from nistoar.pdr import cli
from nistoar.pdr.cli.md import recover
from nistoar.pdr.exceptions import PDRException, ConfigurationException
import nistoar.pdr.config as cfgmod

class TestRecoverCmd(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_test_recover")
        self.cmd = cli.PDRCLI()
        self.cmd.load_subcommand(recover)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_parse(self):
        args = self.cmd.parse_args("-q recover -g -w pdr2222 mds2-88888".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "recover")
        self.assertEqual(args.aipids, ["pdr2222", "mds2-88888"])
        self.assertTrue(args.overwrite)
#        self.assertFalse(args.rmmfmt)
        self.assertFalse(args.latestonly)
        self.assertIsNone(args.rmmbase)
        self.assertIsNone(args.srvrbase)
        self.assertIsNone(args.distbase)
        self.assertIsNone(args.outdir)
        self.assertIsNone(args.idfile)
        self.assertEqual(args.outlist, "aipids.lis")

        recover._process_args(args, {"nist_pdr_base": "https://testdata.nist.gov/"}, "recover")
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "recover")
        self.assertEqual(args.aipids, ["pdr2222", "mds2-88888"])
        self.assertTrue(args.overwrite)
#        self.assertFalse(args.rmmfmt)
        self.assertFalse(args.latestonly)
        self.assertEqual(args.srvrbase, "https://testdata.nist.gov/")
        self.assertEqual(args.distbase, "https://testdata.nist.gov/od/ds/")
        self.assertEqual(args.rmmbase,  "https://testdata.nist.gov/rmm/")
        self.assertIsNone(args.idfile)

        args = self.cmd.parse_args("recover -d . -U https://oardev.nist.gov/ pdr2222 ALL mds2-88888".split())
        recover._process_args(args, {"nist_pdr_base": "https://testdata.nist.gov/"}, "recover")
        self.assertEqual(args.srvrbase, "https://oardev.nist.gov/")
        self.assertEqual(args.distbase, "https://oardev.nist.gov/od/ds/")
        self.assertEqual(args.rmmbase, "https://oardev.nist.gov/rmm/")

        args = self.cmd.parse_args("recover -d . -U /goober/gurn pdr2222 ALL mds2-88888".split())
        with self.assertRaises(cli.PDRCommandFailure):
            recover._process_args(args, {"nist_pdr_base": "https://testdata.nist.gov/"}, "recover")
        
        args = self.cmd.parse_args("recover -d . -R ftp://oardev.nist.gov/ pdr2222 ALL mds2-88888".split())
        with self.assertRaises(cli.PDRCommandFailure):
            recover._process_args(args, {"nist_pdr_base": "https://testdata.nist.gov/"}, "recover")

        args = self.cmd.parse_args("recover".split())
        with self.assertRaises(cli.PDRCommandFailure):
            recover._process_args(args, {"nist_pdr_base": "https://testdata.nist.gov/"}, "recover")

        idfile = os.path.join(str(self.tmpdir.name), "aipids.txt")
        with open(idfile, 'w') as fd:
            fd.write("%s %s\n %s\n" % ("goober", "ALL", "gurn"))
        args = self.cmd.parse_args("recover -d . -I".split()+[idfile])
        self.assertEqual(args.idfile, idfile)
        self.assertEqual(len(args.aipids), 0)
        recover._process_args(args, {"nist_pdr_base": "https://testdata.nist.gov/"}, "recover")
        self.assertIn("goober", args.aipids)
        self.assertIn("ALL", args.aipids)
        self.assertIn("gurn", args.aipids)
        self.assertEqual(len(args.aipids), 3)

    def test_write_aipid_list(self):
        outfile = os.path.join(self.tmpdir.name, "ids.list")
        self.assertTrue(not os.path.exists(outfile))
        ids = "foo bar goob gurn".split()

        recover.write_aipid_list(ids, outfile)
        self.assertTrue(os.path.exists(outfile))

        with open(outfile) as fd:
            red = fd.read().split("\n")
        self.assertEqual(len(red), 5)    # has four lines with trailing newline
        self.assertEqual(red[:4], ids)

        with open(outfile) as fd:
            loaded = recover.read_ids_from_file(fd)
        self.assertEqual(len(loaded), 4)
        self.assertEqual(loaded, ids)
        
        

if __name__ == '__main__':
    test.main()

