#! /usr/bin/env python3
#
import sys, os, csv
import importlib.util as imputil
import unittest as test

from nistoar.testing import * 

scriptfile = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pdrdownload.py")

def import_file(path, name=None):
    if not name:
        name = os.path.splitext(os.path.basename(path))[0]
    import importlib.util as imputil
    spec = imputil.spec_from_file_location(name, path)
    out = imputil.module_from_spec(spec)
    sys.modules["pdrdl"] = out
    spec.loader.exec_module(out)
    return out

pdrdl = None

class TestPDRDownload(test.TestCase):

    def test_import(self):
        self.assertIsNotNone(pdrdl)
        self.assertTrue(hasattr(pdrdl, 'run'))
        self.assertIsNotNone(pdrdl.opts)
        self.assertEqual(pdrdl.prog, pdrdl.def_progname)

    def setUp(self):
        self.tf = Tempfiles()
        self.destdir = self.tf.mkdir("pdrdl")

    def tearDown(self):
        self.tf.clean()

    def test_set_options(self):
        pdrdl.set_options(pdrdl.def_progname, [])
        self.assertEqual(pdrdl.opts.pdrid, pdrdl.def_pdrid)
        self.assertEqual(pdrdl.opts.destdir, os.path.join(os.getcwd(), pdrdl.opts.pdrid))
        self.assertEqual(pdrdl.opts.verbosity, pdrdl.NORMAL)
        self.assertFalse(pdrdl.opts.dodownload)

        pdrdl.set_options("goobit", "-s -d goobdata -I 1899 -D".split())
        self.assertEqual(pdrdl.prog, "goobit")
        self.assertEqual(pdrdl.opts.pdrid, "1899")
        self.assertEqual(pdrdl.opts.destdir, "goobdata")
        self.assertEqual(pdrdl.opts.verbosity, -1)
        self.assertTrue(pdrdl.opts.dodownload)

    @test.skipIf("net" not in os.environ.get("OAR_TEST_INCLUDE",""),
                 "skipping tests requiring access to network")
    def test_download_url_to(self):
        pdrdl.set_options("pdrdownload", "-s".split())
        url = "https://raw.githubusercontent.com/usnistgov/oar-pdr-py/main/README.md"

        outfile = os.path.join(self.destdir,"README.md")
        self.assertFalse(os.path.isfile(outfile))

        result = pdrdl.download_url_to(url, self.destdir)
        self.assertEqual(result[0], outfile)
        self.assertEqual(result[1], "text/plain")
        self.assertTrue(os.path.isfile(outfile))

        outfile = os.path.join(self.destdir,"readme.md")
        self.assertFalse(os.path.isfile(outfile))
        result = pdrdl.download_url_to(url, outfile)
        self.assertEqual(result[0], outfile)
        self.assertEqual(result[1], "text/plain")
        self.assertTrue(os.path.isfile(outfile))

    def test_ensure_filelist_localfile(self):
        listfile = os.path.join(self.destdir, "filelist.csv")

        with self.assertRaises(pdrdl.MortalError):
            lf = pdrdl.ensure_filelist(listfile, self.destdir)

        open(listfile, 'w').close()
        self.assertEqual(pdrdl.ensure_filelist(listfile, self.destdir), listfile)

    @test.skipIf("net" not in os.environ.get("OAR_TEST_INCLUDE",""),
                 "skipping tests requiring access to network")
    def test_get_filelist_csv(self):
        url = "https://data.nist.gov/od/ds/mds2-2417/mds2-2417-filelisting.csv"
        fl = pdrdl.get_filelist(url, self.destdir)
        self.assertEqual(fl, os.path.join(self.destdir, "mds2-2417-filelisting.csv"))
        self.assertTrue(os.path.isfile(fl))

    @test.skipIf("net" not in os.environ.get("OAR_TEST_INCLUDE",""),
                 "skipping tests requiring access to network")
    def test_get_filelist_nerdm(self):
        url = "https://data.nist.gov/rmm/records?@id=ark:/88434/mds2-2417"
        fl = pdrdl.get_filelist(url, self.destdir)
        self.assertEqual(fl, os.path.join(self.destdir, "_filelisting.csv"))
        self.assertTrue(os.path.isfile(fl))
        nlines = 0
        with open(fl) as fd:
            rdr = csv.reader(fd)
            for row in rdr:
                data = row
                nlines += 1
        self.assertEqual(nlines, 7)
        self.assertEqual(row[0], "mds2-2417-filelisting.csv")
        sz = int(row[1])
        self.assertEqual(row[3], "application/vnd.ms-excel")
        self.assertEqual(row[5], "https://data.nist.gov/od/ds/mds2-2417/mds2-2417-filelisting.csv")

    @test.skipIf("net" not in os.environ.get("OAR_TEST_INCLUDE",""),
                 "skipping tests requiring access to network")
    def test_get_default_filelist(self):
        pdrid = "mds2-2417"
        fl = pdrdl.get_default_filelist(pdrid, self.destdir)
        self.assertEqual(fl, os.path.join(self.destdir, "_filelisting.csv"))
        self.assertTrue(os.path.isfile(fl))
        nlines = 0
        with open(fl) as fd:
            rdr = csv.reader(fd)
            for row in rdr:
                data = row
                nlines += 1
        self.assertEqual(nlines, 7)
        self.assertEqual(row[0], "mds2-2417-filelisting.csv")
        sz = int(row[1])
        self.assertEqual(row[3], "application/vnd.ms-excel")
        self.assertEqual(row[5], "https://data.nist.gov/od/ds/mds2-2417/mds2-2417-filelisting.csv")
        
    @test.skipIf("net" not in os.environ.get("OAR_TEST_INCLUDE",""),
                 "skipping tests requiring access to network")
    def test_ensure_filelist_url(self):
        url = "https://data.nist.gov/rmm/records?@id=ark:/88434/mds2-2417"
        fl = pdrdl.ensure_filelist(url, self.destdir)
        self.assertTrue(os.path.isfile(fl))
        nlines = 0
        with open(fl) as fd:
            rdr = csv.reader(fd)
            for row in rdr:
                data = row
                nlines += 1
        self.assertEqual(nlines, 7)
        self.assertEqual(row[0], "mds2-2417-filelisting.csv")
        sz = int(row[1])
        self.assertEqual(row[3], "application/vnd.ms-excel")
        self.assertEqual(row[5], "https://data.nist.gov/od/ds/mds2-2417/mds2-2417-filelisting.csv")
        
    @test.skipIf("net" not in os.environ.get("OAR_TEST_INCLUDE",""),
                 "skipping tests requiring access to network")
    def test_download_files(self):
        url = "https://data.nist.gov/rmm/records?@id=ark:/88434/mds2-2417"
        fl = pdrdl.get_filelist(url, self.destdir)
        self.assertTrue(os.path.isfile(fl))

        failed = os.path.join(self.destdir, "_failed.csv")
        dlc, fldc = pdrdl.download_files(fl, self.destdir, True, failed)

        with open(fl) as fd:
            rdr = csv.reader(fd)
            files = [r[0].lstrip('/') for r in rdr if not r[0].lstrip().startswith('#')]

        for f in files:
            self.assertTrue(os.path.isfile(os.path.join(self.destdir, f)),
                            "Data file not found: "+f)

        self.assertEqual(dlc, 2)
        self.assertEqual(fldc, 0)
        self.assertFalse(os.path.exists(failed))

    def test_check_files(self):
        # set up the fake output directory
        listfile = os.path.join(self.destdir, "filelisting.csv")
        failed = os.path.join(self.destdir, "failed.csv")
        os.mkdir(os.path.join(self.destdir, "sub"))
        with open(listfile, 'w') as fl:
            dfile = os.path.join(self.destdir, "sub", "f1.txt")
            with open(dfile, 'w') as fd:
                print("The quick brown fox jumped over the lazy dogs.", file=fd)
            data = ["sub/f1.txt", str(47), '', 'text/plain',
                    '4ef5816c41c74bcf9528330db5fe259c1fadec92c5318d18f8bb223a308f97e3',
                    "https://example.com/data/sub/f1.txt"]
            print(",".join(data), file=fl)

            data = ["sub/f3.txt", str(31), '', 'text/plain', 'XXX', "https://example.com/data/sub/f3.txt"]
            print(",".join(data), file=fl)

            dfile = os.path.join(self.destdir, "sub", "f2.txt")
            with open(dfile, 'w') as fd:
                print("Every good boy deserves favor.", file=fd)
            data = ["sub/f2.txt", str(31), '', 'text/plain',
                    'XXX',   # '282fcf6aaab7fb33da622614838e62b67bb63584896ab7564f30c788febf1829',
                    "https://example.com/data/sub/f2.txt"]
            print(",".join(data), file=fl)

        self.assertEqual(os.stat(os.path.join(self.destdir, "sub", "f1.txt")).st_size, 47)
        self.assertEqual(os.stat(os.path.join(self.destdir, "sub", "f2.txt")).st_size, 31)
        self.assertFalse(os.path.exists(failed))

        # now test checking
        (dlc, flc) = pdrdl.check_files(listfile, self.destdir, failed, False)

        self.assertTrue(os.path.exists(failed)) # 1 failure
        self.assertEqual(flc, 1)
        self.assertEqual(dlc, 1)

        nf = 0
        with open(failed) as fd:
            rdr = csv.reader(fd)
            for row in rdr:
                if row[0].lstrip().startswith("#"):
                    continue
                if nf > 1:
                    self.fail("Too many failures recorded")
                
                self.assertEqual(row[-1], "Checksum failure")
                self.assertEqual(row[0], "sub/f2.txt")
                nf += 1
        self.assertEqual(nf, 1, "bad checksum not detected")

        self.assertTrue(os.path.exists(os.path.join(self.destdir, "sub", "f1.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.destdir, "sub", "f2.txt")))

        # check only selected
        (dlc, flc) = pdrdl.check_files(listfile, self.destdir, failed, False, ["sub/f1.txt"])
        self.assertEqual(flc, 0)
        self.assertEqual(dlc, 1)
        (dlc, flc) = pdrdl.check_files(listfile, self.destdir, failed, False, ["sub/f2.txt"])
        self.assertEqual(flc, 1)
        self.assertEqual(dlc, 0)
        (dlc, flc) = pdrdl.check_files(listfile, self.destdir, failed, False, ["sub"])
        self.assertEqual(flc, 1)
        self.assertEqual(dlc, 1)
        (dlc, flc) = pdrdl.check_files(listfile, self.destdir, failed, False, ["sub/"])
        self.assertEqual(flc, 1)
        self.assertEqual(dlc, 1)
        
        self.assertTrue(os.path.exists(os.path.join(self.destdir, "sub", "f1.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.destdir, "sub", "f2.txt")))

        # check then remove
        (dlc, flc) = pdrdl.check_files(listfile, self.destdir, failed, True)
        self.assertEqual(flc, 1)
        self.assertEqual(dlc, 1)
        
        self.assertTrue(os.path.exists(os.path.join(self.destdir, "sub", "f1.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.destdir, "sub", "f2.txt")))


        
        
if __name__ == '__main__':
    if len(sys.argv) > 1:
        scriptfile = sys.argv[1]
    pdrdl = import_file(scriptfile)
    test.main()

