import os, sys, pdb, subprocess
import unittest as test

from nistoar.testing import *
import nistoar.pdr.utils.datamgmt as utils
from nistoar.pdr.exceptions import StateException

testdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
testdatadir = os.path.join(testdir, 'data')
testdatadir3 = os.path.join(testdir, 'preserve', 'data')
testdatadir2 = os.path.join(testdatadir3, 'simplesip')
testdatadir4 = os.path.join(testdir, 'distrib', 'data')

class TestMimeTypeLoading(test.TestCase):

    def test_defaults(self):

        self.assertEqual(utils.def_ext2mime['json'], "application/json")
        self.assertEqual(utils.def_ext2mime['txt'], "text/plain")
        self.assertEqual(utils.def_ext2mime['xml'], "text/xml")

    def test_update_mimetypes_from_file(self):
        map = utils.update_mimetypes_from_file(None,
                                  os.path.join(testdatadir, "nginx-mime.types"))
        self.assertEqual(map['mml'], "text/mathml")
        self.assertEqual(map['jpg'], "image/jpeg")
        self.assertEqual(map['jpeg'], "image/jpeg")

        map = utils.update_mimetypes_from_file(map,
                                  os.path.join(testdatadir, "comm-mime.types"))
        self.assertEqual(map['zip'], "application/zip")
        self.assertEqual(map['xml'], "application/xml")
        self.assertEqual(map['xsd'], "application/xml")
        self.assertEqual(map['mml'], "text/mathml")
        self.assertEqual(map['jpg'], "image/jpeg")
        self.assertEqual(map['jpeg'], "image/jpeg")

    def test_build_mime_type_map(self):
        map = utils.build_mime_type_map([])
        self.assertEqual(map['txt'], "text/plain")
        self.assertEqual(map['xml'], "text/xml")
        self.assertEqual(map['json'], "application/json")
        self.assertNotIn('mml', map)
        self.assertNotIn('xsd', map)
        
        map = utils.build_mime_type_map(
            [os.path.join(testdatadir, "nginx-mime.types"),
             os.path.join(testdatadir, "comm-mime.types")])
        self.assertEqual(map['txt'], "text/plain")
        self.assertEqual(map['mml'], "text/mathml")
        self.assertEqual(map['xml'], "application/xml")
        self.assertEqual(map['xsd'], "application/xml")

class TestChecksum(test.TestCase):

    def test_checksum_of(self):
        dfile = os.path.join(testdatadir2,"trial1.json")
        self.assertEqual(utils.checksum_of(dfile), self.syssum(dfile))
        dfile = os.path.join(testdatadir2,"trial2.json")
        self.assertEqual(utils.checksum_of(dfile), self.syssum(dfile))
        dfile = os.path.join(testdatadir2,"trial3/trial3a.json")
        self.assertEqual(utils.checksum_of(dfile), self.syssum(dfile))

    def syssum(self, filepath):
        cmd = ["sha256sum", filepath]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
        (out, err) = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(err + "\nFailed sha256sum command: " +
                               " ".join(cmd))
        return out.decode().split()[0]

class TestMeausreDirSize(test.TestCase):
    def test_measure1(self):
        vals = utils.measure_dir_size(testdatadir)
        self.assertEqual(vals[1], 4)
        self.assertEqual(vals[0], 1405)

    def test_measure2(self):
        vals = utils.measure_dir_size(testdatadir2)
        self.assertEqual(vals[1], 5)
        self.assertEqual(vals[0], 8254)

class TestFormatBytes(test.TestCase):

    def test_format01(self):
        self.assertEqual(utils.formatBytes('goober'), '')
        self.assertEqual(utils.formatBytes(0), '0 Bytes')
        self.assertEqual(utils.formatBytes(1), '1 Byte')
        self.assertEqual(utils.formatBytes(9), '9 Bytes')

    def test_formatD(self):
        self.assertEqual(utils.formatBytes(33812812721, 0), '34 GB')
        self.assertEqual(utils.formatBytes(33812812721, 1), '33.8 GB')
        self.assertEqual(utils.formatBytes(33812812721, 2), '33.81 GB')
        self.assertEqual(utils.formatBytes(33812812721, 3), '33.813 GB')
        self.assertEqual(utils.formatBytes(33812812721, None), '33.8 GB')
        self.assertEqual(utils.formatBytes(33812812721, -5), '33.8 GB')

    def test_formatN(self):
        self.assertEqual(utils.formatBytes(12), '12 Bytes')
        self.assertEqual(utils.formatBytes(999), '999 Bytes')
        self.assertEqual(utils.formatBytes(1002), '1.00 kB')
        self.assertEqual(utils.formatBytes(1078), '1.08 kB')
        self.assertEqual(utils.formatBytes(12781), '12.8 kB')
        self.assertEqual(utils.formatBytes(812721), '812.7 kB')
        self.assertEqual(utils.formatBytes(2812721), '2.81 MB')
        self.assertEqual(utils.formatBytes(12812721), '12.8 MB')
        self.assertEqual(utils.formatBytes(33812812721), '33.8 GB')
        self.assertEqual(utils.formatBytes(1033812812721), '1.03 TB')

class TestRmtree(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()

    def tearDown(self):
        self.tf.clean()

    def touch(self, parent, files):
        if not isinstance(files, (list, tuple)):
            files = [ files ]
        if isinstance(parent, (list, tuple)):
            parent = os.path.join(*parent)

        for f in files:
            with open(os.path.join(parent, f), 'w') as fd:
                fd.write("goober!")

    def test_rmtree(self):
        root = self.tf.mkdir("root")
        os.makedirs(os.path.join(root, "one/two/three"))
        os.makedirs(os.path.join(root, "one/four"))
        self.touch([root, "one/four"], "foo bar chew".split())
        self.touch([root, "one"], "hank snow".split())

        self.assertTrue(os.path.exists(os.path.join(root, "one/two/three")))
        self.assertTrue(os.path.exists(os.path.join(root, "one/four/chew")))

        top = os.path.join(root,"one")
        self.assertTrue(os.path.exists(root))
        self.assertTrue(os.path.exists(top))
        utils.rmtree(top)
        self.assertTrue(os.path.exists(root))
        self.assertFalse(os.path.exists(top))

    def test_rmmtdir(self):
        root = self.tf.mkdir("root")
        top = os.path.join(root,"one")
        os.mkdir(top)
        self.assertTrue(os.path.exists(root))
        self.assertTrue(os.path.exists(top))
        utils.rmtree(top)
        self.assertTrue(os.path.exists(root))
        self.assertFalse(os.path.exists(top))

    def test_rmfile(self):
        root = self.tf.mkdir("root")
        self.touch(root, "one")

        top = os.path.join(root, "one")
        self.assertTrue(os.path.exists(root))
        self.assertTrue(os.path.exists(top))
        utils.rmtree(top)
        self.assertTrue(os.path.exists(root))
        self.assertFalse(os.path.exists(top))

class TestZipFiles(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()

    def tearDown(self):
        self.tf.clean()

    def test_unpack_zip_into(self):
        dest = self.tf.mkdir("bags")
        bagzip = os.path.join(testdatadir4, "pdr1010.mbag0_3-2.zip")
        root = os.path.join(dest, "pdr1010.mbag0_3-2")
        self.assertTrue(not os.path.exists(root))
        
        utils.unpack_zip_into(bagzip, dest)

        self.assertTrue(os.path.exists(root))
        self.assertTrue(os.path.join(root, "stuff"))

    def test_unpack_zip_into_fails(self):
        dest = os.path.join(self.tf.root, "goob")
        self.assertTrue(not os.path.exists(dest))
        bagzip = os.path.join(testdatadir4, "pdr1010.mbag0_3-2.zip")

        try:
            utils.unpack_zip_into(bagzip, dest)
            self.fail("Failed to detect non-existent destination for zip")
        except StateException as ex:
            self.assertIn("zipfile", str(ex))

        try:
            utils.unpack_zip_into(bagzip, dest, "bag")
            self.fail("Failed to detect non-existent destination for zip")
        except StateException as ex:
            self.assertNotIn("zipfile", str(ex))
            self.assertIn("bag", str(ex))

        dest = self.tf.mkdir("bags")
        with self.assertRaises(FileNotFoundError):
            utils.unpack_zip_into("goob.zip", dest)

                          
            


if __name__ == '__main__':
    test.main()
