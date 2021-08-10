import os, pdb, sys, json, logging
import subprocess as sp
import zipfile as zip
import unittest as test

from nistoar.testing import *
from nistoar.pdr.preserve.bagit import serialize as ser
import nistoar.pdr.preserve.bagit.builder as bldr
from nistoar.pdr.preserve.bagit.exceptions import BagSerializationError
from nistoar.pdr.exceptions import StateException

def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_builder.log"))
    loghdlr.setLevel(logging.INFO)
    loghdlr.setFormatter(logging.Formatter(bldr.DEF_BAGLOG_FORMAT))
    rootlog.addHandler(loghdlr)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    rmtmpdir()

exedir = os.path.dirname(__file__)
testdata = os.path.join(os.path.dirname(exedir), "data")
badsip = os.path.join(testdata, "badsip")

log = logging.getLogger()

class TestExec(test.TestCase):

    def test_success(self):
        ser._exec("cat trial1.json".split(), badsip, log)

    def test_failure(self):
        with self.assertRaises(sp.CalledProcessError):
            ser._exec("cat goober.txt".split(), badsip, log)

class TestSerialize(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()
        self.tmpdir = self.tf.mkdir("ser")
        
    def tearDown(self):
        self.tf.clean()
        
    def test_zip_serialize(self):
        destfile = "badsip.zip"
        outzip = os.path.join(self.tmpdir, destfile)
        self.assertTrue(not os.path.exists(outzip))

        self.assertEqual(ser.zip_serialize(badsip, self.tmpdir, log, destfile), outzip)
        self.assertTrue(os.path.exists(outzip))
        self.assertTrue(zip.is_zipfile(outzip))
        z = zip.ZipFile(outzip)
        contents = z.namelist()
        self.assertEqual(len(contents), 2)
        self.assertIn("badsip/", contents)
        self.assertIn("badsip/trial1.json", contents)
        
    def test_zip_determine_bagname(self):
        bagdir = os.path.join(testdata, "metadatabag")
        destfile = "metadatabag.zip"
        outzip = os.path.join(self.tmpdir, destfile)
        self.assertTrue(not os.path.exists(outzip))
        ser.zip_serialize(bagdir, self.tmpdir, log, destfile)
        self.assertTrue(os.path.exists(outzip))

        self.assertEqual(ser.zip_determine_bagname(outzip), "metadatabag")
        
    def test_fail_zip_determine_bagname(self):
        bagdir = os.path.join(testdata, "badsip")
        destfile = "metadatabag.zip"
        outzip = os.path.join(self.tmpdir, destfile)
        self.assertTrue(not os.path.exists(outzip))
        ser.zip_serialize(bagdir, self.tmpdir, log, destfile)
        self.assertTrue(os.path.exists(outzip))

        with self.assertRaises(BagSerializationError) as fd:
            ser.zip_determine_bagname(outzip)

    def test_zip_deserialize(self):
        bagdir = os.path.join(testdata, "metadatabag")
        destfile = "metadatabag.zip"
        outzip = os.path.join(self.tmpdir, destfile)
        self.assertTrue(not os.path.exists(outzip))
        ser.zip_serialize(bagdir, self.tmpdir, log, destfile)
        self.assertTrue(os.path.exists(outzip))

        outbag = os.path.join(self.tmpdir, "metadatabag")
        self.assertTrue(not os.path.exists(outbag))
        self.assertEqual(ser.zip_deserialize(outzip, self.tmpdir, log), outbag)
        self.assertTrue(os.path.exists(outbag))
        self.assertTrue(os.path.exists(os.path.join(outbag,"preserv.log")))
        
    def test_zip_serialize_auto(self):
        destfile = "badsip.zip"
        outzip = os.path.join(self.tmpdir, destfile)
        self.assertTrue(not os.path.exists(outzip))

        ser.zip_serialize(badsip, self.tmpdir, log)
        self.assertTrue(os.path.exists(outzip))
        self.assertTrue(zip.is_zipfile(outzip))
        z = zip.ZipFile(outzip)
        contents = z.namelist()
        self.assertEqual(len(contents), 2)
        self.assertIn("badsip/", contents)
        self.assertIn("badsip/trial1.json", contents)
        
    def test_7z_serialize(self):
        destfile = "badsip.7z"
        outzip = os.path.join(self.tmpdir, destfile)
        self.assertTrue(not os.path.exists(outzip))

        ser.zip7_serialize(badsip, self.tmpdir, log, destfile)
        self.assertTrue(os.path.exists(outzip))
        # can't test contents yet
        
    def test_7z_deserialize(self):
        bagdir = os.path.join(testdata, "metadatabag")
        destfile = "metadatabag.zip"
        outzip = os.path.join(self.tmpdir, destfile)
        self.assertTrue(not os.path.exists(outzip))
        ser.zip7_serialize(bagdir, self.tmpdir, log, destfile)
        self.assertTrue(os.path.exists(outzip))

        outbag = os.path.join(self.tmpdir, "metadatabag")
        self.assertTrue(not os.path.exists(outbag))
        self.assertEqual(ser.zip7_deserialize(outzip, self.tmpdir, log), outbag)
        self.assertTrue(os.path.exists(outbag))
        self.assertTrue(os.path.exists(os.path.join(outbag,"preserv.log")))
        
    def test_7z_serialize_auto(self):
        destfile = "badsip.7z"
        outzip = os.path.join(self.tmpdir, destfile)
        self.assertTrue(not os.path.exists(outzip))

        ser.zip7_serialize(badsip, self.tmpdir, log)
        self.assertTrue(os.path.exists(outzip))
        # can't test contents yet
        
    def test_7z_serialize_fail(self):
        outzip = self.tf.track("badsip.7z")
        destdir, destfile = os.path.split(outzip)
        self.assertTrue(not os.path.exists(outzip))

        baddir = os.path.join(badsip, "goob")
        with self.assertRaises(BagSerializationError):
            ser.zip7_serialize(baddir, destdir, log, destfile)
        self.assertTrue(not os.path.exists(outzip))

class TestDefaultSerializer(test.TestCase):
    def setUp(self):
        self.tf = Tempfiles()
        self.ser = ser.DefaultSerializer()
        
    def tearDown(self):
        self.tf.clean()
        
    def testCtor(self):
        self.assertIn('zip', self.ser.formats)
        self.assertIn('7z', self.ser.formats)

    def test_zip(self):
        bagdir = os.path.join(testdata, "metadatabag")
        tmpdir = self.tf.mkdir("ser")
        outzip = os.path.join(tmpdir, "metadatabag.zip")
        self.assertTrue(not os.path.exists(outzip))

        self.assertEqual(self.ser.serialize(bagdir, os.path.dirname(outzip), "zip", log), outzip)
        self.assertTrue(os.path.exists(outzip))
        self.assertTrue(zip.is_zipfile(outzip))
        z = zip.ZipFile(outzip)
        contents = z.namelist()
        # self.assertEqual(len(contents), 2)
        self.assertIn("metadatabag/", contents)
        self.assertIn("metadatabag/preserv.log", contents)

        outbag = os.path.join(tmpdir, "metadatabag")
        self.assertTrue(not os.path.exists(outbag))
        self.assertEqual(self.ser.deserialize(outzip, tmpdir), outbag)
        self.assertTrue(os.path.exists(outbag))
        self.assertTrue(os.path.exists(os.path.join(outbag, "preserv.log")))
        
    def test_7z(self):
        tmpdir = self.tf.mkdir("ser")
        outzip = os.path.join(tmpdir, "badsip.7z")
        self.assertTrue(not os.path.exists(outzip))

        self.ser.serialize(badsip, os.path.dirname(outzip), "7z", log)
        self.assertTrue(os.path.exists(outzip))
        # can't test contents yet
        
        outbag = os.path.join(tmpdir, "badsip")
        self.assertTrue(not os.path.exists(outbag))
        self.ser.deserialize(outzip, tmpdir)
        self.assertTrue(os.path.exists(outbag))
        self.assertTrue(os.path.exists(os.path.join(outbag, "trial1.json")))
        

if __name__ == '__main__':
    test.main()
