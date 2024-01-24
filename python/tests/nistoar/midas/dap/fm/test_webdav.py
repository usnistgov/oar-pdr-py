import os, json, pdb, tempfile, logging
from pathlib import Path
import unittest as test

import nistoar.midas.dap.fm.webdav as dav

from lxml import etree

datadir = Path(__file__).parents[1] / "data"  # tests/nistoar/midas/dap/data
pfrespfile = datadir / "webdav-propfind.xml"

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

class TestWebdavHelpers(test.TestCase):

    def test_propfind_resp_to_dict(self):
        xmlmsg = etree.parse(pfrespfile)
        respel = xmlmsg.getroot()[0]

        props = dav.propfind_resp_to_dict(respel)
        self.assertEqual(props.get('type'), "folder")
        self.assertEqual(props.get('fileid'), "192")
        self.assertEqual(props.get('size'), "4997166")
        self.assertEqual(props.get('permissions'), "RGDNVCK")
        self.assertIn("created", props)
        self.assertIn("modified", props)

    def test_has_info_request(self):
        self.assertIn("propfind", dav.info_request)

    def test_parse_propfind(self):
        with open(pfrespfile) as fd:
            xmlstr = fd.read()
        path = "mds3-0012/mds3-0012"
        baseurl = "https://goober.net/remote.php/dav/files/oar_api"

        props = dav.parse_propfind(xmlstr, path, baseurl)
        self.assertEqual(props.get('type'), "folder")
        self.assertEqual(props.get('fileid'), "192")
        self.assertEqual(props.get('size'), "4997166")
        self.assertEqual(props.get('permissions'), "RGDNVCK")
        self.assertIn("created", props)
        self.assertIn("modified", props)
        

    
        
        
        
                
        

                         
if __name__ == '__main__':
    test.main()
        
        
