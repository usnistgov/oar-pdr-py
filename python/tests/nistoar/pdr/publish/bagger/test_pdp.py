# These unit tests test the nistoar.pdr.publish.bagger.pdp module.  These tests
# do not include support for updating previously published datasets (via use of 
# the UpdatePrepService class).  Because testing support for updates require 
# simulated RMM and distribution services to be running, they have been 
# seperated out into test_pdp_update.py.
#
import os, sys, pdb, shutil, logging, json, time, re
from pathlib import Path

import unittest as test
from collections import OrderedDict
from collections.abc import Mapping
from copy import deepcopy

from nistoar.testing import *
from nistoar.pdr import utils
import nistoar.pdr.preserve.bagit.builder as bldr
from nistoar.pdr.preserve.bagit import NISTBag, utils as bagutils
import nistoar.pdr.publish.bagger.pdp as pdp
import nistoar.pdr.exceptions as exceptions
from nistoar.pdr.preserve import AIPValidationError
from nistoar.pdr.publish import idmint as minter
from nistoar.pdr.publish import BadSIPInputError
from nistoar.nerdm import constants as consts
from nistoar.pdr.utils import prov

# datadir = nistoar/preserve/data
datadir = Path(__file__).parents[2] / 'preserve' / 'data'
datadir2 = Path(__file__).parents[1] / 'data'
simplenerd = datadir / '1491nerdm.json'
sipbag = datadir / 'mds3sipbag'
sipbagd = sipbag / 'data'
sipbagmd = sipbag / 'metadata'

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
#    logging.basicConfig(filename=os.path.join(tmpdir(),"test_builder.log"),
#                        level=logging.INFO)
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_bagger.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter(bldr.DEF_BAGLOG_FORMAT))
    rootlog.addHandler(loghdlr)
    rootlog.setLevel(logging.DEBUG)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
        loghdlr.close()
        loghdlr = None
    rmtmpdir()

def to_dict(odict):
    # converts all OrderDict instances in a Mapping into plain dicts (so that they can be tested for equality)
    out = dict(odict)
    for prop in out:
        if isinstance(out[prop], OrderedDict):
            out[prop] = to_dict(out[prop])
        if isinstance(out[prop], (list, tuple)):
            for i in range(len(out[prop])):
                if isinstance(out[prop][i], OrderedDict):
                    out[prop][i] = to_dict(out[prop][i])
    return out

tstag = prov.Agent("test", prov.Agent.AUTO, "tester")

class TestPDPBagger(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()
        self.workdir = self.tf.mkdir("work")
        self.mintdir = self.tf.mkdir("idregs")
        self.bagparent = Path(self.workdir) / 'sipbags'
        self.cfg = {
            "working_dir": self.workdir,
            "bag_builder": {
                "validate_id": True,
                "init_bag_info": {
                    'NIST-BagIt-Version': "X.3",
                    "Organization-Address": ["100 Bureau Dr.",
                                             "Gaithersburg, MD 20899"]
                },
                "ensure_nerdm_type_on_add": bldr.NERDM_SCH_ID_BASE + "v0.7"
            },
            "finalize": {},
            "doi_naan": "10.22222",
            "repo_base_url": "https://test.data.gov/"
        }
        self.mntrcfg = {
            "id_shoulder": 'pdp1',
            "naan": "88888",
            "store_dir":  self.mintdir,
            "sequence_start": 17
        }
        self.minter = minter.PDP0Minter(self.mntrcfg)

    def tearDown(self):
        self.tf.clean()

    def set_bagger_for(self, sipid, **kw):
        self.bgr = pdp.PDPBagger(sipid, self.cfg, self.minter, **kw)
        return self.bgr

    def test_ctor(self):
        self.assertFalse(self.bagparent.exists())
        self.set_bagger_for("pdp1:goob")
        self.assertEqual(self.bgr.convention, "pdp0")
        self.assertEqual(self.bgr.sipid, "pdp1:goob")
        self.assertIsNotNone(self.bgr.bagbldr)
        self.assertTrue(self.bagparent.is_dir())
        self.assertEqual(self.bgr.bagdir, str(self.bagparent / 'pdp1:goob'))
        self.assertEqual(self.bgr.cfg['resolver_base_url'], "https://test.data.gov/od/id/")
        self.assertEqual(self.bgr.cfg['assign_doi'], "request")

    def test_ensure_base_bag(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())

        self.cfg['bag_builder']['validate_id'] = False
        self.set_bagger_for("pdp1:goob", id="ark:/88888/goober")
        self.assertEqual(self.bgr.bagdir, str(bagdir))
        self.assertEqual(self.bgr.id, "ark:/88888/goober")

        self.bgr.ensure_base_bag()
        self.assertTrue(bagdir.is_dir())
        self.assertIsNotNone(self.bgr.bag)
        self.assertEqual(self.bgr.bag.dir, str(bagdir))
        self.assertTrue(bagdir.joinpath('metadata').is_dir())
        self.assertTrue(bagdir.joinpath('metadata','nerdm.json').is_file())
        nerd = self.bgr.bag.nerd_metadata_for('')
        self.assertEqual(nerd.get('version'), '1.0.0')
        self.assertEqual(nerd.get('pdr:sipid'), 'pdp1:goob')
        self.assertEqual(nerd.get('pdr:aipid'), 'goober')
        self.assertEqual(nerd.get('programCode'), ["006:045"])
        self.assertIn('publisher', nerd)

        self.assertTrue(bagdir.joinpath('metadata','annot.json').is_file())
        nerd = self.bgr.bag.annotations_metadata_for('')
        self.assertEqual(nerd.get('pdr:sipid'), 'pdp1:goob')
        self.assertEqual(nerd.get('pdr:aipid'), 'goober')
        self.assertEqual(nerd.get('programCode'), ["006:045"])
        self.assertIn('publisher', nerd)
        self.assertNotIn('version', nerd)

    def test_id_for(self):
        self.set_bagger_for("pdp1:goob")
        self.assertIsNone(self.bgr.id)

        self.assertIsNone(self.bgr._id_for("pdp0:foo"))
        id = self.bgr._id_for("pdp0:foo", True)
        self.assertEqual(id, "ark:/88888/pdp1-0017sm")
        self.assertEqual(self.bgr._id_for("pdp0:foo"), id)
        self.assertEqual(self.bgr._id_for("pdp0:foo", True), id)

        self.minter.baseondata = True
        self.assertEqual(self.bgr._id_for("pdp0:foo"), id)
        self.assertEqual(self.bgr._id_for("pdp1:foo", True), "ark:/88888/pdp1-foopj")
        with self.assertRaises(ValueError):
            self.bgr._id_for("pdp0:bar", True)

    def test_aipid_for(self):
        self.set_bagger_for("pdp1:goob")
        self.assertEqual(self.bgr._aipid_for("ark:/88888/pdp1-foopj"), "pdp1-foopj")
        self.assertEqual(self.bgr._aipid_for("ark:/88434/pdp1-foopj"), "pdp1-foopj")
        self.assertEqual(self.bgr._aipid_for("ark:/88434/pdp0-0029x"), "pdp0-0029x")

    def test_ensure_preparation(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())

        self.set_bagger_for("pdp1:goob")
        self.bgr.ensure_preparation()
        self.assertTrue(bagdir.is_dir())
        self.assertIsNotNone(self.bgr.bag)
        self.assertEqual(self.bgr.bag.dir, str(bagdir))
        self.assertTrue(bagdir.joinpath('metadata').is_dir())
        self.assertTrue(bagdir.joinpath('metadata','nerdm.json').is_file())
        self.assertEqual(self.bgr.id, "ark:/88888/pdp1-0017sm")

        md = self.bgr.bag.nerd_metadata_for('', True)
        self.assertNotIn('doi', md)

    def test_ensure_doi(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())

        self.set_bagger_for("pdp1:goob")
        self.assertEqual(self.bgr.cfg['assign_doi'], "request")
        self.bgr.cfg['assign_doi'] = 'never'
        self.bgr.ensure_doi()
        if self.bgr.bag:     # may be None if ensure_doi() has nothing to do
            md = self.bgr.bag.nerd_metadata_for('', True)
            self.assertNotIn('doi', md)
        
        self.bgr.cfg['assign_doi'] = 'request'
        self.bgr.ensure_doi()
        md = self.bgr.bag.nerd_metadata_for('', True)
        self.assertEqual(md.get('doi'), "doi:10.22222/pdp1-0017sm")

    def test_ensure_doi_always(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())

        self.set_bagger_for("pdp1:goob")
        self.assertEqual(self.bgr.cfg['assign_doi'], "request")
        self.bgr.cfg['assign_doi'] = 'always'
        self.bgr.prepare()
        md = self.bgr.bag.nerd_metadata_for('', True)
        self.assertEqual(md.get('doi'), "doi:10.22222/pdp1-0017sm")
        
    def test_set_res_nerdm(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())
        self.set_bagger_for("pdp1:goob")

        # nerd = utils.read_json(str(simplenerd))
        nerd = utils.read_json(str(datadir2 / 'ncnrexp0.json'))
        nerd['bureauCode'] = [ "666:66" ]
        nerd['accessLevel'] = "private"
        nerd['pdr:status'] = "hungry"
        nerd['pdr:siptype'] = "innocent"
        # pubshr = nerd['publisher']
        self.bgr.set_res_nerdm(nerd, None, False)

        saved = self.bgr.bag.nerdm_record(True)
        self.assertEqual(saved.get('version'), '1.0.0')
        self.assertEqual(saved.get('@id'), 'ark:/88888/pdp1-0017sm')
        self.assertEqual(saved.get('pdr:sipid'), 'pdp1:goob')
        self.assertEqual(saved.get('pdr:aipid'), 'pdp1-0017sm')
        self.assertEqual(saved.get('bureauCode'), ["006:55"])
        self.assertEqual(saved.get('programCode'), ["006:045"])
        self.assertEqual(saved.get('accessLevel'), "private")
        self.assertIn('publisher', saved)
        self.assertIn("Neutron", saved['title'])
        self.assertEqual(len(saved['authors']), 2)
        self.assertEqual(saved['contactPoint']['fn'], "Joe Dura")
        self.assertEqual(saved['contactPoint']['@type'], "vcard:Contact")
        self.assertEqual(len(saved['components']), 0)
        self.assertIn(consts.SIP_SCHEMA_URI+"#/definitions/PDRSubmission", saved.get('_extensionSchemas'))
        self.assertEqual(len(saved.get('_extensionSchemas')), 3)
        self.assertNotIn('pdr:siptype', saved)
        self.assertNotIn('pdr:status', saved)

    def test_set_comp_nerdm(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())
        self.set_bagger_for("pdp1:goob")

        nerd = utils.read_json(str(simplenerd))
        nerd['bureauCode'] = [ "666:66" ]
        nerd['accessLevel'] = "private"
        pubshr = nerd['publisher']

        # add in a Hidden component for next test
        accURLcomp = [c for c in nerd['components'] if "nrd:AccessPage" in c['@type']]
        accURLcomp[0]['@type'][0] = "nrd:Hidden"

        with self.assertRaises(BadSIPInputError):
            # because record include Hidden component
            self.bgr.set_res_nerdm(nerd, None, True)
        # partial save?

        acccmp = nerd['components'][2]
        del nerd['components'][2]
        self.bgr.set_res_nerdm(nerd, None, True)  # saves components, too
        saved = self.bgr.bag.nerdm_record('', True)

        self.assertEqual(saved.get('version'), '1.0.0')
        self.assertEqual(saved.get('@id'), 'ark:/88888/pdp1-0017sm')
        self.assertEqual(len(saved['components']), 6)

        cmp = saved['components'][0]
        self.assertIn('downloadURL', cmp)
        self.assertIn('mediaType', cmp)
        self.assertIn('filepath', cmp)
        self.assertNotIn('/', cmp['filepath'])
        self.assertNotIn('format', cmp)

        cmp['format'] = 'Mathmatica notebook'
        cmp['_schema'] = saved["_schema"] + "/definitions/Component"
        self.bgr.set_comp_nerdm(cmp, None)
        saved = self.bgr.bag.nerd_metadata_for(cmp['filepath'], True)
        self.assertEqual(saved['downloadURL'], cmp['downloadURL'])
        self.assertEqual(saved['mediaType'], cmp['mediaType'])
        self.assertEqual(saved['format'], "Mathmatica notebook")

        self.bgr.set_comp_nerdm({
            '_schema': saved["_schema"] + "/definitions/Component",
            'downloadURL': "https://s3.amazonaws.com/nist-midas/1491_README.txt",
        }, None)
        saved = self.bgr.bag.nerdm_record(True)
        self.assertEqual(len(saved['components']), 7)
        saved = self.bgr.bag.nerd_metadata_for('1491_README.txt', True)
        self.assertEqual(saved['filepath'], '1491_README.txt')
        self.assertEqual(saved['downloadURL'], "https://s3.amazonaws.com/nist-midas/1491_README.txt")
        self.assertEqual(saved['mediaType'], "text/plain")
#        self.assertEqual(saved['format'], "text data")

        self.assertIn(consts.PUB_SCHEMA_URI+"#/definitions/DataFile", saved.get('_extensionSchemas'))
        self.assertEqual(len(saved.get('_extensionSchemas')), 1)

    def test_set_comp_nerdm_compfirst(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())
        self.set_bagger_for("pdp1:goob")

        self.bgr.set_comp_nerdm({
            '_schema': consts.CORE_SCHEMA_URI + "#/definitions/Component",
            'downloadURL': "https://s3.amazonaws.com/nist-midas/1491_README.txt",
        }, None)
        saved = self.bgr.bag.nerdm_record(True)
        self.assertEqual(saved.get('version'), '1.0.0')
        self.assertEqual(saved.get('@id'), 'ark:/88888/pdp1-0017sm')
        self.assertEqual(saved.get('pdr:sipid'), 'pdp1:goob')
        self.assertEqual(saved.get('pdr:aipid'), 'pdp1-0017sm')
        self.assertEqual(len(saved['components']), 1)
        saved = self.bgr.bag.nerd_metadata_for('1491_README.txt', True)
        self.assertEqual(saved['filepath'], '1491_README.txt')
        self.assertEqual(saved['downloadURL'], "https://s3.amazonaws.com/nist-midas/1491_README.txt")
        self.assertEqual(saved['mediaType'], "text/plain")

    def test_add_data_file(self):
        self.set_bagger_for("pdp1:goob")
        self.bgr.prepare()
        bnerd = self.bgr.bag.nerdm_record()
        self.assertEqual(len(bnerd.get('components', [])), 0)
        
        nerd = utils.read_json(str(sipbagmd/'trial1.json'/'nerdm.json'))
        self.bgr.set_comp_nerdm(nerd, None, True) 
        dfile = sipbagd/'trial1.json'

        # add file after metadata
        self.bgr.add_data_file(dfile, 'trial1.json')
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, nerd['filepath'])))
        bnerd = self.bgr.bag.nerdm_record()
        self.assertEqual(len(bnerd['components']), 1)
        cnerd = bnerd['components'][0]
        self.assertEqual(cnerd['filepath'], nerd['filepath'])
        self.assertEqual(cnerd['size'], nerd['size'])
        self.assertIn('checksum', cnerd)

        # add file with metadata (and in subcollection)
        nerd = utils.read_json(str(sipbagmd/'trial3'/'trial3a.json'/'nerdm.json'))
        self.assertIn('checksum', nerd)
        dfile = sipbagd/'trial3'/'trial3a.json'
        self.bgr.add_data_file(dfile, 'trial3/trial3a.json', nerd)
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, nerd['filepath'])))
        bnerd = self.bgr.bag.nerdm_record()
        self.assertEqual(len(bnerd['components']), 3)
        fps = [c['filepath'] for c in bnerd['components']]
        self.assertIn('trial1.json', fps)
        self.assertIn('trial3', fps)
        self.assertIn('trial3/trial3a.json', fps)
        cnerd = list(c for c in bnerd['components'] if c['filepath'] == 'trial3/trial3a.json')[0]
        self.assertEqual(cnerd['filepath'], nerd['filepath'])
        self.assertEqual(cnerd['size'], nerd['size'])
        self.assertIn('checksum', cnerd)

        # add file before metadata
        dfile = sipbagd/'trial2.json'
        self.bgr.add_data_file(dfile, 'trial4.json')
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, 'trial4.json')))
        bnerd = self.bgr.bag.nerdm_record()
        self.assertEqual(len(bnerd['components']), 4)
        fps = [c['filepath'] for c in bnerd['components']]
        self.assertIn('trial4.json', fps)
        self.assertIn('trial1.json', fps)
        self.assertIn('trial3', fps)
        self.assertIn('trial3/trial3a.json', fps)
        cnerd = list(c for c in bnerd['components'] if c['filepath'] == 'trial4.json')[0]
        self.assertEqual(cnerd['filepath'], 'trial4.json')
        self.assertIn('@id', cnerd)
        self.assertIn('size', cnerd)
        self.assertNotIn('checksum', cnerd)

        # add metadata after file
        nerd = utils.read_json(str(sipbagmd/'trial2.json'/'nerdm.json'))
        nerd['filepath'] = 'trial4.json'
        nerd['downloadURL'] = re.sub(r'trial2', 'trial4', nerd['downloadURL'])
        nerd['accessLevel'] = 'public'
        self.bgr.set_comp_nerdm(nerd, None, True) 
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, 'trial4.json')))
        bnerd = self.bgr.bag.nerdm_record()
        cnerd = list(c for c in bnerd['components'] if c['filepath'] == 'trial4.json')[0]
        self.assertEqual(cnerd['filepath'], 'trial4.json')
        self.assertIn('@id', cnerd)
        self.assertIn('size', cnerd)
        self.assertIn('checksum', cnerd)
        self.assertEqual(cnerd['accessLevel'], 'public')

        # add file with metadata to merge
        dfile = sipbagd/'trial2.json'
        nerd = utils.read_json(str(sipbagmd/'trial2.json'/'nerdm.json'))
        self.assertIn('checksum', nerd)
        self.assertNotIn('accessLevel', nerd)
        self.bgr.set_comp_nerdm(nerd, None, True)

        self.bgr.add_data_file(dfile, 'trial2.json', {'accessLevel': 'public'})
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, 'trial2.json')))
        bnerd = self.bgr.bag.nerdm_record()
        self.assertEqual(len(bnerd['components']), 5)
        fps = [c['filepath'] for c in bnerd['components']]
        self.assertIn('trial2.json', fps)
        self.assertIn('trial4.json', fps)
        self.assertIn('trial1.json', fps)
        self.assertIn('trial3', fps)
        self.assertIn('trial3/trial3a.json', fps)
        cnerd = list(c for c in bnerd['components'] if c['filepath'] == 'trial2.json')[0]
        self.assertEqual(cnerd['filepath'], 'trial2.json')
        self.assertIn('@id', cnerd)
        self.assertIn('size', cnerd)
        self.assertIn('checksum', cnerd)
        self.assertIn('accessLevel', cnerd)

    def test_ensure_srcinfo_dict(self):
        self.set_bagger_for("pdp1:goob")
        si = {"type": "fs", "a": "b"}
        self.assertEqual(self.bgr._ensure_srcinfo_dict(si), si)
        self.assertEqual(self.bgr._ensure_srcinfo_dict("fs:gurn"), {"type": "fs", "location": "gurn"})
        with self.assertRaises(TypeError):
            self.bgr._ensure_srcinfo_dict([])
        with self.assertRaises(ValueError):
            self.bgr._ensure_srcinfo_dict({})
        with self.assertRaises(ValueError):
            self.bgr._ensure_srcinfo_dict("goo:gurn")

        si['type'] = 'goo'
        with self.assertRaises(ValueError):
            self.bgr._ensure_srcinfo_dict(si)

    def test_import_data_files(self):
        uploads = os.path.join(self.workdir, 'uploads')
        shutil.copytree(sipbagd, uploads)
        self.set_bagger_for("pdp1:goob")
        src = "fs:"+str(uploads)

        # no file metadata loaded, so no files loaded
        self.bgr.import_data_files(src)
        bnerd = self.bgr.bag.nerdm_record()
        self.assertEqual(len(bnerd.get('components', [])), 0)

        # pull in data matching metadata
        self.assertTrue(os.path.isfile(os.path.join(uploads,'trial1.json')))
        self.assertTrue(os.path.isfile(os.path.join(uploads,'trial2.json')))
        nerd = utils.read_json(str(sipbagmd/'trial1.json'/'nerdm.json'))
        self.bgr.set_comp_nerdm(nerd, None, True) 
        self.bgr.import_data_files(src)
        self.assertTrue(not os.path.isfile(os.path.join(uploads,'trial1.json')))
        self.assertTrue(os.path.isfile(os.path.join(uploads,'trial2.json')))
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, 'trial1.json')))
        bnerd = self.bgr.bag.nerdm_record()
        self.assertEqual(len(bnerd['components']), 1)
        cnerd = bnerd['components'][0]
        self.assertEqual(cnerd['filepath'], nerd['filepath'])
        self.assertEqual(cnerd['size'], nerd['size'])
        self.assertIn('checksum', cnerd)

        # load everything but don't delete source files
        src = {'type': 'fs', 'location': uploads, 'consumable': False }
        self.bgr.import_data_files(src, include_all=True)
        self.assertTrue(not os.path.isfile(os.path.join(uploads,'trial1.json')))
        self.assertTrue(os.path.isfile(os.path.join(uploads,'trial2.json')))
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, 'trial1.json')))
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, 'trial2.json')))
        self.assertTrue(os.path.isfile(os.path.join(self.bgr.bag.data_dir, 'trial3/trial3a.json')))
        bnerd = self.bgr.bag.nerdm_record()
        self.assertEqual(len(bnerd['components']), 4)
        fps = [c['filepath'] for c in bnerd['components']]
        self.assertIn('trial2.json', fps)
        self.assertIn('trial1.json', fps)
        self.assertIn('trial3', fps)
        self.assertIn('trial3/trial3a.json', fps)

        # hard link?
        self.assertTrue(os.path.samefile(os.path.join(uploads,'trial2.json'),
                                         os.path.join(self.bgr.bag.data_dir, 'trial2.json')))

    def test_set_data_source(self):
        self.set_bagger_for("pdp1:goob")
        dsf = os.path.join(self.bgr.bagdir, '__data_sources.lis')
        self.assertTrue(not os.path.exists(dsf))

        self.bgr.set_data_source("fs:"+str(sipbagd))
        self.assertTrue(os.path.exists(dsf))
        src = utils.read_json(dsf)
        self.assertEqual(src, {"type": 'fs', 'location': str(sipbagd)})

        self.bgr.set_data_source({'type': 'fs', 'location': "goober", 'consumable': False})
        self.assertTrue(os.path.exists(dsf))
        srcs = []
        with open(dsf) as fd:
            for line in fd:
                srcs.append(json.loads(line))
        self.assertEqual(len(srcs), 2)
        self.assertEqual(srcs[0], {"type": 'fs', 'location': str(sipbagd)})
        self.assertEqual(srcs[1], {"type": 'fs', 'location': 'goober', 'consumable': False})

    def test_ensure_data_files(self):
        self.set_bagger_for("pdp1:goob")
        self.bgr.prepare()

        uploads1 = os.path.join(self.workdir, 'uploads1')
        shutil.copytree(sipbagd, uploads1)
        uploads2 = os.path.join(self.workdir, 'uploads2')
        shutil.copytree(sipbagd, uploads2)
        c = 0
        for dir, sub, files in os.walk(uploads1):
            c += len(files)
        self.assertEqual(c, 3)
        c = 0
        for dir, sub, files in os.walk(uploads2):
            c += len(files)
        self.assertEqual(c, 3)
        c = 0
        for dir, sub, files in os.walk(self.bgr.bag.data_dir):
            c += len(files)
        self.assertEqual(c, 0)
        self.assertEqual(len(self.bgr.bag.nerdm_record().get('components',[])), 0)

        self.bgr.set_data_source("fs:"+str(uploads1))
        self.bgr.set_data_source("fs:"+str(uploads2))

        self.bgr.ensure_data_files(True)
        
        c = 0
        for dir, sub, files in os.walk(uploads1):
            c += len(files)
        self.assertEqual(c, 0)
        c = 0
        for dir, sub, files in os.walk(uploads2):
            c += len(files)
        self.assertEqual(c, 0)
        c = 0
        for dir, sub, files in os.walk(self.bgr.bag.data_dir):
            c += len(files)
        self.assertEqual(c, 3)
        self.assertEqual(len(self.bgr.bag.nerdm_record().get('components',[])), 4)

    def test_delete(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())
        self.set_bagger_for("pdp1:goob")
        self.bgr.prepare()
        self.assertTrue(bagdir.exists())

        self.bgr.delete()
        self.assertTrue(not bagdir.exists())
        
    def test_finalize(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())
        self.set_bagger_for("pdp1:goob")
        self.bgr.prepare(who=tstag)
        self.assertTrue(bagdir.exists())

        nerd = utils.read_json(str(datadir2 / 'ncnrexp0.json'))
        self.assertIn('nrds:PDRSubmission', nerd['@type'])
        self.bgr.set_res_nerdm(nerd, tstag, True)  # saves components, too
        saved = self.bgr.bag.nerdm_record('', True)
        self.assertNotIn('doi', saved)

        self.bgr.cfg['assign_doi'] = 'always'
        self.assertTrue(not os.path.exists(os.path.join(self.bgr.bagdir, "bag-info.txt")))
        self.bgr.finalize(who=tstag)
        self.assertTrue(os.path.exists(os.path.join(self.bgr.bagdir, "bag-info.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.bgr.bagdir, "publish_history.yml")))

        with open(os.path.join(self.bgr.bagdir, "publish_history.yml")) as fd:
            history = prov.load_from_history(fd)
        self.assertTrue(all([a.agent for a in history]))

        saved = utils.read_json(self.bgr.bag.nerd_file_for(''))
        self.assertNotIn('nrds:PDRSubmission', saved['@type'])
        self.assertEqual(saved.get('doi'), "doi:10.22222/pdp1-0017sm")
        self.assertFalse(any([s for s in saved['_extensionSchemas'] if 'Submission' in s]))

    def test_finalize_version(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())
        self.set_bagger_for("pdp1:goob")
        self.bgr.prepare(who=tstag)
        self.assertTrue(bagdir.exists())

        nerd = utils.read_json(str(simplenerd))
        del nerd['components'][2]
        # del nerd['releaseHistory']
        self.bgr.set_res_nerdm(nerd, tstag, True)
        
        self.assertEqual(self.bgr.finalize_version(tstag), "1.0.0")
        bnerd = self.bgr.bag.nerdm_record(True)
        self.assertEqual(bnerd["version"], "1.0.0")
        self.assertIn('releaseHistory', bnerd)
        self.assertIn('hasRelease', bnerd['releaseHistory'])
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['version'], "1.0.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['@id'], bnerd['@id']+"/pdr:v/1.0.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['location'],
                         "https://test.data.gov/od/id/"+bnerd['@id']+"/pdr:v/1.0.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['description'], "initial release")
        self.assertEqual(len(bnerd['releaseHistory']['hasRelease']), 1)

        nerd['version'] = "1.0.2+ (in edit)"
        self.bgr.set_res_nerdm(nerd, tstag, True)
        self.assertEqual(self.bgr.finalize_version(tstag, 2), "1.0.3")
        bnerd = self.bgr.bag.nerdm_record(True)
        self.assertEqual(bnerd["version"], "1.0.3")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['version'], "1.0.3")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['@id'], bnerd['@id']+"/pdr:v/1.0.3")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['location'],
                         "https://test.data.gov/od/id/"+bnerd['@id']+"/pdr:v/1.0.3")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['description'], "minor metadata update")
        self.assertEqual(len(bnerd['releaseHistory']['hasRelease']), 2)

        nerd['version'] = "1.0.2+ (in edit)"
        self.bgr.set_res_nerdm(nerd, tstag, True)
        self.assertEqual(self.bgr.finalize_version(tstag, 1), "1.1.0")
        bnerd = self.bgr.bag.nerdm_record(True)
        self.assertEqual(bnerd["version"], "1.1.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['version'], "1.1.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['@id'], bnerd['@id']+"/pdr:v/1.1.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['location'],
                         "https://test.data.gov/od/id/"+bnerd['@id']+"/pdr:v/1.1.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['description'], "major data update")
        self.assertEqual(len(bnerd['releaseHistory']['hasRelease']), 3)

        nerd['version'] = "1.0.2+ (in edit)"
        self.bgr.set_res_nerdm(nerd, tstag, True)
        self.assertEqual(self.bgr.finalize_version(tstag, 0, "data reprocessed"), "2.0.0")
        bnerd = self.bgr.bag.nerdm_record(True)
        self.assertEqual(bnerd["version"], "2.0.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['version'], "2.0.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['@id'], bnerd['@id']+"/pdr:v/2.0.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['location'],
                         "https://test.data.gov/od/id/"+bnerd['@id']+"/pdr:v/2.0.0")
        self.assertEqual(bnerd['releaseHistory']['hasRelease'][-1]['description'], "data reprocessed")
        self.assertEqual(len(bnerd['releaseHistory']['hasRelease']), 4)

        nerd['version'] = "1.0.2+ (in edit)"
        self.bgr.set_res_nerdm(nerd, tstag, True)
        self.assertEqual(self.bgr.finalize_version(tstag, 3), "1.0.2.1")
        bnerd = self.bgr.bag.nerdm_record(True)
        self.assertEqual(bnerd["version"], "1.0.2.1")

        nerd['version'] = "1.0b.2rc3+ (in edit)"
        self.bgr.set_res_nerdm(nerd, tstag, True)
        self.assertEqual(self.bgr.finalize_version(tstag, 2), "1.0.3")
        bnerd = self.bgr.bag.nerdm_record(True)
        self.assertEqual(bnerd["version"], "1.0.3")

        
        
    def test_describe(self):
        bagdir = self.bagparent / 'pdp1:goob'
        self.assertTrue(not bagdir.exists())
        self.set_bagger_for("pdp1:goob")
        self.bgr.prepare()
        self.assertTrue(bagdir.exists())

        nerd = utils.read_json(str(simplenerd))
        del nerd['components'][2]
        self.bgr.set_res_nerdm(nerd, None, True)  # saves components, too

        saved = self.bgr.describe('')
        self.assertIsNotNone(saved)
        self.assertEqual(saved.get('version'), '1.0.0')
        self.assertEqual(saved.get('@id'), 'ark:/88888/pdp1-0017sm')
        self.assertEqual(len(saved['components']), 6)
        
        md = self.bgr.describe("pdr:f/1491_optSortSph20160701.m")
        self.assertIsNotNone(md)
        self.assertEqual(md.get('filepath'), "1491_optSortSph20160701.m")
        self.assertEqual(md.get('@id'), "pdr:f/1491_optSortSph20160701.m")

        md = self.bgr.describe("pdr:v")
        self.assertIsNotNone(md)
        self.assertTrue(md.get("@id", "").endswith("pdr:v"))

    
        
                         
if __name__ == '__main__':
    test.main()
        
        
