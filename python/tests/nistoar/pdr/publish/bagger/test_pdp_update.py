import os, sys, pdb, shutil, logging, json, time, re
import unittest as test
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path

from nistoar.testing import *
from nistoar.pdr import utils, ARK_NAAN
import nistoar.pdr.exceptions as exceptions
import nistoar.pdr.publish.bagger.pdp as pdp
from nistoar.pdr.publish.bagger import prepupd
from nistoar.pdr.publish.bagger import utils as bagutils
from nistoar.pdr.publish import idmint as minter
from nistoar.pdr.publish import BadSIPInputError
from nistoar.nerdm import constants as consts
from nistoar.pdr.preserve.bagit import builder as bldr
from nistoar.pdr.publish import prov
from nistoar.pdr.preserve.bagit.bag import NISTBag
from nistoar.pdr.preserve.bagit.serialize import zip_deserialize

pdrtstdir = Path(__file__).parents[2]
datadir = pdrtstdir / 'preserve' / 'data'
distarchdir = pdrtstdir / "distrib" / "data"
descarchdir = pdrtstdir / "describe" / "data" / "rmm-test-archive"

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    ensure_tmpdir()
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir(),"test_builder.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter(bldr.DEF_BAGLOG_FORMAT))
    rootlog.addHandler(loghdlr)
    rootlog.setLevel(logging.DEBUG)
    startServices()

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    stopServices()
    rmtmpdir()

tdir = Path(tmpdir())
distarchive = tdir / "distarchive"
mdarchive = tdir / "mdarchive"

def startServices():
    archdir = distarchive
    shutil.copytree(distarchdir, archdir)
    shutil.copyfile(archdir / "1491.1_0.mbag0_4-0.zip",
                    archdir / "3A1EE2F169DD3B8CE0531A570681DB5D1491.1_0.mbag0_4-0.zip")

    srvport = 9091
    pidfile = tdir / "simdistrib{0}.pid".format(str(srvport))
    wpy = pdrtstdir / "distrib" / "sim_distrib_srv.py"
    assert wpy.exists()
    cmd = "uwsgi --daemonize {0} --plugin python3 --http-socket :{1} " \
          "--wsgi-file {2} --set-ph archive_dir={3} --pidfile {4}"
    cmd = cmd.format(str(tdir / "simdistrib.log"), srvport, str(wpy), str(archdir), pidfile)
    os.system(cmd)

    archdir = mdarchive
    shutil.copytree(descarchdir, archdir)
    load_nerdm_from_aip("pdr2210.3_1_3.mbag0_3-5.zip", archdir)

    srvport = 9092
    pidfile = tdir / "simrmm{0}.pid".format(str(srvport))
    wpy = pdrtstdir / "describe" / "sim_describe_svc.py"
    assert wpy.exists()
    cmd = "uwsgi --daemonize {0} --plugin python3 --http-socket :{1} " \
          "--wsgi-file {2} --set-ph archive_dir={3} --pidfile {4}"
    cmd = cmd.format(str(tdir / "simrmm.log"), srvport, str(wpy), str(archdir), pidfile)
    os.system(cmd)
    time.sleep(0.5)

def stopServices():
    srvport = 9091
    pidfile = tdir / ("simdistrib"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(pidfile)
                                    
    os.system(cmd)

    # sometimes stopping with uwsgi doesn't work
    try:
        with open(pidfile) as fd:
            pid = int(fd.read().strip())
        os.kill(pid, signal.SIGTERM)
    except:
        pass

    srvport = 9092
    pidfile = tdir / ("simrmm"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(str(pidfile))
    os.system(cmd)

    time.sleep(1)

    # sometimes stopping with uwsgi doesn't work
    try:
        with open(pidfile) as fd:
            pid = int(fd.read().strip())
        os.kill(pid, signal.SIGTERM)
    except:
        pass

def load_nerdm_from_aip(zipfname, destarch):
    unpackdir = tdir / "unpack"
    if not unpackdir.exists():
        os.mkdir(unpackdir)
        
    root = zip_deserialize(str(distarchdir / zipfname), unpackdir, rootlog)
    bag = NISTBag(root)
    nerdm = bag.nerdm_record()
    sipid = re.sub(r'^ark:/\d+/', '', nerdm['ediid'])
    utils.write_json(nerdm, destarch / "records" / (sipid+".json"))

tstag = prov.PubAgent("test", prov.PubAgent.AUTO, "tester")

class TestPDPBagger(test.TestCase):

    def setUp(self):
        self.tf = Tempfiles()
        self.workdir = Path(self.tf.mkdir("bagger"))
        self.mddir = self.workdir / "mddir"
        os.mkdir(self.mddir)
        self.pubcache = self.tf.mkdir("headcache")

        self.mintdir = self.tf.mkdir("idregs")
        self.bagparent = Path(self.workdir) / 'sipbags'
        self.cfg = {
            "working_dir": str(self.workdir),
            "bag_builder": {
                "validate_id": False,
                "init_bag_info": {
                    'NIST-BagIt-Version': "X.3",
                    "Organization-Address": ["100 Bureau Dr.",
                                             "Gaithersburg, MD 20899"]
                },
                "ensure_nerdm_type_on_add": bldr.NERDM_SCH_ID_BASE + "v0.6"
            },
            "finalize": {},
            'repo_access': {
                'headbag_cache':   self.pubcache,
                'distrib_service': {
                    'service_endpoint': "http://localhost:9091/"
                },
                'metadata_service': {
                    'service_endpoint': "http://localhost:9092/"
                }
            },
            'store_dir': str(distarchdir)
        }
        self.mntrcfg = {
            "id_shoulder": 'pdp1',
            "naan": "88434",
            "store_dir":  self.mintdir,
            "sequence_start": 17
        }
        self.minter = minter.PDP0Minter(self.mntrcfg)
        prpcfg = deepcopy(self.cfg['repo_access'])
        prpcfg['stroe_dir'] = self.cfg['store_dir']
        self.prepsvc = prepupd.UpdatePrepService(prpcfg)

        self.bgr = None

    def tearDown(self):
        self.tf.clean()

    def set_bagger_for(self, sipid, **kw):
        self.bgr = pdp.PDPBagger(sipid, self.cfg, self.minter, self.prepsvc, **kw)
        return self.bgr

    def test_ctor(self):
        self.assertFalse(self.bagparent.exists())
        self.set_bagger_for("mds8-8888", id="ark:/88888/mds8-8888")
        self.assertEqual(self.bgr.convention, "pdp0")
        self.assertEqual(self.bgr.sipid, "mds8-8888")
        self.assertIsNotNone(self.bgr.bagbldr)
        self.assertTrue(self.bagparent.is_dir())
        self.assertEqual(self.bgr.bagdir, str(self.bagparent / 'mds8-8888'))

        prepr = self.bgr._get_prepper()
        self.assertEqual(prepr.aipid, "mds8-8888")
        self.assertEqual(prepr.pdrid, "ark:/88888/mds8-8888")

    def test_ensure_preparation_update_frombag(self):
        bagdir = self.bagparent / 'mds2:7223'
        self.assertTrue(not bagdir.exists())

        self.set_bagger_for("mds2:7223", id="ark:/88434/mds2-7223")
        self.bgr.ensure_preparation(True, tstag)

        self.assertTrue(bagdir.is_dir())
        self.assertIsNotNone(self.bgr.bag)
        self.assertEqual(self.bgr.bag.dir, str(bagdir))
        self.assertTrue(bagdir.joinpath('metadata').is_dir())
        self.assertTrue(bagdir.joinpath('metadata','nerdm.json').is_file())
        nerd = self.bgr.bag.nerd_metadata_for('', True)
        self.assertEqual(nerd.get('@id'), 'ark:/88434/mds2-7223')
        self.assertEqual(nerd.get('pdr:sipid'), 'mds2:7223')
        self.assertEqual(nerd.get('pdr:aipid'), 'mds2-7223')
        self.assertEqual(nerd.get('programCode'), ["006:045"])
        self.assertEqual(nerd.get('accessLevel'), "public")
        self.assertIn('publisher', nerd)
        self.assertEqual(nerd.get('version'), '1.1.0+ (in edit)')

        self.assertTrue(bagdir.joinpath('publish.history').is_file())
        with open(bagdir.joinpath("publish.history")) as fd:
            history = prov.load_from_history(fd)
        self.assertIsNotNone(history[-1].agent)
        self.assertEqual(history[-1].agent.actor, 'tester')
        self.assertEqual(history[-1].type, 'COMMENT')
        
    def test_ensure_preparation_update_fromnerdm(self):
        # a bag for mds2-2106 does not exist
        bagdir = self.bagparent / 'mds2:2106'
        self.assertTrue(not bagdir.exists())

        self.set_bagger_for("mds2:2106", id="ark:/88434/mds2-2106")
        self.bgr.ensure_preparation(True, tstag)

        self.assertTrue(bagdir.is_dir())
        self.assertIsNotNone(self.bgr.bag)
        self.assertEqual(self.bgr.bag.dir, str(bagdir))
        self.assertTrue(bagdir.joinpath('metadata').is_dir())
        self.assertTrue(bagdir.joinpath('metadata','nerdm.json').is_file())
        nerd = self.bgr.bag.nerd_metadata_for('', True)
        self.assertEqual(nerd.get('@id'), 'ark:/88434/mds2-2106')
        self.assertEqual(nerd.get('pdr:sipid'), 'mds2:2106')
        self.assertEqual(nerd.get('pdr:aipid'), 'mds2-2106')
        self.assertEqual(nerd.get('programCode'), ["006:045"])
        self.assertEqual(nerd.get('accessLevel'), "public")
        self.assertIn('publisher', nerd)
        self.assertEqual(nerd.get('version'), '1.6.0+ (in edit)')

        self.assertTrue(bagdir.joinpath('publish.history').is_file())
        with open(bagdir.joinpath("publish.history")) as fd:
            history = prov.load_from_history(fd)
        self.assertIsNotNone(history[-1].agent)
        self.assertEqual(history[-1].agent.actor, 'tester')
        self.assertEqual(history[-1].type, 'COMMENT')
        
    def test_ensure_preparation_notanupdate(self):
        bagdir = self.bagparent / 'mds2:8888'
        self.assertTrue(not bagdir.exists())

        self.set_bagger_for("mds2:8888", id="ark:/88434/mds2-8888")
        self.bgr.ensure_preparation(True, tstag)

        self.assertTrue(bagdir.is_dir())
        self.assertIsNotNone(self.bgr.bag)
        self.assertEqual(self.bgr.bag.dir, str(bagdir))
        self.assertTrue(bagdir.joinpath('metadata').is_dir())
        self.assertTrue(bagdir.joinpath('metadata','nerdm.json').is_file())
        nerd = self.bgr.bag.nerd_metadata_for('', True)
        self.assertEqual(nerd.get('@id'), 'ark:/88434/mds2-8888')
        self.assertEqual(nerd.get('pdr:sipid'), 'mds2:8888')
        self.assertEqual(nerd.get('pdr:aipid'), 'mds2-8888')
        self.assertEqual(nerd.get('programCode'), ["006:045"])
        self.assertEqual(nerd.get('accessLevel'), "public")
        self.assertIn('publisher', nerd)
        self.assertEqual(nerd.get('version'), '1.0.0')

        self.assertTrue(bagdir.joinpath('publish.history').is_file())
        with open(bagdir.joinpath("publish.history")) as fd:
            history = prov.load_from_history(fd)
        self.assertIsNotNone(history[-1].agent)
        self.assertEqual(history[-1].agent.actor, 'tester')
        self.assertEqual(history[-1].type, 'CREATE')

        

if __name__ == '__main__':
    test.main()
    
