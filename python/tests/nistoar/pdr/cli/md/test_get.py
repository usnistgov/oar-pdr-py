import os, sys, logging, argparse, pdb, time, json, shutil, tempfile, re
import unittest as test

from nistoar.pdr.utils import cli
from nistoar.pdr.cli.md import get
from nistoar.pdr.exceptions import PDRException, ConfigurationException
from nistoar.pdr import config as cfgmod
from nistoar.pdr.describe import RMMServerError, IDNotFound
from nistoar.pdr.distrib import DistribServerError, DistribResourceNotFound
from nistoar.pdr.utils import read_nerd
from nistoar.pdr.preserve.bagit import builder as bldr
from nistoar.nerdm.convert.rmm import NERDmForRMM

testdir = os.path.dirname(os.path.abspath(__file__))
pdrmoddir = os.path.dirname(os.path.dirname(testdir))
distarchdir = os.path.join(pdrmoddir, "distrib", "data")
descarchdir = os.path.join(pdrmoddir, "describe", "data")
tmparch = None

uwsgi_opts = "--plugin python3"
if os.environ.get("OAR_UWSGI_OPTS") is not None:
    uwsgi_opts = os.environ['OAR_UWSGI_OPTS']

def startServices(archbase):
    archdir = os.path.join(archbase, "distarchive")

    shutil.copytree(distarchdir, archdir)
    # os.mkdir(archdir)   # keep it empty for now

    srvport = 9091
    pidfile = os.path.join(archbase,"simdistrib"+str(srvport)+".pid")
    wpy = os.path.join(pdrmoddir, "distrib/sim_distrib_srv.py")
    assert os.path.exists(wpy)
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --set-ph archive_dir={4} --pidfile {5}"
    cmd = cmd.format(os.path.join(archbase,"simdistrib.log"), uwsgi_opts,
                     srvport, wpy, archdir, pidfile)
    os.system(cmd)

    archdir = os.path.join(archbase, "mdarchive")
    shutil.copytree(descarchdir, archdir)

    rmmdir = os.path.join(archdir, "rmm-test-archive")
    loadmd(os.path.join(archdir, "pdr2210.json"), rmmdir)
    loadmd(os.path.join(archdir, "pdr02d4t.json"), rmmdir)

    srvport = 9092
    pidfile = os.path.join(archbase,"simrmm"+str(srvport)+".pid")
    wpy = os.path.join(pdrmoddir, "describe/sim_describe_svc.py")
    assert os.path.exists(wpy)
    cmd = "uwsgi --daemonize {0} {1} --http-socket :{2} " \
          "--wsgi-file {3} --set-ph archive_dir={4} --pidfile {5}"
    cmd = cmd.format(os.path.join(archbase,"simrmm.log"), uwsgi_opts,
                     srvport, wpy, rmmdir, pidfile)
    os.system(cmd)
    time.sleep(0.5)

def loadmd(nerdfile, rmmdir):
    rmmrec = NERDmForRMM().to_rmm(read_nerd(nerdfile))
    basen = re.sub(r'^ark:/\d+/', '', rmmrec['record'].get('ediid', rmmrec['record']['@id']))
    for part in "record version releaseSet".split():
        odir = os.path.join(rmmdir, part + "s")
        if not os.path.exists(odir):
            os.mkdir(odir)

        ofile = basen
        if part == "version" and rmmrec['version'].get('version'):
            ver = rmmrec['version']['version'].replace('.','_')
            if not ofile.endswith(ver) and not ofile.endswith(rmmrec['version']['version']):
                ofile += "-v" + ver
        ofile = os.path.join(odir, ofile+".json")

        with open(ofile, 'w') as fd:
            json.dump(rmmrec[part], fd, indent=4, separators=(',', ': '))

    

def stopServices(archbase):
    srvport = 9091
    pidfile = os.path.join(archbase,"simdistrib"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(archbase, "simdistrib"+str(srvport)+".pid"))
    os.system(cmd)

    # sometimes stopping with uwsgi doesn't work
    try:
        with open(pidfile) as fd:
            pid = int(fd.read().strip())
        os.kill(pid, signal.SIGTERM)
    except:
        pass

    srvport = 9092
    pidfile = os.path.join(archbase,"simrmm"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(archbase, "simrmm"+str(srvport)+".pid"))
    os.system(cmd)

    time.sleep(1)

    # sometimes stopping with uwsgi doesn't work
    try:
        with open(pidfile) as fd:
            pid = int(fd.read().strip())
        os.kill(pid, signal.SIGTERM)
    except:
        pass

tmparch = tempfile.TemporaryDirectory(prefix="_test_get.")
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmparch.name,"test_bagger.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter(bldr.DEF_BAGLOG_FORMAT))
    rootlog.addHandler(loghdlr)
    rootlog.setLevel(logging.DEBUG)

    startServices(tmparch.name)

def tearDownModule():
    stopServices(tmparch.name)

    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
        loghdlr.close()
        loghdlr = None
    tmparch.cleanup()

class TestGetCmd(test.TestCase):
    distep = "http://localhost:9091/"
    mdep = "http://localhost:9092/"

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_test_get.", dir=tmparch.name)
        self.cmd = cli.CLISuite("test")
        self.cmd.load_subcommand(get)

        self.config = {
            "pdr_dist_base": self.distep,
            "pdr_rmm_base":  self.mdep
        }

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_parse(self):
        args = self.cmd.parse_args("-q get -A -R http://example.com/id mds2-88888 -V 1.0.0rc4".split())
        self.assertEqual(args.workdir, "")
        self.assertTrue(args.quiet)
        self.assertFalse(args.verbose)
        self.assertEqual(args.cmd, "get")
        self.assertEqual(args.version, "1.0.0rc4")
        self.assertEqual(args.src, "aip")
        self.assertEqual(args.rmmbase, "http://example.com/id")
        self.assertEqual(args.id, "mds2-88888")

    def test_describe(self):
        args = self.cmd.parse_args(("get -D %s -R %s ark:/88434/edi00hw91c" %
                                    (self.distep, self.mdep)).split())
        nerdm = get.describe(args.id, args.rmmbase)
        self.assertIn("@id", nerdm)
        self.assertIn("title", nerdm)

        args = self.cmd.parse_args(("get -D %s -R %s ark:/88434/pdr02d4t" %
                                    (self.distep, self.mdep)).split())
        nerdm = get.describe(args.id, args.rmmbase)
        self.assertIn("@id", nerdm)
        self.assertIn("ediid", nerdm)

        args = self.cmd.parse_args(("get -D %s -R %s ABCDEFG" %
                                    (self.distep, self.mdep)).split())
        nerdm = get.describe(args.id, args.rmmbase)
        self.assertIn("@id", nerdm)
        self.assertIn("ediid", nerdm)

    def test_extract_from_AIP(self):
        args = self.cmd.parse_args(("get -A -D %s -R %s ark:/88434/pdr2210" %
                                    (self.distep, self.mdep)).split())
        nerdm = get.extract_from_AIP(args.id, args.distbase, tmpdir=self.tmpdir.name)
        self.assertIn("@id", nerdm)
        self.assertIn("title", nerdm)
    
        nerdm = get.extract_from_AIP(args.id, args.distbase, mdsvc=args.rmmbase, tmpdir=self.tmpdir.name)
        self.assertIn("@id", nerdm)
        self.assertIn("title", nerdm)

        try:
            nerdm = get.extract_from_AIP(args.id+"/pdr:v/1.0", args.distbase, tmpdir=self.tmpdir.name)
            self.fail("Version ID failed to raise DistribResourceNotFound")
        except DistribResourceNotFound as ex:
            self.assertIn("class", str(ex))
            
        try:
            nerdm = get.extract_from_AIP(args.id+"/pdr:v", args.distbase, tmpdir=self.tmpdir.name)
            self.fail("Version ID failed to raise DistribResourceNotFound")
        except DistribResourceNotFound as ex:
            self.assertIn("class", str(ex))
            
        try:
            nerdm = get.extract_from_AIP(args.id+"/goober", args.distbase, tmpdir=self.tmpdir.name)
            self.fail("ReleaseCollection ID failed to raise DistribResourceNotFound")
        except DistribResourceNotFound as ex:
            self.assertIn("class", str(ex))

    def test_cmd(self):
        # NOTE on pdr2210.json: ediid=ark:/88434/pdr2210; @id=ark:/88434/edi00hw91c
        #
        outf = os.path.join(self.tmpdir.name, "out.json")
        self.assertTrue(not os.path.exists(outf))

        argline = "-q get -D %s -R %s ark:/88434/edi00hw91c -o %s" % (self.distep, self.mdep, outf)
        self.cmd.execute(argline.split(), {})
        self.assertTrue(os.path.isfile(outf))

        nerdm = read_nerd(outf)
        self.assertIn("@id", nerdm)
        self.assertIn("title", nerdm)

        os.remove(outf)
        self.assertTrue(not os.path.exists(outf))

        argline = "-q get -D %s -R %s ark:/88434/pdr2210 -o %s" % (self.distep, self.mdep, outf)
        with self.assertRaises(cli.CommandFailure):
            self.cmd.execute(argline.split(), {})

        argline = "-q get -A -D %s -R %s ark:/88434/pdr2210 -o %s" % (self.distep, self.mdep, outf)
        self.cmd.execute(argline.split(), {})
        self.assertTrue(os.path.isfile(outf))

        nerdm = read_nerd(outf)
        self.assertIn("@id", nerdm)
        self.assertIn("title", nerdm)
        self.assertEqual(nerdm.get("version"), "3.1.3")
        
    def test_cmd_AV(self):
        outf = os.path.join(self.tmpdir.name, "out.json")
        self.assertTrue(not os.path.exists(outf))

        argline = "-q get -A -V 3.1.3 -D %s -R %s ark:/88434/pdr2210 -o %s" % (self.distep, self.mdep, outf)
        self.cmd.execute(argline.split(), {})
        self.assertTrue(os.path.isfile(outf))

        nerdm = read_nerd(outf)
        self.assertIn("@id", nerdm)
        self.assertIn("title", nerdm)
        self.assertEqual(nerdm.get("version"), "3.1.3")

        os.remove(outf)
        self.assertTrue(not os.path.exists(outf))

        argline = "-q get -A -V 2 -D %s -R %s ark:/88434/pdr2210 -o %s" % (self.distep, self.mdep, outf)
        self.cmd.execute(argline.split(), {})
        self.assertTrue(os.path.isfile(outf))

        nerdm = read_nerd(outf)
        self.assertIn("@id", nerdm)
        self.assertIn("title", nerdm)
        self.assertEqual(nerdm.get("version"), "2")
        
        os.remove(outf)
        self.assertTrue(not os.path.exists(outf))

        argline = "-q get -A -V 1.0 -D %s -R %s ark:/88434/pdr2210 -o %s" % (self.distep, self.mdep, outf)
        self.cmd.execute(argline.split(), {})
        self.assertTrue(os.path.isfile(outf))

        nerdm = read_nerd(outf)
        self.assertIn("@id", nerdm)
        self.assertIn("title", nerdm)
        self.assertEqual(nerdm.get("version"), "1.0")
        

            
        
            
        
        

if __name__ == '__main__':
    test.main()

