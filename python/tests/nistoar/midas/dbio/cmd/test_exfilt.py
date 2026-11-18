"""
test show subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path
import yaml

from nistoar.pdr.utils import cli
from nistoar.midas.dbio.cmd import exfilt
from nistoar.midas.dbio import DMP_PROJECTS
from nistoar.midas.dbio.project import ProjectService
from nistoar.midas.dbio.inmem import InMemoryDBClient, InMemoryDBClientFactory
from nistoar.pdr.utils.cli import CommandFailure
from nistoar.midas.cli import get_agent
from nistoar.pdr.utils.prov import Action

tmpdir = tempfile.TemporaryDirectory(prefix="_test_exfilt.")
testdir = Path(__file__).parents[0]

loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_exfilt.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
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

class TestExfiltCmd(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_tmp", dir=tmpdir.name)
        self.cmd = cli.CLISuite("midasadm")
        self.exf = exfilt.ExfiltCommand("dmp", "exfiltrate specified DMP records into a transfer format")
        self.cmd.load_subcommand(self.exf)

        self.cfg = {
            "dbio": {
                "factory": "inmem",
                "project_id_minting": {
                    "default_shoulder": {
                        "public": "mdm0"
                    },
                    "localid_providers": {
                        "admin": "mdm0"
                    }
                }
            }                
        }
        self.parser = argparse.ArgumentParser()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_load_into(self):
        args = self.cmd.parse_args("exfilt -p --exclude-provenance junk.yml goob gurn".split())
        self.assertEqual(args.outfile, "junk.yml")
        self.assertEqual(args.dbid, ["goob", "gurn"])
        self.assertTrue(args.noprov)
        self.assertTrue(args.pretty)

        args = self.cmd.parse_args("exfilt --pretty -- - goob".split())
        self.assertEqual(args.outfile, "-")
        self.assertEqual(args.dbid, ["goob"])
        self.assertFalse(args.noprov)
        self.assertTrue(args.pretty)

#        with self.assertRaises(SystemExit):
#            args = self.cmd.parse_args("exfilt --pretty goob.yml".split())

    def test_checkConfig(self):
        args = self.cmd.parse_args("exfilt -- - goob".split())
        self.exf.checkConfig(args, self.cfg)
        
        with self.assertRaises(CommandFailure):
            self.exf.checkConfig(args, {})

    def test_checkArgs(self):
        args = self.cmd.parse_args("exfilt -- - goob".split())
        self.exf.checkArgs(args, self.cfg)

        args.dbid = []
        with self.assertRaises(CommandFailure):
            self.exf.checkArgs(args, self.cfg)

        args = self.cmd.parse_args("exfilt -- - goob".split())
        args.outfile = ''
        with self.assertRaises(CommandFailure):
            self.exf.checkArgs(args, self.cfg)

    def test_create_record_source(self):
        args = self.cmd.parse_args("exfilt -- - goob".split())
        src = self.exf.create_record_source(args, self.cfg, get_agent(args, self.cfg))
        self.assertTrue(src.get('dbfact'))
        self.assertTrue(src.get('dbclient'))

    def test_write_out(self):
        data = { "goob": [ "gurn", "Cranston" ] }
        outf = os.path.join(self.tmpdir.name, "out.yml")
        with open(outf, 'w') as fd:
            self.exf.write_out(data, fd)
            self.exf.write_out(data, fd)

        lines = []
        with open(outf) as fd:
            for line in fd:
                lines.append(line)

        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], '---\n')
        self.assertEqual(lines[2], '---\n')

        with open(outf, 'w') as fd:
            self.exf.write_out(data, fd, True)
            self.exf.write_out(data, fd, True)

        lines = []
        with open(outf) as fd:
            for line in fd:
                lines.append(line)

        self.assertEqual(len(lines), 8)
        self.assertEqual(lines[0], '---\n')
        self.assertEqual(lines[4], '---\n')

    def test_collect_data_for(self):
        id = "mdm0:goob"
        args = self.cmd.parse_args("exfilt -- - mdm0:goob".split())
        who = get_agent(args, self.cfg)
        src = self.exf.create_record_source(args, self.cfg, who)
        rec = src['dbclient'].create_record("test", "mdm0", who.actor, "goob")
        self.assertEqual(rec.id, id)
        self.assertEqual(rec.name, "test")
        rec.status.act("create", "draft created", who.actor)
        src['dbclient'].record_action(Action(Action.CREATE, rec.id, who, rec.status.message))
        closeact = Action(Action.PROCESS, rec.id, who, "Closing published record")
        src['dbclient']._close_actionlog_with(rec, closeact)
        src['dbclient'].record_action(Action(Action.PROCESS, rec.id, who, "revising record",
                                             obj={"name": "revise"}))

        data = self.exf.collect_data_for(id, src, args, logging.getLogger())
        self.assertEqual(data['type'], "dmp")
        dbrec = data.get('dbio')
        self.assertTrue(dbrec)
        self.assertEqual(dbrec['id'], id)
        self.assertEqual(dbrec['name'], rec.name)
        self.assertEqual(dbrec['acls'], rec.to_dict()['acls'])
        self.assertIn('data', dbrec)
        self.assertIn('meta', dbrec)

        self.assertIn('prov', data)
        self.assertTrue(isinstance(data['prov'], list))
        self.assertEqual(len(data['prov']), 1)
        self.assertEqual(data['prov'][0]['type'], "PROCESS")
        self.assertIn('history', data)
        self.assertTrue(isinstance(data['history'], list))
        self.assertEqual(len(data['history']), 1)
        self.assertTrue(isinstance(data['history'][0], dict))
        self.assertTrue(isinstance(data['history'][0]['history'], list))
        self.assertEqual(len(data['history'][0]['history']), 2)

        args.noprov = True
        data = self.exf.collect_data_for(id, src, args, logging.getLogger())
        dbrec = data.get('dbio')
        self.assertTrue(dbrec)
        self.assertNotIn('prov', data)
        self.assertNotIn('history', data)

    def test_execute(self):
        id = "mdm0:goob"
        outf = os.path.join(self.tmpdir.name, "recs.yml")
        args = self.cmd.parse_args(f"exfilt {outf} mdm0:goob".split())
        who = get_agent(args, self.cfg)
        src = self.exf.create_record_source(args, self.cfg, who)
        rec = src['dbclient'].create_record("test", "mdm0", who.actor, "goob")
        self.assertEqual(rec.id, id)
        self.assertEqual(rec.name, "test")
        rec.status.act("create", "draft created", who.actor)
        src['dbclient'].record_action(Action(Action.CREATE, rec.id, who, rec.status.message))
        closeact = Action(Action.PROCESS, rec.id, who, "Closing published record")
        src['dbclient']._close_actionlog_with(rec, closeact)
        src['dbclient'].record_action(Action(Action.PROCESS, rec.id, who, "revising record",
                                             obj={"name": "revise"}))

        self.assertFalse(os.path.exists(outf))
        self.exf.execute(args, self.cfg, _dbfact=src['dbfact'])
        self.assertTrue(os.path.isfile(outf))
        with open(outf) as fd:
            recs = list(yaml.safe_load_all(fd))
        self.assertEqual(len(recs), 1)
        first = recs[0]
        self.assertIn('dbio', first)
        self.assertIn('prov', first)
        self.assertEqual(first['dbio']['id'], rec.id)
        
class TestImportCmd(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="_tmp", dir=tmpdir.name)
        self.cmd = cli.CLISuite("midasadm")
        self.imp = exfilt.ImportCommand("dmp", "import specified DMP records into a transfer format")
        self.cmd.load_subcommand(self.imp)

        self.cfg = {
            "dbio": {
                "factory": "inmem",
                "project_id_minting": {
                    "default_shoulder": {
                        "public": "mdm0"
                    },
                    "localid_providers": {
                        "admin": "mdm0"
                    }
                }
            }                
        }
        self.parser = argparse.ArgumentParser()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_load_into(self):
        args = self.cmd.parse_args("import junk.yml goob gurn".split())
        self.assertEqual(args.infile, "junk.yml")
        self.assertEqual(args.dbid, ["goob", "gurn"])

        args = self.cmd.parse_args("import -- -".split())
        self.assertEqual(args.infile, "-")
        self.assertEqual(args.dbid, [])

    def test_checkConfig(self):
        args = self.cmd.parse_args("import -- - goob".split())
        self.imp.checkConfig(args, self.cfg)
        
        with self.assertRaises(CommandFailure):
            self.imp.checkConfig(args, {})

    def test_checkArgs(self):
        args = self.cmd.parse_args(["import", __file__])
        self.imp.checkArgs(args, self.cfg)

        args.infile = None
        with self.assertRaises(CommandFailure):
            self.imp.checkArgs(args, self.cfg)

        args = self.cmd.parse_args("import archive.yml".split())
        with self.assertRaises(CommandFailure):
            self.imp.checkArgs(args, self.cfg)

        args = self.cmd.parse_args("import .".split())
        with self.assertRaises(CommandFailure):
            self.imp.checkArgs(args, self.cfg)

    def test_create_record_target(self):
        args = self.cmd.parse_args("import -- - goob".split())
        tgt = self.imp.create_record_target(args, self.cfg, get_agent(args, self.cfg))
        self.assertTrue(tgt.get('dbfact'))
        self.assertTrue(tgt.get('dbclient'))

    def test_execute(self):
        archfile = os.path.join(self.tmpdir.name, "recs.yml")
        self.assertFalse(os.path.exists(archfile))
        args = self.cmd.parse_args(f"import {archfile} mdm0:0001".split())
        who = get_agent(args, self.cfg)
        tgt = self.imp.create_record_target(args, self.cfg, who)
        dbfact = tgt['dbfact']

        # create a record to export
        projsvc = ProjectService("dmp", dbfact, {"dbio": self.cfg}, who)
        prec = projsvc.create_record("test", { "data_needs": "alot" })
        self.assertIn(prec.id, dbfact._db.get('dmp'))
        self.assertIn(prec.id, dbfact._db.get('prov_action_log'))

        # export the record
        exfcmd = cli.CLISuite("exf")
        exf = exfilt.ExfiltCommand("dmp", "exfiltrate specified DMP records into a transfer format")
        exfcmd.load_subcommand(exf)
        exfargs = exfcmd.parse_args(f"exfilt {archfile} {prec.id}".split())
        exf.execute(exfargs, self.cfg, _dbfact=dbfact)
        self.assertTrue(os.path.isfile(archfile))

        # now import it into a new db
        tgt = self.imp.create_record_target(args, self.cfg, who)
        dbfact = tgt['dbfact']
        self.assertEqual(dbfact._db.get('dmp'), {})
        self.assertIsNone(dbfact._db.get('prov_action_log'))

        self.imp.execute(args, self.cfg, _dbfact=dbfact)
        self.assertIn(prec.id, dbfact._db.get('dmp'))
        self.assertEqual(dbfact._db['dmp'][prec.id]['data'].get('data_needs'), 'alot')
        self.assertIn(prec.id, dbfact._db.get('prov_action_log'))
        self.assertEqual(len(dbfact._db['prov_action_log'][prec.id]), 2)
        
        
        
        


if __name__ == '__main__':
    test.main()

