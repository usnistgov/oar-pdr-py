"""
test show subcommand module
"""
import os, sys, logging, argparse, pdb, time, json, shutil, tempfile
import unittest as test
from pathlib import Path
import yaml

from nistoar.pdr.utils import cli
from nistoar.midas.dbio.cmd import exfilt
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
        

if __name__ == '__main__':
    test.main()

