"""
CLI command for exporting a DBIO record

This command is intended to work with any collection type (DAP, DMP, etc.); however, it is designed
to be called as a subcommand of the collection-specific command (i.e. ``dap``, ``dmp``).  This 
module provides the common implementation.  
"""

import logging, argparse, os, sys, json
from logging import Logger
from typing import Mapping
from copy import deepcopy
from pathlib import Path
from collections import OrderedDict

import yaml

from nistoar.base.config import ConfigurationException
from nistoar.midas import MIDASException
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.midas.dbio import (FSBasedDBClientFactory, MongoDBClientFactory, InMemoryDBClientFactory,
                                NotEditable, NotAuthorized, AlreadyExists, ObjectNotFound, ACLs,
                                status, PUBLIC_GROUP, ProjectRecord)
from nistoar.midas.cli import get_agent

class ExfiltCommand:
    """
    an extendable implementation of the exfilt command 
    """
    default_name = "exfilt"
    help = "exfiltrate one or more records into a transfer format"

    def __init__(self, colltype: str, desc: str):
        """
        create the command for a particular database collection (e.g. dmp, dap)
        """
        self.coll = colltype
        self.description = desc

    def load_into(self, subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
        """
        load this command into a CLI by defining the command's arguments and options.
        :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                    interface into it 
        :param list current_dests:  a list of destination names for parameters that have already been 
                                    defined
        :param str as_cmd:  the subcommand name assigned to the action provided by this module
        :rtype: None
        """
        p = subparser
        p.cmd = as_cmd
        p.description = self.description

        p.add_argument("outfile", metavar="OUTFILE", type=str, 
                       help="write exfiltrated records to OUTFILE; use '-- -' to send to standard output")
        p.add_argument("dbid", metavar="DBID", type=str, nargs="+",
                       help="the DBIO identifier of a record to export in transfer format")
        p.add_argument("-p", "--pretty", action="store_true", dest="pretty",
                       help="make the output 'pretty' by using line-feeds and indentation.")
        p.add_argument("--exclude-provenance", action="store_true", dest="noprov",
                       help="exclude the recorded provenance entries for the specified records")

        return None

    def checkConfig(self, args, config):
        """
        ensure that the given configuration is sufficient for carrying out this command

        :raise CommandFailure:  if the configuration is insufficient
        """
        if not config.get("dbio"):
            raise CommandFailure(args.cmd, "Missing required configuration parameter: dbio", 8)

    def checkArgs(self, args, config):
        """
        ensure that the given command-line arguments are sufficient and correct

        :raise CommandFailure:  if a problem is found in the arguments
        """
        if not args.outfile:
            raise CommandFailure(args.cmd, "No output file given (use '-- -' for standard output)", 2)
        if not args.dbid:
            raise CommandFailure(args.cmd, "No identifiers specified", 2)
        

    def execute(self, args, config: Mapping=None, log: Logger=None, _dbfact=None):
        """
        execute this command: load one or more raw DBIO records
        """
        if not log:
            log = logging.getLogger(self.default_name)
        if not config:
            config = {}

        if isinstance(args, list):
            # cmd-line arguments not parsed yet
            p = argparse.ArgumentParser()
            load_command(p)
            args = p.parse_args(args)

        self.checkConfig(args, config)
        self.checkArgs(args, config)

        # create database connection
        who = get_agent(args, config)
        recsrc = self.create_record_source(args, config, who, _dbfact)

        try:
            if args.outfile == "-":
                outfd = sys.stdout
            else:
                outfd = open(args.outfile, 'a')

            for id in args.dbid:
                try:
                    recdata = self.collect_data_for(id, recsrc, args, log)
                except ObjectNotFound as ex:
                    log.warning("Identifier not found: %s", id)
                    continue
                except NotAuthorized as ex:
                    log.warning("Not authorized to retrieve record for id=%s; skipping.", id)
                    continue
                except Exception as ex:
                    raise CommandFailure(args.cmd, "Failed to read in record for id=" + id +
                                         ": "+str(ex), 1) from ex

                self.write_out(recdata, outfd, args.pretty)

        except CommandFailure:
            raise
        except Exception as ex:
            dest = args.outfile if args.outfile != '-' else "standard output"
            raise CommandFailure(args.cmd, f"Failed to write records to {dest}: {str(ex)}", 1)
        finally:
            if outfd and args.outfile != '-':
                outfd.close()
            recsrc['dbclient'].free()

    def create_record_source(self, args, config, who, _dbfact=None):
        out = {}
        if not _dbfact:
            _dbfact = create_DBClientFactory(args, config)
        out['dbfact'] = _dbfact
        out['dbclient'] = _dbfact.create_client(self.coll, {}, who)
        return out

    def write_out(self, recdata, outfd, pretty: bool=False):
        kw = { "explicit_start": True, "sort_keys": False }
        if pretty:
            kw['indent'] = 2
        else:
            kw['default_flow_style'] = None

        ydmpr = yaml.SafeDumper(outfd, **kw)
        try:
            ydmpr.add_representer(OrderedDict, yaml_represent_OrderedDict)
            ydmpr.open()
            ydmpr.represent(recdata)
            ydmpr.close()
        finally:
            ydmpr.dispose()

    def collect_data_for(self, id, recsrc, args, log):
        out = { 'type': self.coll }
        out['dbio'] = recsrc['dbclient'].get_record_for(id).to_dict()
        if not args.noprov:
            data = self.select_prov(id, recsrc, args, log)
            if data:
                out['prov'] = data
            data = recsrc['dbclient'].get_history_for(id)
            if data:
                out['history'] = data
            
        return out

    def select_prov(self, id, recsrc, args, log):
        return recsrc['dbclient']._select_actions_for(id)

from yaml.resolver import BaseResolver
def yaml_represent_OrderedDict(dumper, data):
    return dumper.represent_mapping(BaseResolver.DEFAULT_MAPPING_TAG, data.items())

class ImportCommand:
    """
    an extendable implementation of the import command 
    """
    default_name = "import"
    help = "import one or more records from a transfer-formated file"

    def __init__(self, colltype: str, desc: str):
        """
        create the command for a particular database collection (e.g. dmp, dap)
        """
        self.coll = colltype
        self.description = desc

    def load_into(self, subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
        """
        load this command into a CLI by defining the command's arguments and options.
        :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                    interface into it 
        :param list current_dests:  a list of destination names for parameters that have already been 
                                    defined
        :param str as_cmd:  the subcommand name assigned to the action provided by this module
        :rtype: None
        """
        p = subparser
        p.cmd = as_cmd
        p.description = self.description

        p.add_argument("infile", metavar="FILE", type=str, 
                       help="read in the exfiltrated records from FILE; use '-- -' to read from "
                            "standard input")
        p.add_argument("dbid", metavar="DBID", type=str, nargs="*",
                       help="limit in the import to the records with an identifier amoung those listed")
        p.add_argument("--into-shoulder", metavar="PREFIX", type=str, dest="shoulder",
                       help="reassign new identifiers to all imported records to start with the given "
                            "shoulder, PREFIX.  The local-portion of each new identifier will be drawn "
                            "from the normal sequence for that shoulder (not yet implemented).")

    def checkConfig(self, args, config):
        """
        ensure that the given configuration is sufficient for carrying out this command

        :raise CommandFailure:  if the configuration is insufficient
        """
        if not config.get("dbio"):
            raise CommandFailure(args.cmd, "Missing required configuration parameter: dbio", 8)

    def checkArgs(self, args, config):
        """
        ensure that the given command-line arguments are sufficient and correct

        :raise CommandFailure:  if a problem is found in the arguments
        """
        if args.shoulder:
            raise CommandFailure(args.cmd, "Sorry! --into-shoulder not yet implemented", 2)
        if not args.infile:
            raise CommandFailure(args.cmd, "No input file given (use --- - for standard input)", 2)
        if args.infile != '-' and not os.path.isfile(args.infile):
            raise CommandFailure(args.cmd, f"{args.infile}: does not exist as a file", 3)

    def execute(self, args, config: Mapping=None, log: Logger=None, _dbfact=None):
        """
        execute this command: load one or more raw DBIO records
        """
        if not log:
            log = logging.getLogger(self.default_name)
        if not config:
            config = {}

        if isinstance(args, list):
            # cmd-line arguments not parsed yet
            p = argparse.ArgumentParser()
            load_command(p)
            args = p.parse_args(args)

        self.checkConfig(args, config)
        self.checkArgs(args, config)

        # create database connection
        who = get_agent(args, config)
        rectgt = self.create_record_target(args, config, who, _dbfact)

        try:
            if args.infile == "-":
                infd = sys.stdin
            else:
                infd = open(args.infile)

            e = 0
            m = 0
            dbcli = rectgt['dbclient']
            for rec in yaml.safe_load_all(infd):
                if not rec.get('dbio') or not rec['dbio'].get('id'):
                    e += 1
                    if e > 4:
                        log.error("exfiltrate data appears corrupted/non-compliant; aborting")
                        raise CommandFailure(args.cmd, "Non-compliant input data", 3)
                    log.warning("skipping record misisng DBIO record data")
                    continue
                if args.dbid and rec['dbio'].get('id') not in args.dbid:
                    log.debug("skipping %s: id not requested", rec['dbio']['id'])
                    continue
                    
                prec = ProjectRecord(self.coll, rec['dbio'], dbcli)
                if not prec.authorized(ACLs.WRITE):
                    log.debug("skipping %s: %s is unauthorized", rec['dbio']['id'], dbcli.user_id)
                    continue
                
                if rec.get('history'):
                    for hist in rec['history']:
                        dbcli._save_history(hist)

                if rec.get('prov'):
                    for action in rec['prov']:
                        dbcli._save_action_data(action)

                # now save the DBIO record
                prec.save()
                m += 1

            if not m:
                raise CommandFailure(args.cmd, "No matching/permitted records found in input", 1)

        except CommandFailure:
            raise
        except Exception as ex:
            src = args.infile if args.outfile != '-' else "standard input"
            raise CommandFailure(args.cmd, f"Failed to load records from {src}: {str(ex)}", 1)
        finally:
            if args.infile and args.infile != '-':
                infd.close()
            rectgt['dbclient'].free()


    def create_record_target(self, args, config, who, _dbfact=None):
        out = {}
        if not _dbfact:
            _dbfact = create_DBClientFactory(args, config)
        out['dbfact'] = _dbfact
        out['dbclient'] = _dbfact.create_client(self.coll, {}, who)
        return out

        
        
def create_DBClientFactory(args, config: Mapping):
    """
    create a DBClient instance

    :raises ConfigurationException:  if the given configuration is insufficient or erroneous
    """
    dbiocfg = config.get("dbio", {})
    dbtype = dbiocfg.get("factory")
    if not dbtype:
        raise ConfigurationException("required dbio.factory param missing")

    elif dbtype == "fsbased":
        wdir = args.workdir
        if not wdir:
            wdir = config.get("working_dir", ".")
        dbdir = dbiocfg.get('db_root_dir')
        if not dbdir:
            # use a default under the working directory
            dbdir = os.path.join(wdir, "dbfiles")
        elif not os.path.isabs(dbdir):
            # if relative, make it relative to the work directory
            dbdir = os.path.join(wdir, dbdir)
            if not os.path.exists(wdir):
                raise ConfigurationException(f"{wdir}: working directory does not exist")
            if not os.path.exists(dbdir):
                os.makedirs(dbdir)
        if not os.path.exists(dbdir):
            os.mkdir(dbdir)
        factory = FSBasedDBClientFactory(dbiocfg, dbdir)

    elif dbtype == "mongo":
        dburl = os.environ.get("OAR_MONGODB_URL")
        if not dburl:
            dburl = dbiocfg.get("db_url")
        if not dburl:
            # Build the DB URL from its pieces with env vars taking precedence over the config
            port = ":%s" % os.environ.get("OAR_MONGODB_PORT", dbiocfg.get("port", "27017"))
            user = os.environ.get("OAR_MONGODB_USER", dbiocfg.get("user"))
            cred = ""
            if user:
                pasw = os.environ.get("OAR_MONGODB_PASS", dbiocfg.get("pw", user))
                cred = "%s:%s@" % (user, pasw)
            host = os.environ.get("OAR_MONGODB_HOST", dbiocfg.get("host", "localhost"))
            dburl = "mongodb://%s%s%s/midas" % (cred, host, port)

        factory = MongoDBClientFactory(config.get("dbio", {}), dburl)
    
    elif dbtype == "inmem":
        factory = InMemoryDBClientFactory(config.get("dbio", {}))

    else:
        raise ConfigurationException(f"unrecognized factory: {dbtype}")

    return factory


    

            
