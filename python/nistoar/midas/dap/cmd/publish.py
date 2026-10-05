"""
CLI commnd that will finalize a DAP, preparing it for publication.  
"""
import logging, argparse, os, re
from logging import Logger
from copy import deepcopy
from pathlib import Path
from collections.abc import Mapping
from getpass import getuser
from importlib import import_module
from datetime import datetime

from nistoar.base.config import ConfigurationException
from nistoar.midas import MIDASException
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.midas.dbio import (FSBasedDBClientFactory, MongoDBClientFactory, InMemoryDBClientFactory,
                                NotEditable, NotAuthorized, ObjectNotFound, InvalidRecord, ACLs,
                                status, PUBLIC_GROUP)
from nistoar.midas.dap.nerdstore import NERDResourceStorageFactory
from nistoar.midas.dbio.project import SubmissionFailed

from . import create_DAPService, get_agent

default_name = "publish"
help = "submit a draft DAP to the publishing service"
description = """\
This command will send a DAP to the publishing service for publishing into the PDR.  Unless the 
--force option is provided, this will require that the current state of DAP is "accepted"; apart from 
this, it does no other checks to ensure that the record is, in actuality, ready to be published.  
(If it isn't, the publishing process will likely fail downstream.)  If --force is used, the state 
of the record will first be set to "accepted".  
"""

def load_into(subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
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
    p.description = description
    p.cmd = as_cmd
    p.add_argument("dbid", metavar="ID", type=str,
                   help="the DBIO DAP identifier of the record to publish")
    p.add_argument("-f", "--force", action="store_true", dest="force",
                   help="do not require the record to be in the 'accepted' state before publishing")

    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command:  publish a DAP 
    """
    if not log:
        log = logging.getLogger(default_name)
    if not config:
        config = {}

    if isinstance(args, list):
        # cmd-line arguments not parsed yet
        p = argparse.ArgumentParser()
        load_command(p)
        args = p.parse_args(args)

    if not args.dbid:
        raise CommandFailure(args.cmd, "DAP ID not specified", 2)

    agent = get_agent(args, config)
    try:
        svc = create_DAPService(agent, args, config, log)
    except ConfigurationException as ex:
        raise CommandFailure(args.cmd, "Config error: "+str(ex), 6) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, "Unable to create DAP service: "+str(ex), 1) from ex

    try:
        rec = svc.dbcli.get_record_for(args.dbid, ACLs.WRITE)
    except NotAuthorized as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: insufficient authorization to update", 1)
    except ObjectNotFound as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: DAP not found", 1)

    try:
        if rec.status.state != status.ACCEPTED:
            if args.force:
                log.info("%s: changing record state from %s to %s to force publishing",
                         rec.id, rec.status.state, status.ACCEPTED)
                rec.status.set_state(status.ACCEPTED)
                rec.save()
            else:
                raise CommandFailure(args.cmd, f"{args.dbid}: DAP appears not ready to be published\n"+
                                     "  (consider the finalize command or the --force option)", 1)

        svc.publish(rec.id)

    except CommandFailure as ex:
        raise
    except Exception as ex:
        stat = None
        try:
            stat = svc.get_status(args.dbid).state
        except Exception as eex:
            log.warning("Unexpected error while trying to determine state of %s: %s",
                        args.dbid, str(eex))
        if not stat:
            stat = "unknown"
        if isinstance(ex, SubmissionFailed):
            raise CommandFailure(args.cmd,
                                 f"{args.dbid}: preservation apparently failed:\n  {str(ex)}\n" +
                                 f"  Current state: {stat} "+
                                  "(Consult PDP/preservation logs for details)", 1)
        log.exception(ex)
        raise CommandFailure(args.cmd, f"{args.dbid}: Unexpected failure {str(ex)}\n"+
                                       f"  Current state: {stat} "+
                                        "(Consult PDP/preservation logs for details)", 1)

