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
                                NotEditable, NotAuthorized, ObjectNotFound, InvalidRecord,
                                status, PUBLIC_GROUP)
from nistoar.midas.dap.nerdstore import NERDResourceStorageFactory

from . import create_DAPService, get_agent

default_name = "finalize"
help = "Finalize a DAP making it ready for submission"
description = """\
Apply pre-submission finalization to a draft DAP record.  The changes made to the record are generally
the minimal changes needed to make the record ready to be submitted for publication.  
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
                   help="the DBIO DAP identifier of the record to finalize")
    p.add_argument("-m", "--message", type=str, metavar="MSG", dest="message",
                   help="set MSG as the provenance history message for this action")
                   
    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: register a previously published DAP into the DBIO, marking it published
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
        svc.finalize(args.dbid, args.message)
    except NotAuthorized as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: insufficient authorization to update", 9)
    except ObjectNotFound as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: DAP not found", 1)
    except Exception as ex:
        stat = None
        try:
            stat = svc.get_status(args.dbid).state
        except Exception as eex:
            log.warning("Unexpected error while trying to determine state of %s: %s",
                        args.dbid, str(eex))
        if not stat:
            stat = "unknown"
        if isinstance(ex, NotEditable):
            raise CommandFailure(args.cmd, f"{args.dbid}: not in an editable state ({stat}; "+
                                 "try applying finalize cmd)", 1) from ex
        if isinstance(ex, InvalidRecord):
            if ex.errors:
                msg = "\n  ".join(ex.errors)
                log.error("Sorry, finalization failed to produce a valid record ready for submission\n"+
                          "due to the following issues:\n  "+msg)
                raise CommandFailure(args.cmd, f"{args.dbid}: Record not ready for submission; further "+
                                               "editing required", 1) from ex
            else:
                raise CommandFailure(args.cmd, f"{args.dbid}: Sorry, invalid record detected: {str(ex)}", 1)

        log.execption(ex)
        raise CommandFailure(args.cmd, f"{args.dbid}: Unexpected failure: {str(ex)}") from ex

    
