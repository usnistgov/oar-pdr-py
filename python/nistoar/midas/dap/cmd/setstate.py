"""
CLI command that can change the recorded state of a DAP record.
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
                                NotEditable, NotAuthorized, AlreadyExists, ObjectNotFound, ACLs,
                                status, PUBLIC_GROUP)
from nistoar.midas.dap.nerdstore import NERDResourceStorageFactory

from . import create_DAPService, get_agent

default_name = "setstate"
help = "register a previously published DAP into the DBIO"
description = \
"""Update a DAP record to a given state.  Care should be taken with this command to ensure that the 
new state is consistent with actual state of the record; for example, changing the state to "published"
will not preserve the record nor push it out to its published destination.

"""

KNOWN_STATES = [
    status.EDIT,         # Record is currently being edited for a new released version                
    status.PROCESSING,   # Record is being processed at the moment and cannot be updated              
                         #   further until this processing is complete.                               
    status.READY,        # Record is ready for submission having finalized and passed all             
                         #   validation tests.                                                        
    status.SUBMITTED,    # Record has been submitted and is either processed or is under review       
    status.ACCEPTED,     # Record has been reviewed and is being processed for release                
    status.INPRESS,      # Record was submitted to the publishing service and is still being processed
    status.PUBLISHED,    # Record was successfully preserved and released                             
    status.UNWELL        # Record is in a state that does not allow it to be further processed or     
]                        #   updated and requires administrative care to restore it to a usable state 


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
                   help="the DBIO DAP identifier of the record to change the state of")
    p.add_argument("newstate", metavar="STATE", type=str,
                   help="the desired state, one of 'edit', 'processing', 'ready', 'submitted', "+
                        "'accepted', 'in press', 'published', 'unwell'")
    p.add_argument("-f", "--force", action="store_true",
                   help="force setting state to an unrecognized value")

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
    if not args.newstate:
        raise CommandFailure(args.cmd, "new desired state not specified", 2)

    if args.newstate.lower() not in KNOWN_STATES:
        if not args.force:
            raise CommandFailure(args.cmd, "Unrecognized state requested: "+args.newstate, 2)
        else:
            log.warning("Setting DAP %s to unrecognized state: "+args.newstate.lower())

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
        if rec.status.state != args.newstate:
            log.info("DAP %s: seting state to %s", args.dbid, args.newstate)
            rec.status.set_state(args.newstate)
            rec.save()
        else:
            log.info("DAP %s: already set to state %s; no change made", args.dbid, args.newstate)

    except NotAuthorized as ex:
        raise CommandFailure(args.cmd, f"Unexpected authorization failure: {str(ex)}", 1) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, f"Unexpected failure: {str(ex)}", 1) from ex




    
    
