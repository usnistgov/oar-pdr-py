"""
CLI command that switches the permissions on a DAP record to and from review mode
"""
import logging, argparse, os, sys, json
from logging import Logger
from copy import deepcopy
from pathlib import Path
from collections.abc import Mapping
from collections import OrderedDict

from nistoar.base.config import ConfigurationException
from nistoar.midas import MIDASException
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.midas.dbio import (FSBasedDBClientFactory, MongoDBClientFactory, InMemoryDBClientFactory,
                                NotEditable, NotAuthorized, AlreadyExists, ObjectNotFound, ACLs,
                                status, PUBLIC_GROUP)
from nistoar.midas.dap.extrev import ExternalReviewException
from nistoar.pdr import constants as const

from . import create_DAPService, get_agent

default_name = "revperm"
help = "switch a DAP record's permissions to and from review mode"
description = \
"""Change the permissions on a DAP record to and from those needed for external review.  During 
external review, the permissions to update or manage a DAP record is temporarily taken away from the 
owner and co-editors and given to over a special reviewer account.  In addition, that reviewer account
is also given a special "publish" permission, allowing it to push the record to final publication once 
external review is successful.  By default, this command switches permissions into the review state; 
use --unset to revert them back for editing.  

This command does not update the state or otherwise affect the state of review.  (See also the 
dap command, unsubmit.)
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
    p.add_argument("dbid", metavar="DBID", type=str, 
                   help="the DBIO identifier of the DAP to submit for review")
    p.add_argument("-U", "--unset", action="store_true", dest="unset",
                   help="unset the review permissions (rather than setting them)")
    p.add_argument("-u", "--unset-for-feedback", action="store_true", dest="for_review",
                   help="unset the review permissions to allow response to review feedback")
    p.add_argument("-r", "--reader", metavar="USERID", action="append", dest="readers", default=[],
                   help="grant read access to each USERID provided.  While this option can be "+
                        "used multiple times, multiple IDs can also be provided as a comma-delimited "+
                        "list.  Ignored when used with -u or -U")

    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: set or unset the review permisisons
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

    if args.for_review:
        args.unset = True

    if args.readers:
        if args.unset:
            log.warning("Unset requested; --reader values ignored.")
        else:
            for i in range(len(args.readers)):
                args.readers.extend(args.readers.pop(0).split(','))

    agent = get_agent(args, config)
    try:
        svc = create_DAPService(agent, args, config, log)
    except ConfigurationException as ex:
        raise CommandFailure(args.cmd, "Config error: "+str(ex), 6) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, "Unable to create DAP service: "+str(ex), 1) from ex

    try:
        prec = svc.get_record(args.dbid)

        if args.unset:
            svc._unset_review_permissions(prec, args.for_review)
        else:
            svc._set_review_permissions(prec, args.readers)

        prec.save()

    except ObjectNotFound as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: DAP not found ({str(ex)})", 1) from ex        
    except NotAuthorized as ex:
        raise CommandFailure(args.cmd, f"Unexpected authorization failure: {str(ex)}", 9) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, f"Unexpected failure: {str(ex)}", 1) from ex


