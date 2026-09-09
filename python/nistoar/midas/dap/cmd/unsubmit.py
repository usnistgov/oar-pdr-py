"""
CLI command resets a submitted to record to an editable state
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

default_name = "unsubmit"
help = "Return a submitted DAP record back to an editable state"
description = \
"""Return a submitted DAP record back to an editable state.  This is often done to allow an author 
to respond to reviewer feedback or otherwise make changes in the middle of the review process;
however, it can also be used to pull back a record that was submitted by mistake.  

This command will reset the state to "edit" and re-establish the pre-submit permissions.  (See also 
the dap commands setstate and revperm which do these two operations independently.)  The 
-f or -F option can be used to erase the current status of the previously requested review.  
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
    p.add_argument("-c", "--cancel-review", metavar='REVSYS', type=str, dest='cancel',
                   help="mark the status of the review from the REVSYS review system as cancelled. "+
                        "(Note: this does not attempt to communicate this to the external review " +
                        "system; it only updates the local status)")
    p.add_argument("-f", "--forget-review", metavar='REVSYS', type=str, dest='forget',
                   help="clear the status of the review from the REVSYS review system, as if "+
                        "it was never requested.  (Note: this does not attempt to communicate " +
                        "with the review system; it only updates the local status).")
    p.add_argument("-F", "--forget-all-reviews", action="store_true", dest='forgetall',
                   help="clear the status of all reviews from the REVSYS review system, as if "+
                        "they were never requested.  (Note: this does not attempt to communicate " +
                        "with the review systems; it only updates the local status).")

    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: unsubmit the DAP from review
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

    if args.cancel and (args.forget or args.forgetall):
        log.warning("Ignoring -c; overridden by -f or -F")

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
        prec = svc.get_record(args.dbid)

        needsave = False
        if args.cancel:
            # this will also unset the permissions and set state back to EDIT (and save changes)
            svc.cancel_external_review(args.dbid, args.cancel)
            prec = svc.get_record(args.dbid)

        else:
            # restoring permissions
            prec.status.set_state(status.EDIT)
            for_review = not (args.forgetall or args.forget)
            svc._unset_review_permissions(prec, for_review)
            needsave = True

        if args.forgetall:
            systems = prec.status.get_review_systems()
            if not systems:
                log.warning("No review status found from any review system")
            else:
                for sys in systems:
                    prec.status.delete_review_from(sys)
                needsave = True

        elif args.forget:
            if not prec.status.delete_review_from(args.forget):
                log.warning("No review status found for %s (not started yet?)", args.forget)
            else:
                needsave = True

        if needsave:
            prec.save()

    except ObjectNotFound as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: DAP not found ({str(ex)})", 1) from ex        
    except NotAuthorized as ex:
        raise CommandFailure(args.cmd, f"Unexpected authorization failure: {str(ex)}", 9) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, f"Unexpected failure: {str(ex)}", 1) from ex

