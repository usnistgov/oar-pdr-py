"""
CLI command that advances the external review process through feedback or approval
"""
import logging, argparse, os, re
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

from . import create_DAPService, get_agent

default_name = "review"
help = "Provide external review feedback"
description = \
"""Provide feedback to a DAP on behalf of an external review system (which may be fictional 
for testing purposes).  This can include updating the phase of the review, providing feedback 
statements, or approving the DAP for publication.  
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
                   help="the DBIO DAP identifier of the record to report on")
    p.add_argument("phase", metavar="PHASE", type=str, default="review",
                   help="the phase of review that the DAP is currently in.  This can have any "+
                        "value, but two special values are recognized: 'approve' which indicates "+
                        "successful completion of the review process, and 'cancel' which interrupts "+
                        "the review process")
    p.add_argument("-U", "--info-url", metavar="URL", type=str, dest='infourl',
                   help="set the URL where more info about the review can be obtained")
    p.add_argument("-I", "--review-id", metavar="ID", type=str, dest='revid', 
                   help="set the identifier used by the review system to identify the review "+
                        "process that this feedback is coming from.  If not provided the DBID "+
                        "will be assumed.")
    p.add_argument("-r", "--review-sys", metavar="NAME", type=str, default="testrev", dest='revsys',
                   help="the name of the review system that is providing this feedback")
    p.add_argument("-f", "--feedback", metavar="STATEMENT", action="append", dest='feedback',
                   help="include STATEMENT among the pieces of feedback registered with the DAP")
    p.add_argument("-n", "--reviewer", metavar="NAME", type=str, dest="revname",
                   help="the name of reviewer providing feedback.  If not provided, it will be "+
                        "set to 'unknown'; ignored if -f is not set")
    p.add_argument("-X", "--replace-feedback", action="store_true", dest='replace',
                   help="if provided, remove any old feedback currently attached to the record")
    p.add_argument("-E", "--request-changes", action="store_true", dest='change',
                   help="if DAP is currently in 'submitted' state, change the state to 'edit' so "+
                        "that the authors can update the record in response to feedback.  Ignored "+
                        "if DAP is not currently in the 'submitted' state.")

    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: provide external review feedback
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
    if not args.phase:
        raise CommandFailure(args.cmd, "review phase name not specified", 2)
    if not args.revid:
        args.revid = args.dbid

    agent = get_agent(args, config)
    try:
        svc = create_DAPService(agent, args, config, log)
    except ConfigurationException as ex:
        raise CommandFailure(args.cmd, "Config error: "+str(ex), 6) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, "Unable to create DAP service: "+str(ex), 1) from ex

    try:
        if args.phase == "approve":
            if args.feedback:
                log.warning("feedback statements ignored for phase=approve")
            svc.approve(args.dbid, args.revsys, args.revid, args.infourl) 

        elif args.phase == "cancel":
            if args.feedback:
                log.warning("feedback statements ignored for phase=approve")
            svc.cancel_external_review(args.dbid, args.revsys, args.revid, args.infourl)

        else:
            feedback = [] if args.feedback or args.replace else None
            if args.feedback:
                who = args.revname or "unknown"
                for fb in args.feedback:
                    fbitem = OrderedDict([("reviewer", who)])
                    parts = [p.strip() for p in fb.split(":", 1)]
                    if len(parts) > 1 and parts[0] in ["req", "warn", "rec"]:
                        fbitem["type"] = parts[0]
                        fb = parts[1]
                    fbitem["description"] = fb
                    feedback.append(fbitem)
                        
            svc.apply_external_review(args.dbid, args.revsys, args.phase, args.revid,
                                      args.infourl, feedback, args.change, args.replace)

    except NotAuthorized as ex:
        raise CommandFailure(args.cmd, f"Unexpected authorization failure: {str(ex)}", 1) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, f"Unexpected failure: {str(ex)}", 1) from ex

