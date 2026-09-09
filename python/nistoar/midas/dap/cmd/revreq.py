"""
CLI command that submits a DAP record for review and publication or some part of that process
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
from nistoar.midas.dap.extrev import ExternalReviewException
from nistoar.pdr import constants as const

from . import create_DAPService, get_agent

default_name = "revreq"
help = "Submit a DAP for review"
description = \
"""Submit a DAP for review to the configured external review system (at NIST, this is the NPS). The 
DAP will then be able to accept feedback from the review system (which may eventually trigger 
publication upon successful completion of the review). 

This command will not attempt to validate or finalize the DAP nor alter the access permissions.  Often
when this command is needed, these operations have already occurred.  If not, see the finalize and 
revperms commands, or use the submit command to combine all of these operations into one call.  
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
    p.add_argument("-C", "--change", metavar="NAME", type=str, dest='changes', action='append',
                   choices="newrec mdup mindata majdata newfile rmfile deact".split(),
                   help="a label indicating a type of change that was made (during a revision), one "+
                        "of  'newrec', 'mdup', 'mindata', 'majdata', 'newfile', 'rmfile', "+
                        "or 'deact'.  Repeat this option as needed for multiple changes.")
    p.add_argument("-s", "--software-review", action='store_true', dest='secrev', 
                   help="indicate in the request that software is included in the publication so to "+
                        "trigger a software security review.  If not provided, this will be determined "+
                        "based on the DAP's state")
    p.add_argument("-S", "--no-software-review", action='store_true', dest='nosecrev', 
                   help="prevent triggering a software security review  If not provided, this will be "+
                        "determined based on the DAP's state")
    p.add_argument("-r", "--record-requested", action="store_true", dest='markreq',
                   help="after requesting review, update the review status to indicate that review was "+
                        "requested. This should happen by default if the review system is nps1")
    p.add_argument("-i", "--instruction", metavar="MESSAGE", dest='instruct',
                   help="a message to send as special instructions to the reviewers")
    p.add_argument("--server", metavar="DNSNAME", type=str, dest='server',
                   help="the NPS server to send the request to; this can be used to send the request "+
                        "via the usual API but operating on a different (e.g. test) server.  If not "+
                        "provided, this configured service endpoint will be used")
    p.add_argument("-n", "--service-name", metavar="NAME", type=str, dest='sysname',
                   help="request review by the named review system.  If not provided, the request will "+
                        "be sent to the configured system")

    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: submit the DAP for review
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

    if args.secrev and args.nosecrev:
        raise CommandFailure("-s and -S are incompatible", 2)

    revcfg = config.get('external_review')
    if not revcfg:
        raise CommandFailure(args.cmd, "Config error: missing required property: external_review", 6)

    if not args.dbid:
        raise CommandFailure(args.cmd, "DAP ID not specified", 2)
    if args.sysname:
        revcfg['name'] = args.sysname
    if args.server:
        if revcfg.get('nps_endpoint'):
            epre = re.compile(r'^(https?://)[^:/]+')
            m = epre.search(revcfg['nps_endpoint'])
            if m:
                revcfg['nps_endpoint'] = epre.sub(m.group(1)+args.server, revcfg['nps_endpoint'])
    
    agent = get_agent(args, config)
    try:
        svc = create_DAPService(agent, args, config, log)
    except ConfigurationException as ex:
        raise CommandFailure(args.cmd, "Config error: "+str(ex), 6) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, "Unable to create DAP service: "+str(ex), 1) from ex

    extrevcli = svc._extrevcli
                        
    try:
        prec = svc.get_record(args.dbid)
        version = prec.data.get('version') or prec.data.get('@version') or '1.0.0'

        opts = { 'title': prec.data.get("title") }
        opts['description'] = "\n\n".join(prec.data.get("description", []))
        if ':' in args.dbid:
            opts['pubid'] = "ark:/"+ const.ARK_NAAN + '/' + re.sub(r':', '-', args.dbid)
        if args.instruct:
            opts['instructions'] = args.instruct
        if args.changes:
            opts['changes'] = args.changes
        else:
            # TO DO: try to determine from state of the DAP.  Any new/updated files?
            if re.match(r'^1(\.0)*$', version):
                opts['changes'] = ['newrec']
            else:
                opts['changes'] = ['majdata']
            
        # TO DO: support reviewers

        if args.secrev or (not args.nosecrev and prec.meta.get("software_included")):
            opts['security_review'] = True

        # now submit the review request
        extrevcli.submit(args.dbid, agent.actor, version, **opts)

    except ObjectNotFound as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: DAP not found ({str(ex)})", 1) from ex        
    except NotAuthorized as ex:
        raise CommandFailure(args.cmd, f"Unexpected authorization failure: {str(ex)}", 9) from ex
    except ExternalReviewException as ex:
        raise CommandFailure(args.cmd,
                             f"{extrevcli.system_name} review request failure: {str(ex)}", 1) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, f"Unexpected failure: {str(ex)}", 1) from ex

    if args.markreq or extrevcli.system_name in ["nps1"]:
       try:
            # mark review as requested as the legacy NPS will not respond immediately
            # need to do this before changing permissions
            svc.apply_external_review(prec.id, extrevcli.system_name, "requested", _prec=prec)
       except NotAuthorized as ex:
           raise CommandFailure(args.cmd,
                                f"Unexpected authorization failure during record update: {str(ex)}",
                                9) from ex
       except Exception as ex:
           log.exception(ex)
           raise CommandFailure(args.cmd,
                                f"Unexpected failure during record update: {str(ex)}", 1) from ex

        
            

            

