"""
CLI command that pulls DAP records or portions thereof and sends to standard out
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

default_name = "get"
help = "Return DAP record data"
description = \
"""Pull a draft DAP record from the MIDAS database and display its contents in JSON format.  Portions 
of the record can be displayed by specifying a top-level property.  Note that if no property is specify
the returned DAP record's data property will be a summary version of the data content; if the data 
property is specified, the full NERDm data will be returned (unless --data-summary is also specified).
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
    p.add_argument("prop", metavar="PROPERTY", type=str, nargs='?', default=[],
                   help="restrict the output to the value of the DBIO record property from "+
                        "the DAP record.  Example values include 'data' and 'status'")
    p.add_argument("-o", "--output", metavar="FILE", type=str, dest="outfile",
                   help="write the output to a file with the specified name.  If the file already "+
                        "already exists, it will be overwritten")
    p.add_argument("-s", "--data-summary", action="store_true", dest="dosumm",
                   help="if the data property is requested, return the summarized version.  This will "+
                        "summary will leave out, for example, the full list of NERDm components.")

    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: retrieve and display the DAP record
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
    if args.prop and not isinstance(args.prop, list):
        args.prop = [ args.prop ]
    
    agent = get_agent(args, config)
    try:
        svc = create_DAPService(agent, args, config, log)
    except ConfigurationException as ex:
        raise CommandFailure(args.cmd, "Config error: "+str(ex), 6) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, "Unable to create DAP service: "+str(ex), 1) from ex

    out = None
    try:
        prec = svc.get_record(args.dbid)
    except ObjectNotFound as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: DAP not found ({str(ex)})", 1) from ex        
    except NotAuthorized as ex:
        raise CommandFailure(args.cmd, f"Unexpected authorization failure: {str(ex)}", 9) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, f"Unexpected failure: {str(ex)}", 1) from ex

    out = prec.to_dict()
    if len(args.prop) > 0:
        if args.prop[0] == "data" and not args.dosumm:
            try:
                out = svc.get_nerdm_data(args.dbid)
            except Exception as ex:
                log.exception(ex)
                raise CommandFailure(args.cmd,
                                     f"Unexpected failure getting full data: {str(ex)}", 1) from ex
        else:
            out = out.get(args.prop[0])
            if out == None:
                raise CommandFailure(args.cmd, f"{args.dbid}: {prop[0]} not found", 1) from ex

    ostrm = sys.stdout
    try:
        if args.outfile:
            ostrm = None
            ostrm = open(args.outfile, 'w')
        json.dump(out, ostrm, indent=2)
    except FileNotFoundError as ex:
        raise CommandFailure(args.cmd, f"{args.outfile}: {str(ex)}", 4)
    except IOError as ex:
        raise CommandFailure(args.cmd, f"{args.dbid}: trouble writing output: {str(ex)}", 4)
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, f"{args.dbid}: Unexpected failure: {str(ex)}", 1) from ex
    finally:
        if args.outfile and ostrm:
            try:
                ostrm.close()
            except Exception as ex:
                log.warning("Trouble closing output file: str(ex)")


                

    

    
            
