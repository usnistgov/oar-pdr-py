"""
CLI command that displays the state of an AIP's preservation
"""
import sys, logging, argparse, os, re, math, textwrap
from logging import Logger
from copy import deepcopy
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime

from nistoar.base.config import ConfigurationException
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.exceptions import IDNotFound
from nistoar.pdr.preserve.service import PreservationStatus
from . import create_preservation_service

default_name = "status"
help = "display the status of an AIP's preservation"
description = """\
This command will display the current status of the preservation of a particular archive information 
package (AIP).  An AIP typically originates from an SIP submitted to a publishing service, and the state
of its preservation is held in the working directory for the preservation service.  This space can be 
determined by configuration or specified explicitly via --in-progress-dir.  If the AIP is not currently 
being preserved, it will show information regarding the last run of preservation (successful or otherwise).
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

    p.add_argument("aipid", metavar="AIPID", type=str,
                   help="the AIP identifier of the record to report on")
    p.add_argument("-s", "--silent", action="store_true", dest='silent',
                   help="print nothing (implies -q).  The caller can use the exit code to "
                        "determine state: 0 = in progress or completed at least once, "
                        "11 = no attempt found, 12 = last attempt failed")
    p.add_argument("-t", "--state-only", action='store_true', dest='stateonly',
                   help="print out only the state label ('in progress', 'completed', 'failed')")
    p.add_argument("-o", "--output-file", type=str, metavar='FILE', dest='outfile',
                   help="if provided write status information to FILE instead of standard out")
    p.add_argument("-H", "--history-dir", type=str, metavar='DIR', dest='histdir',
                   help="the directory where preservation history records are stored.  The "
                        "default is taken from the configuration")
    p.add_argument("-P", "--in-progress-dir", type=str, metavar='DIR', dest='presdir',
                   help="the root working directory containing state for AIPs whose preservation are "
                        "currently in progress.  The default is taken from the configuration")

    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: display the preservation status of an AIP
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

    if args.silent:
        args.quiet = True
    if not args.aipid:
        raise CommandFailure(args.cmd, "AIP ID not specified", 2)

    if args.presdir:
        if not os.path.isdir(args.presdir):
            raise CommandFailure(f"{args.presdir}: does not exist as directory", 2)
        config['in_progress_dir'] = args.presdir
    if args.histdir:
        if not os.path.isdir(args.histdir):
            raise CommandFailure(f"{args.histdir}: does not exist as directory", 2)
        config['history_dir'] = args.histdir
    if args.workdir:
        config['working_dir'] = args.workdir

    if args.logfile and config.get('logdir'):
        del config['logdir']

    try:
        svc = create_preservation_service(config, log)  # may raise exceptions
    except ConfigurationException as ex:
        raise CommandFailure(args.cmd, "Config error: "+str(ex), 8)
    except Exception as ex:
        raise CommandFailure(args.cmd, "Unexpected error creating preservation service: "+str(ex), 1)
    try:
        status = svc.status_of(args.aipid)
    except IDNotFound as ex:
        if args.silent:
            raise CommandFailure(args.cmd, "AIP not found", 11)
        status = PreservationStatus(args.aipid, "not yet submitted for preservation")

    # display the state
    version = status.get('version', '')
    if version:
        version = f"version {version} "
    msg = status.get('message', '')
    if msg:
        msg = textwrap.fill(msg, 76, initial_indent='  ', subsequent_indent='  ')

    if args.silent:
        if status.failed:
            raise CommandFailure(args.cmd, "FAILED", 12)
        return

    
    if args.outfile:
        try:
            out = open(args.outfile, 'w')
        except IOError as ex:
            raise CommandFailure(args.cmd, "Unable to write status to file, {args.outfile}: {str(ex)}", 4)
    else:
        out = sys.stdout

    try:
        if args.stateonly:
            label = "unknown"
            if status.get('exitcode') is not None:
                if status.successful:
                    label = "completed"
                else:
                    label = "failed"
            elif status.in_progress:
                label = "in progress"
            else:
                label = "unstarted"
            print(label, file=out)
            return
        
        print(f"Preservation status for {status.aipid}:", file=out)
        if status.get('exitcode') is not None:
            exited = status.get('comptime', '(completion time unknown)')
            if isinstance(exited, float):
                exited = datetime.fromtimestamp(math.trunc(exited)).isoformat()
            if status.successful:
                print(f"COMPLETED {version}{exited}", file=out)
            else:
                print(f"FAILED {version}{exited} ({status.get('exitcode')})")
                print(f"Last completed step: {status.laststep}")
        elif status.in_progress:
            print(f"IN PROGRESS {version}", file=out)
        else:
            print(f"UNSUBMITTED (not found)", file=out)
        if msg:
            print(msg, file=out)

        rtime = status.get('runtime', '')
        if isinstance(rtime, float):
            rtime = f" runtime: {math.trunc(100*rtime+0.5)/100.0}s"
        if status.get('reqtime'):
            qtime = datetime.fromtimestamp(math.trunc(status.get('reqtime'))).isoformat()
            print(f"Requested {qtime}{rtime}", file=out)

    finally:
        if args.outfile:
            out.close()
        
