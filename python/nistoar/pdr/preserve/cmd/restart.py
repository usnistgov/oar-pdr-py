"""
CLI command that restart an AIP preservation process
"""
import sys, logging, argparse, os, re, math, textwrap
from logging import Logger
from pathlib import Path
from collections.abc import Mapping

from nistoar.base.config import ConfigurationException
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.exceptions import IDNotFound
from nistoar.pdr.preserve.service import PreservationStatus, PreservationInProgress
from . import create_preservation_service

default_name = "restart"
help = "restart an AIP's preservation process"
description = """\
This command will display the current status of the preservation of a particular archive information 
package (AIP).  An AIP typically originates from an SIP submitted to a publishing service, and the state
of its preservation is held in the working directory for the preservation service.  This space can be 
determined by configuration or specified explicitly via --in-progress-dir.  If the AIP is not currently 
being preserved, it will show information regarding the last run of preservation (successful or otherwise).
"""

deftimeout = 60

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
                   help="the AIP identifier of the dataset to restart preservation for")
    p.add_argument("-H", "--history-dir", type=str, metavar='DIR', dest='histdir',
                   help="the directory where preservation history records are stored.  The "
                        "default is taken from the configuration")
    p.add_argument("-P", "--in-progress-dir", type=str, metavar='DIR', dest='presdir',
                   help="the root working directory containing state for AIPs whose preservation are "
                        "currently in progress.  The default is taken from the configuration")
    p.add_argument("-I", "--sip-dir", type=str, metavar='DIR', dest='sipdir',
                   help="the directory containing the input SIP bag to preserve.  The default is taken "
                        "from the configuration")
    p.add_argument("-W", "--wait", metavar='TIME', type=str, nargs='?', const='1h', dest='wait',
                   help="")
    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: restart preservation
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

    if args.presdir:
        if not os.path.isdir(args.presdir):
            raise CommandFailure(f"{args.presdir}: does not exist as directory", 2)
        config['in_progress_dir'] = args.presdir
    if args.histdir:
        if not os.path.isdir(args.histdir):
            raise CommandFailure(f"{args.histdir}: does not exist as directory", 2)
        config['history_dir'] = args.histdir
    if args.sipdir:
        if not os.path.isdir(args.sipdir):
            raise CommandFailure(f"{args.histdir}: does not exist as directory", 2)
        config['sip_bags_dir'] = args.sipdir
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

    pstatef = svc._state_file_for(args.aipid)
    if not pstatef.exists():
        raise CommandFailure(args.cmd, "No preservation state found; can't restart.\n")

    try:
        stat = svc.publish(args.aipid, message="Restarting preservation via midasadm")
    except PreservationInProgress as ex:
        raise CommandFailure(args.aipid, "can't restart: preservation is already in progress", 1)
    except Exception as ex:
        log.execption(ex)
        raise CommandFailure(args.aipid, "Unexpected failure: "+str(ex), 1)

    seealso = "See %s/preservation.log for detailed progress messages" % args.aipid
    if stat.in_progress:
        log.info("%s: preservation restarted; last step completed: %s\n  %s",
                 args.aipid, stat.laststop, stat.message)
        if args.wait:
            start = time.time()
            timeout = gettimeout(args.wait)
            log.info("Waiting for preservation to complete...")
            if not args.quiet:
                print("  ", seealso)
            #cycle

        else:
            log.info("Leaving preservation to run in background...")
            if not args.quiet:
                print("\n", seealso)

    elif stat.successful:
        log.info("%s: Preservation complete successfully", args.aipid)
        if not args.quiet:
            print(seealso)

    elif stat.failed:
        log.error("%s: Preservation exited with failure\n  %s", args.aipid, stat.message)
        log.info("Last successful step: %s\n\n%s", stat.laststep, msg)
        if not args.quiet:
            print("\n", seealso)

_tore = re.compile(r"^(\d*\.?\d+)([hms]?)$")
def gettimeout(reqtimeout: str, log: Logger):
    """
    convert a time out string into an integer number of seconds.  

    :param str reqtimeout:  the string-encoded timeout time consisting of an integer or 
                            floating point number and optionally appended with a unit 
                            letter, one of 'h', 'm', 's', for hours, minutes, and seconds,
                            respectively.  If no unit is given, 's' is assumed.
    """
    global deftimeout   # in integer seconds
    if not reqtimeout:
        return deftimeout

    m = _tore.search(reqtimeout)
    try:
        if not m:
            raise ValueError(reqtimeout)
        out = int(m.group(1))
    except ValueError as ex:
        log.warning("Bad wait time format: %s; must match N.Nu format (see help)")
        log.info("Using default wait time, %ds", deftimeout)
        return deftimeout
            
    if not m.group(2) or m.group(2) == 's':
        return out
    if m.group(2) == 'h':
        return out*3600
    return out*60
        

