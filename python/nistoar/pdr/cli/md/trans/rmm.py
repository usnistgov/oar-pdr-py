"""
CLI NERDm transformation commanad that converts a record to a form
used by the PDR.  (See also ``nistoar.nerdm.convert.rmm``.)
"""
import logging, argparse, sys, os, re, json

from nistoar.nerdm.convert import rmm
from nistoar.pdr.distrib import DistribServerError, DistribResourceNotFound
from ._comm import define_comm_trans_opts, process_svcep_args, define_comm_md_opts
from ._comm import _get_record_for_cmd, _write_record_for_cmd                    

default_name = "rmm"
help = "convert a NERDm record to a three-part form useful for loading into the RMM"
description = """
  This comand will read in NERDm record (from a file or standard input) or retrieve it from the PDR
  and convert it to one of two forms useful for loading into the RMM.  If the output is specified 
  (via -o) as an existing directory, the output will be written to three files in that directory; 
  otherwise, it will get written to one file containing a JSON record with three parts in the 
  'record', 'version', and 'releaseSet' properties. 
"""

def load_into(subparser, current_dests, as_cmd=None):
    """
    load this command into a CLI by defining the command's arguments and options
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    :param set current_dests:  the current set of destination names that have been defined so far; this
                               can indicate if a parent command has defined required options already
    :param str as_cmd:  the command name that this command is being loaded as (ignored)
    :rtype: None
    """
    p = subparser
    p.description = description

    if 'src' not in current_dests:
        define_comm_trans_opts(p)
    if 'rmmbase' not in current_dests:
        define_comm_md_opts(p)
    p.add_argument("--keep-pdr-eps", action="store_true", dest="pdreps",
                   help="When it is necessary to augment the output record with URLs pointing, use the "+
                        "standard NIST PDR enpoints; with out this option, endpoints based on the "+
                        "-U/--services-base-url will be used")

    return None

def execute(args, config=None, log=None):
    """
    execute this command: convert the input record to the latest NERDm schemas
    """
    cmd = default_name
    if not log:
        log = logging.getLogger(cmd)
    if not config:
        config = {}

    if isinstance(args, list):
        # cmd-line arguments not parsed yet
        p = argparse.ArgumentParser()
        load_command(p)
        args = p.parse_args(args)

    _process_args(args, config, cmd, log)

    # may raise PDRCommandFailure
    rec = _get_record_for_cmd(args, cmd, config, log)

    defver = "1.0.0"
    if args.version:
        defver = args.version
    pubeps = {}
    if not args.pdreps:
        pubeps["portalBase"] = args.srvrbase

    cvtr = rmm.NERDmForRMM(log, pubeps=pubeps)
    rec = cvtr.to_rmm(rec, defver)

    if args.outfile and os.path.isdir(args.outfile):
        # write out as three files
        raise PDRCommandFailure(cmd, "3-file output not yet implemented", 2)
        
    _write_record_for_cmd(rec, args, cmd, config, log)

def _process_args(args, config, cmd, log):
    if not args.src and args.version:
        log.warning("-V/--version argument ignored when reading from file or stdin")
    process_svcep_args(args, config, cmd, log)
