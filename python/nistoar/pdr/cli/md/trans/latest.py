"""
CLI NERDm transformation commanad that converts a record to use the latest versions of the schemas
used by the PDR.  (See also ``nistoar.nerdm.convert.latest``.)
"""
import logging, argparse, sys, os, re, json

from nistoar.nerdm.convert import latest
from nistoar.pdr.distrib import DistribServerError, DistribResourceNotFound
from ._comm import define_comm_trans_opts, process_svcep_args, define_comm_md_opts
from ._comm import _get_record_for_cmd, _write_record_for_cmd                    

default_name = "latest"
help = "convert a NERDm record to the latest versions of the schemas used by the PDR"
description = """
  This comand will read in NERDm record (from a file or standard input) or retrieve it from the PDR
  and convert it to the latest schemas used by the PDR.  It will update all schema references 
  accordingly, and it will look for deprecated properties and attempt to convert them to their latest 
  equivalent.  Deprecated properties that do not have a modern alternative will generally be left in
  place.
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
    nerdm = _get_record_for_cmd(args, cmd, config, log)

    latest.update_to_latest_schema(nerdm, inplace=True)
        
    _write_record_for_cmd(nerdm, args, cmd, config, log)

def _process_args(args, config, cmd, log):
    if (args.filesrc or not (args.aipsrc or args.mdsrc)) and args.version:
        log.warning("-V/--version argument ignored when reading from file or stdin")
    process_svcep_args(args, config, cmd, log)
