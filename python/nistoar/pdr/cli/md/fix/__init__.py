"""
CLI command suite for making targeted fixes to NERDm records.  See 
:py:module:`nistoar.pdr.cli` and the :program:`pdr` script for info on the general CLI infrastructure.

This module defines a set of subcommands that identify the type of transformed output to create.  These
include:
  - ``dates``:  update specific dates 

See also :py:module:`nistoar.pdr.describe.cmd` for the ``describe`` command.
"""
from nistoar.pdr import cli
from . import dates
from .._args import process_svcep_args, define_comm_md_opts
from ..trans._comm import *
# from ._comm import _get_record_for_cmd, _write_record_for_cmd
# from ..get import describe, extract_from_AIP

default_name = "fix"
help = "fix NERDm records"
description = \
"""This command applies targetted fixes to particular properties of a PDR NERDm record"""

def load_into(subparser, current_dests=None, as_cmd=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    """
    p = subparser
    p.description = description
    if 'rmmbase' not in current_dests:
        define_comm_md_opts(p)
    
    if not as_cmd:
        as_cmd = default_name
    out = cli.CommandSuite(as_cmd, p, current_dests)
    out.load_subcommand(dates)
    return out

