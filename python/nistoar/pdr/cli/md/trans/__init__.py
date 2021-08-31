"""
CLI command suite for transforming NERDm records, including to different formats.  See 
:py:module:`nistoar.pdr.cli` and the :program:`pdr` script for info on the general CLI infrastructure.

This module defines a set of subcommands that identify the type of transformed output to create.  These
include:
  - ``latest``:  transform a NERDm record to use the most up to date schemas used by the PDR
  - ``rmm``:     transform a NERDm record into a three-component record appropriate for ingesting into the RMM.

See also :py:module:`nistoar.pdr.describe.cmd` for the ``describe`` command.
"""
from nistoar.pdr import cli
from . import latest rmm
from .._args import process_svcep_args, define_comm_md_opts
from ._comm import *
# from ._comm import _get_record_for_cmd, _write_record_for_cmd
from ..get import describe, extract_from_AIP

default_name = "trans"
help = "transform NERDm records"
description = \
"""This command transforms a PDR NERDm record into other forms including other formats"""

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
    out.load_subcommand(latest)
    out.load_subcommand(rmm)
    return out

