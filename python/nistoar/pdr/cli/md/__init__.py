"""
CLI command suite for accessing metadata from the PDR public metadata service.  See 
:py:module:`nistoar.pdr.cli` and the :program:`pdr` script for info on the general CLI infrastructure.

This module defines a set of subcommands to a commmand called (by default) "md".  These subcommands
include
  - ``recover``:  extract NERDm records from the public archival information packages (e.g. so that they 
    can be reloaded into the metadata database).

See also :py:module:`nistoar.pdr.describe.cmd` for the ``describe`` command.
"""
from nistoar.pdr import cli
from . import recover

default_name = "md"
help = "provide specialized access to PDR metadata"
description = \
"""This provides a suite of subcommands that provided specialized access to PDR metadata via its 
public APIs (apart from identifier resolution; see the describe command)."""

def load_into(subparser, as_cmd=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    """
    p = subparser
    p.description = description

    if not as_cmd:
        as_cmd = default_name
    out = cli.CommandSuite(as_cmd, p)
    out.load_subcommand(recover)
    return out

