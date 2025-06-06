"""
package that provides implementations of commands that can part of a command-line tool and provides 
administrative operations on DAPs.  This package is incorporated into :py:mod:`nistoar.midas.cli.midasadm`. 

(See :py:mod:`nistoar.pdr.utils.cli` for information on the framework for building up tool suites like 
``midasadm``.)

This module defines a set of subcommands to a command called (by default) "dap".  These subcommands
include
  - ``regpub``:   register a previously published DAP into the DBIO
  - ``prepupd``:  prepare a previously published DAP for editing
"""
import os, argparse
from nistoar.pdr.utils import cli

default_name = "dap"
help = "manage dap records via subcommands"
description = \
"""apply an action to a DAP record"""

def load_into(subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    """
    from . import regpub

    subparser.description = description

    if not as_cmd:
        as_cmd = default_name
    out = cli.CommandSuite(as_cmd, subparser)
    out.load_subcommand(regpub)

    return out

