"""
CLI command suite for creating and managing JWT tokens used for access the MIDAS web API

(See :py:mod:`nistoar.pdr.utils.cli` for information on the framework for building up tool suites like 
``midasadm``.)

This module defines a set of subcommands to a command called (by default) "dap".  These subcommands
include:
  - ``create``:  create a JWT with given content encoded into it
  - ``show``:    display the content of the data encoded into a given token
"""
import os, argparse, logging
from collections.abc import Mapping
from logging import Logger
from importlib import import_module
from argparse import ArgumentParser

from nistoar.pdr.utils import cli

default_name = "jwt"
help = "create and manage JWT authentication tokens"
description = \
"""This provides a suite of subcommands for creating and managing JWT authentication tokens
that can be used to access MIDAS and PDR web APIs.  
"""

def load_into(subparser, current_dests=None, as_cmd=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    :param set current_dests:  the current set of destination names that have been defined so far; this
                               can indicate if a parent command has defined required options already
    :param str as_cmd:  the command name that this command is being loaded as (ignored)
    """
    from . import create, show

    subparser.description = description
    p = subparser
    # define_comm_jwt_opts(p)

    if not as_cmd:
        as_cmd = default_name
    out = cli.CommandSuite(as_cmd, p, current_dests)
    out.load_subcommand(create)
    out.load_subcommand(show)

    return out
