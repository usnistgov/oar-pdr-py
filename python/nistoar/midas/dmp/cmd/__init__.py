"""
package that provides implementations of commands that can part of a command-line tool and provides 
administrative operations on DMPs.  This package is incorporated into :py:mod:`nistoar.midas.cli.midasadm`. 

(See :py:mod:`nistoar.pdr.utils.cli` for information on the framework for building up tool suites like 
``midasadm``.)

This module defines a set of subcommands to a command called (by default) "dmp".  These subcommands
include
  - ``get``:      retrieve and display DMP records in JSON format
  - ``exfilt``:   exfiltrate one or more DMP records for transfering to another DBIO system
  - ``import``:   import a set of DMP records exfiltrated from another DBIO system
"""
import os, argparse, logging, re
from collections.abc import Mapping
from logging import Logger
from importlib import import_module
from argparse import ArgumentParser
from copy import deepcopy

from nistoar.pdr.utils import cli
from nistoar.pdr.utils.prov import Agent
from nistoar.midas.cli import get_agent
from nistoar.midas.dbio import DMP_PROJECTS
from nistoar.base.config import ConfigurationException
from nistoar.base import config as cfgmod

default_name = "dmp"
help = "manage DMP records via subcommands"
description = \
"""apply an action to one or more DMP records

The configuration provided to this command can include a conventions property for configuring 
for multiple DMP processing conventions in the same fashion as the midas-dbio web service.  When 
provided, each subproperty will be the name of a supported convention, and its value will be the 
specific configuration.  The convention is chosen by its name given in one of the following ways, 
in this priority:
  1. the convention name provided by the --convention option,
  2. if set, the value of the OAR_DMP_CONVENTION environment variable,
  3. The default_convention property in the provided configuration (when the midas-dbio schema 
     is used; see midasadm -h for more details), 
  4. The loan convention configured when only one convention is provided.
"""

extfilt_description = \
"""exfiltrate one or more DMP records for transfering to another DBIO system

The footprint of a DMP record in the DBIO database goes beyond just the DBIO record itself; it 
includes provenance and history.  Thus, to replicate a record from one DBIO system to another, all 
of the data must be gathered up transfered.  This command will do gather that data for identifiers for 
one or more DMP records and exported in tranmittable format.  The records and all their history, then,
can be imported into another DBIO system (by-passing the service API).  
"""

import_description = \
"""import records exfiltrated from another DBIO system

This command performs the reverse of exfilt:  it loads records in from an archive file produced by 
exfilt.  This bypasses the service layer which would otherwise filter and alter the record before 
loading it.  Further, all provenance and history included in the archive file will get loaded as well.  
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
    from ...dbio.cmd import exfilt

    subparser.description = description
    p = subparser
    # define_comm_dmp_opts(p)

    if not as_cmd:
        as_cmd = default_name
    out = cli.CommandSuite(as_cmd, p, current_dests)
    out.load_subcommand(exfilt.ExfiltCommand(DMP_PROJECTS, extfilt_description))
    out.load_subcommand(exfilt.ImportCommand(DMP_PROJECTS, import_description))

    return out
