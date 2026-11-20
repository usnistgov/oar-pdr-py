"""
package that provides implementations of commands that can be part of a command-line tool, providiing
administrative operations for publishing SIPs.  This package is incorporated into 
:py:mod:`nistoar.midas.cli.midasadm`. 

(See :py:mod:`nistoar.pdr.utils.cli` for information on the framework for building up tool suites like
 ``midasadm``.)

This module defines a set of subcommands to a command called (by default) "pub" (but might also be 
available as "pdp0" or "pdp1".  These subcommands include:
  - ``status``:   display the status information for a particular SIP
  - ``prepupd``:  reconstruct an SIP bag for publishing
  - ``publish``:  submit an SIP bag to be published
  - ``unsubmit``: pull back an SIP bag previously submitted but failed to complete
"""
import os, argparse, logging
from collections.abc import Mapping
from logging import Logger
from importlib import import_module
from argparse import ArgumentParser
from copy import deepcopy

from nistoar.pdr.utils import cli
from nistoar.pdr.utils.prov import Agent
from nistoar.midas.cli import get_agent
from nistoar.base.config import ConfigurationException
from nistoar.base import config as cfgmod

default_name = "pub"
help = "manage SIPs for publishing"
description = """\
apply an action to a publishing submission information package (SIP).  

This suite of commands can be used to shepherd an SIP through the publishing workflow, particularly 
when publishing through the normal publishing service has failed for some reason.  Executing one of 
the suite commands requires the use of the -C/--convention option to indicate which publishing 
convention (e.g. 'pdp0', 'pdp1') to use.  
"""

# This is used with PubAliasCommand
pdp0_description = """\
This is an alias for "pub -C pdp0" for managing SIPs via the pdp0 convention.  Run "... pub -h" for 
more information.
"""

# This is used with PubAliasCommand
pdp1_description = """\
This is an alias for "pub -C pdp1" for managing SIPs via the pdp0 convention.  Run "... pub -h" for 
more information.
"""

class PubCmd(cli.CommandSuite):
    """
    a :py:class:`~nistoar.pdr.utils.cli.CommandSuite` specialized for the ``pub`` subcommand.

    This implementation allows the command's configuration to be extracted from the pdp web 
    service configuration to ensure to ensure a matching behavior of underlying functions.
    """
    def __init__(self, suitename: str, parent_parser: ArgumentParser, current_dests=None,
                 def_convention: str=None):
        super(PubCmd, self).__init__(suitename, parent_parser, current_dests)
        self._useconv = def_convention

    def extract_config_for_cmd(self, config, cmdname, cmd=None, convention=None):
        """
        extract the convention-specific configuration from the configuration provided.  

        This specialization supports two schemas for the incoming configuration: the normal command 
        schema supported by the general :py:mod:`cli module <nistoar.pdr.utils.cli>` and the 
        pdr-pdp schema provided via its ``conventions`` property.  The latter ensures the configuration 
        provide to the ``pub`` subcommands--and, thus, their behavior--to match that of PDP web 
        service.  The standard cli module command-based schema will be assumed if the given configuration 
        contains ``cmd`` property.  Otherwise, if the configuration contains the ``conventions`` property, 
        the PDP service schema will be assumed.  If neither appears, the input configuration will be 
        returned unchanged.  

        :param dict config:  the configuration to extract the specific command configuration from
        :param str cmdname:  the name of the command to look for
        :param module  cmd:  the module where command's implementation is defined.  If provided and 
                             ``cmdname`` is not found within the ``cmd`` property, the value of 
                             ``default_name`` from the module will be looked for instead.  
        :param str convention:  the name of the DAP processing convention (e.g. ``pdp1``) to assume.
                             This is used to pull out the configuration appropriate for that 
                             convention when the configuration is using the PDP service schema (i.e.
                             it includes a ``conventions`` property).  If not provided, then convention
                             specified by the ``default_convention`` configuration property will be 
                             assumed.  
        :raises ConfigurationException: if ``conventions`` is not specified and a default one cannot be 
                             determined
        """
        if 'cmd' in config:
            # interpret configuration by the standard cli convention
            return super().extract_config_for_cmd(config, cmdname, cmd)

        if not config.get('conventions'):
            return config

        out = deepcopy(config)
        del out['conventions']

        if not convention:
            convention = self._useconv
        if not convention and os.environ.get('OAR_PUB_CONVENTION'):
            convention = os.environ['OAR_PUB_CONVENTION']
        if not convention:
            convs = list(config['conventions'].keys())
            if len(convs) == 1:
                convention = convs[0]
        if not convention:
            raise ConfigurationException("Missing required parameter: services.dap.default_convention")
        if not config['conventions'].get(convention):
            raise ConfigurationException(f"Missing sub-configuration: conventions.{convention}")

        out = cfgmod.merge_config(config['conventions'][convention], out)
        out['convention'] = convention
        return out

    def execute(self, args, config: Mapping=None, log: logging.Logger=None):
        """
        execute a pub subcommand from this command suite
        :param argparse.Namespace args:  the parsed arguments
        :param dict             config:  the configuration to use
        :param Logger              log:  the log to send messages to 
        """
        if hasattr(args, 'conv') and args.conv:
            self._useconv = args.conv
        super().execute(args, config, log)

def load_into(subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    """
    # from . import regpub, setstate, finalize, publish, review, revreq, get, unsubmit, revperm

    subparser.description = description
    p = subparser
    p.add_argument("-C", "--convention", "--conv", metavar="NAME", type=str, dest='conv',
                   help="a label identifying the PDP processing convention to assume when "+
                        "executing a pub command")

    if not as_cmd:
        as_cmd = default_name
    out = PubCmd(as_cmd, subparser)
    # out.load_subcommand(status)
    return out

class PubAliasCommand:
    """
    an alias for the ``pub`` command that sets the convention
    """

    def __init__(self, convention: str, desc: str, defname: str=None):
        if not defname:
            defname = convention
        self.default_name = defname
        self.help = f"an alias for 'pub -C {convention}'"
        self.description = desc
        self.conv = convention

    def load_into(self, subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
        p = subparser
        p.cmd = as_cmd or self.default_name
        p.description = self.description

        out = PubCmd(p.cmd, subparser, def_convention=self.conv)
        return out

