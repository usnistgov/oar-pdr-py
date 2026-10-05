"""
package that provides implementations of commands that can be part of a command-line tool, providiing
administrative operations for preserving SIPs.  This package is incorporated into 
:py:mod:`nistoar.midas.cli.midasadm`. 

(See :py:mod:`nistoar.pdr.utils.cli` for information on the framework for building up tool suites like
 ``midasadm``.)

This module defines a set of subcommands to a command called (by default) "pres".  These subcommands 
include:
  - ``status``:   display the preservation status information for a particular SIP/AIP
  - ``restart``:  restart a previously stopped preservation task
  - ``vaidate``:  validate an AIP bag determining its fitness for preservation
  - ``clear``:    clear out the current state of a failed or incomplete preservation run to allow
                  an SIP to be preserved from scratch.
"""
import os, argparse, logging
from collections.abc import Mapping
from logging import Logger
from importlib import import_module
from argparse import ArgumentParser
from copy import deepcopy

from nistoar.pdr.utils import cli
from nistoar.pdr.utils.prov import Agent
from nistoar.base.config import ConfigurationException
from nistoar.base import config as cfgmod
from nistoar.pdr.preserve.service import AIP1PreservationService

default_name = "pres"
help = "manage AIPs for preservation"
description = """\
apply an action preserving and publishing an archive information package (AIP).  

This suite of commands can be used to shepherd an AIP through the preservation workflow, particularly 
when preservation through the normal publishing service has failed for some reason.  
"""

class PresCmd(cli.CommandSuite):
    """
    a :py:class:`~nistoar.pdr.utils.cli.CommandSuite` specialized for the ``pres`` subcommand 
    for managing preservation requests.

    This implementation allows the command's configuration to be extracted from the pdp web 
    service configuration to ensure to ensure a matching behavior of underlying functions.
    """

    def extract_config_for_cmd(self, config, cmdname, cmd=None, convention=None):
        """
        extract the convention-specific configuration from the configuration provided.  

        This specialization supports two schemas for the incoming configuration: the normal command 
        schema supported by the general :py:mod:`cli module <nistoar.pdr.utils.cli>` and the 
        pdr-pdp schema provided via its ``preservation`` property.  The latter ensures the configuration 
        provide to the ``pres`` subcommands--and, thus, their behavior--to match that of PDP web 
        service.  The standard cli module command-based schema will be assumed if the given configuration 
        contains ``cmd`` property.  Otherwise, if the configuration contains the ``preservation`` property, 
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
                             it includes a ``preservation`` property).  If not provided, then convention
                             specified by the ``default_convention`` configuration property will be 
                             assumed.  
        """
        if 'cmd' in config:
            # interpret configuration by the standard cli convention
            return super().extract_config_for_cmd(config, cmdname, cmd)

        if not config.get('preservation'):
            return config

        out = deepcopy(config)
        del out['preservation']
        if out.get('conventions'):
            # get rid of publishing service configuration
            del out['convnetions']
        if out.get('authorized'):
            del out['authorized']

        out = cfgmod.merge_config(config['preservation'], out)
        return out


def load_into(subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    """
    from . import status

    subparser.description = description
    p = subparser

    if not as_cmd:
        as_cmd = default_name
    out = PresCmd(as_cmd, subparser)
    out.load_subcommand(status)
    return out

def create_preservation_service(config: Mapping, log: Logger):
    return AIP1PreservationService(config, log)


        
    
