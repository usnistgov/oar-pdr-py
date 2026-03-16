"""
package that provides implementations of commands that can part of a command-line tool and provides 
administrative operations on DAPs.  This package is incorporated into :py:mod:`nistoar.midas.cli.midasadm`. 

(See :py:mod:`nistoar.pdr.utils.cli` for information on the framework for building up tool suites like 
``midasadm``.)

This module defines a set of subcommands to a command called (by default) "dap".  These subcommands
include
  - ``regpub``:   register a previously published DAP into the DBIO
  - ``prepupd``:  prepare a previously published DAP for editing
  - ``get``:      retrieve and display DAP records in JSON format
  - ``setstate``: update the state of a DAP record
  - ``revreq``:   request a review of an external reviewer system
  - ``review``:   submit feedback to a DAP record on behalf of an external reviewer
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
from nistoar.midas.dap.nerdstore import NERDResourceStorageFactory
from nistoar.midas.dbio import FSBasedDBClientFactory, MongoDBClientFactory, InMemoryDBClientFactory

default_name = "dap"
help = "manage dap records via subcommands"
description = \
"""apply an action to a DAP record

The configuration provided to this command can include a conventions property for configuring 
for multiple DAP processing conventions in the same fashion as the midas-dbio web service.  When 
provided, each subproperty will be the name of a supported convention, and its value will be the 
specific configuration.  The convention is chosen by its name given in one of the following ways, 
in this priority:
  1. the convention name provided by the --convention option,
  2. if set, the value of the OAR_DAP_CONVENTION environment variable,
  3. The default_convention property in the provided configuration (when the midas-dbio schema 
     is used; see midasadm -h for more details), 
  4. The loan convention configured when only one convention is provided.
"""

class DAPCmd(cli.CommandSuite):
    """
    a :py:class:`~nistoar.pdr.utils.cli.CommandSuite` specialized for the ``dap`` subcommand.

    This implementation allows the command's configuration to be extracted from the midas web 
    service configuration to ensure to ensure a matching behavior of underlying functions.
    """
    def __init__(self, suitename: str, parent_parser: ArgumentParser, current_dests=None,
                 def_convention: str=None):
        super(DAPCmd, self).__init__(suitename, parent_parser, current_dests)
        self._useconv = def_convention

    def extract_config_for_cmd(self, config, cmdname, cmd=None, convention=None):
        """
        extract the dap command-specific configuration from the configuration provided.  

        This specialization supports two schemas for the incoming configuration: the normal command 
        schema supported by the general :py:mod:`cli module <nistoar.pdr.utils.cli>` and the 
        midas-dbio schema provided via its ``services`` property.  The latter ensures the configuration 
        provide to the ``dap`` subcommands--and, thus, their behavior--to match that of midas-dbio web 
        service.  The standard cli module command-based schema will be assumed if the given configuration 
        contains ``cmd`` property.  Otherwise, if the configuration contains the ``conventions`` property, 
        the midas-dbio schema will be assumed.  If neither appears, the input configuration will be 
        returned unchanged.  

        :param dict config:  the configuration to extract the specific command configuration from
        :param str cmdname:  the name of the command to look for
        :param module  cmd:  the module where command's implementation is defined.  If provided and 
                             ``cmdname`` is not found within the ``cmd`` property, the value of 
                             ``default_name`` from the module will be looked for instead.  
        :param str convention:  the name of the DAP processing convention (e.g. ``mds3``) to assume.
                             This is used to pull out the configuration appropriate for that 
                             convention when the configuration is using the midas-dbio schema (i.e.
                             it includes a ``conventions`` property).  If not provided, then convention
                             specified by the ``default_convention`` configuration property will be 
                             assumed.  
        :raises ConfigurationException: if ``convention`` is not specified and a default one cannot be 
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
        if not convention and os.environ.get('OAR_DAP_CONVENTION'):
            convention = os.environ['OAR_DAP_CONVENTION']
        if not convention:
            convention = config.get('default_convention')
        if not convention:
            convs = list(config['conventions'].keys())
            if len(convs) == 1:
                convention = convs[0]
        if not convention:
            raise ConfigurationException("Missing required parameter: services.dap.default_convention")
        if not config['conventions'].get(convention):
            raise ConfigurationException("Missing sub-configuration: " +
                                         f"services.dap.conventions.{convention}")

        out = cfgmod.merge_config(config['conventions'][convention], out)
        out['convention'] = convention
        return out

    def execute(self, args, config: Mapping=None, log: logging.Logger=None):
        """
        execute a dap subcommand from this command suite
        :param argparse.Namespace args:  the parsed arguments
        :param dict             config:  the configuration to use
        :param Logger              log:  the log to send messages to 
        """
        if args.conv:
            self._useconv = args.conv
        super().execute(args, config, log)

def load_into(subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    """
    from . import regpub, setstate, review, revreq, get

    subparser.description = description
    p = subparser
    p.add_argument("-C", "--convention", "--conv", metavar="NAME", type=str, dest='conv',
                   help="a label identifying the DAP processing convention to assume when "+
                        "executing a dap command")

    if not as_cmd:
        as_cmd = default_name
    out = DAPCmd(as_cmd, subparser)
    out.load_subcommand(regpub)
    out.load_subcommand(get)
    out.load_subcommand(setstate)
    out.load_subcommand(revreq)
    out.load_subcommand(review)

    return out

def create_DAPService(who: Agent, args, config: Mapping, log: Logger):
    """
    create a DAPService instance attached to a given user identity 
    :raises ConfigurationException:  if the given configuration is insufficient or erroneous
    """
    # determine which DAPService implementation we're using
    convention = config.get("convention", "mds3")
    modname = f"nistoar.midas.dap.service.{convention}" if '.' not in convention else convention
    mod = import_module(modname)
    if not hasattr(mod, 'DAPServiceFactory'):
        raise ConfigurationException("No DAPServiceFactory found in {convention} module")

    dbiocfg = config.get("dbio", {})
    dbtype = dbiocfg.get("factory")
    if not dbtype:
        raise ConfigurationException("required dbio.factory param missing")

    elif dbtype == "fsbased":
        wdir = args.workdir
        if not wdir:
            wdir = config.get("working_dir", ".")
        dbdir = dbiocfg.get('db_root_dir')
        if not dbdir:
            # use a default under the working directory
            dbdir = os.path.join(wdir, "dbfiles")
        elif not os.path.isabs(dbdir):
            # if relative, make it relative to the work directory
            dbdir = os.path.join(wdir, dbdir)
            if not os.path.exists(wdir):
                raise ConfigurationException(f"{wdir}: working directory does not exist")
            if not os.path.exists(dbdir):
                os.makedirs(dbdir)
        if not os.path.exists(dbdir):
            os.mkdir(dbdir)
        factory = FSBasedDBClientFactory(dbiocfg, dbdir)

    elif dbtype == "mongo":
        dburl = os.environ.get("OAR_MONGODB_URL")
        if not dburl:
            dburl = dbiocfg.get("db_url")
        if not dburl:
            # Build the DB URL from its pieces with env vars taking precedence over the config
            port = ":%s" % os.environ.get("OAR_MONGODB_PORT", dbiocfg.get("port", "27017"))
            user = os.environ.get("OAR_MONGODB_USER", dbiocfg.get("user"))
            cred = ""
            if user:
                pasw = os.environ.get("OAR_MONGODB_PASS", dbiocfg.get("pw", user))
                cred = "%s:%s@" % (user, pasw)
            host = os.environ.get("OAR_MONGODB_HOST", dbiocfg.get("host", "localhost"))
            dburl = "mongodb://%s%s%s/midas" % (cred, host, port)

        factory = MongoDBClientFactory(config.get("dbio", {}), dburl)
    
    elif dbtype == "inmem":
        factory = InMemoryDBClientFactory(config.get("dbio", {}))

    else:
        raise ConfigurationException(f"unrecognized factory: {dbtype}")

    # nstore = InMemoryResourceStorage(logger=log.getChild("nerdstore"))
    nscfg = config.get('nerdstorage', {})
    if not nscfg.get("type"):
        # We're using an in-memory nerdstore instance because we don't want to actually save 
        # the nerdm data, but just get the summary
        nscfg['type'] = "inmem"
    nstore = NERDResourceStorageFactory().open_storage(nscfg, log.getChild("nerdstore"))

    dapfact = mod.DAPServiceFactory(factory, config, log, nstore)

    return dapfact.create_service_for(who)  # may raise exception


