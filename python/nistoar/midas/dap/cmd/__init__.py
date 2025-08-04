"""
package that provides implementations of commands that can part of a command-line tool and provides 
administrative operations on DAPs.  This package is incorporated into :py:mod:`nistoar.midas.cli.midasadm`. 

(See :py:mod:`nistoar.pdr.utils.cli` for information on the framework for building up tool suites like 
``midasadm``.)

This module defines a set of subcommands to a command called (by default) "dap".  These subcommands
include
  - ``regpub``:   register a previously published DAP into the DBIO
  - ``prepupd``:  prepare a previously published DAP for editing
  - ``setstate``: update the state of a DAP record
"""
import os, argparse
from collections.abc import Mapping
from logging import Logger
from importlib import import_module

from nistoar.pdr.utils import cli
from nistoar.pdr.utils.prov import Agent
from nistoar.midas.cli import get_agent
from nistoar.base.config import ConfigurationException
from nistoar.midas.dap.nerdstore import NERDResourceStorageFactory
from nistoar.midas.dbio import FSBasedDBClientFactory, MongoDBClientFactory, InMemoryDBClientFactory

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
    from . import regpub, setstate

    subparser.description = description

    if not as_cmd:
        as_cmd = default_name
    out = cli.CommandSuite(as_cmd, subparser)
    out.load_subcommand(regpub)
    out.load_subcommand(setstate)

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


