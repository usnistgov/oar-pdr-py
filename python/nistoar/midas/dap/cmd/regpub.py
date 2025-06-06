"""
CLI command that will register a previously published DAP into the DBIO.  The record will be set
in the published state.  
"""

import logging, argparse, os, re
from logging import Logger
from copy import deepcopy
from pathlib import Path
from collections.abc import Mapping
from getpass import getuser
from importlib import import_module
from datetime import datetime

import requests

from nistoar.base.config import ConfigurationException
from nistoar.midas import MIDASException
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.utils.io import read_nerd
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.pdr.constants import ARK_PFX_PAT
from nistoar.midas.dbio import (FSBasedDBClientFactory, MongoDBClientFactory, InMemoryDBClientFactory,
                                NotEditable, NotAuthorized, AlreadyExists, ACLs, status, PUBLIC_GROUP)
from nistoar.midas.dap.nerdstore import NERDResourceStorageFactory
from nistoar.nsd.service import PeopleService

ARK_PFX_RE = re.compile(ARK_PFX_PAT)

default_name = "regpub"
help = "register a previously published DAP into the DBIO"
description = \
"""create a DAP record based on a previously published DAP and load it into the DBIO.  
The resulting record will be set into the "published" state.  Use options to set attributes
of the record such as owner and permissions.


"""

def load_into(subparser: argparse.ArgumentParser, current_dests: list=None, as_cmd: str=None):
    """
    load this command into a CLI by defining the command's arguments and options.
    :param argparser.ArgumentParser subparser:  the argument parser instance to define this command's 
                                                interface into it 
    :param list current_dests:  a list of destination names for parameters that have already been 
                                defined
    :param str as_cmd:  the subcommand name assigned to the action provided by this module
    :rtype: None
    """
    p = subparser
    p.description = description
    p.add_argument("nerdref", metavar="AIPID-OR-FILE", type=str, 
                   help="either an AIP-ID for the dataset to load or the path to a file containing "+
                        "the NERDm Resource metadata for the dataset.  If an ID is provided, the "+
                        "NERDm metadata will be loaded from the PDR")
    p.add_argument("-A", "--aipid", action="store_true", dest="as_id",
                   help="force the interpretation of specified DAP as an AIP ID (rather than a file)")
    p.add_argument("-I", "--dbio-id", metavar="ID", type=str, dest="dbioid",
                   help="the ID to assign to the newly registered record within the DBIO (e.g. "+
                        "'mds2:2420')")
    p.add_argument("-n", "--dbio-name", metavar="NAME", type=str, dest="name",
                   help="the name to assign to the newly registered record within the DBIO")
    p.add_argument("-o", "--owner", metavar="USERID", type=str, dest="owner",
                   help="the user to assign as the owner of the record.  If not provided, the owner "
                        "will be discerned by the contact identity")
    p.add_argument("-P", "--publicly-readable", action="store_true", dest="pubread",
                   help="make the record publicly readable")
    p.add_argument("-r", "--readable-by", metavar="USER-OR-GRP", action="extend", nargs="*", type=str,
                   dest="rp", default=[],
                   help="make the record readable by the users or groups with the given IDs")
    p.add_argument("-w", "--writable-by", metavar="USER-OR-GRP", action="extend", nargs="*", type=str,
                   dest="wp", default=[],
                   help="make the record writeable by the users or groups with the given IDs")
    p.add_argument("-a", "--admin-by", metavar="USER-OR-GRP", action="extend", nargs="*", type=str,
                   dest="ap", default=[],
                   help="make the record adminsterable by the users or groups with the given IDs")
    p.add_argument("-R", "--resolver-url", metavar="URL", type=str, dest="resolver",
                   help="use the given base URL as the resolver for AIPIDs (ignored if file provided)")
    p.add_argument("-W", "--overwrite", action="store_true", dest="overwrite",
                   help="if the given record already exists in the DBIO, overwrite its contents")

    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: register a previously published DAP into the DBIO, marking it published
    """
    if not log:
        log = logging.getLogger(default_name)
    if not config:
        config = {}

    if isinstance(args, list):
        # cmd-line arguments not parsed yet
        p = argparse.ArgumentParser()
        load_command(p)
        args = p.parse_args(args)

    if not args.nerdref:
        raise CommandFailure(default_name, "NERDm record not specified", 2)

    nerd = None
    if not args.as_id:
        nerdf = Path(args.nerdref)
        if nerdf.is_file():
            try:
                nerd = read_nerd(nerdf)
            except Exception as ex:
                raise CommandFailure(args.cmd, f"{nerdf}: Unable to read: {str(ex)}", 3)
            if not isinstance(nerd, Mapping):
                raise CommandFailure(args.cmd, f"{nerdf}: Does not contain a JSON object", 3)

    if not nerd:
        nerd = resolve_id(args.nerdref, args, config, args.resolver)

    if "@type" not in nerd or "contactPoint" not in nerd:
        raise CommandFailure(args.cmd, f"Metadata does not appear to describe a NERDm Resource", 2)
    if "@id" not in nerd:
        raise CommandFailure(args.cmd, f"NERDm Resource is missing @id", 2)

    version = nerd.get('version', '1.0.X')
    log.info("loading %s version %s as published", nerd["@id"], version)

    # determine the DBIO ID
    dbid = args.dbioid
    if not dbid:
        try:
            dbid = ARK_PFX_RE.sub('', nerd['@id'])
            dbid = ':'.join(dbid.split('-', 1))
        except Exception as ex:
            raise CommandFailure(args.cmd, f"Illegal NERDm Resource @id: {str(nerd['@id'])}", 2)
    elif ':' not in dbid:
        raise CommandFailure(args.cmd, f"Not an acceptable DBIO-ID: {dbid} (Missing shoulder:)", 2)
    shoulder = None
    localid = None
    if dbid:
        shoulder, localid = dbid.split(':', 1)
    else:
        shoulder = config.get("default_shoulder")
        
    agent = get_agent(args, config)
    svc = create_DAPService(agent, args, config, log)

    rec = None
    if svc.exists(dbid):
        if not args.overwrite:
            raise CommandFailure(args.cmd, f"{dbid}: record exists; won't overwrite", 1)

        try:
            rec = svc.dbcli.get_record_for(dbid, ACLs.WRITE)
        except NotAuthorized as ex:
            raise CommandFailure(args.cmd, f"{dbid}: insufficient authorization to overwrite", 1)
        if rec.status.state != status.PUBLISHED:
            raise CommandFailure(args.cmd, f"{dbid}: record exists in non-published state; can't overwrite", 1)
        try: 
            rec.status.set_state(status.EDIT)
            rec.save()

            log.info("DAP %s: re-registering and overwriting data", dbid)
            svc.replace_data(dbid, nerd, message="re-registering DAP record")

            if args.name and args.name != rec.name:
                log.info("Updating name to %s", args.name)
                svc.rename_record(dbid, args.name)

            if args.owner and args.owner != rec.owner:
                log.info("Changing owner to %s", args.owner)
                svc.reassign_record(dbid, args.owner, True)

        except (NotEditable, NotAuthorized) as ex:
            raise CommandFailure(args.cmd, f"{dbid}: unexpected record state problem: {str(ex)}", 11)
        except Exception as ex:
            raise CommandFailure(args.cmd, f"{dbid}: unexpected failure: {str(ex)}", 11)

    else:
        # determine owner and name
        owner = args.owner
        if not owner and svc.dbcli.people_service:
            owner = owner_from_contact_point(nerd["contactPoint"], svc.dbcli.people_service, args, log)

        name = args.name
        if not name:
            name = dbid

        if svc.dbcli.name_exists(name, owner):
            raise CommandFailure(args.cmd, f"{name}: record name already exists for {owner}; use -n", 1)

        try:
            rec = svc.create_record(name, nerd, {"foruser": owner}, dbid)
        except AlreadyExists as ex:
            raise CommandFailure(args.cmd, f"{name}: {str(ex)}", 1)
        except NotAuthorized as ex:
            raise CommandFailure(args.cmd, f"{name}: {str(ex)}", 9)

        try:
            when = nerd.get('annotated') or nerd.get('revised') or \
                   nerd.get('first_issued') or nerd.get('issued') or 0
            if isinstance(when, str):
                when = datetime.fromisoformat(when).timestamp()
            rec.status.publish(nerd['@id'], version, None, when)
            rec.status.act("registered-published",
                           "Legacy DAP publication registered as published",
                           rec.status.since)
            rec.save()

            svc._record_action(Action(Action.COMMENT, rec.id, svc.who,
                                      "Registered as published legacy DAP"))
                                      
        except NotAuthorized as ex:
            raise CommandFailure(args.cmd, f"Unexpected authorization issue ({str(ex)})", 9)

    if args.pubread and PUBLIC_GROUP not in args.rp:
        args.rp.append(PUBLIC_GROUP)
    if args.rp or args.wp or args.ap:
        if args.rp:
            rec.acls.grant_perm_to(ACLs.READ, *args.rp)
        if args.wp:
            rec.acls.grant_perm_to(ACLs.WRITE, *args.wp)
        if args.ap:
            rec.acls.grant_perm_to(ACLs.ADMIN, *args.ap)
        rec.save()
        

def get_agent(args, config: Mapping):
    who = args.actor
    utype = Agent.USER
    if not who:
        who = getuser()
    if who in config.get("auto_users", []):
        utype = Agent.AUTO
    return Agent(args.cmd, utype, who, Agent.ADMIN)

def create_DAPService(who: Agent, args, config: Mapping, log: Logger):
    # determine which DAPService implementation we're using
    convention = config.get("convention", "mds3")
    modname = f"nistoar.midas.dap.service.{convention}" if '.' not in convention else convention
    mod = import_module(modname)
    if not hasattr(mod, 'DAPServiceFactory'):
        raise CommandFailure(args.cmd, f"Config error: No DAPServiceFactory found in {convention} module", 6)

    dbiocfg = config.get("dbio", {})
    dbtype = dbiocfg.get("factory")
    if not dbtype:
        raise CommandFailure(args.cmd, f"Config error: required dbio.factory param missing", 6)

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
                raise CommandFailure(args.cmd, f"Config error: {wdir}: working directory does not exist", 6)
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
        raise CommandFailure(args.cmd, f"Config error: unrecognized factory: {dbtype}", 6)

    # nstore = InMemoryResourceStorage(logger=log.getChild("nerdstore"))
    nscfg = config.get('nerdstorage', {})
    if not nscfg.get("type"):
        # We're using an in-memory nerdstore instance because we don't want to actually save 
        # the nerdm data, but just get the summary
        nscfg['type'] = "inmem"
    nstore = NERDResourceStorageFactory().open_storage(nscfg, log.getChild("nerdstore"))

    dapfact = mod.DAPServiceFactory(factory, config, log, nstore)
    try:
        return dapfact.create_service_for(who)
    except Exception as ex:
        excode = 1
        if isinstance(ex, ConfigurationException):
            excode = 6
        raise CommandFailure(args.cmd, "Unable to create DAPService: "+str(ex), excode)


def resolve_id(aipid: str, args, config: Mapping, resolverurl: str=None):

    if not resolverurl:
        resolverurl = config.get("resolver_url")
    if not resolverurl:
        raise CommandFailure(args.cmd, "Config error: missing needed parameter: resolver_url", 6)
    if '?' not in resolverurl and not resolverurl.endswith('/'):
        resolverurl += '/'

    out = None
    try:
        res = requests.get(resolverurl+aipid)
        out = res.json()
    except requests.HTTPError as ex:
        raise CommandFailure(args.cmd, "Trouble fetching NERDm metadata from service: "+str(ex), 5)
    except ValueError as ex:
        # json parsing error
        pass

    if res.status_code == 404:
        raise CommandFailure(args.cmd,
                             f"Trouble fetching published NERDm metadata: ID not found: {aipid}", 1)

    elif res.status_code < 200 or res.status_code > 299:
        err = out.get("message") if out and isinstance(out, Mapping) else None
        if not err:
            err = res.reason
        raise CommandFailure(args.cmd, f"Unexpected resolver response ({res.status_code}): {err}", 5)

    return out

def owner_from_contact_point(md, peopsvc: PeopleService, args, log):
    """
    with the help from the people service, convert the contact point metadata into a user ID
    """
    who = md.get('fn')
    email = re.sub(r'^mailto:', '', md.get('hasEmail', ''))
    if not who:
        who = email or md.get('orcid')
    out = None

    if 'orcid' in md:
        # lookup by ORCID
        out = peopsvc.get_person_by_orcid(md['orcid'])
        if out and out.get(peopsvc.EID_PROP):
            return out[peopsvc.EID_PROP]

    if email:
        # lookup by email
        out = peopsvc.get_person_by_email(email)
        if out and out.get(peopsvc.EID_PROP):
            return out[peopsvc.EID_PROP]

    if md.get('fn'):
        # attempt name match
        matches = peopsvc.select_best_person_matches(md['fn'])
        if len(matches) == 1:
            return matches[0][peopsvc.EID_PROP]

        message = f'Failed match "{md["fn"]}" to a single name'
        if len(matches) > 1:
            message += "; matches include...\n  " + \
              "\n  ".join([f"{m.get('lastName')}, {m.get('firstName')} ({m.get('username')})"
                           for m in matches[:4]])
            if len(matches) > 4:
                message += "\n  ..."
            log.warning(message)
        
    raise CommandFailure(args.cmd, "Unable to convert contact point to an owner ID; use -o", 2)

