"""
CLI command that will register a previously published DAP into the DBIO.  The record will be set
to the "published" state.  
"""

import logging, argparse, os, re
from logging import Logger
from copy import deepcopy
from pathlib import Path
from collections.abc import Mapping
from datetime import datetime

import requests

from nistoar.base.config import ConfigurationException
from nistoar.midas import MIDASException
from nistoar.pdr.utils.cli import CommandFailure, explain
from nistoar.pdr.utils.io import read_nerd
from nistoar.pdr.utils.prov import Agent, Action
from nistoar.pdr.constants import ARK_PFX_PAT
from nistoar.midas.dbio import NotEditable, NotAuthorized, AlreadyExists, ACLs, status, PUBLIC_GROUP
from nistoar.nsd.service import PeopleService

from . import create_DAPService, get_agent

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
    p.cmd = as_cmd
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
    try:
        svc = create_DAPService(agent, args, config, log)
    except ConfigurationException as ex:
        raise CommandFailure(args.cmd, "Config error: "+str(ex), 6) from ex
    except Exception as ex:
        log.exception(ex)
        raise CommandFailure(args.cmd, "Unable to create DAP service: "+str(ex), 1) from ex

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

            rec = svc.dbcli.get_record_for(dbid, ACLs.WRITE)

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

    if args.pubread and PUBLIC_GROUP not in args.rp:
        args.rp.append(PUBLIC_GROUP)
    if args.rp or args.wp or args.ap:
        if args.rp:
            rec.acls.grant_perm_to(ACLs.READ, *args.rp)
        if args.wp:
            rec.acls.grant_perm_to(ACLs.WRITE, *args.wp)
        if args.ap:
            rec.acls.grant_perm_to(ACLs.ADMIN, *args.ap)

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

