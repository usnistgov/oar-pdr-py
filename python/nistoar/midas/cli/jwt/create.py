"""
CLI command for creating a JWT token.  

This command is used to create long-lived tokens for a functional identity representing an
automated client that will access a MIDAS API.  
"""
import logging, argparse, sys
from typing import Mapping
from logging import Logger

from nistoar.web.auth.token import JWTGenerator
from nistoar.pdr.utils.cli import CommandFailure

default_name = "create"
help = "create an authentication token"
description = """
  Create a JWT authentication token for use with a MIDAS API.  Information provided via the 
  arguments and options are encoded into the token's claimset.  Unless -o is used, the created 
  token will be printed to standard output.  
"""

def load_into(subparser, current_dests, as_cmd=None):
    """
    load this command into a CLI by defining the command's arguments and options.

    :param set current_dests:  the current set of destination names that have been defined so far; this
                               can indicate if a parent command has defined required options already
    :param str as_cmd:  the command name that this command is being loaded as (ignored)
    :rtype: None    
    """
    p = subparser
    p.description = description
    p.add_argument("subj", metavar="APPUID", type=str,
                   help="the user/actor identifier to record as the subject into the token")
    p.add_argument("agclass", metavar="CLASS", type=str,
                   help="the agent class that defines what permissions should be granted to it.  "
                        "(This is often set to the name of the client system that will make requests.)")
    p.add_argument("-a", "--agent", metavar="NAME", action="append", dest="agents", default=[],
                   help="add an agent or list of agents that client will be acting on behalf of; "
                        "use multiple times or provide a comma-delimited list to add multiple agents.")
    p.add_argument("-u", "--as-user-type", action="store_const", dest="acttype",
                   default='auto', const="user",
                   help="set the agent actor type to 'user' instead of the default 'auto'; 'user' "
                        "indicates that the token represents a real human user, while the default, "
                        "'auto', indicates that the client is an automated process")
    p.add_argument("-p", "--add-prop", metavar="PROP=VAL", action="append", dest="props", default=[],
                   help="include an arbitrary property PROP with the value VAL into the token claimset.")
    p.add_argument("-L", "--lifetime", metavar="TIMEINTV", type=str, dest="lifetime", default="2y",
                   help="the lifetime of the token consisting of an integer followed immediately with "
                        "an optional interval unit (one of 's', 'm', 'h', 'd', or 'y', for seconds, "
                        "minutes, hours, days, and years, respectively, defaulting to 's' "
                        "if not provided)")
    p.add_argument("-o", "--output-file", metavar="FILE", type=str, dest="outfile",
                   help="write the token to FILE (instead of to standard output)")
    
    return None

def execute(args, config: Mapping=None, log: Logger=None):
    """
    execute this command: create a JWT token
    """
    if not log:
        log = logging.getLogger(default_name)
    if not config:
        config = {}
    if config.get('authentication'):
        config = config['authentication']
    elif config.get('auth'):
        config = config['auth']
    secret = config.get('secret') or config.get('key')
    if not secret:
        raise CommandFailure(args.cmd, "JWT secret not found in configuration", 8)

    if isinstance(args, list):
        # cmd-line arguments not parsed yet
        p = argparse.ArgumentParser()
        load_command(p)
        args = p.parse_args(args)

    if not args.subj:
        raise CommandFailure(args.cmd, "Subject/Actor ID not provided", 2)
    if not args.agclass:
        raise CommandFailure(args.cmd, "Agent class not provided", 2)
    if not args.acttype:
        args.acttype='auto'

    lifetime = args.lifetime
    if not lifetime:
        lifetime = "2y"
    fact = 1
    if lifetime[-1] in "smhdy":
        if lifetime[-1] == 'm':
            fact = 60
        elif lifetime[-1] == 'h':
            fact = 3600
        elif lifetime[-1] == 'd':
            fact = 3600*24
        elif lifetime[-1] == 'y':
            fact = 3600*24*365
        lifetime = lifetime[:-1]
    try:
        lifetime = int(float(lifetime) * fact)
    except ValueError as ex:
        raise CommandFailure(args.cmd, "Bad lifetime format: not a float with recognized unit: "+
                             args.lifetime, 2)

    data = _make_data(args)
    try:
        generator = JWTGenerator({'secret': secret})
        token = generator.generate(args.subj, data, lifetime)
    except Exception as ex:
        raise CommandFailure(args.cmd, "Token generation failed unexpectedly: "+str(ex), 1) from ex

    if args.outfile:
        try:
            with open(args.outfile, 'w') as fd:
                print(token, file=fd)
        except Exception as ex:
            raise CommandFailure(args.cmd, f"Unable to write to {args.outfile}: " + str(ex), 4) from ex
    else:
        print(token)

    # if verbose, print token contents to standard out.  

def _make_data(args):
    data = { "actortype": args.acttype, "client_id": args.agclass }
    if args.agents:
        for i in range(len(args.agents)):
            args.agents.extend(args.agents.pop(0).split(','))
        data['agents'] = " ".join(args.agents)
    
    if args.props:
        bad = [p for p in args.props if '=' not in p or not p.strip() or p.strip()[0] == '=']
        if bad:
            raise CommandFailure(args.cmd, "Bad property format" + ("s" if len(bad) > 1 else "") +
                                 ": not in KEY=VAL form: " + " ".join(bad), 2)
        for prop in args.props:
            k,v = prop.split('=', 1)
            if k in ["sub", "exp", "actortype", "client_id", "lifetime"]:
                continue  
            data[k] = v

    return data

