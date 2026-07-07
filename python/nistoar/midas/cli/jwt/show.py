"""
CLI command for displaying contents of a JWT token
"""
import logging, argparse, json, sys
from typing import Mapping
from logging import Logger
from datetime import datetime

import jwt

from nistoar.pdr.utils.cli import CommandFailure

default_name = "show"
help = "display contents of an authentication token"
description = """
  Decode a JWT authentication token and display its claimset contents.  
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

    p.add_argument("token", metavar="TOKEN", type=str, nargs='?', 
                   help="the token whose claimset should be displayed.  This must not be provided "
                        "if --token-file is provided.")
    p.add_argument("-f", "--token-file", metavar="FILE", type=str, dest="infile",
                   help="read the token from a file named FILE.  This file should contain only the "
                        "token and (optionally) leading and trailing space.  If FILE is given as '-', "
                        "the token will be read from standard input.")
    p.add_argument("-o", "--output-file", metavar="FILE", type=str, dest="outfile",
                   help="write the contents out to FILE")
    p.add_argument("-j", "--json-format", action="store_const", const="json", dest="fmt", default="text",
                   help="format the output as a JSON object")
    p.add_argument("-p", "--property", metavar="KEY", dest="select",
                   help="output only the property from the token's claimset with the name KEY")

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

    if args.fmt and args.fmt not in ["json", "text"]:
        raise CommandFailure(args.cmd, "Unsupported output format: "+args.fmt, 2)

    if args.infile:
        if args.token:
            raise CommandFailure(args.cmd, "Ambiguous input: TOKEN argument and -f/--token-file "
                                 "cannot both be provided", 2)
        infd = None
        if args.infile != "-" and not os.path.isfile(args.infile):
            raise CommandFailure(args.cmd, args.infile+": File does not exist as a file", 3)
        try:
            if args.infile == "-":
                infd = sys.stdin
            else:
                infd = open(args.infile)
            args.token = infd.read().strip()

        except Exception as ex:
            src = args.infile if args.infile != '-' else "standard input"
            raise CommandFailure(args.cmd, f"Failed to read token from {src}: {str(ex)}", 3)
        finally:
            if infd and args.infile != '-':
                infd.close()

    if not args.token:
        raise CommandFailure(args.cmd, "Missing token (check input or options)", 2)

    try:
        info = jwt.decode(args.token, config['secret'], "HS256")
    except Exception as ex:
        raise CommandFailure(args.cmd, "Failed to decode token: "+str(ex), 11) from ex

    if not args.outfile:
        args.outfile = '-'
    outfd = None
    try:
        if args.outfile == '-':
            outfd = sys.stdout
        else:
            outfd = open(args.outfile, 'w')

        if args.fmt == "json":
            json.dump(info, outfd, indent=2)
        else:
            write_claimset(info, outfd)

    except Exception as ex:
        raise CommandFailure(args.cmd, "Failed to write out claimset: "+str(ex), 4) from ex
    finally:
        if outfd and args.outfile != '-':
            outfd.close()

def write_claimset(data, fd):
    print("Subject:", str(data.get('sub')), file=fd)

    expire = "never"
    if data.get('exp'):
        try:
            expire = datetime.fromtimestamp(data['exp']).isoformat()
        except Exception as ex:
            expire = "(unparseable/illegal value)"
    print("Expiration:", expire, file=fd)

    linefmt = "  %s: %s"
    for prop in data:
        if prop in ["sub"]:
            continue
        print(linefmt % (prop, str(data[prop])), file=fd)

