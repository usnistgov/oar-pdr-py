"""
midasadm command-line program for executing MIDAS administrative tasks.  Generally, this suite of 
commands operates directly onto the MIDAS database rather than going through the REST API.  
"""
import logging, os, sys
from copy import deepcopy
import traceback as tb
from getpass import getuser
from collections.abc import Mapping

from nistoar.midas import MIDASException
from nistoar.midas.dap import cmd as dap
from nistoar.base import config as cfgmod
from nistoar.base.config import ConfigurationException
from nistoar.pdr.utils import cli
from nistoar.pdr.utils.prov import Agent
from nistoar.pdr import def_etc_dir

description = \
"""execute MIDAS administrative operations

The subcommands generally operate directly on the MIDAS database and related backend storage 
rather than going through the REST interface.  This means that this interface generally has more 
privileges and abilities than the REST interface.  
"""
epilog = None
default_prog_name = "midasadm"
default_conf_file = os.path.join(def_etc_dir, "midasadm_conf.yml")

def main(cmdname, args):
    """
    a function that executes the ``midasadm`` command-line tool.  
    """
    if not cmdname:
        cmdname = default_prog_name

    argparser = cli.define_prog_opts(cmdname, description, epilog)
    midas = cli.CLISuite(cmdname, default_conf_file, argparser)

    midas.load_subcommand(dap)
    # midas.load_subcommand(dmp)

    # execute the command
    # args = midas.parse_args(args)
    midas.execute(args)
    return args

def get_agent(args, config: Mapping):
    who = args.actor
    utype = Agent.USER
    if not who:
        who = getuser()
    if who in config.get("auto_users", []):
        utype = Agent.AUTO
    return Agent(args.vehicle, utype, who, Agent.ADMIN)

if __name__ == "__main__":
    args = None
    try:
        prog = os.path.splitext(os.path.basename(sys.argv[0]))[0]
        args = main(prog, sys.argv[1:])
        sys.exit(0)
    except cli.CommandFailure as ex:
        logging.getLogger(f"{prog} {ex.cmd}").critical(str(ex))
        sys.exit(ex.stat)
    except ConfigurationException as ex:
        logging.getLogger(f"{prog} {ex.cmd}").critical("Config error: "+str(ex))
        sys.exit(4)
    except Exception as ex:
        logging.getLogger(f"{prog} {ex.cmd}").exception(ex)
        sys.exit(200)


    
