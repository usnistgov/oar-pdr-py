"""
pdr command-line program for executing PDR operations, primarily in an administrative capacity.  
"""
import os, sys, logging
import traceback as tb

from nistoar.pdr.utils import cli
from nistoar.pdr import def_etc_dir
from nistoar.pdr.exceptions import ConfigurationException
# import nistoar.pdr.publish.cmd as pub
# import nistoar.pdr.preserv.cmd as preserve
from nistoar.pdr.cli import md

description = "execute PDR operations"
epilog = None
default_prog_name = "pdr"
default_conf_file = os.path.join(def_etc_dir, "pdr-cli-config.yml")

def main(cmdname, args):
    """
    a function that executes the ``pdr`` command-line tool.  
    """
    if not cmdname:
        cmdname = default_prog_name

    # set up the commands
    argparser = cli.define_prog_opts(cmdname, description, epilog)
    pdr = cli.CLISuite(cmdname, default_conf_file, argparser)
#    pdr.load_subcommand(pub)
#    pdr.load_subcommand(preserve)
    pdr.load_subcommand(md)

    # execute the commands
    pdr.execute(args)
    return args

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


        


