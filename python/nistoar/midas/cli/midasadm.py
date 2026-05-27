"""
midasadm command-line program for executing MIDAS administrative tasks.  Generally, this suite of 
commands operates directly onto the MIDAS database rather than going through the REST API.  
"""
import logging, os, sys
from copy import deepcopy
import traceback as tb
from collections.abc import Mapping

from nistoar.midas import MIDASException
from nistoar.midas.dap import cmd as dap
from nistoar.base import config as cfgmod
from nistoar.base.config import ConfigurationException
from nistoar.pdr.utils import cli
from nistoar.pdr import def_etc_dir
from . import get_agent

description = \
"""execute MIDAS administrative operations

The subcommands generally operate directly on the MIDAS database and related backend storage 
rather than going through the REST interface.  This means that this interface generally has more 
privileges and abilities than the REST interface.  
"""
epilog = None
default_prog_name = "midasadm"
default_conf_file = os.path.join(def_etc_dir, "midasadm_conf.yml")

class MidasadmSuite(cli.CLISuite):
    """
    a :py:class:`~nistoar.pdr.utils.cli.CLISuite` specialized for the midasadm command

    This implementation allows a sub-command's configuration to be extracted from the midas web 
    service configuration to ensure to ensure a matching behavior of underlying functions.
    """
    config_svc_app_name = "midas-dbio"   # to get identical behavior to the web service version

    def extract_config_for_cmd(self, config, cmdname, cmd=None):
        """
        extract the subcommand-specific configuration from the configuration provided.  

        This specialization supports two schemas for the incoming configuration: the normal command 
        schema supported by the general :py:mod:`cli module <nistoar.pdr.utils.cli>` and the midas-dbio 
        web service configuration.  The latter ensures the configuration provide to select 
        subcommands--and, thus, their behavior--will match that of midas-dbio web service.  If the given
        ``config`` parameter dictionary contains a top-level ``cmd`` property, the standard cli module 
        command-based schema (where ``cmd`` holds a dictionary where keys are the sub-command names) is 
        assumed.  If the configuration contains a ``services`` property and the requested command 
        (``cmdname``) matches one of its sub-properties, the midas-dbio service configuration schemea 
        is assumed.  If neither property appears, the input configuration will be returned unchanged.  

        :param dict config:  the configuration to extract the specific command configuration from
        :param str cmdname:  the name of the command to look for
        :param module  cmd:  the module where command's implementation is defined.  
        """
        if 'cmd' in config:
            # interpret configuration by the standard cli convention
            return super().extract_config_for_cmd(config, cmdname, cmd)

        if config.get('services', {}).get(cmdname):
            out = deepcopy(config)
            del out['services']
            cfg = config['services'][cmdname]
            cfgmod.merge_config(cfg, out)
            return out

        return config
            

def main(cmdname, args):
    """
    a function that executes the ``midasadm`` command-line tool.  
    """
    if not cmdname:
        cmdname = default_prog_name

    argparser = cli.define_prog_opts(cmdname, description, epilog)
    midas = MidasadmSuite(cmdname, default_conf_file, argparser)

    midas.load_subcommand(dap)
    # midas.load_subcommand(dmp)

    # execute the command
    # args = midas.parse_args(args)
    midas.execute(args)
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
        logging.getLogger(f"{prog}").exception(ex)
        sys.exit(200)


    
