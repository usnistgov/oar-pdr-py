"""
Provide functionality for the Public Data Repository
"""
import os
from pathlib import Path
from abc import ABCMeta, abstractmethod, abstractproperty

from .constants import *
from ..base import SystemInfoMixin, config

try:
    from .version import __version__
except ImportError:
    __version__ = "(unset)"

def _get_platform_profile_name():
    """
    determine the name of the platform environment that the PDR system is 
    running within.  This name is used to retrieve configuration data 
    appropriate for the platform.  

    Currently, this name is passed in via the OAR_PLATFORM_PROFILE environment
    variable.
    """
    return os.environ.get('OAR_PLATFORM_PROFILE', 'unknown')

platform_profile = _get_platform_profile_name()

_PDRSYSNAME = "Public Data Repository"
_PDRSYSABBREV = "PDR"

class PDRSystem(SystemInfoMixin):
    """
    A SystemInfoMixin representing the overall PDR system.
    """
    def __init__(self, subsysname="", subsysabbrev=""):
        super(PDRSystem, self).__init__(_PDRSYSNAME, _PDRSYSABBREV, subsysname, subsysabbrev, __version__)

system = PDRSystem()
    
def find_jq_lib(config=None):
    """
    return the directory containing the jq libraries
    """
    from .exceptions import ConfigurationException
    
    def assert_exists(dir, ctxt=""):
        if not os.path.exists(dir):
            msg = "{0}directory does not exist: {1}".format(ctxt, dir)
            raise ConfigurationException(msg)

    # check local configuration
    if config and 'jq_lib' in config:
        assert_exists(config['jq_lib'], "config param 'jq_lib' ")
        return config['jq_lib']
            
    # check environment variable
    if 'OAR_JQ_LIB' in os.environ:
        assert_exists(os.environ['OAR_JQ_LIB'], "env var OAR_JQ_LIB ")
        return os.environ['OAR_JQ_LIB']

    # look relative to a base directory
    if 'OAR_HOME' in os.environ:
        # this is normally an installation directory (where lib/jq is our
        # directory) but we also allow it to be the source directory
        assert_exists(os.environ['OAR_HOME'], "env var OAR_HOME ")
        basedir = Path(os.environ['OAR_HOME'])
        candidates = [basedir / 'lib' / 'jq',
                      basedir / 'jq']
    else:
        # guess some locations based on the location of the executing code.
        # The code might be coming from an installation, build, or source
        # directory.
        import nistoar.jq
        basedir = Path(nistoar.jq.__file__).parents[3]
        candidates = [basedir / 'jq']
        basedir = basedir.parents[1]
        candidates.append(basedir / 'jq')
        candidates.append(basedir / 'metadata' / 'jq')
        
    for dir in candidates:
        if dir.exists():
            return str(dir)
        
    return None

def_jq_libdir = find_jq_lib()

def find_merge_etc(config=None):
    """
    return the directory containing the merge rules
    """
    from .exceptions import ConfigurationException
    
    def assert_exists(dir, ctxt=""):
        if not os.path.exists(dir):
            msg = "{0}directory does not exist: {1}".format(ctxt, dir)
            raise ConfigurationException(msg)

    # check local configuration
    if config and 'merge_rules_lib' in config:
        assert_exists(config['merge_rules_lib'],
                      "config param 'merge_rules_lib' ")
        return config['merge_rules_lib']
            
    # check environment variable
    if 'OAR_MERGE_ETC' in os.environ:
        assert_exists(os.environ['OAR_MERGE_ETC'], "env var OAR_MERGE_ETC ")
        return os.environ['OAR_MERGE_ETC']

    # look relative to a base directory
    if 'OAR_HOME' in os.environ:
        # this is normally an installation directory (where lib/jq is our
        # directory) but we also allow it to be the source directory
        assert_exists(os.environ['OAR_HOME'], "env var OAR_HOME ")
        basedir = Path(os.environ['OAR_HOME'])
        candidates = [basedir / 'etc' / 'merge']

    else:
        # guess some locations based on the location of the executing code.
        # The code might be coming from an installation, build, or source
        # directory.
        import nistoar.nerdm.merge
        basedir = Path(nistoar.nerdm.merge.__file__).parents[3]
        candidates = [basedir / 'etc' / 'merge']
        candidates.append(basedir / 'metadata' / 'etc' / 'merge')
        basedir = basedir.parents[1]
        candidates.append(basedir / 'etc' / 'merge')

    for dir in candidates:
        if dir.exists():
            return str(dir)
        
    return None

def_merge_etcdir = find_merge_etc()

def find_etc_dir(config=None):
    """
    return the path to the etc directory containing miscellaneous OAR files
    """
    from .exceptions import ConfigurationException
    
    def assert_exists(dir, ctxt=""):
        if not os.path.exists(dir):
            msg = "{0}directory does not exist: {1}".format(ctxt, dir)
            raise ConfigurationException(msg)

    # check local configuration
    if config and 'etc_lib' in config:
        assert_exists(config['etc_lib'],
                      "config param 'etc_lib' ")
        return config['etc_lib']

    # look relative to a base directory
    if 'OAR_HOME' in os.environ:
        # this is might be the install base or the source base directory;
        # either way, etc, is a subdirectory.
        assert_exists(os.environ['OAR_HOME'], "env var OAR_HOME ")
        basedir = Path(os.environ['OAR_HOME'])
        candidates = [basedir / 'etc']

    else:
        # guess some locations based on the location of the executing code.
        # The code might be coming from an installation, build, or source
        # directory.
        candidates = []

        # assume library has been installed; library is rooted at {root}/lib/python,
        basedir = Path(__file__).parents[4]

        # and the etc dir is {root}/etc
        candidates.append(basedir / 'etc')

        # assume library has been built within the source code directory at {root}/python/build/lib*
        basedir = Path(__file__).parents[5]

        # then the schema would be under {root}/etc
        candidates.append(basedir / 'etc')

        # assume library being used from its source code location
        basedir = Path(__file__).parents[3]

        # and is under {root}/metadata/model
        candidates.append(basedir / 'etc')

    for dir in candidates:
        if dir.exists():
            return str(dir)
        
    return None

def_etc_dir = find_etc_dir()

def find_schema_dir(config=None):
    """
    return the directory containing the NERDm schema files
    """
    from .exceptions import ConfigurationException
    
    def assert_exists(dir, ctxt=""):
        if not os.path.exists(dir):
            msg = "{0}directory does not exist: {1}".format(ctxt, dir)
            raise ConfigurationException(msg)

    # check local configuration
    if config and 'nerdm_schemas_dir' in config:
        assert_exists(config['nerdm_schemas_dir'],
                      "config param 'nerdm_schemas_dir' ")
        return config['nerdm_schemas_dir']
            
    # check environment variable
    if 'OAR_SCHEMA_DIR' in os.environ:
        assert_exists(os.environ['OAR_SCHEMA_DIR'],
                      "env var OAR_SCHEMA_DIR ")
        return os.environ['OAR_SCHEMA_DIR']

    # look relative to a base directory
    if 'OAR_HOME' in os.environ:
        # this is normally an installation directory (where etc/schemas is our
        # directory) but we also allow it to be the source directory
        assert_exists(os.environ['OAR_HOME'], "env var OAR_HOME ")
        basedir = Path(os.environ['OAR_HOME'])
        candidates = [basedir / 'etc' / 'schemas']

    else:
        # guess some locations based on the location of the executing code.
        # The code might be coming from an installation, build, or source
        # directory.
        import nistoar.nerdm
        candidates = []

        # assume library has been installed; library is rooted at {root}/lib/python,
        basedir = Path(nistoar.nerdm.__file__).parents[4]

        # and the schema dir is {root}/etc/schemas o
        candidates.append(basedir / 'etc' / 'schemas')

        # assume library has been built within the source code directory at {root}/python/build/lib*
        basedir = Path(nistoar.nerdm.__file__).parents[5]

        # then the schema would be under {root}/model
        candidates.append(basedir / 'model')

        # assume library being used from its source code location
        basedir = Path(nistoar.nerdm.__file__).parents[3]

        # and is under {root}/metadata/model
        candidates.append(basedir / 'model')

    for dir in candidates:
        if dir.exists():
            return str(dir)
        
    return None

def_schema_dir = find_schema_dir()

