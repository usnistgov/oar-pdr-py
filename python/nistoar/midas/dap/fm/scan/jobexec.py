"""
A module for executing a slow scan as part of a :py:mod:`nistoar.jobmgt` Job via its :py:func:`process` 
function.  

The following parameters will be looked for in the configuration provided to the :py:func:`process` 
function:

``service``
     the configuration needed to reconstitute the 
     :py:class:`~nistoar.midas.dap.fm.service.MIDASFileManagerService`
``factory``
     the fully qualified (i.e. module name included) name of a factory function for 
     creating the :py:class:`~nistoar.midas.dap.fm.scan.UserSpaceScanner` instance.
     This function must accept two arguments: an
     :py:class:`~nistoar.midas.dap.fm.service.FMSpace` instance and a scan ID.

"""
import os, logging, importlib
from collections.abc import Mapping
from typing import List
from logging import Logger

from ..service import FMSpace, MIDASFileManagerService
from ..exceptions import FileManagerException
from .base import FileManagerScanException, GOOD_SCAN_FILE
from nistoar.base.config import ConfigurationException
from nistoar.jobmgt import FatalError
from nistoar.pdr.utils import write_json

def make_space(spaceid: str, config: Mapping, log: Logger=None):
    """
    create an instance of the :py:class:`~nistoar.midas.dap.fm.service.FMSpace` to be scanned.

    :param str spaceid:  the ID for the space to be scanned
    :param dict config:  the configuration data used to reconsitute the 
                         :py:class:`~nistoar.midas.dap.fm.service.FMSpace` (see above)
    :param Logger  log:  the base Logger to use
    :raises FileManagerScanException:  if an error occurs while instantiating the space
    """
    if not log:
        log = logging.getLogger("jobmgt.slowscan").getChild(spaceid)
    try:
        service = MIDASFileManagerService(config.get('service', {}), log.getChild("service"))
        return FMSpace(spaceid, service, log)
    
    except ConfigurationException as ex:
        raise FileManagerScanException(f"Failed to create FMSpace for {spaceid} from config: {str(ex)}") from ex

    except Exception as ex:
        raise FileManagerScanException(f"Failure while creating FMSpace for {spaceid}: {str(ex)}") from ex


def make_scanner(space: FMSpace, scanid: str, config: Mapping, log: Logger=None):
    """
    create an instance of the :py:class:`~nistoar.midas.dap.fm.scan.UserSpaceScanner` for 
    executing within a :py:mod:`nistoar.jobmgt` Job.

    The following config parameters will be looked for:

    :param str   space:  the FMSpace instance for the space to be scanned
    :param str  scanid:  the ID of the scan request that this scanning originates from
    :param dict config:  the configuration data used to reconsitute the 
                         :py:class:`~nistoar.midas.dap.fm.scan.UserSpaceScanner` (see above)
    :param Logger  log:  the base Logger to use
    :raises FileManagerScanException:  if an error occurs while instantiating the scanner
    """
    if not log:
        log = logging.getLogger("jobmgt.slowscan").getChild(spaceid)
    try:
        factoryname = config.get('factory')
        if not factoryname:
            raise ConfigurationException("Missing required config param: factory")
        parts = factoryname.rsplit('.', 1)
        mod = importlib.import_module(parts[0])
        if len(parts) > 1:
            factory = getattr(mod, parts[-1])
        else:
            factory = mod
        if not callable(factory):
            raise ConfigurationException(f"factory value, {factoryname}, does not resolve to a callable")

        return factory(space, scanid)

    except ConfigurationException as ex:
        raise FileManagerScanException("Failed to create scanner from config: "+str(ex)) from ex

    except ImportError as ex:
        raise FileManagerScanException("Unable to import scanner module: "+str(ex)) from ex

    except FileManagerScanException:
        raise

    except Exception as ex:
        raise FileManagerScanException("Failure while creating scanner: "+str(ex)) from ex

def process(dataid: str, config: Mapping, args: List[str], log=None, _make_space=None):
    """
    slow-scan the identified space

    :param str  dataid:  the identifier of the space to scan
    :param dict config:  the configuration needed to reconstitute the space and scanner (see
                         :py:func:`make_scanner`)
    :param list   args:  the list of arguments passed to the Job executable; the first argument
                         must be the scan ID that this request originates from.
    :param Logger  log:  the base Logger to use
    """
    if not log:
        log = logging.getLogger("jobmgt.slowscan").getChild(dataid)

    if not args:
        raise FatalError("Scan ID missing from Job arguments", 2)
    scanid = args[0]

    scandir = config.get("scandir")
    if not scandir:
        raise ConfigurationException("Missing required parameter: scandir")
    if not os.path.isdir(scandir):
        raise ConfigurationException("scandir: does not exist as a directory: "+scandir)

    if not _make_space:
        _make_space = make_space

    try:
        space = _make_space(dataid, config, log)
        scanner = make_scanner(space, scanid, config, log)

        scanmd = space.get_scan(scanid)

    except Exception as ex:
        log.exception(ex)
        raise FatalError("Failure while setting up slow_scan: "+str(ex), 4) from ex

    try:
        scanner.slow_scan(scanmd)
    except FileManagerScanException as ex:
        log.exception(ex)
        raise FatalError(str(ex), 1) from ex
    except Exception as ex:
        log.exception(ex)
        raise FatalError("Unexpected slow_scan failure: "+str(ex), 1) from ex

    if scanmd.get('in_progress') is False and not scanmd.get('scan_root'):
        # cache a full, successful scan
        try:
            write_json(scanmd, space.root_dir / space.system_folder / GOOD_SCAN_FILE)
        except Exception as ex:
            log.warning("Problem caching final scan data: %s", str(ex))


