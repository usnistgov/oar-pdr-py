"""
A module for executing a slow scan using simulated file manager API clients.  This is intended for use 
in unit tests only.  
"""
from logging import Logger
from typing import Union, List
from pathlib import Path
from collections.abc import Mapping

from . import base, jobexec
from .. import sim
from .jobexec import make_scanner
from nistoar.jobmgt import JobQueue

def make_space(spaceid: str, config: Mapping, log: Logger=None):
    out = jobexec.make_space(spaceid, config, log)
    try:
        out.svc = sim.SimMIDASFileManagerService(config.get('service', {}), log.getChild("service"))
    except ConfigurationException as ex:
        raise FileManagerScanException(f"Failed to create FMSpace for {spaceid} from config: {str(ex)}") from ex
        
    return out

def process(dataid: str, config: Mapping, args: List[str], log=None):
    return jobexec.process(dataid, config, args, log, make_space)

