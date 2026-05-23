"""
a module for executing a preservation task as a a :py:mod:`nistoar.jobmgt` Job via its :py:func:`process` 
function.  
"""
import os, logging
from typing import List, Mapping
from logging import Logger

from nistoar.jobmgt import FatalError
from . import PreservationException, PreservationStateError
from .task.framework import PreservationStateManager, PreservationTask
from .task.state import JSONPreservationStateManager
from .task.nist.pdr import PDRPreservationTaskFactory

def make_preservation_task(statefile: str, config: Mapping=None, log: Logger=None) -> PreservationTask:
    """
    initialize the PreservationTask that will drive preservation
    """
    statemgr = JSONPreservationStateManager.from_file(statefile)
    taskfact = PDRPreservationTaskFactory(config.get('task'))
    return taskfact.create_task(statemgr, log)

def process(dataid: str, config: Mapping, args: List[str], log=None):
    """
    preserve the AIP with the given identifier.

    :param str  dataid:  the identifier of the space to scan
    :param dict config:  the configuration needed to reconstitute the space and scanner (see
                         :py:func:`make_scanner`)
    :param list   args:  the list of arguments passed to the Job executable; the first argument
                         is the AIP ID that should be processed, and the second is the directory 
                         where the input SIP is located.  
    :param Logger  log:  the base Logger to use
    """
    if not log:
        log = logging.getLogger("jobmgt.preserve").getChild(dataid)

    if not args:
        raise FatalError("Preservation state file missing from Job arguments", 2)
    pstatefile = args[0]

    try:
        ptask = make_preservation_task(pstatefile, config, log)
        if ptask.completed:
            log.info("All preservation steps have already been completed; cleaning up...")
        elif ptask.started:
            log.info("Resuming previously started preservation")
            
        ptask.run()

    except PreservationException as ex:
        log.failure("Preservation task endeed with error: "+str(ex))
        raise FatalError(str(ex), 1) from ex

    except Exception as ex:
        log.exception(ex)
        raise FatalError("Unexpected preservation exception: "+str(ex))

    else:
        log.debug("cleaning up job")

        headbag = os.path.basename(ptask._statemgr.get_state_property('headbag', '(unknown)'))
        version = ptask._statemgr.get_state_property('version', '(unknown)')
            
        return {
            'aipid': ptask.aipid,
            'version': version,
            'headbag': headbag
        }

def notify(jobstatefile, config, log):
    cfg = config.get('callback')
    if not cfg:
        return
    
    ep = cfg.get('service_endpoint')
    if not ep:
        log.error("Unable to call back to preservation service: no service_endpoint specified")
        return
    
    job = Job.from_file(jobstatefile)
    log.debug("Contacting preservation service: NOT FULLY IMPLEMENTED")


