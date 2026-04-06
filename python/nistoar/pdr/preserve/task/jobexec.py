"""
a module for executing a preservation task as a a :py:mod:`nistoar.jobmgt` Job via its :py:func:`process` 
function.  
"""
from typing import List, Mapping
from logging import Logger

from nistoar.jobmgt import FatalError
from .. import PreservationException, PreservationStateException
from .framework import PreservationStateManager, PreservationStateTask

def make_state_manager(aipid: str, aipdir: str,
                       config: Mapping=None, log: Logger=None) -> PreservationStateManager:
    """
    initialize a PreservationStateManager instance to be used during preservation.  

    Normally, the initialized state is already cached to disk when this is run, so creating 
    the instance would in this case start with reading that state from disk.  The decision 
    as to whether to resume or start over has already been set in that file.  
    """
    smcfg = config.get("state_manager", {})
    statefile = smcfg.get("persist_in")
    if not statefile:
        raise ConfigurationException("preservation job: state_manager.persist_in: "
                                     "required but not set")
    if not os.path.is_file(statefile):
        raise PreservationStateException(f"{statefile}: pre-initialized preservation state file "
                                         "not found as a file")
    return JSONPreservationStateManager(smcfg, aipid, aipdir, self.log) # need SIPStatus

def make_preservation_task(aipid: str, stmgr: PreservationStateManager,
                           config: Mapping=None, log: Logger=None) -> PreservationTask:
    """
    initialize the PreservationTask that will drive preservation
    """
    taskfact = PDRPreservationTaskFactory(config.get('task'))
    return taskfact.create_task(aipid, {}, log, statemgr=stmgr)

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
        raise FatalError("AIP-ID and AIP directory missing from Job arguments", 2)
    if len(args) < 2:
        raise FatalError("SIP directory missing from Job arguments", 2)

    aipid, aipdir = args[:2]

    sm = make_state_manager(aipid, aipdir, config, log)

    if sm.steps_completed == sm._all_steps:
        log.info("All preservation steps have already been completed; cleaning up...")
    else:
        if sm.steps_completed > sm.UNSTARTED:
            log.info("Resuming previously started preservation")
            
        try:
            ptask = make_preservation_task(aipid, sm, config, log)
            ptask.publish()

        except PreservationException as ex:
            log.exception(ex)
            raise FatalError(str(ex), 1) from ex

        except Exception as ex:
            log.exception(ex)
            raise FatalError("Unexpected preservation exception: "+str(ex))

    # clean up
    # move headbag to headbag cache
    # append pres-state content and job file content to preservation history file

        

    


