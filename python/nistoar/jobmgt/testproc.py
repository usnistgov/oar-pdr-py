"""
A module availabls in the nistoar package that can serve as a test module executable in the 
:py:mod:`nistoar.jobmgt` framework.  
"""
import logging

def def_process(id, config, args, log=None):
    if not log:
        log = logging.getLogger("goober")
    log.info("fake processing started")

process = def_process

