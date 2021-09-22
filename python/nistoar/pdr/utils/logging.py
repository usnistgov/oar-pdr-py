"""
Utility logging functions
"""
import logging

utilslog = logging.getLogger("pdr.utils")
BLAB = logging.DEBUG - 1

def blab(log, msg, *args, **kwargs):
    """
    log a verbose message. This uses a log level, BLAB, that is lower than 
    DEBUG; in other words when a log's level is set to DEBUG, this message 
    will not be displayed.  This is intended for messages that would appear 
    voluminously if the level were set to BLAB. 

    :param Logger log:  the Logger object to record to
    :param str    msg:  the message to write
    :param args:        treat msg as a template and insert these values
    :param kwargs:      other arbitrary keywords to pass to log.log()
    """
    log.log(BLAB, msg, *args, **kwargs)

