"""
a module that defines various command-line top-level commands for the MIDAS system.
"""
from collections.abc import Mapping
from getpass import getuser

from nistoar.pdr.utils.prov import Agent

def get_agent(args, config: Mapping):
    """
    return an Agent appropriate for a CLI program/command.  The ``auto_users`` will 
    be consulted to mark the Agent as of type AUTO.  
    """
    who = args.actor
    utype = Agent.USER
    if not who:
        who = getuser()
    if who in config.get("auto_users", []):
        utype = Agent.AUTO
    return Agent(args.vehicle, utype, who, Agent.ADMIN)

