"""
This module creates preservation bags (a.k.a. Archive Information Packages, AIPs) 
from Submission Information Packages (SIP) of a known organization.

The :py:class:`SIPBagger` base class provides the abstract interface for preparing
a bag.  The implementation classes use knowledge of particular SIPs to 
create bags (via the BagBuilder class).  For example, the MIDASBagger 
understands how to bag up data provided by MIDAS.  

The :py:module:`pdp` submodule forms the basis for the PDR's Programmatic Data 
Publishing (PDP) framework.  It is built on the assumption that bags are built 
up by processing submitted NERDm records.  

The :py:module:`prepupd` submodule provides helper classes that assist baggers 
with processing updates to previously published datasets by initializing a working 
bag based on the last previously published one.  
"""
from .base import SIPBagger, SIPBaggerFactory
from .pdp import PDPBagger
