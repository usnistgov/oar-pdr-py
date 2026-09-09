"""
The generic WSGI imfrastructure accessing the MIDAS DBIO layer.

This module contributes to the MIDAS WSGI implementation in two ways.  First, it the provides base 
:ref:class:`~nistoar.pdr.publish.service.wsgi.SubApp` class, 
:ref:class:`~nistoar.midas.dbio.wsgi.project.MIDASProjectApp` which can be specialized (or used as 
is) to provide access to the different MIDAS _project_ types--namely, DMP and 
:py:module:`DAP <nistoar.midas.dap>`.  Second it provides the endpoint implementations for the 
non-project collections in the DBIO layer--namely, the groups endpoint (which tracks the user 
groups used for access control).
"""
from .project import MIDASProjectApp
from .group import MIDASGroupApp
from .base import ServiceApp, Handler, DBIOHandler, Agent

