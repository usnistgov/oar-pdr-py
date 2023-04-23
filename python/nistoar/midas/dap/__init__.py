"""
DAP -- a module implementing the Digital Asset Publication (DAP) Authoring Service.

A Digital Asset Publication is a digital publication of data, software, or other digital asset 
that is made availalbe through the NIST Public Data Repository.  It is analagous (and often a 
companion) to a traditional publication in the academic literature.  At the core of this module
is an implementation of the DAP Authoring Service that allows authors to create a draft DAP 
(analagous to a paper manuscript) to be submitted to the PDR for publicaiton.  The service is 
made available primarily as a web service API, allowing for multiple different client tools to 
exist to serve different classes of customers.  

There can be different flavors of the Authoring service supported in this module to support
different interaction models or conventions or evolutions of the interface (i.e. interface 
versions).  The default flavor is targeted for the MIDAS 3 client.  The different flavors are 
implemented within the :py:mod:`service` subpackage.

This package draws on some of the infrastructure from the  :py:mod:`~nistoar.pdr.publish` package,
including :py:mod:`provenence tracking<nistoar.pdr.publish.prov>` and 
:py:mod:`README generation<nistoar.pdr.publish.readme>`.
"""
from nistoar.pdr.publish import prov, readme

# subpackages: 
#   nerdstore
#   service
#   filemgr
#   review

