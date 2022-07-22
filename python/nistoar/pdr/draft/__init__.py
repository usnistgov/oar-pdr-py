"""
Provide services and tools for drafting a submission to a PDR Publishing Service.

A PDR Drafting Service differs from a Publishing Service (from the :py:mod:`~nistoar.pdr.publish`
package) in that the former allows for more user-driven interactivity to assemble and re-arrange a 
submission information package (SIP).  It can interact with a file management service where data 
files are assembled, and its workflow includes processing the submission via a review service.  
Because of its more complicated workflow and interactivity, it manages more state about the 
submission.  In summary, a Drafting Service is intended to be used by a user-driven (GUI) client, 
while the client of the publishing service is expected to be an automated system.  When a user 
has completed assembling their submission and the submission has completed the review process, the 
Drafting Service will submit the SIP to a Publishing Service.  

There can be different flavors of the Drafting service supported in this module to support
different interaction models or conventions or evolutions of the interface (i.e. inteface 
versions).  The default flavor is targeted for the MIDAS 3 client.  The different flavors are 
implemented within the :py:mod`service` subpackage.  

This package draws on some of the infrastructure the :py:mod:`~nistoar.pdr.publish` package, 
including :py:mod:`provenence tracking<nistoar.pdr.publish.prov>` and 
:py:mod:`README generation<nistoar.pdr.publish.readme>`.
"""
from ..publish import prov, readme

# subpackages: 
#   nerdstore
#   service
#   filemgr
#   review


