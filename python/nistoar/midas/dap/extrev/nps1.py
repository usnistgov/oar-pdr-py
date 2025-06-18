"""
An implementation of the ExternalReviewClient that talks to the NPS (version 1)
"""
from nistoar.base.config import ConfigurationException
from . import *

class NPSExternalReviewClient(ExternalReviewClient):
    """
    an ExternalReviewClient implementation that connects to the legacy NPS service
    (consistent with MIDAS v3).  
    """

    def __init__(self, config):
        """
        initialize the client
        """
        super(NPSExternalReviewClient, self).__init__(config)

        # get templates for the home page URL

    def submit(self, id: str, version: str=None, **options=None):
        pass

