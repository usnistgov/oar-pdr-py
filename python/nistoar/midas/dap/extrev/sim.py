"""
An implementation of the ExternalReviewClient that talks to the NPS (version 1)
"""
from collections.abc import Mapping
from typing import List

from nistoar.base.config import ConfigurationException
from nistoar.midas.dbio.project import ProjectService
from . import *

class SimulatedExternalReviewClient(ExternalReviewClient):
    """
    an ExternalReviewClient implementation that simulates communications with an external
    review service.  This is consistent with both with MIDAS v3 and, if the "call_back" 
    config param is set, MIDAS v4 with an updated NPS.  
    """
    system_name = "simulated"

    def __init__(self, config, autoapprove: bool=False, projsvc: ProjectService = None):
        """
        initialize the client
        :param dict config:  the configuration operating this external review client
        :param bool autoaccept:  if True, immediate approve a project upon submission
        """
        super(SimulatedExternalReviewClient, self).__init__(config)
        self.projsvc = projsvc
        self.autoapp = autoapprove
        if config.get('as_system'):
            self.system_name = config['as_system']

        self.cbsvc = None
        cbcfg = self.cfg.get("call_back")
        if cbcfg:
            # setup call backs
            pass

        self.projs = {}

    def submit(self, id: str, submitter: str, version: str=None, **options):
        if id in self.projs and self.projs[id].get('phase','approved') != 'approved':
            raise ExternalReviewException(f"Already under review with the {self.system_name} review system")

        self.projs[id] = {
            "submitter": submitter,
            "options": options,
            "phase": "requested"
        }

        if self.autoapp:
            extra = {}
            if options.get('can_publish'):
                extra['publish'] = True
            self.approve(id, **extra)

    def approve(self, id, publish: bool=False):
        if id not in self.projs:
            self.projs[id] = { "submitter": "nobody" }
        self.projs[id]['phase'] = "approved"

        if self.projsvc:
            self.projsvc.approve(id, self.system_name, "sim:"+id, publish=publish)

        elif self.cbsvc:
            # approve via web endpoint
            raise NotImplemented()

    def update(self, id: str, phase: str, info_url: str=None, feedback: List[Mapping]=None, 
               request_changes: bool=False, fbreplace: bool=True, **extra_info):
        if id not in self.projs:
            self.projs[id] = { "submitter": "nobody", "options": {} }
        self.projs[id]['phase'] = phase
        if feedback:
            self.projs[id]['options']['feedback'] = feedback

        if self.projsvc:
            self.projsvc.apply_external_review(id, self.system_name, phase, "sim:"+id, info_url, 
                                               feedback, request_changes, fbreplace, **extra_info)

        elif self.cbsvc:
            # update via web endpoint
            raise NotImplemented()

    

