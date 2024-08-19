"""
A module for reviewing the completeness of a draft NERDm record.  

This module is built on the validation framework defined in :py:mod:`nistoar.pdr.utils.validate`,
and is intended to feed suggestions and prompts to a DAPTool user about next steps for preparing 
a DAP draft for submission.  To enable integration with the DAPTool front end, the following conventions
are implemented in :py:class:`~nistoar.pdr.utils.validate.ValidationIssue` instances:
  * the test ``label`` value ends with the pattern, ``#``_prop_, where _prop_ indicates the NERDm 
    property that requires some attention.  This allows the DAPTool to direct user to the corresponding
    widget for making the needed change.
  * the first value in the ``comments`` array is a user-oriented suggestion about what the user should
    do. (In contrast, the ``specification`` value is expected to be a more pendantic statement.)
"""
from typing import List

from nistoar.pdr.utils.validate import Validator, ValidationResults, ALL
from .nerdm import DAPNERDmValidator
from ..nerdstore import NERDResourceStorage

class DAPReviewer(Validator):
    """
    A Validator that conducts a review of a draft DAP for completeness and correctness.  
    """

    def __init__(self, dapvals: List[Validator]=[], nrdstor: NERDResourceStorage=None,
                 nerdmvals: List[Validator]=[]):
        """
        :param list(Validator)   dapvals:  a list of validators that take a DAP Project record as its 
                                           target
        :param NERDResourceStore nrdstor:  The NERDResourceStorage where the NERDm record corresponding
                                           to a requested DAP record can be retrieved
        :param list(Validator) nerdmvals:  a list of validators that take the DAP's NERDm document 
                                           as its target
        """
        self.store = nrdstor
        self.nrdvals = list(nerdmvals)
        self.dapvals = list(dapvals)

    def _target_name(self, prec):
        return prec.id

    def validate(self, prec, want=ALL, results: ValidationResults=None, targetname: str=None, **kw):
        if not targetname:
            targetname = self._target_name(prec)

        out = results
        if not out:
            out = ValidationResults(targetname, want, **kw)

        if self.store:
            nerd = self.store.open(prec.id).get_data()
            for val in self.nrdvals:
                val.validate(nerd, want, out)

        for val in self.dapvals:
            val.validate(prec, want, out)

        return out

    @classmethod
    def create_reviewer(cls, nrdstore: NERDResourceStorage=None):
        nrdvals = []
        if nrdstore:
            nrdvals = [ DAPNERDmValidator() ]

        dapvals = []

        return cls(dapvals, nrdstore, nrdvals)
        
