"""
This module provides the base validator classes for validating bags
"""
from nistoar.pdr.utils.validate import *
from nistoar.pdr.utils.validate import issuetypes

class BagValidatorBase(ValidatorBase):
    """
    A base validator class specifically for validating a bag's compliance with various BagIt 
    profiles.  See also :py:class:`~nistoar.pdr.utils.validate.ValidatorBase` from which this 
    class is derived.
    """
    def _target_name(self, target):
        if hasattr(target, 'name'):
            return target.name
        return super(BagValidatorBase, self)._target_name(target)

