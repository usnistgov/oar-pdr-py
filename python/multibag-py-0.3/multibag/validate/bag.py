"""
This module provides the validator implementation for the base BagIt 
specification.  It delegates this to the LOC bagit module.  
"""
from .base import (Validator, ValidationResults, ValidationIssue,
                   ALL, ERROR, WARN, REC, PROB)
from ..access.bagit import BagValidationError, BagError, open_bag

class BagValidator(Validator):
    """
    A validator that tests whether a given Bag (serialized or otherwise) complies
    with the base BagIt specification.
    """

    def __init__(self, bagpath):
        """
        initialize the validator for the bag with a given path.  

        :param str bagpath:  the target bag, either as a directory for an 
                             unserialized bag or a file for a serialized one
        """
        super(BagValidator, self).__init__(bagpath)
        self.bag = open_bag(bagpath)

    def validate(self, want=PROB, results=None):
        if not results:
            results = ValidationResults(str(self.bag), want)

        if (want & ERROR):
            issue = ValidationIssue("2-Bag", ERROR,
                                    "Bag must be compliant BagIt bag")
            passed = True
            comments = []
            try:
                self.bag.validate()
            except BagValidationError as ex:
                passed = False
                comments = [ex.message] + ex.details
            except BagError as ex:
                passed = False
                comments = [ex.message]

            results._err(issue, passed, comments)

        return results

