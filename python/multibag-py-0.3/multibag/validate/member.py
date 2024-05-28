"""
This module provides the validator implementation for validating member bags.
"""
import re

from .base import (Validator, ValidationIssue, ValidationResults, 
                   ALL, ERROR, WARN, REC, PROB, CURRENT_VERSION)
from .bag import BagValidator
from ..access.bagit import BagValidationError, BagError, open_bag
import fs.osfs

class MemberBagValidator(Validator):
    """
    A validator that tests whether a given Bag (serialized or otherwise) complies
    with the Multibag requirements for serving as a member bag of a multibag 
    aggregation.
    """

    def __init__(self, bagpath):
        """
        initialize the validator for the bag with a given path.  

        :param str bagpath:  the target bag, either as a directory for an 
                             unserialized bag or a file for a serialized one
        """
        super(MemberBagValidator, self).__init__(bagpath)
        self.bagpath = bagpath
        self.bag = open_bag(bagpath)

    def validate(self, want=PROB, results=None):
        """
        run the embeded tests, returning a list of errors.  If the returned
        list is empty, then the bag is considered validated.  

        :param want    int:  bit-wise and-ed codes indicating which types of 
                             test results are desired.  A validator may (but 
                             is not required to) use this value to skip 
                             execution of certain tests.
        :param results ValidationResults: a ValidationResults to add result
                             information to; if provided, this instance will 
                             be the one returned by this method.
        :rtype: ValidationResults:  the results of applying requested validation
                             tests
        """
        out = results
        if not out:
            out = ValidationResults(self.target, want)

        # validate against the base BagIt spec
        BagValidator(self.bagpath).validate(want, out)

        version = self.bag.info.get("Multibag-Version")
        if version and isinstance(version, list):
            version = version[-1]

        t = out._issue("3-Version-for-member",
                     "A member multibag should include header: Multibag-Version")
        out._rec(t, bool(version))

        if not version:
            return out

        self.validate_bagname(want, results, version)
        self.validate_as_nonhead(want, results, version)

        return out

    def validate_bagname(self, want=ALL, results=None, version=CURRENT_VERSION):
        """
        ensure the bag complies with restrictions on bag names
        """
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        if version == "0.2":
            # version 0.2 had no restrictions on names
            return out

        name = self.bag._name
        t = out._issue("2.1a-name-TAB",
                       "A name must not contain embedded TAB characters")
        out._err(t, "\t" not in name)

        t = out._issue("2.1b-name-wsp",
                  "A name must not begin nor end with any whitespace characters")
        out._err(t, not re.search(r'^\s+', name) and not re.search(r'\s+$', name))

        return out

    def validate_as_nonhead(self, want=ALL, results=None, version=CURRENT_VERSION):
        out = results
        if not out:
            out = ValidationResults(str(self.bag), want)

        if self.bag.is_head_multibag():
            # these tests don't apply
            return out

        t = out._issue("2-Head-Deprecates",
                       "bag-info.txt: Multibag-Head-Deprecates element "+
                       "should only be set for Head Bags")
        out._warn(t, "Multibag-Head-Deprecates" not in self.bag.info)

        t = out._issue("2-Tag-Directory",
                       "bag-info.txt: Multibag-Tag-Directory element "+
                       "should only be set for Head Bags")
        out._warn(t, "Multibag-Tag-Directory" not in self.bag.info)

        mdir = self.bag.info.get("Multibag-Tag-Directory")
        if not mdir:
            mdir = "multibag"
        if not isinstance(mdir, list):
            mdir = [mdir]

        for d in mdir:
            t = out._issue("2-Tag-Directory",
                           "bag-info.txt: Multibag tag directory ("+
                           d + ") should exist only in Head Bags")
            out._warn(t, not self.bag.exists(d))

        return out
            
def validate(bagpath, want=PROB):
    """
    validate a bag (serialized or not) as a legal member bag of a multibag 
    aggregation.

    :param want int: bit-wise and-ed codes indicating which types of 
                     test results are desired.  A validator may (but 
                     is not required to) use this value to skip 
                     execution of certain tests.
    :raise MultibagValidationError if validation errors are detected
    """
    MemberBagValidator(bagpath).ensure_valid(want)

