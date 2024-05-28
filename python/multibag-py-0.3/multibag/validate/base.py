"""
This module provides base classes and infrastructure for multibag validation
"""
import sys
from collections import Sequence, OrderedDict

from ..constants import CURRENT_VERSION
from ..access.bagit import BagValidationError, BagError, open_bag

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode

ERROR = 1
WARN  = 2
REC   = 4
ALL   = 7
PROB  = 3
issuetypes = [ ERROR, WARN, REC ]

type_labels = { ERROR: "error", WARN: "warning", REC: "recommendation" }
ERROR_LAB = type_labels[ERROR]
WARN_LAB  = type_labels[WARN]
REC_LAB   = type_labels[REC]

class ValidationIssue(object):
    """
    an object capturing issues detected by a validator.  It contains attributes 
    describing the type of error, identity of the recommendation that was 
    violated, and a prose description of the violation.
    """
    ERROR = issuetypes[0]
    WARN  = issuetypes[1]
    REC   = issuetypes[2]
    
    def __init__(self, idlabel='', issuetype=ERROR, spec='', passed=True, 
                 comments=None, profver=CURRENT_VERSION):
        if comments:
            if isinstance(comments, (str, _unicode)):
                comments = [ comments ]
            elif isinstance(comments, Sequence) and \
                 not isinstance(comments, list):
                comments = list(comments)

        self._pver = profver
        self._lab = idlabel
        self._spec = spec
        self.type = issuetype
        self._passed = passed
        self._comm = []
        if comments:
            self._comm.extend([str(c) for c in comments])

    @property
    def profile_version(self):
        """
        The version of the named BagIt profile that this issue references
        """
        return self._pver
    @profile_version.setter
    def profile_version(self, version):
        self._pver = version

    @property
    def label(self):
        """
        A label that identifies the recommendation within the profile that 
        was tested.  
        """
        return self._lab
    @label.setter
    def label(self, value):
        self._lab = value

    @property
    def type(self):
        """
        return the issue type, one of ERROR, WARN, or REC
        """
        return self._type
    @type.setter
    def type(self, issuetype):
        if issuetype not in issuetypes:
            raise ValueError("ValidationIssue: not a recognized issue type: "+
                             issuetype)
        self._type = issuetype

    @property
    def specification(self):
        """
        the explanation of the requirement or recommendation that the test
        checks for
        """
        return self._spec
    @specification.setter
    def specification(self, text):
        self._spec = text

    def add_comment(self, text):
        """
        attach a comment to this issue.  The comment typically provides some 
        context-specific information about how a issue failed (e.g. by 
        specifying a line number)
        """
        self._comm.append(str(text))

    @property
    def comments(self):
        """
        return a tuple of strings giving comments about the issue that are
        context-specific to its application
        """
        return tuple(self._comm)

    def passed(self):
        """
        return True if this test is marked as having passed.
        """
        return self._passed

    def failed(self):
        """
        return True if this test is marked as having passed.
        """
        return not self.passed()

    @property
    def summary(self):
        """
        a one-line description of the issue that was tested.  
        """
        status = (self.passed() and "PASSED") or type_labels[self._type].upper()
        out = "{0}: multibag {1} {2}".format(status, self.profile_version, 
                                             self.label)
        if self.specification:
            out += ": {0}".format(self.specification)
        return out

    @property
    def description(self):
        """
        a potentially lengthier description of the issue that was tested.  
        It starts with the summary and follows with the attached comments 
        providing more details.  Each comment is delimited with a newline; 
        A newline is not added to the end of the last comment.
        """
        out = self.summary
        if self._comm:
            comms = self._comm
            if not isinstance(comms, (list, tuple)):
                comms = [comms]
            out += "\n   "
            out += "\n   ".join(comms)
        return out

    def __str__(self):
        out = self.summary
        if self._comm and self._comm[0]:
            out += " ({0})".format(self._comm[0])
        return out

    def to_tuple(self):
        """
        return a tuple containing the issue data
        """
        return (self.type, "multibag", self.profile_version, self.label, 
                self.specification, self._passed, self._comm)

    def to_json_obj(self):
        """
        return an OrderedDict that can be encoded into a JSON object node
        which contains the data in this ValidationIssue.
        """
        return OrderedDict([
            ("type", type_labels[self.type]),
            ("profile_name", self.profile),
            ("profile_version", self.profile_version),
            ("label", self.label),
            ("spec", self.message),
            ("comments", self.comments)
        ])

    @classmethod
    def from_tuple(cls, data):
        return ValidationIssue(data[1], data[2], data[3], data[0], 
                               data[4], data[5], data[6])

class ValidationResults(object):
    """
    a container for collecting results from validation tests
    """
    ERROR = ERROR
    WARN  = WARN
    REC   = REC
    ALL   = ALL
    PROB  = PROB
    
    def __init__(self, target, want=ALL, version=CURRENT_VERSION):
        """
        initialize an empty set of results for a particular bag

        :param str  target:   a name indicating the bag or bags that are the 
                              target of these results
        :param int    want:   the desired types of tests--one of ERROR, WARN, 
                              REC, ALL, or PROB--to collect.  
                              (ALL=ERROR+WARN+REC, PROB=ERROR+WARN) This 
                              controls the result of ok():  if the types 
                              indicated by this value all pass, then ok() 
                              returns True.
        :param str version:   the default version of the multibag profile to
                              assume is being tested.  (This sets the version
                              on ValidationIssue instances created by _issue().)
        """
        self.target = target
        self.want    = want
        self.defversion = version

        self.results = {
            ERROR: [],
            WARN:  [],
            REC:   []
        }

    def applied(self, issuetype=ALL):
        """
        return a list of the validation tests that were applied of the
        requested types:
        :param int issuetype:  an bit-wise and-ing of the desired issue types
                               (default: ALL)
        """
        out = []
        if ERROR & issuetype:
            out += self.results[ERROR]
        if WARN & issuetype:
            out += self.results[WARN]
        if REC & issuetype:
            out += self.results[REC]
        return out

    def count_applied(self, issuetype=ALL):
        """
        return the number of validation tests of requested types that were 
        applied to the named bag.
        """
        return len(self.applied(issuetype))

    def failed(self, issuetype=ALL):
        """
        return the validation tests of the requested types which failed when
        applied to the named bag.
        """
        return [issue for issue in self.applied(issuetype) if issue.failed()]
    
    def count_failed(self, issuetype=ALL):
        """
        return the number of validation tests of requested types which failed
        when applied to the named bag.
        """
        return len(self.failed(issuetype))

    def passed(self, issuetype=ALL):
        """
        return the validation tests of the requested types which passed when
        applied to the named bag.
        """
        return [issue for issue in self.applied(issuetype) if issue.passed()]
    
    def count_passed(self, issuetype=ALL):
        """
        return the number of validation tests of requested types which passed
        when applied to the named bag.
        """
        return len(self.passed(issuetype))

    def ok(self):
        """
        return True if none of the validation tests of the types specified by 
        the constructor's want parameter failed.
        """
        return self.count_failed(self.want) == 0

    def _add_issue(self, issue, type, passed, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to type and its status set to passed (True) or failed (False).

        :param ValidationIssue issue:  the issue to add
        :param int             type:   the issue type code (ERROR, WARN, 
                                         or REC)
        :param bool            passed: either True or False, indicating whether
                                         the issue test passed or failed
        :param comments:  one or more comments to add to the 
                                         issue instance.
        :type comments: str or list of str
        """
        issue.type = type
        issue._passed = bool(passed)

        if comments:
            if isinstance(comments, (str, _unicode)):
                comments = [ comments ]
            elif isinstance(comments, Sequence) and \
                 not isinstance(comments, list):
                comments = list(comments)
            for comm in comments:
                issue.add_comment(comm)
        
        issue = ValidationIssue(issue._lab, issue.type,issue._spec, issue._passed,
                                (issue.comments and list(issue.comments)) or None,
                                issue._pver)
        self.results[type].append(issue)

    def _err(self, issue, passed, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to ERROR and its status set to passed (True) or failed (False).

        :param issue   ValidationIssue:  the issue to add
        :param passed  bool:             either True or False, indicating whether
                                         the issue test passed or failed
        :param comments str or list of str:  one or more comments to add to the 
                                         issue instance.
        """
        self._add_issue(issue, ERROR, passed, comments)

    def _warn(self, issue, passed, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to WARN and its status set to passed (True) or failed (False).
        """
        self._add_issue(issue, WARN, passed, comments)

    def _rec(self, issue, passed, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to REC and its status set to passed (True) or failed (False).
        """
        self._add_issue(issue, REC, passed, comments)

    def _issue(self, label, message):
        """
        return a new ValidationIssue instance that is part of this validator's
        profile.  The issue type will be set to ERROR and its status, to passed.
        """
        return ValidationIssue(label, ERROR, message, True,
                               profver=self.defversion)
        

class MultibagValidationError(BagValidationError):
    """
    An exception indicating that the target bag(s) are not compliant with 
    the Multibag Bagit Profile in one or more ways.  

    This class differs from the bagit.BagValidationError in that it carries 
    along all of the result details as a ValidationResults instance ("results").
    """
    def __init__(self, results):
        self.results = results

        details = []
        if results.count_failed() == 0:
            # shouldn't happen
            msg = "Unknown Multibag validation failure"
        elif results.count_failed() == 1:
            msg = results.failed()[0].summary
            details = list(results.failed()[0].comments)
        else:
            msg = "{0} validation errors detected".format(results.count_failed())
            details = [i.description for i in results.failed()]

        super(MultibagValidationError, self).__init__(msg, details)

    def __str__(self):
        if not self.results or self.results.count_failed() < 2:
            return super(MultibagValidationError, self).__str__()

        out = self.message
        if self.results.count_failed() > 3:
            out += ", including"
        out += ":"
        failed = self.results.failed()[0:3]
        for f in failed:
            out += "\n\n * "+f.description
        return out

class Validator(object):
    """
    a base class for a class that will apply validation tests to some 
    targets set at construction.

    This base implementation runs no tests; validate() by default simple returns 
    an empty ValidationResults object.  Subclasses should override validate() to 
    run its tests and enter the results into a returned ValidationResults object.
    """

    def __init__(self, target):
        """
        initialize the validator

        :param str target:  a name indicating the target bag or bags being 
                            validated.
        """
        self.target = target

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
        return out

    def is_valid(self, want=PROB):
        """
        run the embedded tests and return True if all tests selected want
        pass.  Return False otherwise.

        :param want    int:  bit-wise and-ed codes indicating which types of 
                             test results are desired.  A validator may (but 
                             is not required to) use this value to skip 
                             execution of certain tests.
        """
        results = self.validate(want)
        return results.ok()

    def ensure_valid(self, want=PROB):
        """
        run the (requested) embedded tests; if any of the requested tests fail,
        raise a BagValidationError.

        :param want    int:  bit-wise and-ed codes indicating which types of 
                             test results are desired.  A validator may (but 
                             is not required to) use this value to skip 
                             execution of certain tests.

        :raise MultibagValidationError:  if any of the requested tests fail.
        """
        results = self.validate(want)
        if not results.ok():
            raise MultibagValidationError(results)


