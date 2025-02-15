"""
This module provides a basis for various validators and data structure checkers.  It is built around 
a model where a series of tests are applied to a data structure (either on disk or in memory),
where each test represent some desired characteristic.  The result of applying the test is either 
true/passed (i.e. the desired characteristic is present in the structure) or false/failed (the 
characteristic is absent or invalid).  Each test is categorized based on how severe not passing it 
should be considered, either "error", "warning", or "recommended".  Results from many tests can be 
collected together to provide an overall evaluation of the data structure.  

The :py:class:`Validator` class represents the abstract base for implementations that encapsulate set 
of tests that can be applied to a data structure.  (The :py:class:`ValidatorBase` is provided to make 
implementation easier by adding individual tests as methods whose names begin with ``test_``.)  All 
the tests in a Validator typically test compliance to a particular standard or prifile of a standard.  
The :py:class:`AggregatedValidator` allows several validators to be combined into an "uber" validator 
to test compliance with multiple profiles.  The :py:class:`Validator`'s 
:py:meth:`~nistoar.pdr.utils.validate.Validator.validate` method returns the results of all the tests 
in a :py:class:`ValidationResults` instance which allows access to the result of each test based on 
whether it passed or not and by category.  Each test result is accessible as a 
:py:class:`ValidatorIssue` instance, which encapsulates a variety of intormation about the test and 
the profile it corresponds to.  Finally, the :py:class:`ValidatorTest` class (the superclass to 
:py:class:`ValidatorIssue`) is an aid for defining a test within an implementation of 
:py:class:`ValidatorBase`.  

Implementations for different types of data structures are found elsewhere in the ``nistoar`` library.  
This includes:
   * :py:mod:`nistoar.pdr.preserve.bagit.validate` -- validators for compliance to various profiles of 
     the BagIt standard
   * :py:mod:`nistoar.midas.dap.review` -- validators for completeness and correctness of a DAP draft
     record.
"""
__all__ = [ "Validator", "ValidationResults", "ValidationTest", "ValidationIssue", "AggregatedValidator",
            "ERROR", "WARN", "REC", "ALL", "PROB", "ValidatorBase" ]

from abc import ABC, ABCMeta, abstractmethod, abstractproperty
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from typing import Union, List

REQ   = 1
ERROR = REQ  # synonym for REQ
WARN  = 2
REC   = 4
ALL   = 7
PROB  = 3
issuetypes = [ REQ, WARN, REC ]

class ValidationTest:
    """
    a class describing a test to be applied to the target data structure.  It does not 
    include the implementation of the test, nor does it include the result of its application
    (see :py:class:`ValidationIssue`).  Rather it contains identifiers and a statement 
    describing the test.
    """
    REQ   = issuetypes[0]
    ERROR = issuetypes[0]  # synonym for REQ
    WARN  = issuetypes[1]
    REC   = issuetypes[2]
    
    def __init__(self, profile: str, profver: str, idlabel: str='', issuetype=ERROR, spec: str=''):
        """
        initialize the test description
        """
        self._prof = profile
        self._pver = profver
        self._lab = idlabel
        self._spec = spec
        self.type = issuetype
    
    @property
    def profile(self):
        """
        The name of the particular data structure profile that this test is part of.
        The target data structure is expected to be compliant with the associated 
        data structure definition and possibly a more specific "profile" or convention
        how the data structure is used.  This name should encapsulate both the base 
        structure definition and the particular profile being tested for.
        """
        return self._prof
    @profile.setter
    def profile(self, name):
        self._prof = name

    @property
    def profile_version(self):
        """
        The version of the named profile that this issue references.  An empty string 
        indicates no particular version.
        """
        return self._pver
    @profile_version.setter
    def profile_version(self, version):
        self._pver = version

    @property
    def label(self):
        """
        A label that identifies the requirement or recommendation within the profile that
        this test covers.
        """
        return self._lab
    @label.setter
    def label(self, value):
        self._lab = value

    @property
    def type(self):
        """
        return the issue type, one of REQ, WARN, or REC
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
        the explanation of the requirement or recommendation that the test checks for
        """
        return self._spec
    @specification.setter
    def specification(self, text):
        self._spec = text

type_labels = { REQ: "requirement", WARN: "warning", REC: "recommendation" }
REQ_LAB   = type_labels[REQ]
ERROR_LAB = type_labels[REQ]
WARN_LAB  = type_labels[WARN]
REC_LAB   = type_labels[REC]

class ValidationIssue(ValidationTest):
    """
    an object capturing issues detected by a validator.  It contains attributes 
    describing the type of error, identity of the recommendation that was 
    violated, and a prose description of the violation.
    """
    
    def __init__(self, profile, profver, idlabel='', issuetype=ERROR, spec='', 
                 passed: bool=True, comments: Union[str, List[str], None]=None):
        super(ValidationIssue, self).__init__(profile, profver, idlabel, issuetype, spec)
        if comments and isinstance(comments, str):
            comments = [ comments ]

        self._passed = passed
        self._comm = []
        if comments:
            self._comm.extend([str(c) for c in comments])

    @classmethod
    def from_test(cls, test: ValidationTest, passed: bool=True,
                  comments: Union[str, List[str], None]=None):
        return cls(test.profile, test.profile_version, test.label, test.type,
                   test.specification, passed, comments)

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
        out = "{0}: {1} {2} {3}".format(status, self.profile, 
                                        self.profile_version, self.label)
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
            out += "\n  "
            out += "\n  ".join(comms)
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
        return (self.type, self.profile, self.profile_version, self.label, 
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
    REQ   = REQ
    ERROR = REQ   # synonym for REQ
    WARN  = WARN
    REC   = REC
    ALL   = ALL
    PROB  = PROB
    
    def __init__(self, targetname, want=ALL):
        """
        initialize an empty set of results for a particular target

        :param targetname str:   the name of the target being validated
        :param want       int:   the desired types of tests to collect.  This 
                                 controls the result of ok().
        """
        self.target = targetname
        self.want   = want

        self.results = {
            REQ: [],
            WARN:  [],
            REC:   []
        }

    def applied(self, issuetype=ALL):
        """
        return a list of the tests of the requested types that were applied to the target.
        :param int issuetype:  a bit-wise and-ing of the desired issue types (default: ALL)
        """
        out = []
        if REQ & issuetype:
            out += self.results[REQ]
        if WARN & issuetype:
            out += self.results[WARN]
        if REC & issuetype:
            out += self.results[REC]
        return out

    def count_applied(self, issuetype=ALL):
        """
        return the number of validation tests of requested types that were 
        applied to the named data structure.
        """
        return len(self.applied(issuetype))

    def failed(self, issuetype=ALL):
        """
        return the validation tests of the requested types which failed when
        applied to the named data structure.
        """
        return [issue for issue in self.applied(issuetype) if issue.failed()]
    
    def count_failed(self, issuetype=ALL):
        """
        return the number of validation tests of requested types which failed
        when applied to the named data structure.
        """
        return len(self.failed(issuetype))

    def passed(self, issuetype=ALL):
        """
        return the validation tests of the requested types which passed when
        applied to the named data structure.
        """
        return [issue for issue in self.applied(issuetype) if issue.passed()]
    
    def count_passed(self, issuetype=ALL):
        """
        return the number of validation tests of requested types which passed
        when applied to the named data structure.
        """
        return len(self.passed(issuetype))

    def ok(self):
        """
        return True if none of the validation tests of the types specified by 
        the constructor's want parameter failed.
        """
        return self.count_failed(self.want) == 0

    def _add_applied(self, test: ValidationTest, passed: bool, comments=None):
        """
        add an issue to this result.  The issue will be updated with its 
        type set to type and its status set to passed (True) or failed (False).

        :param ValidationTest    test:  the assay outcome to add to this result object
        :param bool            passed:  either True or False, indicating whether the assay 
                                        passed or failed
        :param str|list(str) comments:  one or more comments to add to the issue instance.
                                        The first (or only) comment string should provide a 
                                        general description of the condition that should exist;
                                        subsequent values in the list can provide more detailed
                                        (and perhaps target-specific) statements of what is wrong. 
        """
        issue = ValidationIssue.from_test(test, passed, comments)
        self.results[issue.type].append(issue)
        return issue

class Validator(ABC):
    """
    a class for validating a data structure
    """
    
    def __init__(self, config: Mapping=None):
        """
        Initialize the validator.  Implementations should document what configuration parameters 
        it expects.

        :param dict config:  the configuration data for this validator
        """
        if config is None:
            config = {}
        self.cfg = config

    @abstractmethod
    def validate(self, target, want: int=ALL, results: ValidationResults=None,
                 targetname: str=None, **kw):
        """
        run the embeded tests, collecting the results into a returned results object.  

        :param   target:  a representation of the data structure being validated.  Depending 
                             on the specific implementation, this can be a string name or an
                             object.  
        :param int want:  bit-wise and-ed codes indicating which types of 
                             test results are desired.  A validator may (but 
                             is not required to) use this value to skip 
                             execution of certain tests.
        :param ValidationResults results: a ValidationResults to add result information to; if 
                             provided, this instance will be the one returned by this method.
        :param str targetname:  A name to refer to the target data structure as in results.  
                             If not given, an implementation should attempt to discern this 
                             from the data structure itself.  
        :return ValidationResults:  the results of applying requested validation tests
        """
        if not targetname:
            targetname = self._target_name(target)
        return ValidationResults(targetname, want)

    def _target_name(self, target):
        """
        determine a default target name for the given target.  Implementations of this class can 
        override this method to extract the name from the target using knowledge about the expected 
        structure of the target.  
        """
        return str(target)

class AggregatedValidator(Validator):
    """
    a Validator class that combines several validators together
    """
    def __init__(self, *validators):
        super(AggregatedValidator, self).__init__()
        self._vals = list(validators)

    def validate(self, target, want=ALL, results=None, targetname: str=None, **kw):
        if not targetname:
            targetname = self._target_name(target)

        out = results
        if not out:
            out = ValidationResults(self._target_name(target), want, **kw)

        for v in self._vals:
            v.validate(target, want, out)
        return out

    def _target_name(self, target):
        if len(self._vals) == 0:
            return super(AggregatedValidator, self)._target_name(target)

        return self._vals[0]._target_name(target)


class ValidatorBase(Validator):
    """
    a base class for Validator implementations.  

    This validator will recognizes all methods that begin with "test_" as a
    test that can return a list of errors.  The method should accept the 
    data structure instance to be tested as its first argument.
    """
    profile = (None, None)
    
    def __init__(self, config):
        super(ValidatorBase, self).__init__(config)

    def the_test_methods(self):
        """
        returns an ordered list of the method names that should be executed
        as validation tests.  This implementation will look for 'include_tests'
        and 'skip_tests' in the configuration to see if a reduced list should
        be returned.  
        """
        tests = self.all_test_methods()

        if self.cfg:
            if "include_tests" in self.cfg:
                filter = set(self.cfg['include_tests'])
                tests = [t for t in tests if t in filter]
            elif "skip_tests" in self.cfg:
                filter = set(self.cfg['skip_tests'])
                tests = [t for t in tests if t not in filter]

        return tests

    def all_test_methods(self):
        """
        returns an ordered list of names of all the possible methods that 
        can be executed as validation tests.

        This default implementation returns all methods whose name begins 
        with "test_" in arbitrary order.  Subclasses should override this 
        method if a particular order is desired or some other mechanism is 
        needed to identify tests.  
        """
        return [name for name in dir(self) if name.startswith('test_')]

    def validate(self, target, want=ALL, results: ValidationResults=None,
                 targetname: str=None, **kw):
        if not targetname:
            targetname = self._target_name(target)

        out = results
        if not out:
            out = ValidationResults(targetname, want)

        for test in self.the_test_methods():
            try:
                getattr(self, test)(target, want, out, **kw) 
            except Exception as ex:
                out._add_applied( ValidationTest(self.profile[0], self.profile[1],
                                                 f"{test} execution failure", REQ), 
                                  False, f"test method, {test}, raised an exception: {str(ex)}" )
        return out

    def define_test(self, label, desc, type):
        """
        create a new ValidationTest instance that is part of this validator's
        profile.  
        :param str label:  the label that identifies the requirement or recommendation
                           to be tested.
        :param str  desc:  a human-targeted statement of the test.  This should be written 
                           as a statement of what should be true to pass the test.  
        """
        return ValidationTest(self.profile[0], self.profile[1], label, type, desc)

    def _req(self, label, desc):
        """
        define a test of type ``REQ``
        :param str label:  the label that identifies the requirement or recommendation
                           to be tested.
        :param str  desc:  a human-targeted statement of the test.  This should be written 
                           as a statement of what should be true to pass the test.  
        """
        return self.define_test(label, desc, REQ)

    def _err(self, label, desc):
        """
        define a test of type ``REQ`` (an alias for :py:meth:`_req`)
        """
        return self._req(label, desc)

    def _warn(self, label, desc):
        """
        define a test of type ``WARN``
        :param str label:  the label that identifies the requirement or recommendation
                           to be tested.
        :param str  desc:  a human-targeted statement of the test.  This should be written 
                           as a statement of what should be true to pass the test.  
        """
        return self.define_test(label, desc, WARN)

    def _rec(self, label, desc):
        """
        define a test of type ``REC`` (recommendation)
        :param str label:  the label that identifies the recommendation to be tested.
        :param str  desc:  a human-targeted statement of the test.  This should be written 
                           as a statement of what should be true to pass the test.  
        """
        return self.define_test(label, desc, REC)

