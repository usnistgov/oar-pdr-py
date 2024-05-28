"""
exceptions that can be raised while accessing or updating a bag's contents
"""
from __future__ import absolute_import

from .bagit import BagError

class MultibagError(BagError):
    """
    a general exception while working with a multibag aggregation
    """
    pass

class MissingMultibagFileError(MultibagError):
    """
    an exception indicating that a file expected/required by the multibag
    profile specification is missing.
    """
    def __init__(self, filepath, message=None):
        """
        initialize the exception with the name of the missing file
        :param str filepath:   the path to the missing file, relative to the 
                               bag's root directory.
        :param str message:    the exceptions message, overriding the default
                               (generated from the filename)
        """
        self.file = filepath
        if not message:
            message = "Missing multibag file: " + self.file
        super(MissingMultibagFileError, self).__init__(message)

