"""
Exceptions common to all types of ingest
"""
from nistoar.pdr.exceptions import PDRException

class IngestException(PDRException):
    """
    a base class for exceptions occuring while attempting to ingest data into the PDR and other 
    systems related to releasing a publication.  
    """
    pass

class IngestFileNotStaged(IngestException):
    """
    an error indicating an attempt to submit a staged record that has not actually been staged, yet.

    .. seealso:: :py:class:`DOIMintingClient`
    """

    def __init__(self, recname: str, message: str=None):
        """
        create the exception

        :param str recname:  the name of the record that could not be submitted
        :param str message:  a custom explanation; a default is set based on the name if not provided
        """
        if not message:
            message = f"Unable to submit record {recname}: not staged, yet."
        super(IngestFileNotStaged, self).__init__(message)

