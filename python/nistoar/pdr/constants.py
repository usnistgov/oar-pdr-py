"""
some constants for the PDR
"""
import re
PDR_PUBLIC_SERVER_PROD = "data.nist.gov"
PDR_PUBLIC_SERVER_TEST = "testdata.nist.gov"
PDR_PUBLIC_SERVER = PDR_PUBLIC_SERVER_PROD
PDR_PUBLISH_SERVER_PROD = "datapub.nist.gov"
PDR_PUBLISH_SERVER_TEST = "datapubtest.nist.gov"
PDR_PUBLISH_SERVER = PDR_PUBLISH_SERVER_PROD

from ..id import NIST_ARK_NAAN
ARK_NAAN = NIST_ARK_NAAN

# pattern for recognizing ARK identifiers
ARK_PFX_PAT = r"ark:/(\d+)/"

# pattern for recognizing PDR ARK identifiers
ARK_ID_PAT = ARK_PFX_PAT + r"(\w[\w\-]*)(/([^\/\#\?]+))?([#\?](.*)?)?"

# the ARK ID extension used for ReleaseCollection resources
RELHIST_EXTENSION = "/pdr:v"

# the pattern for the ARK ID extension indicating a version of a resource
VERSION_EXTENSION_PAT = RELHIST_EXTENSION + "/(\d+(.\d+)*)"

def to_version_ext(version):
    """
    return a string that can be appended to a normal PDR resource to convert it to a 
    version-specific identifier.
    :param str version:  the version of the resource to be identified
    """
    return RELHIST_EXTENSION + '/' + version

