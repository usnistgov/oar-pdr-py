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
ARK_ID_PAT = ARK_PFX_PAT + r"(\w[\w\-]*)((/([^\/\#\?]+))*)([#\?](.*)?)?"
ARK_ID_NAAN_GRP = 1
ARK_ID_DS_GRP = 2
ARK_ID_PATH_GRP = 3
ARK_ID_PART_GRP = 6

# the ARK ID extension used for ReleaseCollection resources
RELHIST_EXTENSION = "/pdr:v"

# the pattern for the ARK ID extension indicating a version of a resource
VERSION_EXTENSION_PAT = RELHIST_EXTENSION + "/(\d+(.\d+)*)"

# the ARK ID extension used for file/directory-like components
FILECMP_EXTENSION = "/pdr:f"

# the ARK ID extension used for components that are links to other sites
LINKCMP_EXTENSION = "/pdr:see"

# the ARK ID extension used for components that is its own resource
# (making the enclosing resource an aggregation)
AGGCMP_EXTENSION = "/pdr:agg"

# the ARK ID extension use to retrieve resource-level-only metadata
RESONLY_EXTENSION = "/pdr:r"

def to_version_ext(version):
    """
    return a string that can be appended to a normal PDR resource to convert it to a 
    version-specific identifier.
    :param str version:  the version of the resource to be identified
    """
    return RELHIST_EXTENSION + '/' + version

