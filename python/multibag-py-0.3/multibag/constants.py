"""
Common data about the multibag profile.
"""
import sys

CURRENT_VERSION = "0.4"
CURRENT_REFERENCE = "https://github.com/usnistgov/multibag-py/blob/master/docs/multibag-profile-spec.md"

DEFAULT_TAG_DIR = "multibag"

if sys.version_info[0] > 2:
    _unicode = str
else:
    _unicode = unicode

def _2int(sint):
    try:
        return int(sint)
    except ValueError:
        return -1

class Version(object):
    """
    a version class that can facilitate comparisons
    """

    def __init__(self, vers):
        """
        convert a version string to a Version instance
        """
        if isinstance(vers, (str, _unicode)):
            self._vs = vers
            self.fields = [_2int(v) for v  in self._vs.split('.')]
        elif isinstance(vers, tuple):
            self._vs = ".".join([str(v) for v in vers])
            self.fields = tuple(vers)
        else:
            raise TypeError("Input version is not str or tuple: " + str(vers))

    def __str__(self):
        return self._vs

    def __eq__(self, other):
        if not isinstance(other, Version):
            other = Version(other)
        return self.fields == other.fields

    def __lt__(self, other):
        if not isinstance(other, Version):
            other = Version(other)
        return self.fields < other.fields

    def __le__(self, other):
        if not isinstance(other, Version):
            other = Version(other)
        return self < other or self == other

    def __ge__(self, other):
        return not (self < other)
    def __gt__(self, other):
        return not self.__le__(other)
    def __ne__(self, other):
        return not (self == other)

