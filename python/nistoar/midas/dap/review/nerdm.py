"""
NERDm tests for the minimum required elements
"""
from .base import ValidatorBase, ValidationResults, ALL

class DAPNERDmValidator(ValidatorBase):
    """
    a validator that examines the content of the NERDm data for completeness and syntactic correctness.
    """
    profile = ("NERDm-DAP", "0.7")

    def __init__(self, config=None):
        super(DAPNERDmValidator, self).__init__(config)

    def _target_name(self, nerd):
        return nerd.get("@id", "mds:unkn")

    def test_simple_props(self, nerd, want=ALL, out=None, **kw):
        """
        Test that we have values for the simple user-supplied properties required by a NERDm publication
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        for prop in "title description keyword".split():
            t = self._err(f"1.1#{prop}", f"A value for {prop} is required")
            t = out._add_applied(t, bool(nerd.get(prop)))

        return out

