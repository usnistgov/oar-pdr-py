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

        for prop in "title description".split():
            t = self._err(f"1.1#{prop}", f"A value for {prop} is required")
            t = out._add_applied(t, bool(nerd.get(prop)), f"Add a {prop}")

        t = self._err("1.1#keyword", f"A value for keyword is required")
        t = out._add_applied(t, bool(nerd.get(prop)), f"Add some keywords")

        t = self._err("1.1#topic", f"At least one NIST research topic is required")
        t = out._add_applied(t, bool(nerd.get(prop)), f"Add some research topics")

        return out

