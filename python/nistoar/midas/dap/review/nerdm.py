"""
NERDm tests for the minimum required elements
"""
import re
from collections.abc import Mapping

from .base import ValidatorBase, ValidationResults, ALL, REC, REQ, WARN
from nistoar.nerdm import utils as nrdutils

NIST_TAXONOMY_URI="https://data.nist.gov/od/dm/nist-themes/"
ORCID_RE = re.compile("^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")
GITHUB_BASE_URL = "https://github.com/"
NIST_GITLAB_BASE_URL = "https://gitlab.nist.gov/doesnotexist/"
NIST_DOWNLOAD_BASE_URL = "https://data.nist.gov/od/ds/"
NIST_LANDING_BASE_URL = "https://data.nist.gov/od/id/"

class DAPNERDmReviewValidator(ValidatorBase):
    """
    a validator that examines the content of the NERDm data for completeness and syntactic correctness.

    The tests contained in this validator are given identifiers organized into the following categories:
       1. Publication Identity
          1.1. Resource Type
          1.2. Title
          1.3. Authors
          1.4. Collection Membership
          1.5. Contact
          1.6. Version
       2. Description
          2.1. Home Page
          2.2. Abstract
          2.3. Topics  
          2.4. Keywords
       3. Data Access
          3.1. Access Level
          3.2. Rights
          3.3. Links
          3.4. Files and Folders
       4. References
       5. Advanced
    """
    profile = ("NERDm-DAP-Review", "0.7")

    def __init__(self, config=None):
        super(DAPNERDmReviewValidator, self).__init__(config)

    def _target_name(self, nerd):
        return nerd.get("@id", "mds:unkn")

    def _test_prop_exists(self, prop: str, id: str, md: Mapping, out: ValidationResults,
                          desc=None, instruct=None):
        """
        a templated test for testing the existance of a property
        :param str prop:  the property to look for in the given metadata dictionary
        :param str   id:  the identifier to assign to the result
        :param dict  md:  the metadata dictionary to examine
        :param ValidationResults out:  the results object to add the test result to
        :return:   the result of this test
                   :rtype:  ValidationIssue
        """
        if not desc:
            desc = f"A value for {prop} is required"
        if not instruct:
            instruct = f"Add a {prop}"

        t = self._err(id, desc)
        return out._add_applied(t, bool(md.get(prop)), instruct)

    def test_title(self, nerd, want=ALL, out=None, **kw):
        """
        Apply tests to the title.  These include:
          1.2.1.  REQ:  needs a title
          1.2.2.  WARN: should be more than 2 words long
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        if want & (REQ|WARN) == 0:
            # they want what we don't provide
            return out

        # must have one
        t = self._test_prop_exists("title", "1.2.1 title", nerd, out)

        if t.passed():
            # should be more than a few words long
            t = self._warn("1.2.2 title",
                           "A title should be briefly descriptive of the publication's contents")
            t = out._add_applied(t, len(nerd.get("title","").split()) > 2,
                                 ["Expand your title", "This title looks a little too brief"])

        return out

    def test_description(self, nerd, want=ALL, out=None, **kw):
        """
        Apply tests to the title.  These include:
          2.2.1.  REQ:  needs a description
          2.2.2.  REC:  should be more than 3 sentences long
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        if want & (REQ|WARN) == 0:
            # they want what we don't provide
            return out

        # must have one
        t = self._test_prop_exists("description", "2.2.1 description", nerd, out)

        if t.passed():
            # should be more than a few words long
            callit = "description" if not nerd.get("authors",[]) else "abstract"
            t = self._warn("2.2.2 description",
                           f"The {callit} should be one or a few paragraphs explaining the " +
                           "publication's contents using complete sentences.")
                           
            t = out._add_applied(t, len(re.split("\.\s+", "\n".join(nerd.get("description",[])))) > 2,
                                 [f"Consider expanding your {callit}",
                                  f"Your {callit} looks a little brief."])

        return out

    def test_keywords(self, nerd, want=ALL, out=None, **kw):
        """
        Apply tests to the title.  These include:
          2.4.1.  REQ:  need at least one keyword
          2.4.2.  WARN: each element contains one keyword phrase
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        if want & (REC|WARN) == 0:
            # they want what we don't provide
            return out

        # must have one
        t = self._test_prop_exists("keyword", "2.4.1 keyword", nerd, out,
                                   "At least one keyword is required", "Add a keyword")

        if t.passed():
            # look out for possibly concatonated keywords
            t = self._warn("2.4.2 keyword",
                           "Keyword phrases should not be concatonated into a single value")
            t = out._add_applied(t, all(";" not in k for k in nerd.get("keyword", [])),
                                  "Did you combine keyword entries into a single value?")

        return out

    def test_topics(self, nerd, want=ALL, out=None, **kw):
        """
        Apply tests to the title.  These include:
          2.3.1.  REQ:  need at least one topic
          2.3.2.  REQ:  need at least one topic from the NIST taxonomy
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        if want & REQ == 0:
            # they want what we don't provide
            return out

        # must have one
        t = self._test_prop_exists("topic", "2.3.1 topic", nerd, out,
                                   "At least one research topic is required", "Add a topic")

        if t.passed():
            # ensure we have a NIST taxonomy topic
            t = self._req("2.3.2 topic",
                          "At least one research topic from the NIST taxonomy is required")
            t = out._add_applied(t, any(s.get("scheme","").startswith(NIST_TAXONOMY_URI)
                                        for s in nerd.get("topic", [])),
                                 "Add a NIST Research Topic")

        return out

    def test_has_software(self, nerd, want=ALL, out=None, **meta):
        """
        test if a Software Publication includes access to software in some way.  To pass, 
        one of the following must be true:
          *  There is an accessURL that points to a repository
          *  The landing page points to a GitHub repo or a public GitLab repo
          *  There is at least one downloadURL marked as software distribution.  
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        if want & WARN == 0:
            # they want what we don't provide
            return out

        t = self._warn("1.1.1 links", "a software publication should provide access to software")
        ok = not nrdutils.is_type(nerd, "SoftwarePublication")

        # ok=False if it is a software publication, so look of a repo or distribution
        if not ok:
            ok = nerd.get("landingPage", "").startswith(GITHUB_BASE_URL)
        if not ok:
            ok = any(c.get("accessURL", "").startswith(GITHUB_BASE_URL)
                     for c in nerd.get("components",[]) if c.get("accessURL"))
        if not ok:
            ok = any(nrdutils.is_type(c, "SoftwareDistribution")
                     for c in nerd.get("components",[]) if c.get("downloadURL"))

        comm = ["Add a link to software"]
        if not ok:
            comm += ["You indicated that this is a software publication, "+
                     "but it looks like you have not linked to a software repository "+
                     "nor uploaded any software distributions"]
        t = out._add_applied(t, ok, comm)

        return out

    def test_has_data(self, nerd, want=ALL, out=None, **meta):
        """
        make sure the record points to data somehow, somewhere.  It checks for:
          * a non-PDR landing page,
          * at least one file link, or
          * at least one access page link
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        if want & (REQ|REC) == 0:
            # they want what we don't provide
            return out

        label = "3.0.1 "
        label += "links" if not meta.get('willUpload') else "files"
        t = self._warn(label,
            "Publication needs to point to some kind of digital product (e.g. data, software, website)")

        ok = not nerd.get("landingPage", NIST_LANDING_BASE_URL).startswith(NIST_LANDING_BASE_URL)
        if not ok:
            ok = any(nrdutils.is_type(c, "DataFile") or nrdutils.is_type(c, "AccessPage")
                     for c in nerd.get("components"))

        t = out._add_applied(t, ok, "Add Files, Links, or an external Home Page")
        return out
        

    def test_author(self, nerd, want=ALL, out=None, **kw):
        """
        check the author list
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        auths = nerd.get("authors", [])
        if want & REC:
            t = self._rec("1.3.1 authors",
                          "To be considered a full Data Publication, authors should be provided")
            t = out._add_applied(t, len(auths) > 0, "Add some authors")
        if auths:
            orcids = [a["orcid"] for a in auths if a.get("orcid")]
            if want & REC:
                fix = len(auths)-len(orcids)
                t = self._rec("1.3.2 authors", "Each author should include an ORCID")
                t = out._add_applied(t, len(auths) == len(orcids),
                                     f"Add ORCIDs for {fix} author{'s' if fix>1 else ''}")

            if orcids and want & REQ:
                t = self._req("1.3.3 authors", "An ORCID must follow the pattern, NNNN-NNNN-NNNN-NNNN")
                fix = len([d for d in orcids if not ORCID_RE.search(d)])
                t = out._add_applied(t, fix == 0,
                                     f"Fix the ORCID format for {fix} author{'s' if fix>1 else ''}")

                t = self._req("1.3.4 authors", "ORCIDs must be unique to an author")
                uniqids = set(orcids)
                fix = len(orcids) - len(uniqids)
                t = out._add_applied(t, fix == 0, 
                                     f"Ensure ORCIDs are unique for {fix} author{'s' if fix>1 else ''}")

            if want & WARN:
                t = self._warn("1.3.5 authors",
                               "An author should have both a first name (or initials) and a last name")
                fix = len([a for a in auths if not (a.get("givenName") and a.get("familyName"))])
                t = out._add_applied(t, fix == 0,
                                     f"Provide full names for {fix} author{'s' if fix>1 else ''}")

            if want & REC:
                t = self._rec("1.3.6 authors", "Each author should have at least one affiliation")
                fix = len([a for a in auths if not a.get("affiliation")
                                or len([f for f in a.get("affiliation") if not f.get("title")]) > 0])
                t = out._add_applied(t, fix == 0,
                                     f"Provide affiliation for {fix} author{'s' if fix>1 else ''}")

        return out

    def test_files(self, nerd, want=ALL, out=None, **meta):
        """
        apply checks to the file list
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        files = [c for c in nerd.get("components", []) if nrdutils.is_type(c, "DataFile")]
        if want & WARN and meta.get("willUpload"):
            t = self._warn("3.4.1 files", "You indicated you would upload files but none are found")
            t = out._add_applied(t, len(files) > 0, "Upload or rescan files")

        if len(files) > 0:
            # tests on each of the files will be placed here
            pass

        return out

    def test_links(self, nerd, want=ALL, out=None, **meta):
        """
        apply checks to the list of links
        """
        if not out:
            out = ValidationResults(self._target_name(nerd), want)

        links = [c for c in nerd.get("components", [])
                   if not nrdutils.is_type(c, "Hidden") and 
                      (nrdutils.is_type(c, "AccessPage") or c.get("accessURL"))]

        rtype = meta.get("resourceType")
        if want & WARN and rtype in ["portal", "website", "service"]:
            t = self._warn("1.1.2 links",
                           f"a {rtype} publication should include a link to the {rtype}")
            t = out._add_applied(t, len(links) > 0, f"Add a link to a {rtype}")

        if links:
            if want & REQ:
                t = self._req("3.3.1 links",
                              "A URL must be provided for each link")
                fix = len([k for k in links if not k.get("accessURL")])
                t = out._add_applied(t, fix == 0, f"Add a URL to {fix} link{'s' if fix>1 else ''}")

            if want & WARN:
                t = self._warn("3.3.2 links",
                               "Each link should have a title to use as the displayed link text")
                fix = len([k for k in links if not k.get("title")])
                t = out._add_applied(t, fix == 0, f"Add a title to {fix} link{'s' if fix>1 else ''}")

            if want & REC:
                t = self._rec("3.3.3 links",
                              "An extended explanation can be provided about a link")
                fix = len([k for k in links if not k.get("description")])
                t = out._add_applied(t, fix == 0, f"Add a title to {fix} link{'s' if fix>1 else ''}")

        return out
                          
