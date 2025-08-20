import unittest as test
from unittest.mock import patch, MagicMock
import json

from nistoar.midas.dap.extrev.nps1 import NPSExternalReviewClient
from nistoar.midas.dap.extrev import ExternalReviewException
from nistoar.base.config import ConfigurationException

# Example reviewers
OWNER = {
    "nistId": "900123",
    "firstName": "Isaac",
    "lastName": "Newton",
    "eMail": "Isaac.Newton@nist.gov"
}
TECH1 = {
    "nistId": "900124",
    "firstName": "Alan",
    "lastName": "Turing",
    "eMail": "alan.turing@nist.gov"
}

GOOD_CFG = {
    "nps_endpoint": "https://nps.example.com/api",
    "draft_url_template": "https://datapub.example.com/draft/%s",
    "published_url_template": "https://data.example.com/od/ds/%s",
    "tokenService": {
        "service_endpoint": "https://auth.example.com/token",
        "client_id": "clientid",
        "secret": "secret"
    }
}


def fake_response(status=200, json_data=None, reason="OK"):
    resp = MagicMock()
    resp.status_code = status
    resp.reason = reason
    resp.text = json.dumps(json_data or {})
    resp.json.return_value = json_data or {}
    return resp


class TestNPSExternalReviewClient(test.TestCase):

    # Construction/Config Errors
    def test_missing_required_config_raises(self):
        for key in ["draft_url_template", "published_url_template", "nps_endpoint"]:
            bad_cfg = GOOD_CFG.copy()
            bad_cfg.pop(key)
            with self.assertRaises(ConfigurationException):
                NPSExternalReviewClient(bad_cfg)

    def test_good_config(self):
        cli = NPSExternalReviewClient(GOOD_CFG)
        self.assertIsInstance(cli, NPSExternalReviewClient)

    def test_select_review_reason(self):
        cli = NPSExternalReviewClient(GOOD_CFG)
        self.assertEqual(cli.select_review_reason([], None), "New Record")
        self.assertEqual(cli.select_review_reason(None), "New Record")
        self.assertEqual(cli.select_review_reason(["foo"], None), "Data change (major)")
        self.assertEqual(cli.select_review_reason(["foo", "metadata", "change_major", ""]),
                         "Data change (major)")
        self.assertEqual(cli.select_review_reason(["foo", "change_minor", "deactivate", ""]),
                         "Record deactivation")
        self.assertEqual(cli.select_review_reason(["metadata", "change_minor"]),
                         "Data change (minor)")
        self.assertEqual(cli.select_review_reason(["metadata", "add_files", "change_minor"]),
                         "New file addition")
        self.assertEqual(cli.select_review_reason(["metadata", "add_readmes", "change_minor"]),
                         "New file addition")
        self.assertEqual(cli.select_review_reason(["metadata"]),
                         "Metadata Update")

    def test_build_urls(self):
        cli = NPSExternalReviewClient(GOOD_CFG)
        draft_url, pub_url = cli._build_urls("ABC123", "PUB456")
        self.assertIn("ABC123", draft_url)
        self.assertIn("PUB456", pub_url)

        # pubid falls back to record id
        draft_url2, pub_url2 = cli._build_urls("XYZ789", None)
        self.assertIn("XYZ789", draft_url2)
        self.assertIn("XYZ789", pub_url2)

    # submit: success, all required fields
    @patch("nistoar.midas.dap.extrev.nps1.requests.post")
    @patch("nistoar.midas.dap.extrev.nps1.get_nsd_auth_token")
    def test_submit_success(self, mock_token, mock_post):
        mock_token.return_value = "token123"
        mock_post.return_value = fake_response(json_data={"result": "OK"})
        cli = NPSExternalReviewClient(GOOD_CFG)

        result = cli.submit(
            id="REC001",
            submitter=OWNER["nistId"],
            title="A Test Title",
            description="Test Description",
            pubid="PUB001",
            reviewers=[OWNER, TECH1],
            instructions=["Do something extra"],
            changes=["major"],
            security_review=True
        )

        # Response returned
        self.assertEqual(result, {"result": "OK"})

        # Check that requests.post was called with expected headers and payload
        args, kwargs = mock_post.call_args
        url = args[0]
        self.assertTrue(url.endswith("/review/REC001"))
        headers = kwargs["headers"]
        self.assertEqual(headers["Authorization"], "Bearer token123")
        self.assertEqual(headers["Content-Type"], "application/json")

        payload = json.loads(kwargs["data"])
        # DataSetID and reviewer list
        self.assertEqual(payload["dataSetID"], "REC001")
        self.assertEqual(payload["reviewers"][0]["contactTypeId"], 7)
        self.assertEqual(payload["reviewers"][1]["contactTypeId"], 21)
        self.assertEqual(payload["instructions"], ["Do something extra"])
        self.assertEqual(payload["reviewReason"], "Data change (major)")

    # submit: no reviewers provided, fallback owner
    @patch("nistoar.midas.dap.extrev.nps1.requests.post")
    @patch("nistoar.midas.dap.extrev.nps1.get_nsd_auth_token")
    def test_submit_fallback_owner(self, mock_token, mock_post):
        mock_token.return_value = "tok"
        mock_post.return_value = fake_response(json_data={"ok": True})
        cli = NPSExternalReviewClient(GOOD_CFG)

        res = cli.submit(
            id="X12",
            submitter="OWNERX12"
        )
        payload = json.loads(mock_post.call_args[1]["data"])
        self.assertEqual(payload["submitterID"], "OWNERX12")
        self.assertEqual(payload["reviewers"][0]["nistId"], "OWNERX12")
        self.assertEqual(payload["reviewers"][0]["contactTypeId"], 7)

    # submit: no token
    def test_token_missing_raises(self):
        bad_cfg = dict(GOOD_CFG)
        bad_cfg.pop("tokenService")
        cli = NPSExternalReviewClient(bad_cfg)
        with self.assertRaises(ConfigurationException):
            cli._get_token()

    # submit: http 401 unauthorized
    @patch("nistoar.midas.dap.extrev.nps1.requests.post")
    @patch("nistoar.midas.dap.extrev.nps1.get_nsd_auth_token", return_value="TTT")
    def test_submit_401(self, mock_token, mock_post):
        mock_post.return_value = fake_response(status=401, reason="Unauthorized")
        cli = NPSExternalReviewClient(GOOD_CFG)
        with self.assertRaises(ExternalReviewException) as cm:
            cli.submit(id="RECFAIL", submitter="X")
        self.assertIn("Unauthorized", str(cm.exception))

    # submit: http 403 forbidden
    @patch("nistoar.midas.dap.extrev.nps1.requests.post")
    @patch("nistoar.midas.dap.extrev.nps1.get_nsd_auth_token", return_value="TTT")
    def test_submit_403(self, mock_token, mock_post):
        mock_post.return_value = fake_response(status=403, reason="Forbidden")
        cli = NPSExternalReviewClient(GOOD_CFG)
        with self.assertRaises(ExternalReviewException) as cm:
            cli.submit(id="RECFORBID", submitter="X")
        self.assertIn("Forbidden", str(cm.exception))

    # submit: http 500 (other error)
    @patch("nistoar.midas.dap.extrev.nps1.requests.post")
    @patch("nistoar.midas.dap.extrev.nps1.get_nsd_auth_token", return_value="TTT")
    def test_submit_500(self, mock_token, mock_post):
        mock_post.return_value = fake_response(status=500, reason="Internal Server Error", json_data={"error": "fail"})
        cli = NPSExternalReviewClient(GOOD_CFG)
        with self.assertRaises(ExternalReviewException) as cm:
            cli.submit(id="RECSERVER", submitter="X")
        self.assertIn("NPS API error", str(cm.exception))
        self.assertIn("Internal Server Error", str(cm.exception))

    # submit: actual requests.post throws (network error)
    @patch("nistoar.midas.dap.extrev.nps1.requests.post", side_effect=Exception("network fail"))
    @patch("nistoar.midas.dap.extrev.nps1.get_nsd_auth_token", return_value="TTT")
    def test_submit_requests_error(self, mock_token, mock_post):
        cli = NPSExternalReviewClient(GOOD_CFG)
        with self.assertRaises(ExternalReviewException) as cm:
            cli.submit(id="RECNETERR", submitter="X")
        self.assertIn("Failed to POST to NPS", str(cm.exception))

    # submit: people service used to look up submitter
    @patch("nistoar.midas.dap.extrev.nps1.requests.post")
    @patch("nistoar.midas.dap.extrev.nps1.get_nsd_auth_token", return_value="TTT")
    def test_submit_uses_people_service(self, mock_token, mock_post):
        # Prepare a mock PeopleService
        ps = MagicMock()
        ps.get_person.return_value = {
            "nistId": "123987",
            "firstName": "Diogo",
            "lastName": "Jota",
            "eMail": "diogo.jota@nist.gov"
        }

        cli = NPSExternalReviewClient(GOOD_CFG, peopsvc=ps)

        mock_post.return_value = fake_response(json_data={"done": True})
        result = cli.submit(id="PSTEST1", submitter="123987")

        # Ensure get_person was called with the submitter ID
        ps.get_person.assert_called_once_with("123987")

        # The reviewer info in the payload must match what the PeopleService returned
        payload = json.loads(mock_post.call_args[1]["data"])
        reviewer = payload["reviewers"][0]
        assert reviewer["nistId"] == "123987"
        assert reviewer["firstName"] == "Diogo"
        assert reviewer["lastName"] == "Jota"
        assert reviewer["eMail"] == "diogo.jota@nist.gov"
        assert reviewer["contactTypeId"] == 7  # Owner
        assert payload["submitterID"] == "123987"
        assert result == {"done": True}


if __name__ == '__main__':
    test.main()
