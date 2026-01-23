import os, json, logging, tempfile
from io import StringIO
import unittest as test

from nistoar.midas.export import wsgi as expwsgi
from nistoar.midas.export.wsgi import wsgi as wsgi_mod

tmpdir = tempfile.TemporaryDirectory(prefix="_test_export_wsgi.")
loghdlr = None
rootlog = None


def setUpModule():
    global loghdlr, rootlog
    rootlog = logging.getLogger()
    rootlog.setLevel(logging.DEBUG)
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name, "test_export_wsgi.log"))
    loghdlr.setLevel(logging.DEBUG)
    loghdlr.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    rootlog.addHandler(loghdlr)


def tearDownModule():
    global loghdlr
    if loghdlr:
        rootlog.removeHandler(loghdlr)
        loghdlr.flush()
        loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()


class TestExportHandler(test.TestCase):
    def start(self, status, headers=None, extup=None):
        self.resp.append(status)
        for head in headers or []:
            self.resp.append(f"{head[0]}: {head[1]}")

    def body_text(self, body):
        # ServiceApp returns iterable of bytes; errors are plain text
        return "".join([b.decode() for b in body]) if body else ""

    def body_json(self, body):
        # Try to parse as JSON; return None if not JSON
        txt = self.body_text(body)
        try:
            return json.loads(txt) if txt else None
        except Exception:
            return None

    def setUp(self):
        self.resp = []
        # Save and patch where ExportHandler actually calls it
        self._orig_run = getattr(wsgi_mod, "run_export", None)

        def fake_run(**kw):
            """
            Fake run_export that matches the signature and return shape:
            - accepts keyword args (input_data, output_format, output_directory, ...)
            - returns a single dict including filename/mimetype so streaming could work too
            """
            input_data = kw.get("input_data", [])
            output_format = kw.get("output_format")
            output_directory = kw.get("output_directory")
            return {
                "ok": True,
                "fmt": output_format,
                "count": len(input_data),
                "outdir": str(output_directory),
                "filename": "combined.out",
                "mimetype": "application/octet-stream",
                "file_extension": ".out",
                "path": os.path.join(str(output_directory), "combined.out"),
            }

        wsgi_mod.run_export = fake_run

        self.app = expwsgi.ExportApp(rootlog, {})

    def tearDown(self):
        if self._orig_run is not None:
            wsgi_mod.run_export = self._orig_run
        else:
            try:
                delattr(wsgi_mod, "run_export")
            except Exception:
                pass

    class FakeRecord:
        """Minimal record with id and data attributes"""

        def __init__(self, rec_id):
            self.id = rec_id
            self.data = {"foo": "bar"}

    class FakeDBClient:
        """Minimal DB client exposing select_records_by_ids"""

        def __init__(self, rec_ids):
            self._recs = {rid: TestExportHandler.FakeRecord(rid) for rid in rec_ids}

        def select_records_by_ids(self, ids, perm=None):
            for rid in ids:
                rec = self._recs.get(rid)
                if rec is not None:
                    # Returns iterator like the real function
                    yield rec

    def test_handler_instance_type(self):
        handler = self.app.create_handler(
            env={"REQUEST_METHOD": "POST", "PATH_INFO": "/"},
            start_resp=self.start,
            path="/",
            who=None
        )
        from nistoar.midas.export.wsgi.wsgi import ExportHandler
        self.assertIsInstance(handler, ExportHandler)

    def test_post_ok_single_result(self):
        payload = {
            "output_format": "pdf",
            "inputs": [{"data": {"a": 1}}],
            "output_dir": "/tmp/out",
        }
        req = {"REQUEST_METHOD": "POST", "PATH_INFO": "/", "wsgi.input": StringIO(json.dumps(payload))}
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        data = self.body_json(body)
        self.assertIsInstance(data, dict)
        self.assertTrue(data["ok"])
        self.assertEqual(data["fmt"], "pdf")
        self.assertEqual(data["count"], 1)

    def test_post_ok_multiple_results(self):
        def fake_multi(**kw):
            input_data = kw.get("input_data", [])
            return {
                "ok": True,
                "fmt": kw.get("output_format"),
                "count": len(input_data),
                "filename": "combined_multi.out",
                "mimetype": "application/octet-stream",
                "file_extension": ".out",
                "path": os.path.join(str(kw.get("output_directory")), "combined_multi.out"),
            }

        wsgi_mod.run_export = fake_multi

        payload = {
            "output_format": "markdown",
            "inputs": [{"data": {"a": 1}}, {"data": {"b": 2}}],
            "output_dir": "/tmp/out",
        }
        req = {"REQUEST_METHOD": "POST", "PATH_INFO": "/", "wsgi.input": StringIO(json.dumps(payload))}
        self.resp = []
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        data = self.body_json(body)
        self.assertIsInstance(data, dict)
        self.assertEqual(data["fmt"], "markdown")
        self.assertEqual(data["count"], 2)

    def test_options_preflight(self):
        req = {"REQUEST_METHOD": "OPTIONS", "PATH_INFO": "/"}
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        # No specific body assertion; just ensure success

    def test_bad_path(self):
        req = {"REQUEST_METHOD": "POST", "PATH_INFO": "/not-root", "wsgi.input": StringIO("{}")}
        body = self.app(req, self.start)
        self.assertIn("404 ", self.resp[0])
        # Body may be empty for 404
        txt = self.body_text(body)
        self.assertIsInstance(txt, str)

    def test_missing_body(self):
        req = {"REQUEST_METHOD": "POST", "PATH_INFO": "/"}  # no wsgi.input
        body = self.app(req, self.start)
        self.assertIn("400 ", self.resp[0])
        txt = self.body_text(body).lower()
        self.assertIn("missing", txt)
        self.assertIn("json", txt)

    def test_invalid_json(self):
        req = {"REQUEST_METHOD": "POST", "PATH_INFO": "/", "wsgi.input": StringIO("{not: json}")}
        body = self.app(req, self.start)
        self.assertIn("400 ", self.resp[0])
        txt = self.body_text(body).lower()
        self.assertIn("json", txt)
        self.assertTrue("error" in txt or "parse" in txt)

    def test_bad_output_format(self):
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/",
            "wsgi.input": StringIO(json.dumps({
                "output_format": "docx",
                "inputs": [{"data": {"a": 1}}],
                "output_dir": "/tmp/out"
            }))
        }
        body = self.app(req, self.start)
        self.assertIn("400 ", self.resp[0])
        txt = self.body_text(body).lower()
        self.assertIn("output_format", txt)
        self.assertIn("pdf", txt)
        self.assertIn("markdown", txt)

    def test_inputs_required(self):
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/",
            "wsgi.input": StringIO(json.dumps({
                "output_format": "pdf",
                "inputs": [],
                "output_dir": "/tmp/out"
            }))
        }
        body = self.app(req, self.start)
        self.assertIn("400 ", self.resp[0])
        txt = self.body_text(body).lower()
        self.assertIn("inputs", txt)
        self.assertIn("array", txt)

    def test_export_runtime_error(self):
        wsgi_mod.run_export = lambda **kw: (_ for _ in ()).throw(RuntimeError("kaboom"))
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/",
            "wsgi.input": StringIO(json.dumps({
                "output_format": "pdf",
                "inputs": [{"data": {"a": 1}}],
                "output_dir": "/tmp/out",
            }))
        }
        self.resp = []
        body = self.app(req, self.start)
        self.assertIn("500 ", self.resp[0])
        txt = self.body_text(body)
        self.assertIsInstance(txt, str)

    def test_inputs_ids_resolved_via_dbclient(self):
        # Capture what input_data gets passed to run_export
        captured = {}

        def fake_run(**kw):
            captured["input_data"] = kw.get("input_data", [])
            captured["output_format"] = kw.get("output_format")
            captured["output_directory"] = kw.get("output_directory")
            return {
                "ok": True,
                "fmt": captured["output_format"],
                "count": len(captured["input_data"]),
                "outdir": str(captured["output_directory"]),
                "filename": "combined.out",
                "mimetype": "application/octet-stream",
                "file_extension": ".out",
                "path": os.path.join(str(captured["output_directory"]), "combined.out"),
            }

        wsgi_mod.run_export = fake_run

        # Attach a fake DB client with one known record "rec-1"
        self.app.dbcli = TestExportHandler.FakeDBClient(rec_ids=["rec-1"])

        payload = {
            "output_format": "pdf",
            "inputs": ["rec-1"], # list of IDs (strings)
            "output_dir": "/tmp/out",
        }
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/",
            "wsgi.input": StringIO(json.dumps(payload)),
        }

        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])

        # Check JSON response shape
        data = self.body_json(body)
        self.assertIsInstance(data, dict)
        self.assertTrue(data["ok"])
        self.assertEqual(data["fmt"], "pdf")
        self.assertEqual(data["count"], 1)

        # Assert that run_export received a FakeRecord instance
        input_data = captured.get("input_data")
        self.assertIsInstance(input_data, list)
        self.assertEqual(len(input_data), 1)
        self.assertIsInstance(input_data[0], TestExportHandler.FakeRecord)
        self.assertEqual(input_data[0].id, "rec-1")

    def test_inputs_unknown_id_returns_404(self):
        # Fake DB client with NO records
        self.app.dbcli = TestExportHandler.FakeDBClient(rec_ids=[])

        payload = {
            "output_format": "pdf",
            "inputs": ["missing-id"],
            "output_dir": "/tmp/out",
        }
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/",
            "wsgi.input": StringIO(json.dumps(payload)),
        }

        body = self.app(req, self.start)
        self.assertIn("404 ", self.resp[0])
        txt = self.body_text(body).lower()
        self.assertIn("not found", txt)
        self.assertIn("project record", txt)


if __name__ == "__main__":
    test.main()
