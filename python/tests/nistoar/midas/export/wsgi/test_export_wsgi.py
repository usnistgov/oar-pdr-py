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

        def fake_run(input_data, output_format, output_directory, template_dir=None, template_name=None):
            return [{
                "ok": True,
                "fmt": output_format,
                "count": len(input_data),
                "outdir": str(output_directory)
            }]
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
            "inputs": [{"id": "x"}],
            "output_dir": "/tmp/out"
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
        wsgi_mod.run_export = lambda **kw: [{"name": "a"}, {"name": "b"}]
        payload = {
            "output_format": "markdown",
            "inputs": [{"id": 1}, {"id": 2}],
            "output_dir": "/tmp/out"
        }
        req = {"REQUEST_METHOD": "POST", "PATH_INFO": "/", "wsgi.input": StringIO(json.dumps(payload))}
        self.resp = []
        body = self.app(req, self.start)
        self.assertIn("200 ", self.resp[0])
        data = self.body_json(body)
        self.assertIsInstance(data, list)
        self.assertEqual([r["name"] for r in data], ["a", "b"])

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
                "inputs": [{"a": 1}],
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

    def test_output_dir_required(self):
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/",
            "wsgi.input": StringIO(json.dumps({
                "output_format": "markdown",
                "inputs": [{"id": 1}]
                # output_dir missing
            }))
        }
        body = self.app(req, self.start)
        self.assertIn("400 ", self.resp[0])
        txt = self.body_text(body).lower()
        self.assertIn("output_dir", txt)
        self.assertIn("required", txt)

    def test_export_runtime_error(self):
        wsgi_mod.run_export = lambda **kw: (_ for _ in ()).throw(RuntimeError("kaboom"))
        req = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/",
            "wsgi.input": StringIO(json.dumps({
                "output_format": "pdf",
                "inputs": [{"id": 1}],
                "output_dir": "/tmp/out"
            }))
        }
        self.resp = []
        body = self.app(req, self.start)
        self.assertIn("500 ", self.resp[0])
        txt = self.body_text(body)
        self.assertIsInstance(txt, str)


if __name__ == "__main__":
    test.main()
