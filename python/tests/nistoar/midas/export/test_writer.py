import unittest as test
from pathlib import Path
import tempfile

from nistoar.midas.export.utils.writer import write_file


class WriterTest(test.TestCase):
    def test_write_file_bytes(self):
        with tempfile.TemporaryDirectory() as td:
            outdir = Path(td)
            result = {
                "filename": "x.pdf",
                "bytes": b"%PDF-sample%",
                "format": "pdf",
                "mimetype": "application/pdf",
                "file_extension": ".pdf",
            }
            path = write_file(outdir, result)
            p = Path(path)
            self.assertTrue(p.is_file())
            self.assertEqual(p.read_bytes(), b"%PDF-sample%")

    def test_write_file_text(self):
        with tempfile.TemporaryDirectory() as td:
            outdir = Path(td)
            result = {
                "filename": "x.md",
                "text": "# hello\n",
                "format": "markdown",
                "mimetype": "text/markdown",
                "file_extension": ".md",
            }
            path = write_file(outdir, result)
            p = Path(path)
            self.assertTrue(p.is_file())
            self.assertEqual(p.read_text(encoding="utf-8"), "# hello\n")

    def test_write_file_missing_content(self):
        with tempfile.TemporaryDirectory() as td:
            outdir = Path(td)
            result = {
                "filename": "x.bin",
                "format": "bin",
                "mimetype": "application/octet-stream",
                "file_extension": ".bin",
            }
            with self.assertRaises(ValueError):
                write_file(outdir, result)


if __name__ == "__main__":
    test.main()
