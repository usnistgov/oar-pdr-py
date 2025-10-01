import json
import unittest as test
from pathlib import Path

from nistoar.midas.export.utils.loader import _load_json_source, _set_filename_output, normalize_input


class LoaderTest(test.TestCase):
    def setUp(self):
        self.datadir = Path(__file__).parent / "data"
        self.json_path = self.datadir / "exampleDMP.json"
        self.loaded = json.loads(self.json_path.read_text(encoding="utf-8"))

    def test_load_json_source_from_path(self):
        got = _load_json_source(self.json_path)
        self.assertIsInstance(got, dict)
        self.assertIn("data", got)

    def test_load_json_source_from_dict(self):
        got = _load_json_source(self.loaded)
        self.assertIs(got, self.loaded)  # returns as-is

    def test_load_json_source_wrong_type(self):
        # strings are not allowed by design
        with self.assertRaises(TypeError):
            normalize_input("sample.json", 0)

    def test_set_filename_output_from_dict_explicit(self):
        item = {"output_filename": "my_result"}
        name = _set_filename_output(item, 0)
        self.assertEqual(name, "my_result")

    def test_set_filename_output_from_path(self):
        name = _set_filename_output(self.json_path, 0)
        self.assertEqual(name, "exampleDMP")

    def test_set_filename_output_fallback_random(self):
        name = _set_filename_output(12345, 7)  # neither dict nor Path
        self.assertRegex(name, r"^record_[0-9a-f]{8}_7$")  # record_<8hex>_<index>

    def test_normalize_input_explicit_dict_with_path(self):
        info = normalize_input({"input_type": "json", "source": self.json_path, "output_filename": "explicit"}, 3)
        self.assertEqual(info["input_type"], "json")
        self.assertEqual(info["output_filename"], "explicit")
        self.assertIn("data", info["payload"])

    def test_normalize_input_bare_dict(self):
        info = normalize_input(self.loaded, 1)
        self.assertEqual(info["input_type"], "json")
        # For a bare dict with no explicit name and no Path, loader uses a randomized fallback
        self.assertRegex(info["output_filename"], r"^record_[0-9a-f]{8}_1$")

    def test_normalize_input_path(self):
        info = normalize_input(self.json_path, 5)
        self.assertEqual(info["input_type"], "json")
        self.assertEqual(info["output_filename"], "exampleDMP")
        self.assertIn("data", info["payload"])

    def test_normalize_input_unsupported(self):
        with self.assertRaises(TypeError):
            normalize_input(42, 0)


if __name__ == "__main__":
    test.main()
