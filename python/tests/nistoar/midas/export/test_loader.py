import json
import unittest as test
from pathlib import Path
from collections.abc import Mapping


from nistoar.midas.export.utils.loader import _load_json_source, _set_filename, normalize_input


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

    def test_set_filename_from_dict_explicit(self):
        item = {"filename": "my_result"}
        name = _set_filename(item, 0)
        self.assertEqual(name, "my_result")

    def test_set_filename_from_path(self):
        name = _set_filename(self.json_path, 0)
        self.assertEqual(name, "exampleDMP")

    def test_set_filename_fallback_random(self):
        name = _set_filename(12345, 7)  # neither dict nor ProjectRecord
        self.assertRegex(name, r"^record_[0-9a-f]{8}_7$")  # record_<8hex>_<index>

    def test_normalize_input_explicit_dict_with_path(self):
        info = normalize_input({"input_type": "json", "source": self.json_path, "filename": "explicit"}, 3)
        self.assertEqual(info["input_type"], "json")
        self.assertEqual(info["filename"], "explicit")
        self.assertIn("data", info["payload"])

    def test_normalize_input_bare_dict(self):
        info = normalize_input(self.loaded, 1)
        self.assertEqual(info["input_type"], "json")
        # For a bare dict with no explicit name and no Path, loader uses a randomized fallback
        self.assertRegex(info["filename"], r"^record_[0-9a-f]{8}_1$")

    def test_normalize_input_path(self):
        info = normalize_input(self.json_path, 5)
        self.assertEqual(info["input_type"], "json")
        self.assertEqual(info["filename"], "exampleDMP")
        self.assertIn("data", info["payload"])

    def test_normalize_input_unsupported(self):
        with self.assertRaises(TypeError):
            normalize_input(42, 0)


class _FakeProjectRecord:
    """ Object has .data (Mapping) and optionally .name (str) and/or .id (str)
    """
    def __init__(self, data=None, name=None, rec_id=None):
        self.data = data or {"title": "Demo"}
        if name is not None:
            self.name = name
        if rec_id is not None:
            self.id = rec_id


def _is_mapping(obj):
    return isinstance(obj, Mapping)


class LoaderProjectRecordTest(test.TestCase):
    def test_normalize_input_project_record_uses_name(self):
        rec = _FakeProjectRecord(name="nice_name")
        info = normalize_input(rec, 0)
        self.assertEqual(info["input_type"], "json")
        self.assertEqual(info["filename"], "nice_name")
        self.assertIn("data", info["payload"])
        self.assertTrue(_is_mapping(info["payload"]["data"]))

    def test_normalize_input_project_record_uses_id_if_no_name(self):
        rec = _FakeProjectRecord(rec_id="abc123")
        info = normalize_input(rec, 1)
        self.assertEqual(info["input_type"], "json")
        self.assertEqual(info["filename"], "abc123")

    def test_normalize_input_project_record_fallback_random(self):
        # No name, no id so fallback
        rec = _FakeProjectRecord()
        if hasattr(rec, "name"):
            delattr(rec, "name")
        if hasattr(rec, "id"):
            delattr(rec, "id")

        info = normalize_input(rec, 7)
        self.assertRegex(info["filename"], r"^record_[0-9a-f]{8}_7$")
        self.assertIn("data", info["payload"])


if __name__ == "__main__":
    test.main()
