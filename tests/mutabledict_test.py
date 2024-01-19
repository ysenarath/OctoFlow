import contextlib
import unittest
from contextvars import ContextVar

from octoflow.utils.collections import MutableDict

# track changes to the context
changed_var = ContextVar("changed")


class TestMutableDict(unittest.TestCase):
    def setUp(self):
        self.d = MutableDict()
        self.d.add_event_listener("change", lambda: changed_var.set(True))

    @contextlib.contextmanager
    def assertChanged(self):  # noqa: N802
        changed_var.set(False)
        yield
        self.assertTrue(changed_var.get(), msg="the container did not change")

    def test_add_element(self):
        with self.assertChanged():
            self.d["a"] = 1
        self.assertEqual(self.d["a"], 1)

    def test_nested_element(self):
        with self.assertChanged():
            self.d["a"] = {}
        self.assertEqual(self.d["a"], {})
        with self.assertChanged():
            self.d["a"]["b"] = 1
        self.assertEqual(self.d["a"]["b"], 1)

    def test_modify_list_element(self):
        self.d["b"] = {}
        with self.assertChanged():
            self.d["b"]["c"] = [1, 2, 3]
        self.assertListEqual(self.d["b"]["c"]._data, [1, 2, 3])
        with self.assertChanged():
            self.d["b"]["c"][1] = 3
        self.assertListEqual(self.d["b"]["c"]._data, [1, 3, 3])
        with self.assertChanged():
            self.d["b"]["c"].append(4)
        self.assertListEqual(self.d["b"]["c"]._data, [1, 3, 3, 4])

    def test_modify_set_element(self):
        self.d["b"] = {}
        with self.assertChanged():
            self.d["b"]["c"] = {1, 2, 3, 4, 5}
        self.assertSetEqual(self.d["b"]["c"]._data, {1, 2, 3, 4, 5})
        with self.assertChanged():
            self.d["b"]["c"].remove(1)
        self.assertSetEqual(self.d["b"]["c"]._data, {2, 3, 4, 5})
        with self.assertChanged():
            self.d["b"]["c"].add(6)
        self.assertSetEqual(self.d["b"]["c"]._data, {2, 3, 4, 5, 6})


if __name__ == "__main__":
    unittest.main()
