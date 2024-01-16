import contextlib
import unittest
from contextvars import ContextVar

from octoflow.utils.mutation import MutableList

# track changes to the context
changed_var = ContextVar("changed")


class TestMutableList(unittest.TestCase):
    def setUp(self):
        self.l = MutableList()
        self.l.add_event_listener("change", lambda: changed_var.set(True))

    @contextlib.contextmanager
    def assertChanged(self):  # noqa: N802
        changed_var.set(False)
        yield
        self.assertTrue(changed_var.get(), msg="the container did not change")

    def test_add_element(self):
        with self.assertChanged():
            self.l.append(1)
        self.assertEqual(self.l[0], 1)

    def test_remove_element(self):
        self.l.extend([1, 2, 3])
        with self.assertChanged():
            self.l.remove(2)
        self.assertListEqual(self.l._data, [1, 3])

    def test_insert_element(self):
        self.l.extend([1, 2, 3])
        with self.assertChanged():
            self.l.insert(1, 4)
        self.assertListEqual(self.l._data, [1, 4, 2, 3])

    def test_pop_element(self):
        self.l.extend([1, 2, 3])
        with self.assertChanged():
            self.l.pop()
        self.assertListEqual(self.l._data, [1, 2])

    def test_clear_list(self):
        self.l.extend([1, 2, 3])
        with self.assertChanged():
            self.l.clear()
        self.assertListEqual(self.l._data, [])

    def test_sort_list(self):
        self.l.extend([3, 1, 2])
        with self.assertChanged():
            self.l.sort()
        self.assertListEqual(self.l._data, [1, 2, 3])

    def test_reverse_list(self):
        self.l.extend([1, 2, 3])
        with self.assertChanged():
            self.l.reverse()
        self.assertListEqual(self.l._data, [3, 2, 1])

    def test_modify_element_of_nested_list(self):
        self.l.append([1, 2, 3])
        with self.assertChanged():
            self.l[0][1] = 4
        self.assertListEqual(self.l[0]._data, [1, 4, 3])


if __name__ == "__main__":
    unittest.main()
