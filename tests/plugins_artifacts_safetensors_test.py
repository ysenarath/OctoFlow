import unittest

import numpy as np

from octoflow.plugins.artifacts.safetensors_ import DictNumpyArrayHandler


class TestDictNumpyArrayHandler(unittest.TestCase):
    def test_can_handle_empty_dict(self):
        obj = {}
        self.assertTrue(DictNumpyArrayHandler.can_handle(obj))

    def test_can_handle_dict_with_np_array(self):
        obj = {"key": np.array([1, 2, 3])}
        self.assertTrue(DictNumpyArrayHandler.can_handle(obj))

    def test_can_handle_non_mapping_object(self):
        # Test case: obj is not a Mapping
        obj = [1, 2, 3]
        self.assertFalse(DictNumpyArrayHandler.can_handle(obj))

    def test_can_handle_dict_with_np_arrays(self):
        # Test case: obj is a Mapping and all values are np.ndarrays
        obj = {"key1": np.array([1, 2, 3]), "key2": np.array([4, 5, 6])}
        self.assertTrue(DictNumpyArrayHandler.can_handle(obj))

    def test_can_handle_dict_with_non_np_array(self):
        # Test case: obj is a Mapping but not all values are np.ndarrays
        obj = {"key1": np.array([1, 2, 3]), "key2": "value"}
        self.assertFalse(DictNumpyArrayHandler.can_handle(obj))


if __name__ == "__main__":
    unittest.main()
