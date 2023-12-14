import contextlib
import unittest

from octoflow.model import Client


class TestClientMethods(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.experiment_name = "experiment_name_1"
        cls.client = Client()
        with contextlib.suppress(ValueError):
            cls.client.create_experiment(cls.experiment_name)

    def test_create_experiment_existing(self):
        with self.assertRaises(ValueError):
            self.client.create_experiment(self.experiment_name)

    def test_get_experiment_by_name(self):
        expr = self.client.get_experiment_by_name(self.experiment_name)
        self.assertIsNotNone(expr)
        self.assertEqual(expr.name, self.experiment_name)


if __name__ == "__main__":
    unittest.main()
