import math
import random
import unittest

from tests.octoflow_unit_test.model.utils import (
    create_client_and_experiment,
    create_experiment,
)


class TestExperimentMethods(unittest.TestCase):
    def setUp(self):
        self.experiment_name = "experiment_name_1"

    def test_experiment_attrs(self):
        expr = create_experiment(self.experiment_name)
        self.assertEqual(expr.name, self.experiment_name)
        self.assertIsNone(expr.description)

    def test_vars(self):
        expr = create_experiment(self.experiment_name)
        self.assertEqual(len(list(expr.vars)), 0)

    def test_create_delete_var(self):
        expr = create_experiment(self.experiment_name)
        var_name = "test"
        test_var = expr.vars[var_name]
        self.assertEqual(type(test_var).__name__, "Variable")
        del expr.vars[var_name]

    def test_vars_count(self):
        expr = create_experiment(self.experiment_name)
        self.assertEqual(len(expr.vars), 0)

    def test_vars_list(self):
        expr = create_experiment(self.experiment_name)
        var_name = f"test_{math.floor(random.random() * 100):d}"  # noqa: S311
        _ = expr.vars[var_name]
        var_name = next(iter(expr.vars))
        self.assertIsInstance(var_name, str)
        del expr.vars[var_name]

    def test_update_name(self):
        original_name, updated_name = "original_name", "updated_name"
        client, expr = create_client_and_experiment(original_name)
        expr = client.get_experiment_by_name(expr.name)
        self.assertIsNotNone(expr, "experiment with original name not found")
        # update the name and see if it works
        expr.name = updated_name
        expr = client.get_experiment_by_name(expr.name)
        self.assertIsNotNone(expr, "experiment with updated name not found")
        self.assertEqual(expr.name, "updated_name")


if __name__ == "__main__":
    unittest.main()
