import unittest

from octoflow.tracking_db import variable


class TestVariableType(unittest.TestCase):
    def test_enum_values(self):
        self.assertEqual(variable.VariableType.unknown.value, 0)
        self.assertEqual(variable.VariableType.parameter.value, 1)
        self.assertEqual(variable.VariableType.metric.value, 2)

    def test_enum_names(self):
        self.assertEqual(variable.VariableType.unknown.name, "unknown")
        self.assertEqual(variable.VariableType.parameter.name, "parameter")
        self.assertEqual(variable.VariableType.metric.name, "metric")

    def test_enum_iteration(self):
        expected_names = ["unknown", "parameter", "metric"]
        expected_values = [0, 1, 2]
        for var_type in variable.VariableType:
            self.assertIn(var_type.name, expected_names)
            self.assertIn(var_type.value, expected_values)


if __name__ == "__main__":
    unittest.main()
