import unittest

from tests.octoflow_unit_test.model.utils import create_experiment


class TestRunMethods(unittest.TestCase):
    def setUp(self):
        self.experiment_name = "experiment_name_1"

    def test_start_run(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        self.assertGreater(run.id, 0)

    def test_created_at(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        self.assertIsNotNone(run.created_at)

    def test_updated_at(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        self.assertIsNotNone(run.updated_at)

    def test_log_scaler_none(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        val_to_test = None
        value = run.log_scaler("octoflow.test_scaler", val_to_test)
        self.assertIsNotNone(value)
        self.assertGreater(value.variable_id, 0)
        self.assertEqual(value.value, val_to_test)
        self.assertIsInstance(value.value, type(val_to_test))

    def test_log_scaler_invalid_variable_name(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        val_to_test = 100
        with self.assertRaises(ValueError):
            _ = run.log_scaler("octoflow.test scaler", val_to_test)
        with self.assertRaises(ValueError):
            _ = run.log_scaler("octoflow.test..scaler", val_to_test)
        with self.assertRaises(ValueError):
            _ = run.log_scaler("octoflow.test$scaler", val_to_test)

    def test_log_scaler_int(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        val_to_test = 100
        value = run.log_scaler("octoflow.test_scaler", val_to_test)
        self.assertIsNotNone(value)
        self.assertGreater(value.variable_id, 0)
        self.assertEqual(value.value, val_to_test)
        self.assertIsInstance(value.value, type(val_to_test))

    def test_log_scaler_int_step(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        step_to_test = 1
        step_val = run.log_scaler("octoflow.step", step_to_test)
        val_to_test = 1
        value = run.log_scaler("octoflow.test_scaler", val_to_test, step=step_val)
        self.assertEqual(value.step_id, step_val.id)

    def test_log_scaler_int_step_id(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        step_to_test = 1
        step_val = run.log_scaler("octoflow.step", step_to_test)
        val_to_test = 1
        value = run.log_scaler("octoflow.test_scaler", val_to_test, step=step_val.id)
        self.assertEqual(value.step_id, step_val.id)

    def test_log_scaler_int_step_parent_step(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        step_to_test = 1
        step_val = run.log_scaler("octoflow.step", step_to_test)
        nested_step_to_test = 2
        nested_step_val = run.log_scaler("octoflow.step.nested_step", nested_step_to_test, step=step_val)
        self.assertEqual(nested_step_val.step_id, step_val.id)
        val_to_test = 1
        value = run.log_scaler("octoflow.step.nested_step.value", val_to_test, step=nested_step_val)
        self.assertEqual(value.step_id, nested_step_val.id)

    def test_log_scaler_float(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        val_to_test = 0.234223
        value = run.log_scaler("octoflow.test_scaler", val_to_test)
        self.assertIsNotNone(value)
        self.assertGreater(value.variable_id, 0)
        self.assertEqual(value.value, val_to_test)
        self.assertIsInstance(value.value, type(val_to_test))

    def test_log_scaler_string(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        val_to_test = "This is an example for a string."
        value = run.log_scaler("octoflow.test_scaler", val_to_test)
        self.assertIsNotNone(value)
        self.assertGreater(value.variable_id, 0)
        self.assertEqual(value.value, val_to_test)
        self.assertIsInstance(value.value, type(val_to_test))

    def test_log_scaler_dict(self):
        expr = create_experiment(self.experiment_name)
        run = expr.start_run()
        val_to_test = {"test_int": 100}
        value = run.log_scaler("octoflow.test_scaler", val_to_test)
        self.assertIsNotNone(value)
        self.assertGreater(value.variable_id, 0)
        self.assertEqual(value.value, val_to_test)
        self.assertIsInstance(value.value, type(val_to_test))
        self.assertNotIsInstance(value.value, str)


if __name__ == "__main__":
    unittest.main()
