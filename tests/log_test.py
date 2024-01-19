import unittest

from octoflow.tracking import Client

EXPERIMENT_DESCRIPTION = "This is a test experiment"


class TestLogging(unittest.TestCase):
    def setUp(self):
        client = Client()
        experiment = client.create_experiment(
            name="Test Experiment",
            description=EXPERIMENT_DESCRIPTION,
            return_if_exist=False,
        )
        self.run_ = experiment.start_run()

    def test_log_param(self):
        val = self.run_.log_param("num_layers", 3)
        self.assertEqual(val.value, 3)
        self.assertIsNotNone(val.id)

    def test_log_metrics_loop(self):
        num_epochs = 10
        val = self.run_.log_param("num_epochs", num_epochs)
        self.assertEqual(val.value, num_epochs)
        self.assertIsNotNone(val.id)
        for epoch in range(1, num_epochs + 1):  # 1 ... 10
            epoch_val = self.run_.log_param("epoch", epoch)
            accuracy = 0.8 * epoch / 10
            val = self.run_.log_metric("train.accuracy", accuracy, step=epoch_val)
            self.assertEqual(val.value, accuracy)
            self.assertIsNotNone(val.id)
            self.assertIsNotNone(val.step_id)

    def test_log_metric(self):
        val = self.run_.log_metric("test.accuracy", 0.99)
        self.assertEqual(val.value, 0.99)
        self.assertIsNotNone(val.id)


if __name__ == "__main__":
    unittest.main()
