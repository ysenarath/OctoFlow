import unittest

from octoflow.model import Client, Experiment

EXPERIMENT_DESCRIPTION = "This is a test experiment"


class TestExperiment(unittest.TestCase):
    def setUp(self):
        self.client = Client()

    def test_create_experiment(self):
        experiment: Experiment = self.client.create_experiment(
            name="Test Experiment",
            description=EXPERIMENT_DESCRIPTION,
            return_if_exist=False,
        )
        self.assertEqual(experiment.name, "Test Experiment")
        self.assertEqual(experiment.description, EXPERIMENT_DESCRIPTION)

    def test_existing_experiment(self):
        experiment: Experiment = self.client.create_experiment(
            name="Test Experiment 2",
            description=EXPERIMENT_DESCRIPTION,
            return_if_exist=False,
        )
        experiment: Experiment = self.client.create_experiment(
            name="Test Experiment 2",
            return_if_exist=True,
        )
        self.assertEqual(experiment.name, "Test Experiment 2")
        self.assertEqual(experiment.description, EXPERIMENT_DESCRIPTION)


if __name__ == "__main__":
    unittest.main()
