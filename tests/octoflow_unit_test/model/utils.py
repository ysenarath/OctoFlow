from octoflow.model import Client


def create_experiment(experiment_name: str):
    _, experiment = create_client_and_experiment(experiment_name)
    return experiment


def create_client_and_experiment(experiment_name: str):
    client = Client()
    # delete all experiments if any
    for exp in client.list_experiments():
        exp.delete()
    experiment = client.create_experiment(experiment_name)
    return client, experiment
