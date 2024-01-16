import os
from pathlib import Path

import pandas as pd

from octoflow import Client

database_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".." / "logs" / "octoflow.db"

if database_path.exists():
    os.remove(database_path)

client = Client("sqlite:///logs/octoflow.db")

expr = client.create_experiment(
    "experiment_log_artifacts",
    artifact_uri="logs/",
)

run = expr.start_run()

num_epochs_val = run.log_param("num_epochs", 5)  # [1 -> 5]

for epoch in range(1, num_epochs_val.value + 1):
    epoch_val = run.log_param("epoch", epoch)
    run.log_metric("train.loss", 1 / epoch, step=epoch_val)
    run.log_metric("valid.loss", 1 / epoch, step=epoch_val)
    run.log_metric("valid.accuracy", 1 / epoch, step=epoch_val)

run.log_metric("test.accuracy", 0.5, step=epoch_val)

# e.g. sentiment analysis example data
pred_df = pd.DataFrame({
    "text": [
        "The movie was great!",
        "The direction was fantastic!",
        "The movie was terrible!",
        "The acting was horrible!",
    ],
    "sentiment": ["positive", "positive", "negative", "negative"],
    "pred": ["positive", "negative", "positive", "negative"],
    "pred_prob": [0.9, 0.4, 0.6, 0.2],
})

run.log_artifact("test.predictions", pred_df, version="1.0.0")
run.log_artifact("test.predictions", pred_df, version="1.0.1")
run.log_artifact("test.predictions", pred_df, version="1.0.2")

# delete the latest version

artifact = run.get_artifact("test.predictions", version="latest")

print("latest version:", artifact.version)

pred_df: pd.DataFrame = artifact.load()

print("loaded predictions of latest version:", pred_df)

print(f"deleting latest version ({artifact.version})...", end=" ")

artifact.delete(unlink=True)

print("success")

artifact = run.get_artifact("test.predictions", version="latest")

print("latest version after deleting:", artifact.version)
