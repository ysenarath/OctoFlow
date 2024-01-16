import os
import time
from pathlib import Path

from octoflow import Client

database_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".." / "logs" / "octoflow.db"

if database_path.exists():
    os.remove(database_path)

client = Client("sqlite:///logs/octoflow.db")

expr = client.create_experiment("sample_experiment_1")

run = expr.start_run()

run.log_param("num_time_steps", 10)
run.log_param("num_epochs", 5)

for time_step in range(1, 3):
    time_step_val = run.log_param("time_step", time_step)
    for epoch in range(1, 3):
        epoch_val = run.log_param("epoch", epoch, step=time_step_val)
        time.sleep(0.1)
        run.log_metric("f1_score", 1 / epoch, step=epoch_val)
        run.log_metric("accuracy", 1 / epoch, step=epoch_val)
    run.log_metrics(
        {
            "final": {
                "f1_score": 0.23,
                "accuracy": 0.23,
            }
        },
        step=time_step_val,
    )

has_completed = run.get_param("completed", False)

print(f"Has completed (Before): {'Yes' if has_completed else 'No'}")

run.log_param("completed", True)

has_completed = run.get_param("completed", False)

print(f"Has completed (After): {'Yes' if has_completed else 'No'}")

logs = run.get_logs()

print(logs.select("num_time_steps", "time_step", "final.f1_score", "epoch", "f1_score"))

print("Done!")
