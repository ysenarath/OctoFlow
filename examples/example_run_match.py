import os
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

base_run = run

run = expr.start_run()

run.log_param("num_time_steps", 10)
run.log_param("num_epochs", 5)

matching_runs = list(base_run.match())

print("number of other runs matching base run:", len(list(base_run.match())))
print("other runs matching base run:", matching_runs)

previous_runs_matching_base_run = []

for run_id in base_run.match():
    if run_id < base_run.id:
        previous_runs_matching_base_run.append(run_id)

print(f"previous runs matching base run: {previous_runs_matching_base_run}")

previous_runs_matching_current_run = []

for run_id in run.match():
    if run_id < run.id:
        previous_runs_matching_current_run.append(run_id)

print(f"previous runs matching current run: {previous_runs_matching_current_run}")
