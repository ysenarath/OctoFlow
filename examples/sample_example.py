from octoflow import ns
from octoflow.model import Client

client = Client()

expr = client.create_experiment("sample_experiment_1")

run = expr.start_run()

run.log_param(ns.num_time_steps, 10)
run.log_param(ns.num_epochs, 5)

for time_step in range(1, 11):
    time_step_val = run.log_param(ns.time_step, time_step)
    for epoch in range(1, 6):  # goes from 1 to 5
        epoch_val = run.log_param(ns.epoch, epoch, step=time_step_val)
        run.log_metric(ns.epoch.f1_score, 1 / epoch, step=epoch_val)
    __ = run.log_metrics(
        {
            "f1_score": 0.23,
            "accuracy": 0.23,
        },
        step=time_step_val,
    )

logs = run.get_logs()

print(logs)
