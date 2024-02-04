from octoflow.tracking.client import TrackingClient
from octoflow.tracking.store import LocalFileSystemStore


def main():
    store = LocalFileSystemStore("logs/tracking")
    client = TrackingClient(store)
    expr_name = "example_experiment"
    try:
        expr = client.get_experiment_by_name(expr_name)
        for run in expr.search_runs():
            print(run.experiment.name)
        client.delete_experiment(expr)
        print(f"Deleted experiment '{expr_name}'")
    except ValueError as e:
        print(e)
    expr = client.create_experiment(expr_name)
    run = expr.start_run("abc-abcd-1234-1234")
    for step in range(1, 5):
        step_val = run.log_param("step", step)
        for epoch in range(1, 5):
            epoch_val = run.log_param("epoch", epoch, step=step_val)
            run.log_metric("loss", 1 / epoch, step=epoch_val)
        run.log_metric("loss", 1 / step, step=step_val)
    step_val = run.log_metrics({
        "f1-score": 0.9,
        "accuracy": 0.8,
        "validation": {"f1-score": 0.9, "accuracy": 0.8},
    })
    logs = run.get_logs()
    print(logs)


if __name__ == "__main__":
    main()
