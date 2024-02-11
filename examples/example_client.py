import shutil
from pathlib import Path

from octoflow.tracking import SQLAlchemyTrackingStore, TrackingClient

base_dir = Path("~/Downloads/TempFiles/octoflow").expanduser()
database_path = base_dir / "tracking.db"

if base_dir.exists():
    shutil.rmtree(base_dir)

base_dir.mkdir(parents=True, exist_ok=True)

store = SQLAlchemyTrackingStore(f"sqlite:///{database_path}")

client = TrackingClient(store)

# Create an experiment
expr = client.create_experiment("test_experiment")

run = expr.start_run("test_run", ruid="12")

tag = run.add_tag("test_tag")

tags = run.list_tags()

for tag in tags:
    run.remove_tag(tag)

run.log_param("num_epochs", 10)

for ll_step in range(10):
    ll_step_val = run.log_param("ll_step", ll_step)
    run.log_metric("loss", 0.1 * ll_step, step=ll_step_val)
    for epoch in range(10):
        epoch_val = run.log_param("epoch", epoch, step=ll_step_val)
        # accuracy
        run.log_metric("metrics.accuracy", 0.2 * epoch, step=epoch_val)
        # additional metrics
        run.log_metrics(
            {
                "precision": {
                    "micro": 0.3 * epoch,
                    "macro": 0.4 * epoch,
                },
                "recall": {
                    "micro": 0.5 * epoch,
                    "macro": 0.6 * epoch,
                },
                "f1": {
                    "micro": 0.7 * epoch,
                    "macro": 0.8 * epoch,
                },
            },
            step=epoch_val,
            prefix="metrics",
        )
    run.log_metric("loss", 0.1 * ll_step, step=ll_step_val)

print(store.get_value_tree(run.id))
