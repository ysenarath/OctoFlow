import shutil
from pathlib import Path

from octoflow.tracking import SQLAlchemyTrackingStore, TrackingClient
from octoflow.tracking.models import TreeNode

base_dir = Path("~/Downloads/TempFiles/octoflow").expanduser()
database_path = base_dir / "tracking.db"

if base_dir.exists():
    shutil.rmtree(base_dir)

base_dir.mkdir(parents=True, exist_ok=True)

dburi = f"sqlite:///{database_path}"

print(f"Database URI: {dburi}")

store = SQLAlchemyTrackingStore(dburi)

client = TrackingClient(store)

try:
    expr = client.create_experiment("test_experiment")
except ValueError:
    expr = client.get_experiment_by_name("test_experiment")

run = expr.start_run("test_run")

run.tags["octoflow.run.hash"] = "1234567890"

run.log_param("num_epochs", 5)

for ll_step in range(2):
    ll_step_val = run.log_param("ll_step", ll_step)
    for epoch in range(5):
        epoch_val = run.log_param("epoch", epoch, step=ll_step_val)
        # accuracy
        run.log_metric("metrics.accuracy", 0.2 * epoch, step=epoch_val)
        # additional metrics
        run.log_metrics(
            {
                "f1": {
                    "micro": 0.7 * epoch,
                    "macro": 0.8 * epoch,
                },
            },
            step=epoch_val,
            prefix="metrics",
        )
    run.log_metric("loss", 0.1 * ll_step, step=ll_step_val)

# add completed tag
run.tags["octoflow.run.status"] = "completed"

print(run.tags)

values = TreeNode.from_values(run.get_values())

print(values.flatten())
