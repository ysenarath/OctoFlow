import shutil

from octoflow.tracking import Run

shutil.rmtree("examples/example_run", ignore_errors=True)

run = Run("examples/example_run", "example_run_01")

run.log_param("model", "CNN")
run.log_param("epochs", 10)
run.log_param("batch_size", 32)

run.log_metric("accuracy", 0.95)
run.log_metric("loss", 0.1)
run.log_metrics({"precision": 0.9, "recall": 0.8, "ROC AUC": 0.85})


print(run.get_values())
