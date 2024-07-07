import shutil

from octoflow.pipeline.task import Task, TaskManager
from octoflow.tracking.run import Run


class SampleTask(Task):
    message: str = "Hello, world!"

    def run(self, run: Run) -> None:
        print(self.message)


tasks = TaskManager(
    "./examples/runs",
    [
        SampleTask(),
        SampleTask(message="Hello, universe!"),
    ],
)

tasks.run(0)

for i, _, runs in tasks.iter_task_runs():
    if runs:
        continue
    tasks.run(i)

for _, _, runs in tasks.iter_task_runs():  # clean up
    for run in runs:
        # remove the run directory
        shutil.rmtree(run.path)
