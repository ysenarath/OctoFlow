from dataclasses import field
from typing import List, Optional

from octoflow.pipeline.task import Task, TaskState


class Pipeline(Task):
    tasks: List[Task] = field(default_factory=list)

    def add_step(self, task: Task):
        self.tasks.append(task)

    def run(self, state: Optional[TaskState] = None):
        for task in self.tasks:
            task.run(state)
