from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, List, Tuple, Union

from typing_extensions import Self

from octoflow.data.dataclass import BaseModel
from octoflow.tracking.experiment import Experiment
from octoflow.tracking.run import Run, RunState
from octoflow.utils import hashing, objects

__all__ = [
    "Module",
    "Task",
    "TaskManager",
]


class Module(BaseModel):
    def get_params(self, deep: bool = True) -> dict:
        if not deep:
            raise NotImplementedError
        return objects.dump(self)

    def set_params(self, **params: Any) -> Self:
        for key, value in params.items():
            if key not in self.__annotations__:
                msg = f"invalid parameter: {key}"
                raise ValueError(msg)
            value = objects.load(value)
            setattr(self, key, value)


class Task(Module):
    def run(self, run: Run) -> None:
        raise NotImplementedError


class TaskManager:
    def __init__(
        self,
        path: Union[Path, str],
        tasks: Union[Task, Iterable[Task]],
        validate: bool = True,
    ) -> None:
        self.path = Path(path)
        if isinstance(tasks, Task):
            tasks = [tasks]
        # sequential execution of each task in the list
        # just like applying a function to each element in a list
        for i, task in enumerate(tasks):
            hash = hashing.hash(task.get_params())
            # try to write the hash of the task to a file
            # to keep track of the task that was run
            hash_filepath = self.path / f"task-{i}.hash"
            try:
                with open(hash_filepath, "x", encoding="utf-8") as f:
                    f.write(hash)
            except FileExistsError as e:
                if not validate:
                    continue
                existing_hash = hash_filepath.read_text()
                if existing_hash.strip() != hash.strip():
                    msg = "integrity error: task hash mismatch"
                    raise ValueError(msg) from e
        self._tasks = tuple(tasks)

    @property
    def tasks(self) -> Tuple[Task, ...]:
        return self._tasks

    def iter_task_runs(self) -> Iterable[Tuple[int, Task, List[Run]]]:
        expr = Experiment(self.path)
        for i in range(len(self.tasks)):
            hash_filepath = self.path / f"task-{i}.hash"
            if not hash_filepath.exists():
                msg = "task hash file not found"
                raise ValueError(msg)
            hash = hash_filepath.read_text().strip()
            runs = expr.search_runs(
                name_regex=f"^{hash}$",
                state=[RunState.COMPLETED, RunState.FAILED, RunState.RUNNING],
            )
            yield i, self.tasks[i], runs

    def run(self, index: int) -> Task:
        hash_filepath = self.path / f"task-{index}.hash"
        hash = hash_filepath.read_text().strip()
        expr = Experiment(self.path)
        run = expr.start_run(hash)
        self.tasks[index].run(run)
