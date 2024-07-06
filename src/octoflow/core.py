from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, List, Tuple, Union

from typing_extensions import Self

from octoflow.data.dataclass import BaseModel
from octoflow.exceptions import IntegrityError
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
        """
        Initialize the TaskManager.

        Parameters
        ----------
        path : Union[Path, str]
            Path to store task hashes.
        tasks : Union[Task, Iterable[Task]]
            A single Task or an iterable of Tasks.
        validate : bool, optional
            If True, validate task integrity against existing hashes (default is True).
        """
        self.path = Path(path)
        if isinstance(tasks, Task):
            tasks = [tasks]
        for i, task in enumerate(tasks):
            hash = self._hash_task(task)
            hash_filepath = self.path / f"task-{i}.hash"
            try:
                with open(hash_filepath, "x", encoding="utf-8") as f:
                    f.write(hash)
            except FileExistsError as e:
                if not validate:
                    continue
                existing_hash = hash_filepath.read_text().strip()
                if existing_hash != hash.strip():
                    msg = f"task {i} hash mismatch: {existing_hash} != {hash}"
                    raise IntegrityError(msg) from e
        self._tasks = tuple(tasks)

    @staticmethod
    def _hash_task(task: Task) -> str:
        """
        Hash a task's parameters.

        Parameters
        ----------
        task : Task
            The task to hash.

        Returns
        -------
        str
            A string representation of the task hash.
        """
        return hashing.hash(task.get_params())

    @property
    def tasks(self) -> Tuple[Task, ...]:
        """
        Get the tuple of tasks.

        Returns
        -------
        Tuple[Task, ...]
            A tuple containing all tasks.
        """
        return self._tasks

    def iter_task_runs(self) -> Iterable[Tuple[int, Task, List[Run]]]:
        """
        Iterate over tasks and their runs.

        Yields
        ------
        Tuple[int, Task, List[Run]]
            A tuple containing the task index, the task itself, and a list of its runs.

        Raises
        ------
        FileNotFoundError
            If a task hash file is not found.
        """
        expr = Experiment(self.path)
        for i, task in enumerate(self.tasks):
            hash_filepath = self.path / f"task-{i}.hash"
            if not hash_filepath.exists():
                msg = f"task hash file not found for task {i}: {hash_filepath}"
                raise FileNotFoundError(msg)
            hash = hash_filepath.read_text().strip()
            runs = expr.search_runs(
                name_regex=f"^{hash}$",
                state=[RunState.COMPLETED, RunState.FAILED, RunState.RUNNING],
            )
            yield i, task, runs

    def run(self, index: int) -> None:
        """
        Run a specific task by index.

        Parameters
        ----------
        index : int
            Index of the task to run.

        Raises
        ------
        IndexError
            If the index is out of range.
        RuntimeError
            If the task execution fails.
        """
        if index < 0 or index >= len(self.tasks):
            msg = f"task index {index} is out of range"
            raise IndexError(msg)
        hash_filepath = self.path / f"task-{index}.hash"
        hash = hash_filepath.read_text().strip()
        expr = Experiment(self.path)
        run = expr.start_run(hash)
        try:
            self.tasks[index].run(run)
        except Exception as e:
            msg = f"task {index} execution failed: {e!s}"
            raise RuntimeError(msg) from e
