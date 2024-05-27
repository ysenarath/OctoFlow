from __future__ import annotations

import enum
import json
import shutil
import sqlite3 as sqlite
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from packaging.version import Version

from octoflow.core import Task
from octoflow.tracking.artifact.handler import (
    get_handler_type,
    get_handler_type_by_object,
    get_handler_type_by_path,
)
from octoflow.utils import hashing
from octoflow.utils.collections import flatten


class RunState(enum.Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    RUNNING = "running"


class Experiment:
    def __init__(
        self,
        path: Union[Path, str],
        /,
        name: str,
        description: Optional[str] = None,
    ):
        self.path = Path(path)
        if not name.isidentifier():
            msg = f"invalid name: {name}"
            raise ValueError(msg)
        self.name = name
        self.description = description
        self.save(exist_ok=True)

    def save(self, exist_ok: bool = False) -> None:
        path = Path(self.path) / self.name / "metadata.json"
        if not exist_ok and path.exists():
            msg = f"already exists: {path}"
            raise FileExistsError(msg)
        path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {"name": self.name, "description": self.description}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(metadata, f)

    @classmethod
    def load(cls, path: Union[Path, str], name: str) -> Optional[Experiment]:
        path = Path(path)
        metadata_path = path / name / "metadata.json"
        if not metadata_path.exists():
            return None
        with open(metadata_path, encoding="utf-8") as f:
            metadata = json.load(f)
        return cls(path, **metadata)

    def start_run(self, name: Union[str, Task]) -> Run:
        return Run(Path(self.path) / self.name / "runs" / name)

    def run_task(self, task: Task) -> None:
        fingerprint = hashing.hash(task.get_params())
        run = self.start_run(fingerprint)
        return task.run(run)

    def cleanup(
        self, name_or_task: Union[str, Task], force: bool = False
    ) -> List[Path]:
        if isinstance(name_or_task, Task):
            run_name = hashing.hash(name_or_task.get_params())
        else:
            run_name = str(name_or_task)
        deleted_paths = []
        if not (self.path / self.name / "runs").exists():
            return deleted_paths
        for dir in (self.path / self.name / "runs").iterdir():
            # if state is COMPLETED, then skip
            run = Run.from_existing(dir)
            if not force and run.state == RunState.COMPLETED:
                continue
            if not dir.name.startswith(f"{run_name}-"):
                continue
            shutil.rmtree(dir)
            deleted_paths.append(dir)
        return deleted_paths

    def get_run(
        self,
        name_or_task: Union[str, Task],
        version: Union[str, Version] = "latest",
        state: Union[RunState, List[RunState], None] = RunState.COMPLETED,
    ) -> Optional[Run]:
        if state is not None:
            if not isinstance(state, list):
                state = [state]
            state = [
                RunState(s.lower()) if isinstance(s, str) else s for s in state
            ]
        if isinstance(name_or_task, Task):
            run_name = hashing.hash(name_or_task.get_params())
        else:
            run_name = str(name_or_task)
        if version == "latest":
            latest_version = None
            for dir in (self.path / self.name / "runs").glob(f"{run_name}-v*"):
                _, vstring = dir.name.rsplit("-v", 1)
                # get the state of the run
                run = Run.from_existing(dir)
                if state is not None and run.state not in state:
                    continue
                if latest_version is None:
                    latest_version = Version(vstring)
                    continue
                latest_version = max(latest_version, Version(vstring))
            if latest_version is None:
                return None
            version = latest_version
        elif not isinstance(version, Version):
            version = Version(version)
        run_path = self.path / self.name / "runs" / f"{run_name}-v{version}"
        if run_path.exists():
            return Run.from_existing(run_path)
        return None


TABLE_CREATE_SQL = """CREATE TABLE IF NOT EXISTS runs (
id INTEGER PRIMARY KEY AUTOINCREMENT,
key TEXT,
value JSON,
type TEXT,
step INTEGER,
FOREIGN KEY (step) REFERENCES runs (id) ON DELETE CASCADE,
CHECK (type IN ('param', 'metric')),
CHECK (key NOT GLOB '*[^a-zA-Z_0-9]*'),
CHECK (key GLOB '[^0-9]*')
)"""

UNIQUE_METRIC_INDEX_SQL = """CREATE UNIQUE INDEX IF NOT EXISTS
ix_runs_key_step_metric ON runs (key, step)
WHERE type = 'metric'"""

UNIQUE_METRIC_NULL_STEP_INDEX_SQL = """CREATE UNIQUE INDEX IF NOT EXISTS
ix_runs_key_null_step_metric ON runs (key)
WHERE type = 'metric' AND step IS NULL"""

UNIQUE_PARAM_INDEX_SQL = """CREATE UNIQUE INDEX IF NOT EXISTS
ix_runs_key_step_param ON runs (key, value, step)
WHERE type = 'param'"""

UNIQUE_PARAM_NULL_STEP_INDEX_SQL = """CREATE UNIQUE INDEX IF NOT EXISTS
ix_runs_key_null_step_param ON runs (key, value)
WHERE type = 'param' AND step IS NULL"""


class Run:
    @classmethod
    def from_existing(cls, path: Union[Path, str]) -> Run:
        self = cls.__new__(cls)
        self.path = Path(path)
        return self

    def __init__(self, path: Union[Path, str]):
        self.path = self._prepare(path)
        self.state = RunState.RUNNING

    @property
    def state(self) -> RunState:
        if not (self.path / "state").exists():
            return RunState.FAILED
        with open(self.path / "state", encoding="utf-8") as f:
            return RunState[f.read()]

    @state.setter
    def state(self, value: str) -> None:
        # convert to enum to validate
        state = RunState(value)
        with open(self.path / "state", "w", encoding="utf-8") as f:
            f.write(state.name)

    @staticmethod
    def _prepare(path: Path) -> Path:
        path = Path(path)
        name = path.name
        version = 0
        while True:
            version += 1
            run_path = path.with_name(f"{name}-v{version}")
            try:
                # try to create the directory
                run_path.mkdir(parents=True, exist_ok=False)
                break
            except FileExistsError:
                # check with the next version
                continue
        conn = sqlite.connect(run_path / "values.db")
        try:
            cursor = conn.cursor()
            cursor.execute(TABLE_CREATE_SQL)
            cursor.execute(UNIQUE_METRIC_INDEX_SQL)
            cursor.execute(UNIQUE_METRIC_NULL_STEP_INDEX_SQL)
            cursor.execute(UNIQUE_PARAM_INDEX_SQL)
            cursor.execute(UNIQUE_PARAM_NULL_STEP_INDEX_SQL)
            conn.commit()
        except sqlite.Error as e:
            conn.rollback()
            msg = f"failed to create database at '{run_path}'"
            raise ValueError(msg) from e
        finally:
            conn.close()
        return run_path

    def log_value(
        self, key: str, value: Any, type: str, step: Optional[int] = None
    ) -> int:
        if value is not None and not isinstance(
            value, (float, int, str, bool)
        ):
            msg = f"invalid value: {value}"
            raise TypeError(msg)
        conn = sqlite.connect(self.path / "values.db")
        # make sure step is a param type
        if step is not None:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM runs WHERE id = ?",
                (step,),
            )
            result = cursor.fetchone()
            if result is None:
                msg = f"step '{step}' not found"
                raise ValueError(msg)
            _, _, _, step_type, _ = result
            if step_type != "param":
                msg = f"step '{step}' is not a param type"
                raise ValueError(msg)
        try:
            cursor = conn.cursor()
            result = cursor.execute(
                "INSERT INTO runs VALUES (NULL, ?, ?, ?, ?)",
                (key, value, type, step),
            )
            conn.commit()
        except sqlite.Error as e:
            conn.rollback()
            msg = f"failed to log {type} '{key}' with value '{value}'"
            raise ValueError(msg) from e
        finally:
            conn.close()
        return result.lastrowid

    def log_param(
        self, key: str, value: Any, *, step: Optional[int] = None
    ) -> int:
        return self.log_value(key, value, "param", step=step)

    def log_params(
        self, params: Dict[str, Any], *, step: Optional[int] = None
    ) -> Dict[str, int]:
        out = {}
        for key, value in flatten(params).items():
            out[key] = self.log_param(key, value, step=step)
        return out

    def log_metric(
        self, key: str, value: Any, *, step: Optional[int] = None
    ) -> int:
        return self.log_value(key, value, "metric", step=step)

    def log_metrics(
        self, metrics: Dict[str, Any], *, step: Optional[int] = None
    ) -> Dict[str, int]:
        out = {}
        for key, value in flatten(metrics).items():
            out[key] = self.log_metric(key, value, step=step)
        return out

    def log_artifact(
        self, __key: str, __obj: Any, *args: Any, **kwargs: Any
    ) -> None:
        handler = get_handler_type_by_object(__obj)
        handler = handler(self.path / "artifacts" / __key)
        return handler.save(__obj, *args, **kwargs)

    def get_artifact(self, __key: str, __type: Optional[str] = None) -> Any:
        if __type is None:
            handler = get_handler_type_by_path(self.path / "artifacts" / __key)
        else:
            handler = get_handler_type(__type)
        handler = handler(self.path / "artifacts" / __key)
        return handler.load()

    def get_values(
        self,
        var: Union[str, Tuple[str], None] = None,
    ) -> dict:
        conn = sqlite.connect(self.path / "values.db")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM runs")
            results = cursor.fetchall()
        except sqlite.Error as e:
            msg = f"failed to get values from '{self.path}'"
            raise ValueError(msg) from e
        finally:
            conn.close()
        edges: Dict[int, List[int]] = {}
        for id, _, _, _, step in results:
            if step is None:
                step = -1
            if step not in edges:
                edges[step] = []
            edges[step].append(id)
        nodes = {id: (key, value, type) for id, key, value, type, _ in results}
        tree = build_nested_tree(nodes, edges)
        values = filter_by_var(tree, var)
        return pd.DataFrame(values)


def build_nested_tree(
    nodes: Dict[int, Tuple[str, Any, str]],
    edges: Dict[int, List[int]],
    root: int = -1,
) -> dict:
    tree = {}
    for id in edges.get(root, []):
        key, value, type = nodes[id]
        if id in edges:
            if type != "param":
                msg = f"invalid type '{type}' for node with children"
                raise ValueError(msg)
            if key not in tree:
                tree[key] = {}
            tree[key][value] = build_nested_tree(nodes, edges, id)
        else:
            tree[key] = value
    return tree


def filter_by_var(
    tree: dict,
    var: Union[str, Tuple[str], None],
    **kwargs,
) -> Any:
    if var is None:
        return [tree]
    if kwargs.get("_vars_preprocess", True):
        if not isinstance(var, str):
            var = ".".join(var)
        var = var.split(".")
    key = var.pop(0)
    if key not in tree:
        msg = f"variable '{(key, *var)}' not found in tree"
        raise KeyError(msg)
    tree = tree[key]
    if len(var) == 0:
        return [{key: tree}]
    if not isinstance(tree, dict):
        msg = f"variable '{(key, *var)}' not found in tree"
        raise KeyError(msg)
    output = []
    for value, subtree in tree.items():
        for item in filter_by_var(
            subtree, var.copy(), **{"_vars_preprocess": False}
        ):
            item.update({key: value})
            output.append(item)
    return output
