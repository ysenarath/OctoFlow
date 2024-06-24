from __future__ import annotations

import enum
import json
import sqlite3 as sqlite
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import pandas as pd
from typing_extensions import Literal

from octoflow.tracking.artifact.handler import (
    get_handler_type,
    get_handler_type_by_object,
    get_handler_type_by_path,
)
from octoflow.utils import string
from octoflow.utils.collections import flatten

__all__ = [
    "Run",
    "RunState",
]


class RunState(enum.Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    RUNNING = "running"


TABLE_CREATE_SQL = """CREATE TABLE IF NOT EXISTS runs (
id INTEGER PRIMARY KEY AUTOINCREMENT,
key TEXT,
value TEXT,
type TEXT,
step INTEGER,
FOREIGN KEY (step) REFERENCES runs (id) ON DELETE CASCADE,
CHECK (type IN ('param', 'metric'))
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
    def __init__(self, path: Union[Path, str], name: str):
        self.path = self._prepare(path, name)
        self.state = RunState.RUNNING

    @property
    def state(self) -> RunState:
        if not (self.path / "state").exists():
            return RunState.FAILED
        return RunState[
            (self.path / "state").read_text(encoding="utf-8").strip()
        ]

    @state.setter
    def state(self, value: str) -> None:
        if not isinstance(value, RunState):
            # convert to enum to validate
            value = RunState(value)
        (self.path / "state").write_text(value.name, encoding="utf-8")

    @staticmethod
    def _prepare(path: Union[str, Path], name: str) -> Path:
        path = Path(path)
        version = 0
        while True:
            # version starts from 1
            version += 1
            run_path = path / f"{name}-v{version}"
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
                (key, json.dumps(value), type, step),
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
        self, __key: str, __obj: Any, /, *args: Any, **kwargs: Any
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

    def get_values(self, var: Union[str, Tuple[str], None] = None) -> dict:
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
        nodes = {
            id: (key, json.loads(value), type)
            for id, key, value, type, _ in results
        }
        tree = build_nested_tree(nodes, edges)
        values = filter_by_var(tree, var)
        return pd.DataFrame(values)

    @classmethod
    def from_existing(
        cls,
        path: Union[Path, str],
        name: str,
        *,
        version: Union[int, Literal["latest"], None] = "latest",
    ) -> Run:
        self = cls.__new__(cls)
        path = Path(path)
        if version is None:
            version = "latest"
        if version == "latest":
            version = 0
            for temp_path in path.glob(f"{name}-v*"):
                temp_version = int(temp_path.name.rsplit("-v", maxsplit=1)[-1])
                version = max(version, temp_version)
            if version == 0:
                msg = f"run '{name}' not found in '{path}'"
                raise FileNotFoundError(msg)
        else:
            temp_path = path / f"{name}-v{version}"
            if not temp_path.exists():
                msg = f"run '{name}' with version '{version}' not found"
                raise FileNotFoundError(msg)
        self.path = path / f"{name}-v{version}"
        return self

    @classmethod
    def parse_path(cls, path: Path) -> Tuple[Path, str, int]:
        try:
            if not path.is_dir():
                raise ValueError
            name, version = path.name.rsplit("-v", maxsplit=1)
            version = int(version)
        except ValueError:
            msg = f"invalid run path '{path}'"
            raise ValueError(msg) from None
        return RunArgs(path.parent, name, version)


class RunArgs(NamedTuple):
    path: Path
    name: str
    version: int


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


def _filter_by_var(tree: dict, var: List[str]) -> Any:
    if len(var) == 0:
        return []
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
        for item in _filter_by_var(subtree, var.copy()):
            item.update({key: value})
            output.append(item)
    return output


def filter_by_var(tree: dict, var: Union[str, Tuple[str], None]) -> dict:
    if var is None:
        return [tree]
    if not isinstance(var, str):
        var = string.join(var, ".")
    var = string.split(var, ".")
    return _filter_by_var(tree, var)
