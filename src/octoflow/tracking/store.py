from __future__ import annotations

import contextlib
import json
import os
import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Dict, Generator, Hashable, List, Mapping, Optional, Tuple, TypeVar, Union
from typing import MutableMapping as MutableMappingType

import orjson

from octoflow.tracking.base import Base
from octoflow.tracking.experiment import Experiment, get_experiment_id
from octoflow.tracking.run import EMPTY_DICT, FilterExpression, Run, get_run_id
from octoflow.tracking.value import Value

__all__ = [
    "TrackingStore",
    "LocalFileSystemMap",
]

T = TypeVar("T")


class TrackingStore:
    def create_experiment(self, expr: Experiment) -> Experiment:
        raise NotImplementedError

    def get_experiment(self, id: str) -> Experiment:
        raise NotImplementedError

    def get_experiment_by_name(self, name: str) -> Experiment:
        raise NotImplementedError

    def list_all_experiments(self) -> List[Experiment]:
        raise NotImplementedError

    def create_run(self, run: Run) -> Run:
        raise NotImplementedError

    def get_run(self, expr: Experiment, id: str) -> Run:
        raise NotImplementedError

    def search_runs(
        self,
        expr: Union[Experiment, str],
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Run]:
        raise NotImplementedError

    def log_value(self, value: Value) -> Value:
        raise NotImplementedError

    def bind(
        self,
        obj: Union[
            Base,
            List[Base],
            Dict[Hashable, Base],
            Any,
        ] = None,
    ) -> Base:
        if isinstance(obj, Sequence) and not isinstance(obj, str):
            return [self.bind(o) for o in obj]
        elif isinstance(obj, Mapping):
            return {self.bind(k): self.bind(v) for k, v in obj.items()}
        elif not isinstance(obj, Base):
            return obj
        if obj.store is not None and obj.store is not self:
            msg = "object already bound to a store"
            raise RuntimeError(msg)
        obj.store = self
        # bind nested objects using class annotations
        for key, value in obj.__dict__.items():
            setattr(obj, key, self.bind(value))
        return obj

    def delete_experiment(self, expr: Experiment) -> None:
        raise NotImplementedError

    def delete_run(self, run: Run) -> None:
        raise NotImplementedError

    def has_run(self, expr: Experiment, name: str) -> bool:
        raise NotImplementedError

    def get_logs(self, run: Run, *, filters: FilterExpression = EMPTY_DICT) -> dict:
        raise NotImplementedError


class LocalFileSystemMap(MutableMappingType[str, bytearray]):
    def __init__(self, path: Union[Path, str]) -> None:
        super().__init__()
        self.path: Path = Path(path)

    @classmethod
    def _join_path_flatten(cls, s: Union[T, Sequence[T]]) -> Tuple[T]:
        if isinstance(s, (tuple, list)):
            args_flat = []
            for item in s:
                args_flat.extend(cls._join_path_flatten(item))
            return tuple(args_flat)
        return (s,)

    def _root_path(self, *suffix: Union[str, Path]) -> Path:
        args = self._join_path_flatten(suffix)
        args = list(map(str, args))
        return self.path / os.path.join(*args)

    def __truediv__(self, key: str) -> LocalFileSystemMap:
        return LocalFileSystemMap(self._root_path(key))

    def dirs(self) -> Generator[LocalFileSystemMap, None, None]:
        yield from (LocalFileSystemMap(d) for d in self.path.iterdir() if d.is_dir())

    def __contains__(self, key: str) -> bool:
        return self._root_path(key).exists()

    def __getitem__(self, key: Union[str, tuple]) -> bytearray:
        with open(self._root_path(key), "rb") as f:
            return f.read()

    def __setitem__(self, key: str, value: bytearray) -> None:
        wp = self._root_path(key)
        os.makedirs(wp.parent, exist_ok=True)
        try:
            wp.touch(exist_ok=False)
        except FileExistsError as e:
            raise e
        with open(wp, "wb") as f:
            f.write(value)

    def __delitem__(self, key: str) -> None:
        path = self._root_path(key)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

    def __len__(self) -> int:
        return len(list(self.path.rglob("*")))

    def __iter__(self) -> Generator[str, None, None]:
        yield from (str(p.relative_to(self.path)) for p in self.path.rglob("*"))


class LocalFileSystemStore(TrackingStore):
    def __init__(self, resource_uri: Union[Path, str]) -> None:
        super().__init__()
        self.resource_uri = resource_uri

    def create_experiment(self, expr: Experiment) -> Experiment:
        mapper = LocalFileSystemMap(self.resource_uri)
        if expr.id in mapper:
            msg = f"experiment with name '{expr.name}' already exists"
            raise ValueError(msg)
        expr_path = f"{expr.id}/experiment.json"
        d = expr.to_dict()
        mapper[expr_path] = json.dumps(d).encode()
        return self.bind(expr)

    def get_experiment(self, id: str) -> Experiment:
        mapper = LocalFileSystemMap(self.resource_uri)
        expr_path = f"{id}/experiment.json"
        if expr_path not in mapper:
            msg = f"experiment with id '{id}' does not exist"
            raise ValueError(msg)
        expr = Experiment.from_dict(json.loads(mapper[expr_path]))
        return self.bind(expr)

    def get_experiment_by_name(self, name: str) -> Experiment:
        return self.get_experiment(get_experiment_id(name))

    def list_all_experiments(self) -> List[Experiment]:
        mapper = LocalFileSystemMap(self.resource_uri)
        exprs = []
        for expr_mapper in mapper.dirs():
            expr = Experiment.from_dict(json.loads(expr_mapper["experiment.json"]))
            exprs.append(expr)
        return self.bind(exprs)

    def delete_experiment(self, expr: Experiment) -> None:
        mapper = LocalFileSystemMap(self.resource_uri)
        expr_path = f"{expr.id}/experiment.json"
        if expr_path not in mapper:
            msg = f"experiment with id '{expr.id}' does not exist"
            raise ValueError(msg)
        del mapper[expr.id]

    def create_run(self, run: Run) -> Run:
        run_path = f"{run.experiment.id}/runs/{run.id}/run.json"
        mapper = LocalFileSystemMap(self.resource_uri)
        if run_path in mapper:
            msg = f"run with name '{run.name}' already exists"
            raise ValueError(msg)
        mapper[run_path] = json.dumps(run.to_dict(exclude="experiment")).encode()
        return self.bind(run)

    def get_run(self, expr: Experiment, id: str) -> Run:
        expr_path = f"{expr.id}/experiment.json"
        run_path = f"{expr.id}/runs/{id}/run.json"
        mapper = LocalFileSystemMap(self.resource_uri)
        if run_path not in mapper:
            msg = f"run with id '{id}' does not exist"
            raise ValueError(msg)
        run = json.loads(mapper[run_path])
        run["experiment"] = json.loads(mapper[expr_path])
        run = Run.from_dict(run)
        return self.bind(run)

    def has_run(self, expr: Experiment, name: str) -> bool:
        run_id = get_run_id(name)
        run_path = f"{expr.id}/runs/{run_id}/run.json"
        mapper = LocalFileSystemMap(self.resource_uri)
        return run_path in mapper

    def search_runs(
        self,
        expr: Union[Experiment, str],
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Run]:
        if isinstance(expr, str):
            expr = self.get_experiment_by_name(expr)
        mapper = LocalFileSystemMap(self.resource_uri)
        expr_mapper = mapper / expr.id
        expr_dict = json.loads(expr_mapper["experiment.json"])
        runs = []
        if name is None:
            runs_mapper = expr_mapper / "runs"
            for run_mapper in runs_mapper.dirs():
                try:
                    run = json.loads(run_mapper["run.json"])
                except FileNotFoundError:
                    continue
                run["experiment"] = expr_dict
                run = Run.from_dict(run)
                runs.append(run)
        else:
            run_id = get_run_id(name)
            run_mapper = expr_mapper / "runs" / run_id
            if "run.json" in run_mapper:
                run = None
                with contextlib.suppress(FileNotFoundError):
                    run = json.loads(mapper["run.json"])
                if run is not None:
                    run["experiment"] = expr_dict
                    run = Run.from_dict(run)
                    runs.append(run)
        return self.bind(runs)

    def delete_run(self, run: Run) -> None:
        run_path = f"{run.experiment.id}/runs/{run.id}/run.json"
        mapper = LocalFileSystemMap(self.resource_uri)
        if run_path not in mapper:
            msg = f"run with id '{run.id}' does not exist"
            raise ValueError(msg)
        run_dir = f"{run.experiment.id}/runs/{run.id}"
        # remove the entire run directory
        del mapper[run_dir]

    def log_value(self, value: Value) -> Value:
        expr_id = value.run.experiment.id
        run_id = value.run.id
        path = f"{value.key}"
        temp = value.step
        while temp is not None:
            if temp.id is None:
                msg = "step value must have an id"
                raise ValueError(msg)
            path = f"{temp.key}-{temp.id}/{path}"
            temp = temp.step
        mapper = LocalFileSystemMap(self.resource_uri)
        value_id = 0
        while True:
            value_path = f"{expr_id}/runs/{run_id}/values/{path}-{value_id}/value.json"
            try:
                value.id = value_id
                mapper[value_path] = json.dumps(value.to_dict(exclude=("run", "step"))).encode()
                break
            except FileExistsError:
                value_id += 1
        return self.bind(value)

    def get_logs(self, run: Run, *, filters: FilterExpression = EMPTY_DICT) -> dict:
        values_uri = f"{self.resource_uri}/{run.experiment.id}/runs/{run.id}/values/"
        return self._cls_recursive_walk(values_uri, filters=filters)

    @classmethod
    def _cls_recursive_walk(
        cls,
        values_uri: str,
        *,
        filters: FilterExpression = EMPTY_DICT,
        **kwargs,
    ) -> dict:
        base_path = kwargs.get("base_path", "")
        value_file = f"{values_uri}/value.json"
        value_data = None
        if os.path.exists(value_file) and os.path.isfile(value_file):
            with open(value_file, "rb") as f:
                value_data = orjson.loads(f.read())
        key_path = os.path.join(
            base_path,
            "" if value_data is None else value_data["key"],
        )
        if value_data is not None:  # noqa: SIM102
            if not filters.get("type", {}).get(value_data["type"], True):
                return {}
        values = {}
        for value_dir in os.listdir(values_uri):
            value_dir = os.path.join(values_uri, value_dir)
            if not os.path.isdir(value_dir):
                continue
            for key, value in cls._cls_recursive_walk(
                value_dir,
                filters=filters,
                base_path=key_path,
            ).items():
                if not isinstance(value, dict):
                    value = {value: {}}
                if key in values:
                    existing_values = set(value).intersection(values[key])
                    if len(existing_values) > 0:
                        existing_values_str = ", ".join(map(str, existing_values))
                        qual_key_path = os.path.join(key_path, key)
                        msg = f"values '{existing_values_str}' of '{qual_key_path}' already exists"
                        raise ValueError(msg)
                    values[key].update(value)
                else:
                    values[key] = value
        if value_data is not None and len(values) == 0:
            return {value_data["key"]: value_data["value"]}
        if value_data is not None:
            return {value_data["key"]: {value_data["value"]: values}}
        return values
