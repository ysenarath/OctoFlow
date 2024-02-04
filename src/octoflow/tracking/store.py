from __future__ import annotations

import json
import os
import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Dict, Generator, Hashable, List, Mapping, Optional, Union
from typing import MutableMapping as MutableMappingType

from octoflow.tracking.base import Base
from octoflow.tracking.experiment import Experiment, get_experiment_id
from octoflow.tracking.run import EMPTY_DICT, FilterExpression, Run, get_run_id
from octoflow.tracking.value import Value


class TrackingStore:
    def create_experiment(
        self,
        expr: Experiment,
    ) -> Experiment:
        raise NotImplementedError

    def get_experiment(
        self,
        id: int,
    ) -> Experiment:
        raise NotImplementedError

    def get_experiment_by_name(
        self,
        name: str,
    ) -> Experiment:
        raise NotImplementedError

    def list_all_experiments(self) -> List[Experiment]:
        raise NotImplementedError

    def create_run(
        self,
        run: Run,
    ) -> Run:
        raise NotImplementedError

    def get_run(
        self,
        expr: Union[Experiment, str],
        uuid: str,
    ) -> Run:
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
    def __init__(self, path: str) -> None:
        super().__init__()
        self.path = Path(path)

    def path_to(self, suffix: Union[str, tuple, list]) -> Path:
        if isinstance(suffix, str):
            suffix = (suffix,)
        # make sure suffixes are strings, use map
        suffix = list(map(str, suffix))
        return self.path / os.path.join(*suffix)

    def __contains__(self, key: str) -> bool:
        return self.path_to(key).exists()

    def __getitem__(self, key: Union[str, tuple]) -> bytearray:
        with open(self.path_to(key), "rb") as f:
            return f.read()

    def __setitem__(self, key: str, value: bytearray) -> None:
        wp = self.path_to(key)
        os.makedirs(wp.parent, exist_ok=True)
        try:
            wp.touch(exist_ok=False)
        except FileExistsError as e:
            raise e
        with open(wp, "wb") as f:
            f.write(value)

    def __delitem__(self, key: str) -> None:
        path = self.path_to(key)
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()

    def __len__(self) -> int:
        return len(list(self.path.rglob("*")))

    def __iter__(self) -> Generator[str, None, None]:
        yield from (str(p.relative_to(self.path)) for p in self.path.rglob("*"))


class LocalFileSystemStore(TrackingStore):
    def __init__(self, resource_uri: str) -> None:
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
        for key in mapper:
            if not key.endswith("/experiment.json"):
                continue
            expr = Experiment.from_dict(json.loads(mapper[key]))
            exprs.append(expr)

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
        expr: Experiment,
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Run]:
        expr_path = f"{expr.id}/experiment.json"
        mapper = LocalFileSystemMap(self.resource_uri)
        expr_dict = json.loads(mapper[expr_path])
        runs = []
        if name is None:
            run_prefix = f"{expr.id}/runs/"
            for key in mapper:
                if key.startswith(run_prefix) and key.endswith("/run.json"):
                    run = json.loads(mapper[key])
                    run["experiment"] = expr_dict
                    run = Run.from_dict(run)
                    runs.append(run)
        else:
            run_id = get_run_id(name)
            run_prefix = f"{expr.id}/runs/{run_id}/"
            for key in mapper:
                if key.startswith(run_prefix) and key.endswith("/run.json"):
                    run = json.loads(mapper[key])
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
            with open(value_file, encoding="utf-8") as f:
                value_data = json.load(f)
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
