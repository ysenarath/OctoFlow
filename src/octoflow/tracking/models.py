from __future__ import annotations

import datetime as dt
from dataclasses import field
from typing import Any, Dict, Iterator, List, Mapping, MutableMapping, Optional, Tuple, Union

from octoflow.tracking import store
from octoflow.tracking.store import StoredModel, TrackingStore, ValueType, VariableType
from octoflow.tracking.utils import ValueTree, flatten

__all__ = [
    "Experiment",
    "Run",
    "Value",
    "Variable",
    "TrackingStore",
]

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class TrackingClient:
    def __init__(self, store: TrackingStore) -> None:
        super().__init__()
        self._store: Optional[TrackingStore] = store

    @property
    def store(self) -> TrackingStore:
        if self._store is None:
            msg = "store not set"
            raise RuntimeError(msg)
        return self._store

    def create_experiment(
        self,
        name: str,
        description: Optional[str] = None,
        artifact_uri: Optional[str] = None,
    ) -> Experiment:
        return self._store.create_experiment(name, description, artifact_uri)

    def get_experiment_by_name(self, name: str) -> Optional[Experiment]:
        return self._store.get_experiment_by_name(name)

    def get_or_create_experiment(
        self,
        name: str,
        *,
        description: Optional[str] = None,
        artifact_uri: Optional[str] = None,
    ) -> Experiment:
        err = None
        try:
            # try to create if exists - if fails then it is there
            return self.create_experiment(
                name,
                description,
                artifact_uri,
            )
        except ValueError as e:
            err = e
        try:
            return self.get_experiment_by_name(name)
        except ValueError as ex:
            if err is not None:
                raise err from ex
            raise ex

    def list_experiments(self):
        return self._store.list_experiments()


class Experiment(StoredModel):
    id: int = field(init=False)
    name: str
    description: Optional[str]
    artifact_uri: Optional[str]

    @store.wrap
    def start_run(self, name: str, description: Optional[str] = None) -> Run:
        return self.store.create_run(self.id, name, description)

    @store.wrap
    def search_runs(self, **kwargs) -> List[Run]:
        return self.store.search_runs(self.id, **kwargs)

    @store.wrap
    def delete_run(self, run: Union[Run, int]) -> None:
        run_id = run.id if isinstance(run, Run) else run
        self.store.delete_run(self.id, run_id)


class TagsMapping(MutableMapping[str, JSONType]):
    def __init__(self, run: Run) -> None:
        self._run = run

    def __getitem__(self, key: str) -> JSONType:
        with self._run._store:
            return self._run.store.get_tag(self._run.id, key)

    def __setitem__(self, key: str, value: JSONType) -> None:
        with self._run._store:
            self._run.store.set_tag(self._run.id, key, value)

    def __delitem__(self, key: str) -> None:
        with self._run._store:
            self._run.store.delete_tag(self._run.id, key)

    @property
    def data(self) -> Dict[str, JSONType]:
        with self._run._store:
            return self._run.store.get_tags(self._run.id)

    def __iter__(self) -> Iterator[str]:
        for key, _ in self.data.items():
            yield key

    def __len__(self) -> int:
        with self._run._store:
            return self._run.store.count_tags(self._run.id)

    def __repr__(self) -> str:
        return repr(self.data)


class Run(StoredModel):
    id: int = field(init=False)
    experiment_id: int
    name: str
    description: Optional[str]
    created_at: Optional[dt.datetime] = None

    tags: MutableMapping[str, JSONType] = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        self.tags = TagsMapping(self)

    @store.wrap
    def log_param(
        self,
        key: str,
        value: ValueType,
        *,
        step: Union[Value, int, None] = None,
    ) -> Value:
        step_id = step.id if isinstance(step, Value) else step
        return self.store.log_value(
            self.id,
            key,
            value,
            step_id=step_id,
            type="param",
            is_step=None,
        )

    @store.wrap
    def log_params(
        self,
        values: Mapping[str, ValueType],
        *,
        step: Optional[Value] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        step_id = step.id if isinstance(step, Value) else step
        input_vals = []
        for key, value in flatten(values, parent_key=prefix).items():
            input_vals.append({
                "key": key,
                "value": value,
                "type": "param",
                "step_id": step_id,
                "is_step": None,
            })
        return self.store.log_values(self.id, input_vals)

    @store.wrap
    def log_metric(
        self,
        key: str,
        value: ValueType,
        *,
        step: Union[Value, int, None] = None,
    ) -> Value:
        step_id = step.id if isinstance(step, Value) else step
        return self.store.log_value(
            self.id,
            key,
            value,
            step_id=step_id,
            type="metric",
            is_step=False,
        )

    @store.wrap
    def log_metrics(
        self,
        values: Mapping[str, ValueType],
        *,
        step: Optional[Value] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        step_id = step.id if isinstance(step, Value) else step
        input_vals = []
        for key, value in flatten(values, parent_key=prefix).items():
            input_vals.append({
                "key": key,
                "value": value,
                "type": "metric",
                "step_id": step_id,
                "is_step": False,
            })
        return self.store.log_values(self.id, input_vals)

    @store.wrap
    def get_values(self) -> List[Tuple[Variable, Value]]:
        return self.store.get_values(self.id)

    @store.wrap
    def get_value_tree(self) -> ValueTree:
        return self.store.get_value_tree(self.id)


class Variable(StoredModel):
    id: int = field(init=False)
    experiment_id: int
    key: str
    parent_id: Optional[int]
    type: Optional[VariableType] = None
    is_step: Optional[bool] = None


class Value(StoredModel):
    id: int = field(init=False)
    run_id: int
    variable_id: int
    value: ValueType
    timestamp: Optional[dt.datetime] = None
    step_id: Optional[int] = None


class RunTags(StoredModel):
    id: int = field(init=False)
    run_id: int
    tag_id: int
    value: JSONType = None


class Tag(StoredModel):
    id: int = field(init=False)
    name: str
