from __future__ import annotations

import datetime as dt
from dataclasses import field
from typing import List, Literal, Mapping, Optional, Union

from octoflow.tracking import store
from octoflow.tracking.store import StoredModel, TrackingStore
from octoflow.tracking.utils import flatten

__all__ = [
    "Experiment",
    "Run",
    "RunTag",
    "Value",
    "Variable",
    "TrackingStore",
]


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

    def list_experiments(self):
        return self._store.list_experiments()


class Experiment(StoredModel):
    id: int = field(init=False)
    name: str
    description: Optional[str]
    artifact_uri: Optional[str]

    @store.wrap
    def start_run(
        self,
        name: str,
        description: Optional[str] = None,
        *,
        ruid: Optional[str] = None,
    ) -> Run:
        return self.store.create_run(
            self.id,
            name,
            description,
            ruid=ruid,
        )


class Run(StoredModel):
    id: int = field(init=False)
    experiment_id: int
    name: str
    description: Optional[str]
    created_at: Optional[dt.datetime] = None
    ruid: Optional[str] = None

    @store.wrap
    def add_tag(self, label: str) -> RunTag:
        return self.store.add_tag(self.id, label)

    @store.wrap
    def remove_tag(self, label: Union[RunTag, str]):
        if isinstance(label, RunTag):
            label = label.label
        self.store.remove_tag(self.id, label)

    @store.wrap
    def list_tags(self):
        return self.store.list_tags(self.id)

    @store.wrap
    def log_param(
        self,
        key: str,
        value: Union[str, int, float, bool],
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
        values: Mapping[str, Union[str, int, float, bool]],
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
        value: Union[str, int, float, bool],
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
        values: Mapping[str, Union[str, int, float, bool]],
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
    def get_values(self) -> List[Value]:
        return self.store.get_values(self.id)


class RunTag(StoredModel):
    id: int = field(init=False)
    run_id: int
    label: str


class Variable(StoredModel):
    id: int = field(init=False)
    experiment_id: int
    key: str
    parent_id: Optional[int]
    type: Optional[Literal["param", "metric"]] = None
    is_step: Optional[bool] = None


class Value(StoredModel):
    id: int = field(init=False)
    run_id: int
    variable_id: int
    value: Union[str, float, int, bool, None]
    timestamp: Optional[dt.datetime] = None
    step_id: Optional[int] = None
