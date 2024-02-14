from __future__ import annotations

import abc
import datetime as dt
import functools
from contextvars import ContextVar
from types import FunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    TypedDict,
    Union,
)

from octoflow.tracking.utils import value_tree
from octoflow.utils.model import ModelBase

if TYPE_CHECKING:
    from octoflow.tracking.models import Experiment, JSONType, Run, RunTags, Value, Variable

__all__ = [
    "TrackingStore",
    "wrap",
]

store_cv = ContextVar("store", default=None)

VariableType = Literal["param", "metric"]

ValueType = Union[str, float, int, bool, None]


class StoredModel(ModelBase):
    def __post_init__(self):
        super().__post_init__()
        store = store_cv.get()
        if store is None:
            msg = "unable to bind object context to a tracking store"
            raise RuntimeError(msg)
        self._store: TrackingStore = store

    @property
    def store(self) -> TrackingStore:
        store = store_cv.get()
        if store is None:
            msg = "tracking store context is not set"
            raise RuntimeError(msg)
        return store


def wrap(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        if isinstance(self, StoredModel):
            store = self._store
        elif isinstance(self, TrackingStore):
            store = self
        with store:
            return method(self, *args, **kwargs)

    return wrapped


class ValueMapping(TypedDict):
    key: str
    value: ValueType
    type: VariableType
    step_id: Optional[int]
    timestamp: Optional[dt.datetime]
    is_step: Optional[bool]


class ValueTuple(NamedTuple):
    key: str
    value: ValueType
    type: VariableType
    step_id: Optional[int] = None
    timestamp: Optional[dt.datetime] = None
    is_step: Optional[bool] = None


class TrackingStoreMetaClass(abc.ABCMeta):
    def __new__(cls, name, bases, attrs: Dict[str, Any], **kwargs):
        for method_name in attrs:
            method = attrs[method_name]
            if method_name.startswith("_"):
                continue
            if not isinstance(method, FunctionType):
                continue
            attrs[method_name] = wrap(method)
        return super().__new__(cls, name, bases, attrs, **kwargs)


class TrackingStore(metaclass=TrackingStoreMetaClass):
    """Abstract class for tracking store.

    This class is used to define the interface for tracking store.
    """

    def __enter__(self):
        if not hasattr(self, "_tokens") or self._tokens is None:
            self._tokens = []
        self._tokens.append(store_cv.set(self))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not hasattr(self, "_tokens"):
            return
        store_cv.reset(self._tokens.pop())

    @abc.abstractmethod
    def create_experiment(
        self,
        name: str,
        description: Optional[str] = None,
        artifact_uri: Optional[str] = None,
    ) -> Experiment:
        raise NotImplementedError

    @abc.abstractmethod
    def list_experiments(self) -> List[Experiment]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_experiment(self, experiment_id: int) -> Experiment:
        raise NotImplementedError

    @abc.abstractmethod
    def get_experiment_by_name(self, name: str) -> Experiment:
        raise NotImplementedError

    @abc.abstractmethod
    def create_run(
        self,
        experiment_id: int,
        name: str,
        description: Optional[str] = None,
    ) -> Run:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_run(self, experiment_id: int, run_id: int) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def list_runs(self, experiment_id: int) -> List[Run]:
        raise NotImplementedError

    @abc.abstractmethod
    def search_runs(self, experiment_id: int, **kwargs) -> List[Run]:
        raise NotImplementedError

    @abc.abstractmethod
    def set_tag(self, run_id: int, name: str, value: JSONType = None) -> RunTags:
        raise NotImplementedError

    @abc.abstractmethod
    def get_tag(self, run_id: int, name: str) -> JSONType:
        raise NotImplementedError

    @abc.abstractmethod
    def get_tags(self, run_id: int) -> Dict[str, JSONType]:
        raise NotImplementedError

    @abc.abstractmethod
    def count_tags(self, run_id: int) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_tag(self, run_id: int, name: str) -> RunTags:
        raise NotImplementedError

    @abc.abstractmethod
    def log_value(
        self,
        run_id: int,
        key: str,
        value: str,
        *,
        step_id: Optional[int] = None,
        type: Optional[VariableType] = None,
        is_step: Optional[bool] = None,
    ) -> Value:
        raise NotImplementedError

    def _log_value(
        self,
        run_id: int,
        value: Union[ValueMapping, ValueTuple],
        *,
        step_id: Optional[int] = None,
        type: Optional[VariableType] = None,
    ) -> Value:
        if isinstance(value, Mapping):
            value = ValueTuple(**value)
        elif isinstance(value, ValueTuple):
            pass
        elif isinstance(value, Tuple):
            value = ValueTuple(*value)
        else:
            msg = f"expected 'dict' or 'tuple', got '{value.__class__.__name__}'"
            raise TypeError(msg)
        return self.log_value(
            run_id,
            value.key,
            value.value,
            step_id=step_id or value.step_id,
            type=type or value.type,
            is_step=value.is_step,
        )

    def log_values(
        self,
        run_id: int,
        values: List[Union[ValueMapping, ValueTuple, Value]],
        *,
        step_id: Optional[int] = None,
        type: Optional[VariableType] = None,
    ) -> List[Value]:
        return [self._log_value(run_id, value, step_id=step_id, type=type) for value in values]

    @abc.abstractmethod
    def get_values(self, run_id: int) -> List[Tuple[Variable, Value]]:
        raise NotImplementedError

    def get_value_tree(self, run_id: int) -> Dict[str, Any]:
        values = self.get_values(run_id)
        nodes = {None: "__root__"}
        is_step = {}
        for var, value in values:
            if var.type == "metric":
                pass
            is_step[value.id] = var.is_step
            nodes[value.id] = (var.key, value.value)
        tree = {}
        for _, value in values:
            sn, vn = nodes[value.step_id], nodes[value.id]
            if vn not in tree:
                tree[vn] = {} if is_step[value.id] else None
            if sn not in tree:
                tree[sn] = {}
            tree[sn][vn] = tree[vn]
        root = tree["__root__"]
        return value_tree(root)

    def import_store(self, other: TrackingStore):
        for other_experiment in other.list_experiments():
            try:
                this_experiment = self.create_experiment(
                    other_experiment.name,
                    description=other_experiment.description,
                    artifact_uri=other_experiment.artifact_uri,
                )
            except ValueError:
                this_experiment = self.get_experiment_by_name(other_experiment.name)
            for other_run in other.list_runs(other_experiment.id):
                this_run = self.create_run(
                    this_experiment.id,
                    other_run.name,
                    description=other_run.description,
                )
                value_map = {}
                for var, other_value in other.get_values(other_run.id):
                    step_id = None if other_value.step_id is None else value_map[other_value.step_id]
                    this_value = self.log_value(
                        this_run.id,
                        var.key,
                        other_value.value,
                        step_id=step_id,
                        type=var.type,
                        is_step=var.is_step,
                    )
                    value_map[other_value.id] = this_value.id
        return self
