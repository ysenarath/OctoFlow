from __future__ import annotations

import abc
import contextlib
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
    from octoflow.tracking.models import Experiment, Run, RunTag, Value, Variable

__all__ = [
    "TrackingStore",
    "wrap",
]

store_cv = ContextVar("store", default=None)


def wrap(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        if isinstance(self, StoredModel):
            with self.get_store():
                return method(self, *args, **kwargs)
        elif isinstance(self, TrackingStore):
            with self:
                return method(self, *args, **kwargs)

    return wrapped


class ValueMapping(TypedDict):
    key: str
    value: Union[str, int, float, bool]
    type: Literal["param", "metric"]
    step_id: Optional[int]
    timestamp: Optional[dt.datetime]
    is_step: Optional[bool]


class ValueTuple(NamedTuple):
    key: str
    value: Union[str, int, float, bool, None]
    type: Literal["param", "metric"]
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
        *,
        ruid: Optional[str] = None,
    ) -> Run:
        raise NotImplementedError

    @abc.abstractmethod
    def list_runs(self, experiment_id: int) -> List[Run]:
        raise NotImplementedError

    @abc.abstractmethod
    def add_tag(self, run_id: int, label: str) -> RunTag:
        raise NotImplementedError

    @abc.abstractmethod
    def remove_tag(self, run_id: int, label: str):
        raise NotImplementedError

    @abc.abstractmethod
    def list_tags(self, run_id: int):
        raise NotImplementedError

    @abc.abstractmethod
    def log_value(
        self,
        run_id: int,
        key: str,
        value: str,
        *,
        step_id: Optional[int] = None,
        type: Optional[str] = None,
        is_step: Optional[bool] = None,
    ) -> Value:
        raise NotImplementedError

    def _log_value(
        self,
        run_id: int,
        value: Union[ValueMapping, ValueTuple],
        step_id: Optional[int] = None,
        type: Optional[str] = None,
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
        step_id: Optional[int] = None,
        type: Optional[str] = None,
    ) -> List[Value]:
        return [self._log_value(run_id, value) for value in values]

    @abc.abstractmethod
    def get_values(self, run_id: int) -> List[Tuple[Variable, Value]]:
        raise NotImplementedError

    def get_value_tree(self, run_id: int) -> Dict[str, Any]:
        values = self.get_values(run_id)
        nodes = {None: "__root__"}
        is_step = {}
        for var, value in values:
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

    def __enter__(self):
        if not hasattr(self, "_tokens") or self._tokens is None:
            self._tokens = []
        self._tokens.append(store_cv.set(self))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not hasattr(self, "_tokens"):
            return
        store_cv.reset(self._tokens.pop())


class StoredModel(ModelBase):
    def __post_init__(self):
        store = store_cv.get()
        if store is None:
            msg = "object not bound to a tracking store"
            raise RuntimeError(msg)
        self._store: TrackingStore = store

    @property
    def store(self) -> TrackingStore:
        store = store_cv.get()
        if store is None:
            msg = "object not bound to a tracking store"
            raise RuntimeError(msg)
        return store

    @contextlib.contextmanager
    def get_store(self) -> TrackingStore:
        token = store_cv.set(self._store)
        try:
            yield self._store
        finally:
            store_cv.reset(token)
