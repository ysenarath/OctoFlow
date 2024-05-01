from __future__ import annotations

import functools
import inspect
import pickle  # noqa: S403
from typing import (
    Any,
    Literal,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    TypeVar,
    Union,
    runtime_checkable,
)

import dill  # noqa: S403
import xxhash

__all__ = [
    "hash",
]

T = TypeVar("T")


@runtime_checkable
class Hashable(Protocol):
    def _update_hash(self, m: xxhash.xxh64):
        raise NotImplementedError


def _update_hash_header(
    obj: Any, m: xxhash.xxh64, name: Optional[str] = None
) -> None:
    if name is None:
        module = obj.__class__.__module__
        if module is None or module == str.__class__.__module__:
            name = obj.__class__.__name__
        else:
            name = module + "." + obj.__class__.__name__
    header = f"=={name}=="
    m.update(header.encode("utf-8"))


def _update_hash_bytes(
    obj: bytes, m: xxhash.xxh64, *, name: Optional[str] = None
) -> None:
    _update_hash_header(obj, m, name=name)
    m.update(obj)


def _update_hash_partial(
    obj: functools.partial, m: xxhash.xxh64, *, name: Optional[str] = None
) -> None:
    _update_hash_header(obj, m, name=name)
    _update_hash(obj.func, m, name="tklearn.utils.hash.PartialFunc")
    _update_hash(obj.args, m, name="tklearn.utils.hash.PartialArgs")
    _update_hash(obj.keywords, m, name="tklearn.utils.hash.PartialKeywords")


def _update_hash_mapping(
    obj: Mapping, m: xxhash.xxh64, *, name: Optional[str] = None
) -> None:
    _update_hash_header(obj, m, name=name)
    for key, value in obj.items():
        _update_hash((key, value), m, name="tklearn.utils.hash.MappingItem")


def _update_hash_sequence(
    obj: Sequence, m: xxhash.xxh64, *, name: Optional[str] = None
) -> None:
    _update_hash_header(obj, m, name=name)
    for item in obj:
        _update_hash(item, m, name="tklearn.utils.hash.SequenceItem")


def _update_hash_any(
    obj: Any, m: xxhash.xxh64, *, name: Optional[str] = None
) -> None:
    _update_hash_header(obj, m, name=name)
    m.update(dill.dumps(obj))


def _update_hash(
    obj: Any, m: xxhash.xxh64, *, name: Optional[str] = None
) -> None:
    if isinstance(obj, bytes):
        _update_hash_bytes(obj, m, name=name)
    elif isinstance(obj, functools.partial):
        _update_hash_partial(obj, m, name=name)
    elif isinstance(obj, Mapping):
        _update_hash_mapping(obj, m, name=name)
    elif isinstance(obj, Sequence) and not isinstance(obj, str):
        _update_hash_sequence(obj, m, name=name)
    elif isinstance(obj, Hashable):
        obj._update_hash(m)
    else:
        _update_hash_any(obj, m, name=name)


def hash(value: Any, *, __m: Optional[xxhash.xxh64] = None) -> str:
    m = xxhash.xxh64() if __m is None else __m
    _update_hash(value, m)
    return m.hexdigest()


class UpdateHashFunc:
    def __init__(self, func: Any, *, mode, version) -> None:
        self.func = func
        self.mode = mode
        self.version = version

    def __call__(self, m: xxhash.xxh64) -> None:
        _update_hash_header(self.func, m, name=None)
        if self.mode == "dill":
            val_hash = dill.dumps(self.func)
        elif self.mode in {"pickle", "pkl"}:
            val_hash = pickle.dumps(self.func)
        elif self.mode in {"source", "src"}:
            val_hash = inspect.getsource(self.func).encode("utf-8")
        elif callable(self.mode):
            val_hash = self.mode(self.func)
        else:
            msg = f"invalid mode: {self.mode}"
            raise ValueError(msg)
        m.update(val_hash)


def hashable(
    mode: Union[
        Literal["dill", "pickle", "pkl", "source", "src"],
        callable,
    ] = "dill",
    version: None[str] = None,
    func: Optional[T] = None,
) -> T:
    if func is None:
        return functools.partial(hashable, mode, version)
    func._update_hash = UpdateHashFunc(func, mode=mode, version=version)
    return func
