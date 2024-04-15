from __future__ import annotations

import functools
import inspect
import pickle  # noqa: S403
from typing import (
    Any,
    Literal,
    Optional,
    Protocol,
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


def _update_hash(
    object_to_hash: Any,
    m: xxhash.xxh64,
    *,
    mode: Union[
        Literal["dill", "pickle", "pkl", "source", "src"],
        callable,
    ] = "dill",
    version: Optional[str] = None,
) -> None:
    # add a sub header
    header = f"=={type(object_to_hash)}=="
    m.update(header.encode("utf-8"))
    # add the version
    version_hash = version.encode("utf-8") if version else b""
    m.update(version_hash)
    # add the object hash
    if mode == "dill":
        val_hash = dill.dumps(object_to_hash)
    elif mode in {"pickle", "pkl"}:
        val_hash = pickle.dumps(object_to_hash)
    elif mode in {"source", "src"}:
        val_hash = inspect.getsource(object_to_hash).encode("utf-8")
    elif callable(mode):
        val_hash = mode(object_to_hash)
    else:
        msg = f"invalid mode: {mode}"
        raise ValueError(msg)
    m.update(val_hash)


@runtime_checkable
class Hashable(Protocol):
    def _update_hash(self, m: xxhash.xxh64):
        _update_hash(self, m)


def hash(value: Any, *, __m: Optional[xxhash.xxh64] = None) -> str:
    m = xxhash.xxh64() if __m is None else __m
    header = f"=={type(value)}=="
    m.update(header.encode("utf-8"))
    try:
        for x in [value] if isinstance(value, bytes) else value:
            m.update(x)
    except TypeError:
        if isinstance(value, Hashable):
            value._update_hash(m)
        else:
            m.update(dill.dumps(value))
    return m.hexdigest()


def hashable(
    mode: Union[
        Literal["dill", "pickle", "pkl", "source", "src"],
        callable,
    ] = "dill",
    version: None[str] = None,
    obj: Optional[T] = None,
) -> T:
    if obj is None:
        return functools.partial(hashable, mode, version)
    obj._update_hash = functools.partial(
        _update_hash,
        obj,
        mode=mode,
        version=version,
    )
    return obj
