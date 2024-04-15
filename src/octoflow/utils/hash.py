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


@runtime_checkable
class Hashable(Protocol):
    def __octoflow_update_hash__(self, m: xxhash.xxh64):  # noqa: PLW3201
        m.update(dill.dumps(self))


def hash(value: Any, *, __m: Optional[xxhash.xxh64] = None) -> str:
    m = xxhash.xxh64() if __m is None else __m
    header = f"=={type(value)}=="
    m.update(header.encode("utf-8"))
    try:
        for x in [value] if isinstance(value, bytes) else value:
            m.update(x)
    except TypeError:
        if isinstance(value, Hashable):
            value.__octoflow_update_hash__(m)
        else:
            m.update(dill.dumps(value))
    return m.hexdigest()


class HashUpdater:
    def __init__(
        self,
        mode: Union[
            Literal["dill", "pickle", "pkl", "source", "src"],
            callable,
        ],
        version: Optional[str] = None,
        obj: Any = None,
    ) -> None:
        self.mode = mode
        self.version = version
        self.obj = obj

    def __call__(self, m: xxhash.xxh64) -> None:
        # add a sub header
        header = f"=={type(self.obj)}=="
        m.update(header.encode("utf-8"))
        # add the version
        version_hash = self.version.encode("utf-8") if self.version else b""
        m.update(version_hash)
        # add the object hash
        if self.mode == "dill":
            val_hash = dill.dumps(self.obj)
        elif self.mode in {"pickle", "pkl"}:
            val_hash = pickle.dumps(self.obj)
        elif self.mode in {"source", "src"}:
            val_hash = inspect.getsource(self.obj).encode("utf-8")
        elif callable(self.mode):
            val_hash = self.mode(self.obj)
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
    obj: T = None,
) -> T:
    if obj is None:
        return functools.partial(hashable, mode, version)
    obj.__octoflow_update_hash__ = HashUpdater(mode, version, obj)
    return obj
