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
from typing_extensions import ParamSpec

__all__ = [
    "UpdateHashFunc",
    "hash",
    "hashable",
]

T = TypeVar("T")
P = ParamSpec("P")

ModeType = Union[
    Literal["dill", "pickle", "pkl", "source", "src"],
    callable,
]


@runtime_checkable
class Hashable(Protocol):
    def _update_hash(self, m: xxhash.xxh64):
        raise NotImplementedError


def _update_hash_header(
    m: xxhash.xxh64,
    obj: Any,
    *,
    name: Optional[str] = None,
    deep: Optional[bool] = None,
) -> None:
    if name is None:
        module = obj.__class__.__module__
        if module is None or module == str.__class__.__module__:
            name = obj.__class__.__name__
        else:
            name = module + "." + obj.__class__.__name__
    m.update(f"==Type:{name}==".encode())


def _update_hash_bytes(
    m: xxhash.xxh64,
    obj: bytes,
    *,
    name: Optional[str] = None,
    deep: Optional[bool] = None,
) -> None:
    _update_hash_header(m, obj, name=name)
    m.update(obj)


def _update_hash_mapping(
    m: xxhash.xxh64,
    obj: Mapping,
    *,
    name: Optional[str] = None,
    deep: bool = False,
) -> None:
    _update_hash_header(m, obj, name=name)
    for key, value in obj.items():
        _update_hash(
            m,
            (key, value),
            name="tklearn.utils.hash.MappingItem",
            deep=deep,
        )


def _update_hash_sequence(
    m: xxhash.xxh64,
    obj: Sequence,
    *,
    name: Optional[str] = None,
    deep: bool = False,
) -> None:
    _update_hash_header(m, obj, name=name)
    for item in obj:
        _update_hash(
            m,
            item,
            name="tklearn.utils.hash.SequenceItem",
            deep=deep,
        )


def _update_hash_partial(
    m: xxhash.xxh64,
    obj: functools.partial,
    *,
    name: Optional[str] = None,
    deep: bool = False,
) -> None:
    _update_hash_header(m, obj, name=name)
    _update_hash(m, obj.func)
    _update_hash_sequence(
        m, obj.args, name="tklearn.utils.hash.PartialArgs", deep=deep
    )
    _update_hash_mapping(
        m,
        obj.keywords,
        name="tklearn.utils.hash.PartialKeywords",
        deep=deep,
    )


def _update_hash_any(
    m: xxhash.xxh64,
    obj: Any,
    *,
    name: Optional[str] = None,
    deep: Optional[bool] = None,
) -> None:
    _update_hash_header(m, obj, name=name)
    try:
        m.update(pickle.dumps(obj))
    except Exception:
        m.update(dill.dumps(obj, recurse=True))


def _update_hash(
    m: xxhash.xxh64,
    obj: Any,
    *,
    name: Optional[str] = None,
    deep: bool = False,
) -> None:
    """Update the hash with the object.

    Parameters
    ----------
    m : xxhash.xxh64
        The hash object.
    obj : Any
        The object to hash.
    name : str, optional
        The name of the object, by default None.
    deep : bool, optional
        Whether to hash the object recursively, by default False.
    """
    if isinstance(obj, bytes):
        _update_hash_bytes(m, obj, name=name)
    elif isinstance(obj, Hashable):
        obj._update_hash(m)
    elif isinstance(obj, functools.partial):
        _update_hash_partial(m, obj, name=name, deep=deep)
    elif deep and isinstance(obj, Mapping):
        _update_hash_mapping(m, obj, name=name, deep=deep)
    elif deep and isinstance(obj, Sequence) and not isinstance(obj, str):
        _update_hash_sequence(m, obj, name=name, deep=deep)
    else:
        _update_hash_any(m, obj, name=name)


def update_hash(m: xxhash.xxh64, obj: Any, deep: bool = False) -> None:
    """Update the hash with the object.

    Parameters
    ----------
    m : xxhash.xxh64
        The hash object.
    obj : Any
        The object to hash.
    deep : bool, optional
        Whether to hash the object recursively, by default False.
    """
    _update_hash(m, obj, deep=deep)


def hash(*value: Any, deep: bool = False) -> str:
    """Hash the object.

    Parameters
    ----------
    value : Any
        The object to hash.
    deep : bool, optional
        Whether to hash the object recursively, by default False.

    Returns
    -------
    str
        The hash value.
    """
    m = xxhash.xxh64()
    _update_hash_header(m, None, name="tklearn.utils.hash.hash")
    for item in value:
        update_hash(m, item, deep=deep)
    return m.hexdigest()


class UpdateHashFunc:
    def __init__(
        self,
        obj: Any,
        *,
        mode: str,
        version: Optional[str] = None,
        name: Optional[str] = None,
    ) -> None:
        self.obj = obj
        self.mode = mode
        self.version = version
        self.name = name

    def _update_hash_header(self, m: xxhash.xxh64) -> None:
        _update_hash_header(m, self.obj, name=self.name)
        # add version to hash
        version = "" if self.version is None else self.version
        m.update(f"==Version:{version}==".encode())

    def __call__(self, m: xxhash.xxh64) -> None:
        self._update_hash_header(m)
        if self.mode == "dill":
            repr_ = dill.dumps(self.obj, recurse=True, byref=True)
        elif self.mode in {"pickle", "pkl"}:
            # works for pickleable objects
            repr_ = pickle.dumps(self.obj)
        elif self.mode in {"source", "src"}:
            # this works for functions and classes
            repr_ = inspect.getsource(self.obj).encode("utf-8")
        elif callable(self.mode):
            repr_ = self.mode(self.obj)
        else:
            msg = f"invalid mode: {self.mode}"
            raise ValueError(msg)
        m.update(repr_)


def hashable(
    mode: ModeType = "dill", version: None[str] = None, obj: Optional[T] = None
) -> T:
    if obj is None:
        return functools.partial(hashable, mode, version)
    obj._update_hash = UpdateHashFunc(obj, mode=mode, version=version)
    return obj


class InitRecordUpdateHashFunc:
    def __init__(self, cls: T, *args, **kwargs) -> None:
        self.cls = cls
        self.args = args
        self.kwargs = kwargs

    def __call__(self, m: xxhash.xxh64) -> None:
        module = self.cls.__module__
        if module is None or module == str.__class__.__module__:
            name = self.cls.__name__
        else:
            name = module + "." + self.cls.__name__
        _update_hash_header(m, self, name=name)
        _update_hash_sequence(m, self.args, name="tklearn.utils.hash.InitArgs")
        _update_hash_mapping(
            m, self.kwargs, name="tklearn.utils.hash.InitKeywords"
        )


def decorate_init(cls: T) -> T:
    base__init__ = cls.__init__

    def decorated__init__(self, *args, **kwargs) -> None:
        base__init__(self, *args, **kwargs)
        self._init_record_update_hash = InitRecordUpdateHashFunc(
            cls, *args, **kwargs
        )

    cls.__init__ = decorated__init__
    return cls


def use_init_based_hash(obj: T) -> T:
    obj._update_hash = obj._init_record_update_hash
    return obj
