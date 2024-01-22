import functools
import inspect
from collections.abc import Mapping, Sequence
from types import BuiltinFunctionType, FunctionType

import joblib

__all__ = [
    "hash",
]


def partial_to_hashable(obj: functools.partial):
    func = obj.func
    try:
        s = inspect.signature(func)
        bound = s.bind_partial(*obj.args, **obj.keywords)
        bound.apply_defaults()
        hashable_args = create_hashable(bound.arguments)
    except ValueError:
        hashable_args = (
            create_hashable(obj.args),
            create_hashable(obj.keywords),
        )
    hashable_func = create_hashable(func)
    return hashable_func, hashable_args


def create_hashable(obj):
    if isinstance(obj, Mapping):
        hashable_dict = tuple(sorted((k, create_hashable(v)) for k, v in obj.items()))
        return ("dict-like", hashable_dict)
    elif isinstance(obj, Sequence) and not isinstance(obj, str):
        hashable_list = tuple(create_hashable(v) for v in obj)
        return ("list-like", hashable_list)
    elif isinstance(obj, functools.partial):
        partial = partial_to_hashable(obj)
        return ("partial-function", partial)
    elif isinstance(obj, (BuiltinFunctionType, FunctionType)):
        source = inspect.getsource(obj)
        return ("callable", source)
    return ("object", obj)


def hash(obj):
    obj = create_hashable(obj)
    return joblib.hash(obj)
