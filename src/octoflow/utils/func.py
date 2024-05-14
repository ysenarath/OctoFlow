from __future__ import annotations

import functools
import inspect
from typing import Callable, TypeVar, Union

from typing_extensions import ParamSpec

__all__ = [
    "bind",
]

P = ParamSpec("P")
T = TypeVar("T")


def bind(
    __func: Callable[P, T], *args: P.args, **kwargs: P.kwargs
) -> Union[functools.partial, Callable[..., T]]:
    """Bind arguments to a function and return a new function.

    This function is similar to `functools.partial` but it allows to bind
    arguments by name and by position (converting positional arguments to
    keyword arguments when possible).

    Parameters
    ----------
    __func : Callable
        The function to bind arguments to.
    *args : P.args
        Positional arguments to bind to the function.
    **kwargs : P.kwargs
        Keyword arguments to bind to the function.

    Returns
    -------
    functools.partial or Callable
        A new function with the arguments bound.
    """
    args: list = list(args)
    parameters = inspect.signature(__func).parameters
    arg_names = []
    part_args = []
    part_kwargs = {}
    extra_args = []
    for param in parameters.values():
        default = param.default
        if param.kind == param.POSITIONAL_ONLY and len(args) > 0:
            arg = args.pop(0)
            part_args.append(arg)
            arg_names.append(param.name)
        elif param.kind == param.VAR_POSITIONAL:
            while len(args) > 0:
                arg = args.pop(0)
                part_args.append(arg)
                arg_names.append(param.name)
        elif param.kind == param.POSITIONAL_OR_KEYWORD:
            if len(args) > 0:
                value = args.pop(0)
                if param.name in kwargs:
                    extra_args.append(value)
                    value = kwargs.pop(param.name)
                part_kwargs[param.name] = value
            else:
                part_kwargs[param.name] = kwargs.get(param.name, default)
        elif param.kind == param.KEYWORD_ONLY and param.name in kwargs:
            part_kwargs[param.name] = kwargs.pop(param.name)
        elif param.kind == param.VAR_KEYWORD:
            while len(kwargs) > 0:
                key, value = kwargs.popitem()
                part_kwargs[key] = value
    if len(args) > 0 or len(extra_args) > 0:
        msg = f"too many arguments for function {__func.__name__}"
        raise TypeError(msg)
    return functools.partial(__func, *part_args, **part_kwargs)
