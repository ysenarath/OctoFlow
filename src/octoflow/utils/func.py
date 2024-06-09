from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Generic, Union

from typing_extensions import Concatenate, ParamSpec, Self, TypeVar

__all__ = [
    "MethodMixin",
    "bind",
    "method",
]

P = ParamSpec("P")
T = TypeVar("T")


class Method(Generic[P, T]):
    def __init__(self, container: Any, default: Callable[P, T]) -> None:
        self.container = (
            container  # the instance of the class that this method is bound to
        )
        self.func = default

    def register(self, func: Any) -> Any:
        self.func = func
        return func

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        return self.func(self.container, *args, **kwargs)


class MethodDescriptor(Generic[P, T]):
    def __init__(self, func: Callable[Concatenate[Any, P], T]) -> None:
        super().__init__()
        self.func = func

    def __set_name__(self, owner: Any, name: str) -> None:
        self.name = name
        self.private_name = f"_{name}"

    def __get__(self, instance: Any, owner: Any) -> Method[P, T]:
        if not hasattr(instance, self.private_name):
            method = Method(instance, default=self.func)
            setattr(instance, self.private_name, method)
        return getattr(instance, self.private_name)


def method(func: Callable[Concatenate[Any, P], T]) -> MethodDescriptor[P, T]:
    return MethodDescriptor(func)


class MethodMixin:
    def register(self, name: str, func: Callable) -> Self:
        method = getattr(self, name)
        if not isinstance(method, Method):
            msg = f"'{name}' is not a registerable method"
            raise TypeError(msg)
        method.register(func)
        return self


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
