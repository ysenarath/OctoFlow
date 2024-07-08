from __future__ import annotations

import inspect
from collections import UserDict
from typing import (
    Any,
    Callable,
    Optional,
    Type,
    TypeVar,
    overload,
)

from typing_extensions import ParamSpec, Self

from octoflow.core import OctoFlowMeta
from octoflow.data.dataclass import BaseModel, ModelMeta
from octoflow.tracking import Run
from octoflow.utils import hashing, objects

__all__ = [
    "Task",
    "TaskState",
    "task",
]

T = TypeVar("T")
P = ParamSpec("P")


class TaskState(Run, UserDict):
    def __init__(self, __path: str, __name: Optional[str] = None):
        super().__init__(path=__path, name=__name)
        super(Run, self).__init__()


class Task(BaseModel):
    def get_params(self, deep: bool = True) -> dict:
        if not deep:
            raise NotImplementedError
        return objects.dump(self)

    def set_params(self, **params: Any) -> Self:
        for key, value in params.items():
            if key not in self.__annotations__:
                continue
            value = objects.load(value)
            setattr(self, key, value)

    def hash(self) -> str:
        return hashing.hash(self.get_params())

    @overload
    def run(self) -> Any: ...

    def run(self, state: Optional[TaskState] = None) -> Any:
        raise NotImplementedError

    def copy(self) -> Task:
        return objects.load(self.get_params())


def task(func: Callable[P, T]) -> Type[Task]:
    if hasattr(func, "__octoflow__") and "FunctionTask" in func.__octoflow__:
        return func.__octoflow__["FunctionTask"]

    # Get the function's parameters
    signature = inspect.signature(func)
    parameters = signature.parameters

    # Create field definitions for the dataclass
    annotations = {}
    defaults = {}
    state_arg = None
    for attr, param in parameters.items():
        if issubclass(param.annotation, TaskState):
            if state_arg is not None:
                msg = "only one TaskState parameter is allowed"
                raise ValueError(msg)
            state_arg = attr
            continue
        if param.annotation == inspect.Parameter.empty:
            annotations[attr] = Any
        else:
            annotations[attr] = param.annotation
        if param.default != inspect.Parameter.empty:
            defaults[attr] = param.default

    # run function
    def run(self, state: Optional[TaskState] = None):
        kwargs = {attr: getattr(self, attr) for attr in annotations}
        if state_arg is not None:
            kwargs[state_arg] = state
        return func(**kwargs)

    if not hasattr(func, "__octoflow__"):
        func.__octoflow__ = OctoFlowMeta()

    # create the task
    cls = ModelMeta(
        "FunctionTask",
        (Task,),
        {
            "__module__": func.__module__,
            "__doc__": func.__doc__,
            "__annotations__": annotations,
            "__qualname__": func.__qualname__ + ".__octoflow__.FunctionTask",
            "run": run,
            "__octoflow__": func.__octoflow__,
            **defaults,
        },
    )

    func.__octoflow__["FunctionTask"] = cls

    return cls
