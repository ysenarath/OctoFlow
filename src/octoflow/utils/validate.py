from typing import Callable, Generic, TypeVar
from weakref import WeakKeyDictionary

from typing_extensions import ParamSpec

__all__ = [
    "Validator",
]

P = ParamSpec("P")
T = TypeVar("T")


class Validator(Generic[P, T]):
    def __init__(self, func: Callable[P, T]):
        self.func = func
        self.values = WeakKeyDictionary()

    def __get__(self, instance, owner) -> T:
        if instance is None:
            return self
        return self.values.get(instance)

    def __set__(self, instance, value) -> T:
        self.values[instance] = self.func(instance, value)


def validator(func: Callable[P, T]) -> Validator[P, T]:
    return Validator(func)
