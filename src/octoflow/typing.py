from typing import Generic, TypeVar

__all__ = [
    "Property",
]

T = TypeVar("T")


class Property(Generic[T]):
    def __get__(self, obj, objtype=None) -> T: ...
    def __set__(self, obj, value: T) -> None: ...
    def __delete__(self, obj): ...
