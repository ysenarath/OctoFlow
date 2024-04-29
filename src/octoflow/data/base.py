from __future__ import annotations

from typing import Final, Generic, TypeVar

import pyarrow.dataset as ds
from typing_extensions import ParamSpec

__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_FORMAT",
    "BaseDataset",
    "BaseDatasetLoader",
    "BaseExpression",
    "PyArrowWrapper",
]

ArrowType = TypeVar("ArrowType")

P = ParamSpec("P")

R = TypeVar("R")

DEFAULT_BATCH_SIZE: Final[int] = 1_048_576

DEFAULT_FORMAT: Final[str] = "arrow"


class PyArrowWrapper(Generic[ArrowType]):
    def __init__(self, wrapped: ArrowType) -> None:
        self._wrapped_ = wrapped

    @property
    def _wrapped(self) -> ArrowType:
        return self._wrapped_

    @_wrapped.setter
    def _wrapped(self, value: ArrowType) -> None:
        self._wrapped_ = value

    def to_pyarrow(self) -> ArrowType:
        return self._wrapped


BaseExpression = PyArrowWrapper[ds.Expression]

BaseDataset = PyArrowWrapper[ds.Dataset]


class BaseDatasetLoader(Generic[P, R]): ...
