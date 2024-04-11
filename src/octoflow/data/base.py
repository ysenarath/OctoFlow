from __future__ import annotations

from typing import Generic, TypeVar

import pyarrow.dataset as ds
from typing_extensions import ParamSpec

__all__ = [
    "PyArrowWrapper",
    "BaseExpression",
    "BaseDataset",
    "BaseDatasetLoader",
]

ArrowType = TypeVar("ArrowType")
P = ParamSpec("P")
R = TypeVar("R")


class PyArrowWrapper(Generic[ArrowType]):
    def __init__(self, wrapped: ArrowType) -> None:
        self._wrapped = wrapped

    def to_pyarrow(self) -> ArrowType:
        return self._wrapped


BaseExpression = PyArrowWrapper[ds.Expression]

BaseDataset = PyArrowWrapper[ds.Dataset]


class BaseDatasetLoader(Generic[P, R]): ...
