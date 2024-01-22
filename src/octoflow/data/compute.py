from __future__ import annotations

from typing import Any, Tuple, Union

import pyarrow.dataset as ds
from pyarrow.dataset import field as pyarrow_field
from pyarrow.dataset import scalar as pyarrow_scalar

from octoflow.data.base import PyArrowWrapper

__all__ = [
    "Expression",
]

BaseExpression = PyArrowWrapper[ds.Expression]


class Expression(BaseExpression):
    def __init__(self, expression: Union[Expression, ds.Expression]):
        if isinstance(expression, Expression):
            expression = expression._wrapped
        super().__init__(expression)

    def __eq__(self, other: Any) -> Expression:
        return Expression(self._wrapped == Expression(other)._wrapped)

    def __ne__(self, other: Any) -> Expression:
        return Expression(self._wrapped != Expression(other)._wrapped)

    def __lt__(self, other: Any) -> Expression:
        return Expression(self._wrapped < Expression(other)._wrapped)

    def __le__(self, other: Any) -> Expression:
        return Expression(self._wrapped <= Expression(other)._wrapped)

    def __gt__(self, other: Any) -> Expression:
        return Expression(self._wrapped > Expression(other)._wrapped)

    def __ge__(self, other: Any) -> Expression:
        return Expression(self._wrapped >= Expression(other)._wrapped)

    def __and__(self, other: Any) -> Expression:
        return Expression(self._wrapped & Expression(other)._wrapped)

    def __or__(self, other: Any) -> Expression:
        return Expression(self._wrapped | Expression(other)._wrapped)

    def __invert__(self) -> Expression:
        return Expression(~self._wrapped)

    def is_nan(self) -> Expression:
        return Expression(self._wrapped.is_nan())

    def is_null(self, nan_is_null: bool = False):
        return Expression(self._wrapped.is_null(nan_is_null=nan_is_null))

    def is_valid(self) -> Expression:
        return Expression(self._wrapped.is_valid())

    def isin(self, other: Expression) -> Expression:
        return Expression(self._wrapped.isin(Expression(other)._wrapped))

    def equals(self, other: Expression) -> Expression:
        return Expression(self._wrapped.equals(Expression(other)._wrapped))

    @classmethod
    def field(cls, *name_or_index: Tuple[str]) -> Expression:
        field = pyarrow_field(*name_or_index)
        return cls(field)

    @classmethod
    def scalar(cls, value: Any) -> Expression:
        scaler = pyarrow_scalar(value)
        return cls(scaler)

    def __hash__(self) -> int:
        return hash(self._wrapped)

    def __repr__(self) -> str:
        return f"Expression({self._wrapped})"
