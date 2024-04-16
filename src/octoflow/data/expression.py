from __future__ import annotations

from operator import itemgetter
from typing import Any, Mapping, Tuple, Union

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow.dataset import field as pyarrow_field
from pyarrow.dataset import scalar as pyarrow_scalar
from typing_extensions import Self

from octoflow.data.base import BaseExpression

__all__ = [
    "Expression",
]


class Expression(BaseExpression):
    """A class representing an expression in Octoflow."""

    def __init__(self, expression: Union[Expression, ds.Expression]):
        """
        Initialize an expression.

        Parameters
        ----------
        expression : Union[Expression, ds.Expression]
            The (pyarrow) expression to wrap.
        """
        if isinstance(expression, Expression):
            expression = expression._wrapped
        super().__init__(expression)

    def __eq__(self, other: Any) -> Expression:
        """
        Compare two expressions for equality.

        Parameters
        ----------
        other : Any
            The other expression to compare to.

        Returns
        -------
        Expression
            The expression representing the result of the comparison.
        """
        return Expression(self._wrapped == Expression(other)._wrapped)

    def __ne__(self, other: Any) -> Expression:
        """
        Compare two expressions for inequality.

        Parameters
        ----------
        other : Any
            The other expression to compare to.

        Returns
        -------
        Expression
            The expression representing the result of the comparison.
        """
        return Expression(self._wrapped != Expression(other)._wrapped)

    def __lt__(self, other: Any) -> Expression:
        """
        Compare two expressions for less than.

        Parameters
        ----------
        other : Any
            The other expression to compare to.

        Returns
        -------
        Expression
            The expression representing the result of the comparison.
        """
        return Expression(self._wrapped < Expression(other)._wrapped)

    def __le__(self, other: Any) -> Expression:
        """
        Compare two expressions for less than or equal to.

        Parameters
        ----------
        other : Any
            The other expression to compare to.

        Returns
        -------
        Expression
            The expression representing the result of the comparison.
        """
        return Expression(self._wrapped <= Expression(other)._wrapped)

    def __gt__(self, other: Any) -> Expression:
        """
        Compare two expressions for greater than.

        Parameters
        ----------
        other : Any
            The other expression to compare to.

        Returns
        -------
        Expression
            The expression representing the result of the comparison.
        """
        return Expression(self._wrapped > Expression(other)._wrapped)

    def __ge__(self, other: Any) -> Expression:
        """
        Compare two expressions for greater than or equal to.

        Parameters
        ----------
        other : Any
            The other expression to compare to.

        Returns
        -------
        Expression
            The expression representing the result of the comparison.
        """
        return Expression(self._wrapped >= Expression(other)._wrapped)

    def __and__(self, other: Any) -> Expression:
        """
        Combine two expressions with a logical and.

        Parameters
        ----------
        other : Any
            The other expression to combine with.

        Returns
        -------
        Expression
            The expression representing the result of the combination.
        """
        return Expression(self._wrapped & Expression(other)._wrapped)

    def __or__(self, other: Any) -> Expression:
        """
        Combine two expressions with a logical or.

        Parameters
        ----------
        other : Any
            The other expression to combine with.

        Returns
        -------
        Expression
            The expression representing the result of the combination.
        """
        return Expression(self._wrapped | Expression(other)._wrapped)

    def __invert__(self) -> Expression:
        """
        Invert an expression.

        Returns
        -------
        Expression
            The expression representing the inverted expression.
        """
        return Expression(~self._wrapped)

    def is_nan(self) -> Expression:
        """
        Check if an expression is NaN.

        Returns
        -------
        Expression
            The expression representing the result of the check.
        """
        return Expression(self._wrapped.is_nan())

    def is_null(self, nan_is_null: bool = False):
        """
        Check if an expression is null.

        Parameters
        ----------
        nan_is_null : bool, optional
            Whether to consider NaN values as null, by default False

        Returns
        -------
        Expression
            The expression representing the result of the check.
        """
        return Expression(self._wrapped.is_null(nan_is_null=nan_is_null))

    def is_valid(self) -> Expression:
        """
        Check if an expression is valid.

        Returns
        -------
        Expression
            The expression representing the result of the check.
        """
        return Expression(self._wrapped.is_valid())

    def isin(self, other: Expression) -> Expression:
        """
        Check if an expression is in a set of values.

        Parameters
        ----------
        other : Expression
            The set of values to check against.

        Returns
        -------
        Expression
            The expression representing the result of the check.
        """
        return Expression(self._wrapped.isin(Expression(other)._wrapped))

    def equals(self, other: Expression) -> Expression:
        """
        Check if an expression is equal to another expression.

        Parameters
        ----------
        other : Expression
            The other expression to check against.

        Returns
        -------
        Expression
            The expression representing the result of the check.
        """
        return Expression(self._wrapped.equals(Expression(other)._wrapped))

    def __hash__(self) -> int:
        """
        Get the hash of the expression.

        Returns
        -------
        int
            The hash of the expression.
        """
        return hash(self._wrapped)

    def __repr__(self) -> str:
        """
        Get the representation of the expression.

        Returns
        -------
        str
            The representation of the expression.
        """
        return f"Expression({self._wrapped!r})"


class Field(Expression):
    def __new__(
        cls,
        field: Union[itemgetter, str, Tuple[str], Self],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Create a new field getter.

        Parameters
        ----------
        field : Union[itemgetter, str, Tuple[str], FieldGetter]
            The field to be accessed.
        args : Any
            Additional arguments.
        kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        FieldGetter
            The field getter.
        """
        if isinstance(field, Field):
            return field
        return super().__new__(cls)

    def __init__(
        self,
        field: Union[str, Tuple[str], itemgetter],
        /,
        type: Union[pa.DataType, None] = None,
        preprocessor: Union[callable, None] = None,
    ):
        """Create a new field getter.

        Parameters
        ----------
        field : Union[itemgetter, str, FieldGetter]
            The field to be accessed.
        type : Union[pa.DataType, None], optional
            The type of the field, by default None.
        preprocessor : Union[callable, None], optional
            A function to preprocess the field, by default None.
        """
        if isinstance(field, tuple):
            field = itemgetter(*field)
        elif isinstance(field, str):
            field = itemgetter(field)
        super().__init__(pyarrow_field(*field._items))
        self.getter = field
        self.type = type
        self.preprocessor = preprocessor

    def __call__(self, data: Mapping[str, Any]) -> Any:
        """Get the value of the field.

        Parameters
        ----------
        data : dict
            The data to be accessed.
        """
        if self.preprocessor is not None:
            return self.preprocessor(self.getter(data))
        return self.getter(data)


def field(
    field: Union[str, itemgetter, Field],
    /,
    type: Union[pa.DataType, None] = None,
    preprocessor: Union[callable, None] = None,
) -> Field:
    """Create a new field getter."""
    return Field(field, type=type, preprocessor=preprocessor)


def scalar(cls, value: Any) -> Expression:
    """
    Create an expression from a scalar.

    Parameters
    ----------
    value : Any
        The value of the scalar.

    Returns
    -------
    Expression
        The expression representing the scalar.
    """
    return cls(pyarrow_scalar(value))
