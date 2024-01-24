from __future__ import annotations

from typing import Any, Tuple, Union

import pyarrow.dataset as ds
from pyarrow.dataset import field as pyarrow_field
from pyarrow.dataset import scalar as pyarrow_scalar

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

    @classmethod
    def field(cls, *name_or_index: Tuple[str]) -> Expression:
        """
        Create an expression from a field.

        Parameters
        ----------
        *name_or_index : Tuple[str]
            The name or index of the field.

        Returns
        -------
        Expression
            The expression representing the field.
        """
        field = pyarrow_field(*name_or_index)
        return cls(field)

    @classmethod
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
        scaler = pyarrow_scalar(value)
        return cls(scaler)

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
