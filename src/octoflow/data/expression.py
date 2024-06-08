from __future__ import annotations

import dataclasses as dc
import functools
import inspect
from typing import (
    Any,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

import pyarrow.dataset as ds
from pyarrow.dataset import scalar as pyarrow_scalar

from octoflow.data.base import BaseExpression

__all__ = [
    "Expression",
]

T = TypeVar("T")


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


def scalar(value: Any) -> Expression:
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
    return Expression(pyarrow_scalar(value))


class Field(dc.Field, Expression):
    def __init__(
        self,
        name: Optional[str] = None,
        *,
        default=dc.MISSING,
        default_factory=dc.MISSING,
        init=True,
        repr=True,
        hash=None,
        compare=True,
        metadata=None,
        kw_only=dc.MISSING,
    ):
        """Initialize a field getter."""
        if default is not dc.MISSING and default_factory is not dc.MISSING:
            msg = "cannot specify both default and default_factory"
            raise ValueError(msg)
        kwargs = {}
        if inspect.signature(dc.Field).parameters.get("kw_only") is not None:
            # if python >= 3.10 => should be
            # explicitly passed
            kwargs["kw_only"] = kw_only
        super().__init__(
            default=default,
            default_factory=default_factory,
            init=init,
            repr=repr,
            hash=hash,
            compare=compare,
            metadata=metadata,
            **kwargs,
        )
        self.name = name
        # will be initialized when accessed
        Expression.__init__(self, ...)

    @property
    def _wrapped(self) -> ds.Expression:
        """Get the wrapped field."""
        if self._wrapped_ is ...:
            self._wrapped_ = ds.field(self.name)
        return self._wrapped_

    @_wrapped.setter
    def _wrapped(self, value):
        """Get the name of the field."""
        self._wrapped_ = value

    def __call__(self, data: Mapping[str, Any]) -> Any:
        """Get the value of the field.

        Parameters
        ----------
        data : dict
            The data to be accessed.
        """
        return data[self.name]


@functools.wraps(dc.field)
def field(*args, **kwargs) -> Field:
    """Create a new field getter."""
    return Field(*args, **kwargs)


@functools.wraps(dc.field)
def field_from_dataclass_field(field: dc.Field) -> Field:
    """Create a new field getter."""
    kwargs = {}
    if hasattr(field, "kw_only"):
        # if python >= 3.10 => should be
        kwargs["kw_only"] = field.kw_only
    return Field(
        name=field.name,
        default=field.default,
        default_factory=field.default_factory,
        init=field.init,
        repr=field.repr,
        hash=field.hash,
        compare=field.compare,
        metadata=field.metadata,
        **kwargs,
    )
