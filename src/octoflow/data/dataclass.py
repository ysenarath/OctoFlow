from __future__ import annotations

import dataclasses as dc
import functools
from dataclasses import Field, dataclass, field
from typing import (
    Any,
    Generic,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
)

import pyarrow.dataset as ds
from sqlalchemy import Table
from sqlalchemy.orm import registry as registry_cls
from typing_extensions import dataclass_transform, get_type_hints

from octoflow.data.expression import Expression

__all__ = [
    "BaseModel",
    "ModelMeta",
]

T = TypeVar("T")


@dataclass_transform(field_specifiers=(Field, field))
class ModelMeta(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        cls = super().__new__(cls, name, bases, attrs)
        table: Optional[Table] = kwargs.get(
            "table", getattr(cls, "__table__", None)
        )
        if table is not None and not hasattr(cls, "__table__"):
            cls.__table__ = table
        cls = dataclass(cls)
        registry: registry_cls = kwargs.get("registry", None)
        if table is not None and registry is not None:
            return registry.mapped(cls)
        return cls

    def update_forward_refs(cls, **kwargs: Any) -> None:
        # update forward references in all subclasses
        for subclass in cls.__subclasses__():
            subclass.update_forward_refs(**kwargs)
            # update forward references in type hints
        for name, value in get_type_hints(cls, localns=kwargs).items():
            cls.__annotations__[name] = value


class BaseModel(metaclass=ModelMeta):
    def __post_init__(self):
        pass


class Field(Expression, dc.Field):
    def __new__(
        cls,
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
        if default is not dc.MISSING and default_factory is not dc.MISSING:
            msg = "cannot specify both default and default_factory"
            raise ValueError(msg)
        self = dc.Field.__new__(cls)
        dc.Field.__init__(
            self,
            default=default,
            default_factory=default_factory,
            init=init,
            repr=repr,
            hash=hash,
            compare=compare,
            metadata=metadata,
            kw_only=kw_only,
        )
        return self

    def __init__(self, name: Optional[str] = None, *args, **kwargs):
        """Initialize a field getter."""
        self.name = name
        super().__init__(None)

    @property
    def _wrapped(self) -> ds.Expression:
        """Get the wrapped field."""
        if self._wrapped_ is None:
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


class FieldAccessor(Generic[T]):
    def __init__(self, obj: Type[T]) -> None:
        self._fields = obj

    def __getattr__(self, __name: str) -> Field:
        return self._fields.__dataclass_fields__[__name]


def fieldset(cls: Type[T]) -> Union[FieldAccessor[T], T]:
    return FieldAccessor(cls)
