from __future__ import annotations

import dataclasses as dc
import functools
import inspect
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


class FieldAccessor(tuple, Generic[T]):
    def __new__(cls, obj: Type[T]) -> FieldAccessor[T]:
        values = []
        field_idx_map = {}
        for idx, field in enumerate(dc.fields(obj)):
            values.append(field)
            field_idx_map[field.name] = idx
        self = super().__new__(cls, tuple(values))
        self._field_idx_map = field_idx_map
        return self

    def __getattr__(self, name: str) -> Field:
        return self[self._field_idx_map[name]]


def fields(
    cls: Type[T],
) -> Union[FieldAccessor[T], Type[T]]:
    return FieldAccessor(cls)


@dataclass_transform(field_specifiers=(Field, field))
class ModelMeta(type):
    def __new__(mcs, name, bases, attrs, **kwargs):  # noqa: N804
        # create empty field if not defined
        if "__annotations__" in attrs:
            annotations: dict = attrs["__annotations__"]
            for attr, _ in annotations.items():
                if attr not in attrs:
                    # create empty field - need so that dataclass
                    # will not create it's default field
                    attrs[attr] = field()
                elif isinstance(attrs[attr], Field):
                    # do not need further processing
                    attrs[attr] = attrs[attr]
                elif isinstance(attrs[attr], dc.Field):
                    # convert dataclass-field to field
                    attrs[attr] = field_from_dataclass_field(attrs[attr])
                else:
                    # create field with default value
                    attrs[attr] = field(default=attrs[attr])
        cls = super().__new__(mcs, name, bases, attrs)
        table: Optional[Table] = kwargs.get(
            "table", getattr(cls, "__table__", None)
        )
        if table is not None and not hasattr(cls, "__table__"):
            cls.__table__ = table
        cls = dc.dataclass(cls)
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
