from __future__ import annotations

import dataclasses as dc
from typing import (
    Any,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
)

from sqlalchemy import Table
from sqlalchemy.orm import registry as registry_cls
from typing_extensions import (
    TypedDict,
    Unpack,
    dataclass_transform,
    get_type_hints,
)

from octoflow.data.expression import Field, field, field_from_dataclass_field

__all__ = [
    "BaseModel",
    "ModelMeta",
]

T = TypeVar("T")


class FieldAccessor(tuple, Generic[T]):
    def __new__(cls, obj: Type[T]) -> FieldAccessor[T]:
        values = []
        field_idx_map = {}
        for idx, field_ in enumerate(dc.fields(obj)):
            values.append(field_)
            field_idx_map[field_.name] = idx
        self = super().__new__(cls, tuple(values))
        self._field_idx_map = field_idx_map
        return self

    def __getattr__(self, name: str) -> Field:
        return self[self._field_idx_map[name]]


def fields(cls: Type[T]) -> Union[FieldAccessor[T], Type[T]]:
    return FieldAccessor(cls)


class ModelMetaArgs(TypedDict):
    table: Optional[Table] = None
    registry: Optional[registry_cls] = None


@dataclass_transform(field_specifiers=(Field, field))
class ModelMeta(type):
    __table__: Optional[Table] = None

    def __new__(mcs, name, bases, attrs, **kwargs: Unpack[ModelMetaArgs]):  # noqa: N804
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
        table = kwargs.get("table", getattr(cls, "__table__", None))
        if table is not None and not hasattr(cls, "__table__"):
            cls.__table__ = table
        cls = dc.dataclass(cls)
        registry = kwargs.get("registry", None)
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
