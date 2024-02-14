from __future__ import annotations

from dataclasses import Field, dataclass, field
from typing import Any, Optional, TypeVar

from sqlalchemy import Table
from sqlalchemy.orm import registry as registry_cls
from typing_extensions import dataclass_transform, get_type_hints

__all__ = [
    "ModelBase",
    "ModelMeta",
]


@dataclass_transform(field_specifiers=(Field, field))
class ModelMeta(type):
    def __new__(cls, name, bases, attrs, **kwargs):
        cls = super().__new__(cls, name, bases, attrs)
        table: Optional[Table] = kwargs.get("table", getattr(cls, "__table__", None))
        if table is not None and not hasattr(cls, "__table__"):
            cls.__table__ = table
        cls = dataclass(cls)
        # schema: bool = kwargs.get("schema", len(bases) > 0)
        # if schema:
        #     cls = add_schema_to_dataclass(cls)
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


class ModelBase(metaclass=ModelMeta):
    def __post_init__(self):
        pass


T = TypeVar("T", bound=ModelMeta)
