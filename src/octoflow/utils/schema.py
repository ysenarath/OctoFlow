"""This module provides a function to create a marshmallow schema from a dataclass.

Following marshmallow fields are not supported:
    Field(*, load_default, missing, ...)
    Function([serialize, deserialize])
    Method([serialize, deserialize])
    Number(*[, as_string])
    Constant(constant, **kwargs)
    Raw(*, load_default, missing, dump_default, ...)
    Pluck(nested, field_name, **kwargs)
    UUID(*, load_default, missing, dump_default, ...)
    Url(*[, relative, absolute, schemes, ...])
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
from decimal import Decimal
from types import NoneType
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Self,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_origin,
)

from marshmallow import Schema, class_registry, fields, post_load
from marshmallow import missing as missing_
from marshmallow.fields import Field as MarshmallowField
from typing_extensions import get_args, get_type_hints

from octoflow.constants import MISSING

try:
    import marshmallow_union
except ImportError:
    marshmallow_union = None

__all__ = [
    "add_schema_to_dataclass",
    "ModelSchema",
]

T = TypeVar("T")


STR_FORMAT_MAPPING = {
    "email": "Email",
    "e_mail": "Email",
    "url": "URL",
    "uuid": "UUID",
    "ip": "IP",
    "ip_interface": "IPv4",
    "ipv4": "IPv4",
    "ipv4_interface": "IPv4Interface",
    "ipv6": "IPv6",
    "ipv6_interface": "IPv6Interface",
}


def create_schema(
    obj: type,
    *,
    field: Optional[dataclasses.Field] = None,
    default: Any = MISSING,
) -> MarshmallowField:
    # read typing from annotations if it is a dataclass
    if dataclasses.is_dataclass(obj):
        _ = create_or_get_schema_class_from_dataclass(obj)
        return fields.Nested(obj.__name__)
    origin = get_origin(obj) or obj
    args = get_args(obj)
    if default is MISSING:
        default = MISSING if field is None else field.default
    if default is MISSING:
        default = missing_
    field_metadata = getattr(field, "metadata", {})
    if origin is Union and marshmallow_union is not None:
        # Union(fields, [reverse_serialize_candidates])
        nullable = any(arg is None or arg is NoneType for arg in args)
        return marshmallow_union.Union(
            [create_schema(arg) for arg in args if arg is not None and arg is not NoneType],
            load_default=default,
            nullable=nullable,
        )
    if origin is Union or origin is Any:
        # Union(fields, [reverse_serialize_candidates])
        return fields.Raw(
            load_default=default,
        )
    if issubclass(origin, enum.EnumType):
        # Enum(enum, *[, by_value])
        return fields.Enum(
            obj,
            by_value=field_metadata.get("by_value", False),
            load_default=default,
        )
    if issubclass(origin, bool):
        # Bool / Boolean(*[, truthy, falsy])
        return fields.Boolean(
            truthy=field_metadata.get("truthy", None),
            falsy=field_metadata.get("falsy", None),
            load_default=default,
        )
    if issubclass(origin, int):
        # Number(*[, as_string])
        # Int / Integer(*[, strict])
        return fields.Integer(
            strict=field_metadata.get("strict", False),
            as_string=field_metadata.get("as_string", False),
            load_default=default,
        )
    if issubclass(origin, Decimal):
        # Number(*[, as_string])
        # Decimal([places, rounding, allow_nan, as_string])
        return fields.Decimal(
            places=field_metadata.get("places", None),
            rounding=field_metadata.get("rounding", None),
            allow_nan=field_metadata.get("allow_nan", True),
            as_string=field_metadata.get("as_string", False),
            load_default=default,
        )
    if issubclass(origin, float):
        # Number(*[, as_string])
        # Float(*[, allow_nan, as_string])
        return fields.Float(
            allow_nan=field_metadata.get("allow_nan", True),
            as_string=field_metadata.get("as_string", False),
            load_default=default,
        )
    if issubclass(origin, str) and "format" in field_metadata:
        str_format = str(field_metadata["format"])
        str_format = str_format.lower().replace("-", "_")
        field_type = STR_FORMAT_MAPPING[str_format]
        field_builder = getattr(
            fields,
            field_type,
        )
        return field_builder(
            version=field_metadata.get("version", None),
            load_default=default,
        )
    if issubclass(origin, str):
        # Str / String(*, load_default, missing, ...)
        return fields.String(
            dump_default=field_metadata.get("dump_default", missing_),
            load_default=default,
        )
    if issubclass(origin, Dict):
        # Dict([keys, values])
        return fields.Dict(
            keys=create_schema(args[0]) if len(args) >= 1 else fields.Raw(),
            values=create_schema(args[1]) if len(args) >= 2 else fields.Raw(),
            load_default=default,
        )
    if issubclass(origin, dt.datetime) and field_metadata.get("aware", False):
        return fields.AwareDateTime(
            format=field_metadata.get("format", None),
            default_timezone=field_metadata.get("default_timezone", None),
            load_default=default,
        )
    if issubclass(origin, dt.datetime):
        return fields.DateTime(
            format=field_metadata.get("format", None),
            load_default=default,
        )
    if issubclass(origin, dt.date):
        return fields.Date(
            format=field_metadata.get("format", None),
            load_default=default,
        )
    if issubclass(origin, dt.time) and field_metadata.get("native", False):
        # NaiveDateTime([format, timezone])
        return fields.NaiveDateTime(
            format=field_metadata.get("format", None),
            timezone=field_metadata.get("timezone", None),
            load_default=default,
        )
    if issubclass(origin, dt.time):
        return fields.Time(
            format=field_metadata.get("format", None),
            load_default=default,
        )
    if issubclass(origin, dt.timedelta):
        return fields.TimeDelta(
            precision=field_metadata.get("precision", None),
            serialization_type=field_metadata.get("serialization_type", None),
            load_default=default,
        )
    if issubclass(origin, Mapping):
        # Mapping([keys, values])
        return fields.Mapping(
            keys=create_schema(args[0]) if len(args) >= 1 else fields.Raw(),
            values=create_schema(args[1]) if len(args) >= 2 else fields.Raw(),
            load_default=default,
        )
    if issubclass(origin, Tuple):
        # Tuple(tuple_fields, *args, **kwargs)
        return fields.Tuple(
            [create_schema(arg) for arg in args],
            load_default=default,
        )
    if issubclass(origin, List):
        # List(cls_or_instance, **kwargs)
        cls_or_instance = create_schema(args[0]) if args else fields.Raw()
        return fields.List(
            cls_or_instance,
            load_default=default,
        )
    msg = f"type '{obj}' is not supported"
    raise NotImplementedError(msg)


def build_load_from_dict(cls: Type) -> Any:
    @post_load
    def load_from_dict(self, data: Dict[str, Any], **kwargs) -> Self:
        inst = cls.__new__(cls)
        for field_name, value in data.items():
            setattr(inst, field_name, value)
        return inst

    return load_from_dict


def create_or_get_schema_class_from_dataclass(cls: Type) -> Type[Schema]:
    if hasattr(cls, "__marshmallow_schema_class__"):
        return cls.__marshmallow_schema_class__
    fields = {}
    dataclass_fields = {field.name: field for field in dataclasses.fields(cls)}
    # read typing from annotations if it is a dataclass
    for field_name, type_ in get_type_hints(cls).items():
        if field_name.startswith("_"):
            continue
        origin = get_origin(type_) or type_
        if origin is ClassVar:
            continue
        field_or_value: Union[dataclasses.Field, Any] = dataclass_fields[field_name]
        field, default = None, MISSING
        if isinstance(field_or_value, dataclasses.Field):
            field = field_or_value
        else:
            default = field_or_value
        fields[field_name] = create_schema(
            type_,
            field=field,
            default=default,
        )
    attrs = fields.copy()
    attrs["load_from_dict"] = build_load_from_dict(cls)
    attrs["Meta"] = type(
        "GeneratedMeta",
        (getattr(Schema, "Meta", object),),
        {"register": False},
    )
    schema_class = type(f"{cls.__name__}Schema", (Schema,), attrs)
    class_registry.register(cls.__name__, schema_class)
    cls.__marshmallow_schema_class__ = schema_class
    cls.__marshmallow_base_schema__ = schema_class()
    return create_or_get_schema_class_from_dataclass(cls)


def add_schema_to_dataclass(cls: T) -> T:
    create_or_get_schema_class_from_dataclass(cls)
    return cls


class ModelSchema(Schema):
    def __new__(cls, model_cls: type) -> Self:
        if not hasattr(model_cls, "__marshmallow_base_schema__"):
            create_or_get_schema_class_from_dataclass(model_cls)
        return model_cls.__marshmallow_base_schema__


class ValidationError(ValueError):
    def __init__(self, errors: Any):
        self.errors = errors
        super().__init__(errors)

    def __str__(self):
        return str(self.errors)
