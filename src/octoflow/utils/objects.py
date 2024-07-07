import datetime
import decimal
import enum
import functools
import importlib
import json
import typing
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Generic, Type, TypeVar, Union
from uuid import UUID

from typing_extensions import ParamSpec

__all__ = [
    "invoke",
]

P = ParamSpec("P")
T = TypeVar("T")


def object_to_string(obj):
    module_name = obj.__module__
    qual_name = obj.__qualname__
    return f"{module_name}:{qual_name}"


def string_to_object(import_path):
    if isinstance(import_path, dict):
        import_path = import_path["@type"]

    module_name, qual_name = import_path.split(":", 1)
    module = importlib.import_module(module_name)

    # Navigate through the qualified name
    obj = module
    for part in qual_name.split("."):
        obj = getattr(obj, part)

    return obj


def invoke(
    __callable: Union[str, Callable[P, T]],
    __partial: bool = False,
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    obj = __callable
    if isinstance(obj, str):
        obj: Callable[P, T] = string_to_object(obj)
    if __partial:
        return functools.partial(obj, *args, **kwargs)
    try:
        return obj(obj, *args, **kwargs)
    except Exception as ex:
        raise ex


class JSONDecoder(json.JSONDecoder):
    def __init__(self, **kwargs):
        super().__init__(object_hook=self.__class__.object_hook, **kwargs)

    @staticmethod
    def object_hook(obj: Any) -> Any:
        if isinstance(obj, dict) and obj.get("@type"):
            type_obj = string_to_object(obj.pop("@type"))
            args = obj.pop("@args", [])
            kwargs: dict = obj.pop("@kwargs", {})
            kwargs.update(obj)
            return type_obj(*args, **kwargs)
        return obj


def encode_dataclass(obj):
    if is_dataclass(obj):
        pkl = object_to_string(obj.__class__)
        result = [("@type", pkl)]
        for f in fields(obj):
            value = dump(getattr(obj, f.name))
            result.append((f.name, value))
        return dict(result)
    return dump(obj)


class JSONEncoder(json.JSONEncoder):
    dispatch: typing.ClassVar[Dict[str, Any]] = {}

    def default(self, obj: Any) -> Any:
        cls = self.__class__
        if is_dataclass(obj.__class__):
            return encode_dataclass(obj)
        for dis, encoder in cls.dispatch.items():
            if not isinstance(obj, dis):
                continue
            return encoder(obj)
        return super().default(obj)

    @classmethod
    def register(
        cls,
        typ: Type[T],
        encoder: typing.Optional[Callable[[T], dict]] = None,
    ) -> None:
        if encoder is None:
            return functools.partial(cls.register, typ)
        cls.dispatch[typ] = encoder
        return encoder


@JSONEncoder.register(Path)
def encode_path(obj: Path) -> dict:
    return {
        "@type": object_to_string(Path),
        "@args": [str(obj)],
    }


@JSONEncoder.register(enum.Enum)
def encode_enum(obj: enum.Enum) -> dict:
    return {
        "@type": object_to_string(obj.__class__),
        "@args": [obj.value],
    }


@JSONEncoder.register(datetime.datetime)
def encode_datetime(obj: datetime.datetime) -> dict:
    return {
        "@type": object_to_string(datetime.datetime),
        "@args": [obj.isoformat()],
    }


@JSONEncoder.register(datetime.date)
def encode_date(obj: datetime.date) -> dict:
    return {
        "@type": object_to_string(datetime.date),
        "@args": [obj.isoformat()],
    }


@JSONEncoder.register(datetime.time)
def encode_time(obj: datetime.time) -> dict:
    return {
        "@type": object_to_string(datetime.time),
        "@args": [obj.isoformat()],
    }


@JSONEncoder.register(datetime.timedelta)
def encode_timedelta(obj: datetime.timedelta) -> dict:
    return {
        "@type": object_to_string(datetime.timedelta),
        "@args": [obj.total_seconds()],
    }


@JSONEncoder.register(decimal.Decimal)
def encode_decimal(obj: decimal.Decimal) -> dict:
    return {
        "@type": object_to_string(decimal.Decimal),
        "@args": [str(obj)],
    }


@JSONEncoder.register(UUID)
def encode_uuid(obj: UUID) -> dict:
    return {
        "@type": object_to_string(UUID),
        "@args": [str(obj)],
    }


def josn_dumps(obj: Any) -> str:
    return json.dumps(obj, cls=JSONEncoder)


def json_loads(obj: str) -> Any:
    return json.loads(obj, cls=JSONDecoder)


def dump(obj: Any) -> Any:
    return json.loads(josn_dumps(obj))


def load(obj: Any) -> Any:
    return json_loads(json.dumps(obj))


class Mapped(Generic[T]):
    def __new__(cls) -> Union[T, Dict[str, Any], str]:
        return super().__new__()
