import datetime
import decimal
import enum
import functools
import importlib
import json
import typing
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Type, TypeVar, Union
from uuid import UUID

from typing_extensions import ParamSpec

__all__ = [
    "invoke",
]

P = ParamSpec("P")
T = TypeVar("T")


def import_object(s: Union[str, dict]) -> Any:
    if not isinstance(s, (str, dict)):
        msg = f"expected str or dict, got {s.__class__.__name__}"
        raise TypeError(msg)
    if isinstance(s, dict):
        s = s["@type"]
    try:
        module, name = s.rsplit(".", 1)
    except ValueError:
        module, name = "builtins", s
    return getattr(importlib.import_module(name=module), name)


def invoke(
    __obj: Union[str, Callable[P, T]],
    __partial: bool = False,
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    obj = __obj
    if isinstance(obj, str):
        obj: Callable[P, T] = import_object(obj)
    if __partial:
        return functools.partial(obj, *args, **kwargs)
    try:
        return obj(obj, *args, **kwargs)
    except Exception as ex:
        raise ex


class JSONEncoder(json.JSONEncoder):
    dispatch: typing.ClassVar[Dict[str, Any]] = {}

    def default(self, obj: Any) -> Any:
        cls = self.__class__
        for dis, encoder in cls.dispatch.items():
            if not isinstance(obj, dis):
                continue
            return encoder(obj)
        if is_dataclass(obj.__class__):
            return encode_dataclass(obj)
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


def encode_dataclass(obj: Any) -> dict:
    return {
        "@type": f"{obj.__class__.__module__}.{obj.__class__.__name__}",
        **asdict(obj),
    }


@JSONEncoder.register(Path)
def encode_path(obj: Path) -> dict:
    return {
        "@type": "pathlib.Path",
        "@args": [str(obj)],
    }


@JSONEncoder.register(enum.Enum)
def encode_enum(obj: enum.Enum) -> dict:
    return {
        "@type": f"{obj.__class__.__module__}.{obj.__class__.__name__}",
        "@args": [obj.value],
    }


@JSONEncoder.register(datetime.datetime)
def encode_datetime(obj: datetime.datetime) -> dict:
    return {
        "@type": "datetime.datetime",
        "@args": [obj.isoformat()],
    }


@JSONEncoder.register(datetime.date)
def encode_date(obj: datetime.date) -> dict:
    return {
        "@type": "datetime.date",
        "@args": [obj.isoformat()],
    }


@JSONEncoder.register(datetime.time)
def encode_time(obj: datetime.time) -> dict:
    return {
        "@type": "datetime.time",
        "@args": [obj.isoformat()],
    }


@JSONEncoder.register(datetime.timedelta)
def encode_timedelta(obj: datetime.timedelta) -> dict:
    return {
        "@type": "datetime.timedelta",
        "@args": [obj.total_seconds()],
    }


@JSONEncoder.register(decimal.Decimal)
def encode_decimal(obj: decimal.Decimal) -> dict:
    return {
        "@type": "decimal.Decimal",
        "@args": [str(obj)],
    }


@JSONEncoder.register(UUID)
def encode_uuid(obj: UUID) -> dict:
    return {
        "@type": "uuid.UUID",
        "@args": [str(obj)],
    }


class JSONDecoder(json.JSONDecoder):
    def __init__(self, **kwargs):
        super().__init__(object_hook=self.__class__.object_hook, **kwargs)

    @staticmethod
    def object_hook(obj: Any) -> Any:
        if isinstance(obj, dict) and obj.get("@type"):
            type_obj = import_object(obj.pop("@type"))
            args = obj.pop("@args", [])
            kwargs: dict = obj.pop("@kwargs", {})
            kwargs.update(obj)
            return type_obj(*args, **kwargs)
        return obj


def josn_dumps(obj: Any) -> str:
    return json.dumps(obj, cls=JSONEncoder)


def json_loads(obj: str) -> Any:
    return json.loads(obj, cls=JSONDecoder)


def dump(obj: Any) -> Any:
    return json.loads(josn_dumps(obj))


def load(obj: Any) -> Any:
    return json_loads(json.dumps(obj))
