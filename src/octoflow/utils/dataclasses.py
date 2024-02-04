from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Dict,
    ForwardRef,
    List,
    Literal,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from typing_extensions import Self, dataclass_transform, get_args, get_origin, get_type_hints

from octoflow.typing import Property


@dataclass_transform()
class BaseModelMeta(type):
    def __new__(cls, name, bases, attrs):
        cls = super().__new__(cls, name, bases, attrs)
        return dataclass(cls)

    def update_forward_refs(cls, **kwargs: Any) -> None:
        # update forward references in all subclasses
        for subclass in cls.__subclasses__():
            subclass.update_forward_refs(**kwargs)
            # update forward references in type hints
        for name, value in get_type_hints(cls, localns=kwargs).items():
            cls.__annotations__[name] = value


class BaseModel(metaclass=BaseModelMeta):
    def to_dict(self, exclude: Optional[Set[str]] = None) -> dict:
        return to_dict(self, exclude=exclude)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Self:
        return from_dict(cls, data)


def to_dict(obj: object, exclude: Optional[Set[str]] = None) -> Dict[str, Any]:
    if isinstance(obj, BaseModel):
        if isinstance(exclude, str):
            exclude = {exclude}
        exclude = set() if exclude is None else set(map(str, exclude))
        out = {}
        for attr, field in get_type_hints(obj.__class__).items():
            if attr in exclude:
                continue
            origin = get_origin(field) or field
            if origin is ClassVar:
                # ignore ClassVar fields
                continue
            value = getattr(obj, attr)
            out[attr] = to_dict(value)
        return out
    if isinstance(obj, dt.datetime):
        return obj.isoformat()
    if isinstance(obj, (List, Set, Tuple)):
        return [to_dict(item) for item in obj]
    if isinstance(obj, Mapping):
        return {key: to_dict(value) for key, value in obj.items()}
    return obj


def from_dict(cls: type, data: Dict[str, Any]) -> BaseModel:
    origin = get_origin(cls)
    subtypes = get_args(cls)
    if origin is None:
        origin = cls
    if isinstance(origin, ForwardRef):
        origin = origin._evaluate(globals(), locals(), frozenset())
    if origin is Property:
        if len(subtypes) == 1:
            # subtype is the type of Property
            return from_dict(subtypes[0], data)
        # do not attempt to convert the data to a type of Property
        return data
    if origin is ClassVar and len(subtypes) == 1 and get_origin(subtypes[0]) is Property:
        return from_dict(subtypes[0], data)
    if origin is ClassVar:
        msg = f"unsupported type: {origin}"
        raise ValueError(msg)
    if origin is Literal:
        return data
    if origin is Union:
        for subtype in subtypes:
            try:
                return from_dict(subtype, data)
            except ValueError:
                continue
    if origin is None or issubclass(origin, type(None)):
        return None
    if isinstance(origin, BaseModelMeta):
        kwargs = {}
        type_hints = get_type_hints(origin)
        for name, value in data.items():
            if name not in type_hints:
                continue
            value_type = type_hints[name]
            try:
                value = from_dict(value_type, value)
                kwargs[name] = value
            except ValueError:
                pass
        return origin(**kwargs)
    if issubclass(origin, Set):
        if len(subtypes) == 0:
            return set(data)
        if len(subtypes) == 1:
            value_type = subtypes[0]
            return {from_dict(value_type, item) for item in data}
        msg = f"unsupported Sequence type: {origin}"
        raise ValueError(msg)
    if issubclass(origin, Tuple):
        if len(subtypes) == 0:
            return tuple(data)
        if len(subtypes) == 1:
            value_type = subtypes[0]
            return tuple(from_dict(value_type, item) for item in data)
        msg = f"unsupported Sequence type: {origin}"
        raise ValueError(msg)
    if issubclass(origin, List):
        if len(subtypes) == 0:
            return list(data)
        if len(subtypes) == 1:
            value_type = subtypes[0]
            return [from_dict(value_type, item) for item in data]
        msg = f"unsupported Sequence type: {origin}"
        raise ValueError(msg)
    if issubclass(origin, Mapping):
        if len(subtypes) == 0:
            return data
        if len(subtypes) == 2:  # noqa: PLR2004
            key_type, value_type = subtypes
            temp = {}
            for key, value in data.items():
                key = from_dict(key_type, key)
                temp[key] = from_dict(value_type, value)
            return temp
        msg = f"unsupported Dict type: {origin}"
        raise ValueError(msg)
    if issubclass(origin, dt.datetime):
        return dt.datetime.fromisoformat(data)
    if issubclass(origin, (int, float, str, bool)):
        return origin(data)
    msg = f"unsupported type: {origin}"
    raise ValueError(msg)
