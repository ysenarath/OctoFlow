from __future__ import annotations

import datetime as dt
import typing
from dataclasses import is_dataclass
from typing import Any, NamedTuple, Union

import numpy as np
import pyarrow as pa

from octoflow.data.metadata import unify_metadata

try:
    from typing import _TypedDictMeta
except ImportError:
    from typing_extensions import _TypedDictMeta  # noqa: PLC2701


__all__ = [
    "MonthDayNano",
    "from_dtype",
    "infer_type",
    "is_undefined",
    "undefined",
    "unify_types",
]


class MonthDayNano(NamedTuple):
    months: int
    days: int
    nanoseconds: int


class Undefined(pa.ExtensionType):
    def __init__(self):
        super().__init__(pa.null(), "tklearn.base.arrow.undefined")

    def __arrow_ext_serialize__(self) -> bytes:  # noqa: PLW3201
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized) -> Undefined:  # noqa: PLW3201
        return Undefined()


def undefined() -> Undefined:
    return Undefined()


UNDEFINED = undefined()

pa.register_extension_type(UNDEFINED)


def is_undefined(obj: pa.DataType) -> bool:
    return UNDEFINED.equals(obj)


def from_dataclass(cls: type) -> pa.DataType:
    """Return the PyArrow data type of a dataclass.

    Parameters
    ----------
    cls : type
        The dataclass.

    Returns
    -------
    pa.DataType
        The PyArrow data type.
    """
    fields = {}
    for field_name, field_type in typing.get_type_hints(cls).items():
        f = cls.__dataclass_fields__[field_name]
        nullable = True
        if hasattr(f, "nullable"):
            nullable = f.nullable
        fields[field_name] = pa.field(
            field_name,
            from_dtype(field_type),
            nullable,
        )
    fields = list(fields.values())
    return pa.struct(fields)


def from_typed_dict(cls: _TypedDictMeta) -> pa.DataType:
    """Return the PyArrow data type of a TypedDict.

    Parameters
    ----------
    cls : _TypedDictMeta
        The TypedDict.

    Returns
    -------
    pa.DataType
        The PyArrow data type.
    """
    fields = {}
    for field_name, field_type in typing.get_type_hints(cls).items():
        fields[field_name] = pa.field(
            field_name,
            from_dtype(field_type),
            True,
        )
    fields = list(fields.values())
    return pa.struct(fields)


def from_union(args: tuple[type, ...]) -> pa.DataType:
    unified_type = undefined()
    for arg in args:
        unified_type = unify_types(unified_type, from_dtype(arg))
    return unified_type


def from_dtype(dtype: Union[type, np.dtype, None]) -> pa.DataType:
    """Return the PyArrow data type of a provided native/NumPy data type.

    Parameters
    ----------
    dtype : type | np.dtype | None
        The native or NumPy data type.

    Returns
    -------
    pa.DataType
        The PyArrow data type.
    """
    type_args = typing.get_args(dtype)
    dtype = typing.get_origin(dtype) or dtype
    # null
    if dtype is None or dtype is type(None):
        return pa.null()
    # special types
    if dtype is Union:
        return from_union(type_args)
    # basic python data types
    if isinstance(dtype, type):
        if is_dataclass(dtype):
            return from_dataclass(dtype)
        if issubclass(dtype, _TypedDictMeta):
            return from_typed_dict(dtype)
        if issubclass(dtype, list):
            (item_arg,) = type_args
            return pa.list_(pa.field("item", from_dtype(item_arg)))
        if issubclass(dtype, dict):
            key_arg, value_arg = type_args
            return pa.map_(from_dtype(key_arg), from_dtype(value_arg))
        if issubclass(dtype, str):
            return pa.string()
        if issubclass(dtype, bytes):
            return pa.binary()
        if issubclass(dtype, dt.datetime):
            # The smallest possible difference between
            #   non-equal datetime objects is timedelta(microseconds=1).
            return pa.timestamp("us", tz=None)
        if issubclass(dtype, dt.date):
            return pa.date32()
        if issubclass(dtype, dt.time):
            # The smallest possible difference between
            #   non-equal time objects, timedelta(microseconds=1).
            return pa.time64("us")
        if issubclass(dtype, dt.timedelta):
            # The smallest possible difference between
            #   non-equal timedelta objects, timedelta(microseconds=1).
            return pa.duration("us")
        if issubclass(dtype, MonthDayNano):
            return pa.month_day_nano_interval()
    # numpy data types
    if not isinstance(dtype, np.dtype):
        # convert dtype (obj/type) to np.dtype
        dtype = np.dtype(dtype)
    try:
        return pa.from_numpy_dtype(dtype)
    except NotImplementedError as ex:
        msg = f"unsupported data type '{dtype}'"
        raise ValueError(msg) from ex


def _is_compatible(
    op: callable, left: pa.DataType, right: pa.DataType
) -> bool:
    """Return whether both left and right are of the same type.

    Parameters
    ----------
    op : callable
        The function to check the type.
    left : pa.DataType
        The left PyArrow data type.
    right : pa.DataType
        The right PyArrow data type.

    Returns
    -------
    bool
        Whether both left and right are of the same type.
    """
    is_left, is_right = op(left), op(right)
    if is_left and is_right:
        return True
    if is_left or is_right:
        msg = f"types {left} and {right} are not compatible"
        raise ValueError(msg)
    return False


def unify_types(left: pa.DataType, right: pa.DataType) -> pa.DataType:
    """Return the PyArrow data type that can represent both left and right.

    Parameters
    ----------
    left : pa.DataType
        The left PyArrow data type.
    right : pa.DataType
        The right PyArrow data type.

    Returns
    -------
    pa.DataType
        The PyArrow data type.
    """
    if is_undefined(left) or pa.types.is_null(left):
        return right
    if is_undefined(right) or pa.types.is_null(right):
        return left
    if left.equals(right):
        return left
    if _is_compatible(pa.types.is_list, left, right):
        left_field, right_field = left.field(0), right.field(0)
        nullable = (
            left_field.nullable
            or left.value_type.equals(pa.null())
            or right_field.nullable
            or right.value_type.equals(pa.null())
        )
        metadata = unify_metadata(left_field, right_field)
        return pa.list_(
            pa.field(
                "item",
                unify_types(left.value_type, right.value_type),
                nullable,
                metadata,
            )
        )
    if _is_compatible(pa.types.is_struct, left, right):
        fields = {}
        # there may be fields that are in left but not in right
        #   those fields are optional i.e., nullable fields
        left_only_fields = set()
        for left_field in left:
            fields[left_field.name] = left_field
            left_only_fields.add(left_field.name)
        for right_field in right:
            right_field_name = right_field.name
            left_only_fields.discard(right_field_name)
            if right_field_name in fields:
                # existing field - promote the type
                left_field = fields[right_field_name]
                nullable = (
                    left_field.nullable
                    or left_field.type.equals(pa.null())
                    or right_field.nullable
                    or right_field.type.equals(pa.null())
                )
                try:
                    promoted_type = unify_types(
                        left_field.type, right_field.type
                    )
                except ValueError as ex:
                    msg = (
                        f"cannot unify types '{left_field.type}' and "
                        f"'{right_field.type}' of field '{right_field_name}'"
                    )
                    raise ValueError(msg) from ex
                metadata = unify_metadata(left_field, right_field)
                fields[right_field_name] = pa.field(
                    right_field_name, promoted_type, nullable, metadata
                )
            else:
                # new field (right only) is nullable
                fields[right_field_name] = right_field.with_nullable(True)
        # replace the left only fields with nullable fields
        for left_field_name in left_only_fields:
            left_field = fields[left_field_name]
            if left_field.nullable:
                # already nullable
                continue
            fields[left_field_name] = left_field.with_nullable(True)
        fields = list(fields.values())
        return pa.struct(fields)
    # for any other types, use the NumPy data type
    dtype = np.promote_types(left.to_pandas_dtype(), right.to_pandas_dtype())
    try:
        return from_dtype(dtype)
    except ValueError as ex:
        msg = f"cannot unify types '{left}' and '{right}'"
        raise ValueError(msg) from ex


def infer_type(obj: Any) -> pa.DataType:
    """Return the PyArrow data type of an object.

    Parameters
    ----------
    obj : Any
        The object.

    Returns
    -------
    pa.DataType
        The PyArrow data type.
    """
    try:
        if not np.isscalar(obj):
            # e.g., list, dict, etc.
            raise NotImplementedError
        dtype = np.min_scalar_type(obj)
        return pa.from_numpy_dtype(dtype)
    except NotImplementedError:
        pass
    if isinstance(obj, list):
        item_type = undefined()
        nullable = False
        for item in obj:
            current_item_type = infer_type(item)
            if not nullable and current_item_type.equals(pa.null()):
                nullable = True
            if is_undefined(item_type):
                item_type = current_item_type
            else:
                item_type = unify_types(item_type, current_item_type)
        value_field = pa.field("item", item_type, nullable)
        return pa.list_(value_field)
    if isinstance(obj, dict):
        fields = {}
        for key, value in obj.items():
            value_type = infer_type(value)
            nullable = value_type.equals(pa.null())
            value_field = pa.field(key, value_type, nullable)
            fields[key] = value_field
        fields = list(fields.values())
        return pa.struct(fields)
    if isinstance(obj, dt.time):
        unit = "us" if obj.microsecond else "s"
        tz = obj.tzname()
        if tz is not None:
            return pa.timestamp(unit, tz=tz)
        if unit == "s":
            return pa.time32(unit)
        return pa.time64(unit)
    return from_dtype(type(obj))
