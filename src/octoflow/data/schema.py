import functools
import itertools
from contextlib import suppress
from typing import Any, Dict, Generator, Optional, Tuple, TypeVar

import pyarrow as pa
from typing_extensions import Self

from octoflow.data import types
from octoflow.data.metadata import unify_metadata
from octoflow.data.types import infer_type, unify_types
from octoflow.exceptions import ValidationError

__all__ = [
    "infer_schema",
    "unify_schemas",
]

T = TypeVar("T")


def unify_schemas(this: pa.Schema, other: Optional[pa.Schema]) -> pa.Schema:
    if other is None:
        return this
    _fields = {}
    this_schema_fields = set()
    for this_field in this:
        this_field_name = this_field.name
        _fields[this_field.name] = pa.field(
            this_field.name,
            this_field.type,
            this_field.nullable,
            this_field.metadata,
        )
        this_schema_fields.add(this_field_name)
    for other_field in other:
        other_field_name = other_field.name
        this_schema_fields.discard(other_field_name)
        if other_field_name in _fields:
            this_field = _fields[other_field_name]
            try:
                promoted_type = unify_types(this_field.type, other_field.type)
            except ValueError as ex:
                msg = (
                    f"cannot unify types '{this_field.type}' and "
                    f"'{other_field.type}' of field '{other_field_name}'"
                )
                raise ValueError(msg) from ex
            nullable = (
                this_field.nullable
                or this_field.type.equals(pa.null())
                or other_field.nullable
                or other_field.type.equals(pa.null())
            )
            metadata = unify_metadata(this_field, other_field)
            _fields[other_field_name] = pa.field(
                other_field_name,
                promoted_type,
                nullable,
                metadata,
            )
        else:
            _fields[other_field_name] = pa.field(
                other_field_name,
                other_field.type,
                True,  # nullable because this field is not in the existing
                other_field.metadata,
            )
    for this_field_name in this_schema_fields:
        this_field = _fields[this_field_name]
        _fields[this_field_name] = pa.field(
            this_field_name,
            this_field.type,
            True,  # nullable because this field is not in the other schema
            this_field.metadata,
        )
    metadata = unify_metadata(this, other)
    return pa.schema(list(_fields.values()), metadata=metadata)


def infer_schema(
    data: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
) -> Self:
    _fields = {}
    for key, value in data.items():
        dtype = infer_type(value)
        _fields[key] = pa.field(key, dtype, True, None)
    return pa.schema(list(_fields.values()), metadata=metadata)


def validate(schema: pa.Schema, data: dict) -> bool:
    """
    Validates a dictionary against a PyArrow schema.

    Parameters
    ----------
    schema : pyarrow.Schema
        The PyArrow schema to validate against.
    data : dict
        The dictionary to validate.

    Raises
    ------
    ValidationError
        If the dictionary does not match the schema.

    Examples
    --------
    >>> schema = pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.string())])
    >>> valid_dict = {'id': 1, 'name': 'Alice'}
    >>> validate(schema, valid_dict)
    >>> invalid_dict = {'id': '1', 'name': 'Alice'}
    >>> validate(schema, invalid_dict)
    Traceback (most recent call last):
    ...
    ValidationError: ...
    """  # noqa: E501
    try:
        pa.RecordBatch.from_pylist([data], schema)
    except pa.lib.ArrowInvalid as e:
        raise ValidationError(str(e).lower()) from e


def get_schema(data: T) -> Tuple[T, pa.Schema]:
    """
    Extracts the schema from a PyArrow schema or a generator of PyArrow
    record batches.

    Parameters
    ----------
    data : Any
        The PyArrow schema or generator of record batches.

    Returns
    -------
    Tuple[Any, pa.Schema]
        The data and the schema.
    """
    with suppress(AttributeError):
        return data, data.schema
    if isinstance(data, Generator):
        data, _data = itertools.tee(data)
        try:
            return data, next(_data).schema
        except StopIteration:
            return data, pa.schema([])
    msg = (
        "expected data to be of type 'Generator', "
        f"got '{data.__class__.__name__}'"
    )
    raise TypeError(msg)


def from_dataclass(cls: T) -> pa.Schema:
    """
    Converts a dataclass to a PyArrow schema.

    Parameters
    ----------
    cls : Type[T]
        The dataclass to convert.

    Returns
    -------
    pa.Schema
        The PyArrow schema.

    Examples
    --------
    >>> import dataclasses
    >>> @dataclasses.dataclass
    ... class Record:
    ...     id: int
    ...     name: str
    >>> from_dataclass(Record)
    pyarrow.Schema([...])
    """
    fields = []
    for field in cls.__dataclass_fields__.values():
        nullable = True
        if hasattr(field, "nullable"):
            nullable = field.nullable
        fields.append(
            pa.field(
                field.name,
                types.from_dtype(field.type),
                nullable,
                None,
            )
        )
    return pa.schema(fields)


@functools.wraps(from_dataclass)
def get_schema_from_dataclass(*args, **kwargs) -> pa.Schema:
    """
    Alias for `from_dataclass`.

    Examples
    --------
    >>> import dataclasses
    >>> @dataclasses.dataclass
    ... class Record:
    ...     id: int
    ...     name: str
    >>> get_schema_from_dataclass(Record)
    pyarrow.Schema([...])
    """
    return from_dataclass(*args, **kwargs)
