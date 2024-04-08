import itertools
from typing import Any, Dict, Generator, Optional, Tuple, TypeVar

import pyarrow as pa
from typing_extensions import Self

from octoflow.data.metadata import unify_metadata
from octoflow.data.types import infer_type, unify_types
from octoflow.exceptions import ValidationError

__all__ = [
    "unify_schemas",
    "infer_schema",
]

T = TypeVar("T")


class SchemaBuilder:
    def __init__(self):
        self._fields = {}
        # Keys and values must be coercible to bytes.
        self.metadata = None

    def has_field(self, name: str) -> bool:
        return name in self._fields

    def get_field(self, name: str) -> pa.Field:
        return self._fields[name]

    def update_field(
        self,
        name: str,
        dtype: pa.DataType,
        nullable: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self._fields[name] = pa.field(name, dtype, nullable, metadata)

    def remove_field(self, name: str):
        del self._fields[name]

    def build(self) -> pa.Schema:
        fields = list(self._fields.values())
        return pa.schema(fields, metadata=self.metadata)


def unify_schemas(this: pa.Schema, other: Optional[pa.Schema]) -> pa.Schema:
    if other is None:
        return this
    builder = SchemaBuilder()
    this_schema_fields = set()
    for this_field in this:
        this_field_name = this_field.name
        builder.update_field(
            this_field.name,
            this_field.type,
            this_field.nullable,
            this_field.metadata,
        )
        this_schema_fields.add(this_field_name)
    for other_field in other:
        other_field_name = other_field.name
        this_schema_fields.discard(other_field_name)
        if builder.has_field(other_field_name):
            this_field = builder.get_field(other_field_name)
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
            builder.update_field(
                other_field_name,
                promoted_type,
                nullable,
                metadata,
            )
        else:
            builder.update_field(
                other_field_name,
                other_field.type,
                True,  # nullable because this field is not in the existing
                other_field.metadata,
            )
    for this_field_name in this_schema_fields:
        this_field = builder.get_field(this_field_name)
        builder.update_field(
            this_field_name,
            this_field.type,
            True,  # nullable because this field is not in the other schema
            this_field.metadata,
        )
    builder.metadata = unify_metadata(this, other)
    return builder.build()


def infer_schema(data: Dict[str, Any]) -> Self:
    schema_builder = SchemaBuilder()
    for key, value in data.items():
        pa_type = infer_type(value)
        schema_builder.update_field(key, pa_type)
    return schema_builder.build()


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
    try:
        return data, data.schema
    except AttributeError as e:
        if not isinstance(data, Generator):
            msg = (
                "expected data to be of type 'Generator', "
                f"got '{data.__class__.__name__}'"
            )
            raise TypeError(msg) from e
        data, data_ = itertools.tee(data)
        return data, next(data_).schema
