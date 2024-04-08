from __future__ import annotations

from operator import itemgetter
from typing import TYPE_CHECKING, Any, Mapping, Union

import pyarrow as pa
from typing_extensions import Self

__all__ = [
    "Field",
]


if TYPE_CHECKING:
    pass


class Field:
    def __new__(
        cls, field: Union[itemgetter, str, Self], /, *args, **kwargs
    ) -> Self:
        """Create a new field getter.

        Parameters
        ----------
        field : Union[itemgetter, str, FieldGetter]
            The field to be accessed.
        type : Union[pa.DataType, None], optional
            The type of the field, by default None.
        preprocessor : Union[callable, None], optional
            A function to preprocess the field, by default None.

        Returns
        -------
        FieldGetter
            The field getter.
        """
        if isinstance(field, Field):
            return field
        return super().__new__(cls)

    def __init__(
        self,
        field: Union[itemgetter, str],
        /,
        type: Union[pa.DataType, None] = None,
        preprocessor: Union[callable, None] = None,
    ):
        """Create a new field getter.

        Parameters
        ----------
        field : Union[itemgetter, str, FieldGetter]
            The field to be accessed.
        type : Union[pa.DataType, None], optional
            The type of the field, by default None.
        preprocessor : Union[callable, None], optional
            A function to preprocess the field, by default None.
        """
        if not isinstance(field, itemgetter):
            field = itemgetter(field)
        self.getter = field
        self.type = type
        self.preprocessor = preprocessor

    def __call__(self, data: Mapping[str, Any]) -> Any:
        """Get the value of the field.

        Parameters
        ----------
        data : dict
            The data to be accessed.
        """
        if self.preprocessor is not None:
            return self.preprocessor(self.getter(data))
        return self.getter(data)


def field(
    field: Union[str, itemgetter, Field],
    /,
    type: Union[pa.DataType, None] = None,
    preprocessor: Union[callable, None] = None,
) -> Field:
    """Create a new field getter."""
    return Field(
        field,
        type=type,
        preprocessor=preprocessor,
    )
