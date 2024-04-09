from __future__ import annotations

from dataclasses import MISSING
from operator import itemgetter
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
    overload,
)

import orjson
import pyarrow as pa
from typing_extensions import Self

from octoflow.data.field import Field
from octoflow.data.schema import infer_schema, unify_schemas
from octoflow.data.types import UNDEFINED, infer_type, unify_types
from octoflow.utils.escape import escape, unescape

__all__ = [
    "Document",
    "DocumentBatch",
]


class Document(Mapping[str, Any]):
    """Document class."""

    id: Any
    _id_type: pa.DataType
    _data: Mapping[str, Any]
    _schema: pa.Schema

    def __init__(
        self,
        data: Mapping[str, Any],
        /,
        id: Union[itemgetter, Field, Any] = MISSING,
        *,
        schema: Union[pa.Schema, None] = None,
    ):
        """Create a new document.

        Parameters
        ----------
        data : dict
            The data of the document.
        id : Union[itemgetter, Any], optional
            The id of the document, by default MISSING.
        schema : dict, optional
            The schema of the document, by default None.
        """
        # Data (must be json serializable with orjson)
        if not isinstance(data, dict):
            msg = "data must be a dictionary"
            raise ValueError(msg)
        data = orjson.loads(orjson.dumps(data))
        # Id
        if id is MISSING:
            id = Field("id")
        elif isinstance(id, itemgetter):
            id = Field(id)
        id_type = None
        if isinstance(id, Field):
            id_type = id.type
            id = id(data)
        if id_type is None:
            id_type = infer_type(id)
        # Schema
        if not isinstance(schema, pa.Schema):
            schema = infer_schema(data)
        self.id = id
        self._id_type = id_type
        self._data = data
        self._schema = schema

    @property
    def schema(self) -> pa.Schema:
        """Return the schema of the document.

        Returns
        -------
        Schema
            The schema of the document.
        """
        return self._schema

    def __getitem__(self, key: str):
        """Get the value of a field.

        Parameters
        ----------
        key : str
            The field to be accessed.

        Returns
        -------
        Any
            The value of the field.
        """
        return self._data[key]

    def __iter__(self):
        """Iterate over the fields of the document.

        Returns
        -------
        iter
            An iterator over the fields of the document.
        """
        return iter(self._data)

    def __len__(self):
        """Return the number of fields in the document.

        Returns
        -------
        int
            The number of fields in the document.
        """
        return len(self._data)

    def __repr__(self):
        """Return a string representation of the document.

        Returns
        -------
        str
            The string representation of the document.
        """
        return f"Document({self._data})"


class DocumentBatch(Sequence[Document]):
    """DocumentBatch class -- list version of a table."""

    _ids: List[Any]
    _data: Dict[str, List[Any]]
    _schema: Union[pa.Schema, None]
    _id_index: Dict[int, int]
    _id_type: pa.DataType
    _length: int

    def __init__(self, docs: Optional[Sequence[Document]] = None):
        """Create a new document index."""
        self._length = 0
        self._ids = []
        self._data = {}
        self._id_index = {}
        self._schema = None
        self._id_type = UNDEFINED
        if docs is not None:
            self.extend(docs)

    @property
    def schema(self) -> pa.Schema:
        """Return the schema of the document list.

        Returns
        -------
        Schema
            The schema of the document list.
        """
        return self._schema

    @overload
    def __getitem__(self, item: slice) -> DocumentBatch: ...

    @overload
    def __getitem__(self, item: int) -> Document: ...

    def __getitem__(
        self, item: Union[int, slice]
    ) -> Union[Document, DocumentBatch]:
        """Get a document by index.

        Parameters
        ----------
        item : int
            The index of the document to be accessed.

        Returns
        -------
        Document
            The document at the specified index.
        """
        if isinstance(item, slice):
            dlist = self.__class__()
            # filter the data
            dlist._data = {k: v[item] for k, v in self._data.items()}
            # filter the ids
            dlist._ids = self._ids[item]
            dlist._id_index = {id: i for i, id in enumerate(dlist._ids)}
            # copy the schema and id type
            dlist._schema = self._schema
            dlist._id_type = self._id_type
            return dlist
        return Document(
            {k: v[item] for k, v in self._data.items()},
            id=self._ids[item],
            schema=self._schema,
        )

    def append(self, doc: Document):
        """Add a document to the index.

        Parameters
        ----------
        doc : Document
            The document to be added.

        Raises
        ------
        ValueError
            If the document already exists in the index.
        """
        # validate the document
        if doc.id in self._id_index:
            msg = f"document with id {doc.id} already exists"
            raise ValueError(msg)
        unified_schema = unify_schemas(doc.schema, self._schema)
        unified_id_type = unify_types(self._id_type, doc._id_type)
        # update the list
        self._schema = unified_schema
        self._id_type = unified_id_type
        for col in self._schema.names:
            self._data.setdefault(
                col, [None for _ in range(self._length)]
            ).append(doc.get(col, None))
        self._ids.append(doc.id)
        self._id_index[doc.id] = self._length
        self._length += 1

    def extend(self, docs: Sequence[Document]):
        """Add multiple documents to the index.

        Parameters
        ----------
        docs : Sequence[Document]
            The documents to be added.

        Raises
        ------
        ValueError
            If any document already exists in the index.
        """
        for doc in docs:
            self.append(doc)

    def __len__(self) -> int:
        """Return the number of documents in the index.

        Returns
        -------
        int
            The number of documents in the index.
        """
        return self._length

    def __iter__(self) -> Generator[Document, None, None]:
        """Iterate over the documents in the index.

        Returns
        -------
        iter
            An iterator over the documents in the index.
        """
        for i in range(self._length):
            yield self[i]

    def __contains__(self, item: Document) -> bool:
        """Check if a document is in the index.

        Parameters
        ----------
        item : Document
            The document to be checked.

        Returns
        -------
        bool
            True if the document is in the index, False otherwise.
        """
        return item.id in self._id_index

    def __repr__(self) -> str:
        """Return a string representation of the document list.

        Returns
        -------
        str
            The string representation of the document list.
        """
        if self._length > 3:  # noqa: PLR2004
            sample = self._data[:2] + ["...", self._data[-1]]
        else:
            sample = self._data
        return f"DocumentList[{self._length}]({sample})"

    def to_arrow(self) -> pa.RecordBatch:
        """Return the document list as a table.

        Returns
        -------
        pa.RecordBatch
            The document list as a RecordBatch.
        """
        id_field = pa.field("_id", self._id_type)
        fields = [id_field]
        arrays = [pa.array(self._ids, type=self._id_type)]
        for col in self._schema.names:
            f = self._schema.field(col)
            fields.append(f.with_name(escape(f.name)))
            arrays.append(pa.array(self._data[col], type=f.type))
        return pa.record_batch(arrays, schema=pa.schema(fields))

    @classmethod
    def from_arrow(cls, batch: Union[pa.RecordBatch, pa.Table]) -> Self:
        """Create a document batch from a pyarrow.RecordBatch or pyarrow.Table.

        Parameters
        ----------
        batch : pa.RecordBatch | pa.Table
            The table to be converted to a document batch.

        Returns
        -------
        DocumentBatch
            The document list.
        """
        this = cls.__new__(cls)
        id_field_index = batch.schema.get_field_index("_id")
        if id_field_index < 0:
            msg = "missing '_id' field"
            raise ValueError(msg)
        id_col = batch.column(id_field_index)
        this._ids = id_col.to_pylist()
        columns = [(f, unescape(f)) for f in batch.schema.names if f != "_id"]
        this._data = {
            key: batch.column(col).to_pylist() for col, key in columns
        }
        this._schema = pa.schema([
            batch.schema.field(col).with_name(key) for col, key in columns
        ])
        this._id_index = {id: i for i, id in enumerate(this._ids)}
        this._id_type = id_col.type
        this._length = id_col.length()
        return this
