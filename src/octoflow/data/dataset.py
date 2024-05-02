from __future__ import annotations

import contextlib
import functools
import os
import shutil
import tempfile
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
from numpy.typing import ArrayLike
from pandas import DataFrame
from pyarrow import dataset as ds
from tqdm import auto as tqdm

from octoflow import logging
from octoflow.data.base import DEFAULT_BATCH_SIZE, DEFAULT_FORMAT, BaseDataset
from octoflow.data.dataclass import BaseModel
from octoflow.data.expression import Expression
from octoflow.data.loaders import DatasetLoader, loaders
from octoflow.data.schema import get_schema, get_schema_from_dataclass
from octoflow.utils import hashutils
from octoflow.utils.cacheutils import cache

logger = logging.get_logger(__name__)

SourceType = Union[
    str,
    List[str],
    Union[Path, List[Path]],
    "Dataset",
    List["Dataset"],
]


def _map_func_wrapper(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        output = func(*args, **kwargs)
        if isinstance(output, pd.Series):
            return output
        return pd.Series(output)

    return wrapped


class Dataset(BaseDataset):  # noqa: PLR0904
    @overload
    def __init__(
        self,
        data: Union[List[dict], Dict[str, list], DataFrame] = None,
        format: str = DEFAULT_FORMAT,
    ): ...

    @overload
    def __init__(
        self,
        data: Union[List[dict], Dict[str, list], DataFrame],
        format: str = DEFAULT_FORMAT,
        *,
        path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[Union[str, Path]] = None,
    ): ...

    @overload
    def __init__(
        self,
        loader: DatasetLoader,
        format: str = DEFAULT_FORMAT,
        *,
        path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[Union[str, Path]] = None,
        loader_args: Optional[Tuple[Any, ...]] = None,
        loader_kwargs: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ): ...

    def __init__(
        self,
        data_or_loader: Union[
            List[dict], Dict[str, list], DataFrame, DatasetLoader, str
        ] = None,
        format: str = DEFAULT_FORMAT,
        *,
        schema: Union[pa.Schema, BaseModel, None] = None,
        path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[Union[str, Path]] = None,
        loader_args: Optional[Tuple[Any, ...]] = None,
        loader_kwargs: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ):
        """
        Create a new dataset.

        Parameters
        ----------
        data_or_loader : list of dict, dict of list, DataFrame, BaseDatasetLoader, str
            The data to load into the dataset or the (name of) loader to use.
        format : str
            The format of the dataset.
        path : str, Path, None
            The path to the dataset.
        cache_dir : str, Path, None
            The directory to use for caching.
        loader_args : tuple, None
            The arguments to pass to the loader function if provided
                as the first argument.
        loader_kwargs : dict, None
            The keyword arguments to pass to the loader function if provided
                as the first argument.
        """  # noqa: E501
        if isinstance(data_or_loader, str):
            data_or_loader = loaders[data_or_loader]
        if isinstance(data_or_loader, DatasetLoader):
            if loader_args is None:
                loader_args = ()
            if loader_kwargs is None:
                loader_kwargs = {}
            data_or_loader = data_or_loader.bind(*loader_args, **loader_kwargs)
            data = data_or_loader()  # will result in a generator in most cases
        else:
            data = data_or_loader
        if path is None:
            # path resulted in here is user expanded and resolved
            path = generate_unique_path(data_or_loader, cache_dir=cache_dir)
        else:
            path = Path(path).expanduser().resolve()
        if force:
            shutil.rmtree(path, ignore_errors=True)
        if format is None:
            format = DEFAULT_FORMAT
        created = write_dataset(path, data, schema=schema, format=format)
        if not created:
            msg = (
                f"existing dataset found at '{path}', loading existing file(s)"
            )
            logger.warning(msg)
        dataset = read_dataset(path, format=format)
        super().__init__(dataset)
        self._path = path
        self._format = format

    @property
    def path(self) -> Path:
        """
        The path to the dataset.

        Returns
        -------
        Path
            The path to the dataset.
        """
        return self._path

    @property
    def format(self) -> str:
        """
        The format of the dataset.

        Returns
        -------
        str
            The format of the dataset.
        """
        return self._format

    @property
    def _wrapped_format_default_extname(self) -> str:
        return self._wrapped.format.default_extname

    def count_rows(self) -> int:
        """
        Count the number of rows in the dataset.

        Returns
        -------
        int
            The number of rows in the dataset.
        """
        return self._wrapped.count_rows()

    def __len__(self) -> int:
        """
        Get the number of rows in the dataset.

        Returns
        -------
        int
            The number of rows in the dataset.
        """
        return self.count_rows()

    def head(
        self,
        num_rows: int = 5,
        columns: Union[str, List[str], None] = None,
        filter: Expression = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> DataFrame:
        """
        Get the first rows of the dataset as a pandas DataFrame.

        Parameters
        ----------
        num_rows : int
            The number of rows to get.
        columns : str, list of str, None
            Names of columns to get. If None, all columns are returned.
        filter : Expression
            The filter expression.
        batch_size : int
            Number of rows to get at a time.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the first rows of the dataset.
        """
        filter = filter.to_pyarrow() if filter else None
        if isinstance(columns, str):
            columns = [columns]
        table: pa.Table = self._wrapped.head(
            num_rows=num_rows,
            columns=columns,
            filter=filter,
            batch_size=batch_size,
        )
        return table.to_pandas()

    @overload
    def __getitem__(self, indices: int) -> Dict[str, Any]: ...

    @overload
    def __getitem__(
        self, indices: Union[slice, List[int], ArrayLike]
    ) -> pa.Table: ...

    def __getitem__(
        self, indices: Union[int, slice, List[int], ArrayLike]
    ) -> Union[dict, pa.Table]:
        """Get rows from the dataset."""
        return self.take(indices=indices)

    @overload
    def take(
        self,
        *,
        indices: Optional[int] = None,
        columns: Union[str, List[str], None] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Dict[str, Any]: ...

    @overload
    def take(
        self,
        *,
        indices: Union[slice, List[int], ArrayLike] = None,
        columns: Union[str, List[str], None] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> DataFrame: ...

    def take(
        self,
        *,
        indices: Union[int, slice, List[int], ArrayLike] = None,
        columns: Union[str, List[str], None] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Union[dict, pa.Table]:
        """
        Take rows from the dataset.

        Parameters
        ----------
        indices : int, slice, list of int, array-like
            Indices of rows to take.
        columns : str, list of str, None
            Names of columns to take. If None, all columns are taken.
        batch_size : int
            Number of rows to take at a time.

        Returns
        -------
        Document, pyarrow.Table
            The taken rows or row.
        """
        return_batch = True
        if indices is None:
            indices = []
        elif isinstance(indices, int):
            return_batch = False
            indices = [indices]
        elif isinstance(indices, slice):
            step_size = indices.step
            if step_size is None:
                # default step size is 1
                step_size = 1
            elif not isinstance(step_size, int):
                msg = "expected indices.step to be int, got '{indices.step}'"
                raise ValueError(msg)
            elif step_size < 1:
                msg = (
                    "expected indices.step to be greater than 0, "
                    f"got '{step_size}'"
                )
                raise ValueError(msg)
            start = indices.start or 0
            stop = indices.stop or self.count_rows()
            indices = np.arange(start, stop, step_size)
        if isinstance(columns, str):
            columns = [columns]
        batch = self._wrapped.take(
            indices=indices,  # array or array-like
            columns=columns,  # list of str or None
            batch_size=batch_size,  # int
        )
        if return_batch:
            return batch
        return batch.to_pylist()[0]

    def map(
        self,
        func: Any,
        batch_size: int = DEFAULT_BATCH_SIZE,
        batched: bool = False,
        verbose: Union[bool, int] = 1,
    ) -> Dataset:
        """
        Map a function over the dataset.

        Parameters
        ----------
        func : Any
            The function to map over the dataset.
        batch_size : int
            Number of rows to map at a time.
        batched : bool
            Whether the function is batched.
        verbose : bool | int
            Whether to show a progress bar.

        Returns
        -------
        Dataset
            A new dataset containing the mapped rows.
        """
        fingerprint = hashutils.hash(func)
        path = self.path / f"map-{fingerprint}"
        num_batches = ((self.count_rows() - 1) // batch_size) + 1
        batch_iter = self._wrapped.to_batches(batch_size=batch_size)
        if verbose:
            progress_bar = tqdm.tqdm(
                batch_iter,
                total=num_batches,
                desc=f"Mapping [{fingerprint}]",
            )
        else:
            progress_bar = None
        batch_iter = (
            record_batch(
                func(batch)
                if batched
                else batch.to_pandas().apply(
                    _map_func_wrapper(func),
                    axis=1,
                )
            )
            for batch in (batch_iter if progress_bar is None else progress_bar)
        )
        state = write_dataset(
            path,
            batch_iter,
            format=self.format,
        )
        if not state:
            logger.warning(
                f"existing dataset found at '{path}', "
                "loading existing file(s)"
            )
        if progress_bar is not None:
            progress_bar.update(num_batches)
            progress_bar.close()
        return type(self).load_dataset(path, format=self.format)

    def filter(self, expression: Expression = None) -> Dataset:
        """
        Filter the dataset.

        Parameters
        ----------
        expression : Expression
            The filter expression.

        Returns
        -------
        Dataset
            A new dataset containing only the rows that match the filter expression.
        """  # noqa: E501
        if expression is None:
            return self
        pyarrow_expression = expression.to_pyarrow()
        fingerprint = hashutils.hash(pyarrow_expression)
        path = self.path / f"filter-{fingerprint}"
        dataset = self._wrapped.filter(pyarrow_expression)
        state = write_dataset(
            path,
            dataset,
            schema=dataset.schema,
            format=self.format,
        )
        if not state:
            logger.warning(
                f"existing dataset found at '{path}', "
                "loading existing file(s)"
            )
        return self.load_dataset(path, format=self.format)

    def select(
        self,
        columns: Union[str, List[str]],
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Dataset:
        """
        Select columns from the dataset.

        Parameters
        ----------
        columns : str, list of str
            Names of columns to select.

        Returns
        -------
        Dataset
            A new dataset containing only the selected columns.
        """
        if isinstance(columns, str):
            columns = [columns]
        fingerprint = hashutils.hash(columns)
        path = self.path / f"select-{fingerprint}"
        scanner = ds.Scanner.from_dataset(
            self._wrapped,
            columns=columns,
            batch_size=batch_size,
        )
        state = write_dataset(
            path,
            scanner,
            format=self.format,
        )
        if not state:
            logger.warning(
                f"existing dataset found at '{path}', "
                "loading existing file(s)"
            )
        return self.load_dataset(path, format=self.format)

    def cleanup(self):
        """
        Delete the directory containing the dataset.

        Returns
        -------
        None
        """
        if not self.path.exists():
            return
        shutil.rmtree(self.path)

    def __enter__(self) -> Dataset:
        """
        Context manager entry point.

        Returns
        -------
        Dataset
            The dataset.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit point.

        This will delete the resources allocated for this dataset.

        Parameters
        ----------
        exc_type : Exception
            The exception type.
        exc_value : Exception
            The exception value.
        traceback : Traceback
            The traceback.

        Returns
        -------
        None
        """
        with contextlib.suppress(Exception):
            self.cleanup()

    @classmethod
    def load_dataset(
        cls,
        path: Union[Path, str],
        format: str = DEFAULT_FORMAT,
    ) -> Dataset:
        """
        Load an existing dataset.

        Parameters
        ----------
        path : str, Path
            The path to the dataset.
        format : str
            The format of the dataset.

        Returns
        -------
        Dataset
            The loaded dataset.
        """
        inst = cls.__new__(cls)
        inst._wrapped = read_dataset(path, format=format)
        inst._path = path
        inst._format = format
        return inst

    def to_polars(self) -> pl.LazyFrame:
        """
        Convert the dataset to a Polars DataFrame.

        Returns
        -------
        pl.LazyFrame
            The Polars Lazy DataFrame.
        """
        return pl.scan_ipc(self.path / "data" / "*.arrow")


def write_dataset(
    path: Union[str, Path],
    data: Union[
        ds.Dataset,
        pa.Table,
        pa.RecordBatch,
        # Iterable[pa.Table],
        Iterable[pa.RecordBatch],  # pa.RecordBatchReader.from_batches
        pa.RecordBatchReader,
        pd.DataFrame,  # from_pandas
        Mapping[str, List[Any]],  # from_pydict
        Sequence[Mapping[str, Any]],  # from_pylist
    ],
    schema: pa.Schema = None,
    format: Optional[str] = None,
) -> bool:
    path = Path(path).expanduser().resolve()
    if not path.exists():
        path.mkdir(
            parents=True,
            exist_ok=True,
        )
    # first write to temporary directory
    temp_path = Path(
        tempfile.mkdtemp(
            prefix=".temp-",
            dir=path,
        )
    )
    out_data_path = get_data_path(path)
    if out_data_path.exists():
        return False
    if isinstance(schema, type) and issubclass(schema, BaseModel):
        schema = get_schema_from_dataclass(schema)
    if not isinstance(data, (ds.Dataset, ds.Scanner)):
        data = record_batch(data, schema=schema)
    if isinstance(data, pa.RecordBatchReader):
        schema = None
    ds.write_dataset(data, temp_path, schema=schema, format=format)
    try:
        os.rename(temp_path, out_data_path)
    except FileExistsError:
        return False
    return True


def read_dataset(path: Union[str, Path], format: str) -> ds.dataset:
    path = Path(path).expanduser().resolve()
    data_path = get_data_path(path)
    return ds.dataset(
        data_path,
        format=format,
    )


def get_data_path(path: Union[str, Path]) -> Path:
    path = Path(path).expanduser().resolve()
    if path.is_dir() or not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        data_path = path / "data"
    else:
        msg = f"expected path to be directory, got '{path}'"
        raise ValueError(msg)
    return data_path


def generate_unique_path(
    reference: Any,
    cache_dir: Union[str, Path, None] = None,
) -> Path:
    # create a temporary directory in system temp directory
    if cache_dir is None:
        cache_dir = cache.path / "datasets"
    cache_dir = Path(cache_dir).expanduser().resolve()
    if not cache_dir.exists():
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            msg = f"failed to create cache directory at '{cache_dir}'"
            raise OSError(msg) from e
    try:
        fingerprint = hashutils.hash(reference)
        path = cache_dir / f"data-{fingerprint}"
    except Exception as e:
        # an error occurred while hashing data
        msg = (
            "Using temporary directory; "
            f"Failed to create unique path due to the following error: {e}."
        )
        logger.warning(msg)
        path = Path(tempfile.mkdtemp(dir=cache_dir))
    return path


def record_batch(
    data: Any, schema: Optional[pa.Schema] = None
) -> Union[pa.RecordBatch, pa.RecordBatchReader]:
    # if generator, return a generator
    if isinstance(data, pa.RecordBatch):
        return data
    if isinstance(data, Generator):
        # Note: generator of batches not instances
        data = (record_batch(d, schema=schema) for d in data)
        if schema is None:
            data, schema = get_schema(data)
        return pa.RecordBatchReader.from_batches(schema, data)
    if isinstance(data, pa.Table):
        # zero-copy
        if schema is None:
            schema = data.schema
        return pa.RecordBatchReader.from_batches(
            schema, (d for d in data.to_batches())
        )
    if isinstance(data, pd.DataFrame):
        return pa.RecordBatch.from_pandas(data, schema=schema)
    if isinstance(data, Mapping):
        return pa.RecordBatch.from_pydict(data, schema=schema)
    if isinstance(data, Sequence) and not isinstance(data, str):
        return pa.RecordBatch.from_pylist(list(data), schema=schema)
    dtype = data.__class__.__name__
    msg = (
        f"expected data to be of type 'pa.RecordBatch', 'pa.Table', "
        f"'pd.DataFrame', Mapping, or Sequence, got '{dtype}'"
    )
    raise ValueError(msg)
