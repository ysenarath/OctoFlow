from __future__ import annotations

import contextlib
import functools
import itertools
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, overload

import numpy as np
import pyarrow as pa
from numpy.typing import ArrayLike

from octoflow import logging
from octoflow.data.base import BaseDataset, BaseDatasetLoader
from octoflow.data.compute import Expression
from octoflow.data.constants import DEFAULT_BATCH_SIZE, DEFAULT_FORMAT
from octoflow.data.utils import create_table, generate_unique_path, read_dataset, write_dataset
from octoflow.utils import hashutils

try:
    import pandas as pd
    from pandas import DataFrame as DataFrameType
except ImportError:
    pd = None
    DataFrameType = None

logger = logging.get_logger(__name__)

SourceType = Union[str, List[str], Union[Path, List[Path]], "Dataset", List["Dataset"]]


def _map_func_wrapper(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        output = func(*args, **kwargs)
        if isinstance(output, pd.Series):
            return output
        return pd.Series(output)

    return wrapped


class Dataset(BaseDataset):
    @overload
    def __init__(
        self,
        data: Union[List[dict], Dict[str, list], DataFrameType] = None,
        format: str = DEFAULT_FORMAT,
    ): ...

    @overload
    def __init__(
        self,
        data: Union[List[dict], Dict[str, list], DataFrameType],
        format: str = DEFAULT_FORMAT,
        *,
        path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[Union[str, Path]] = None,
    ): ...

    @overload
    def __init__(
        self,
        loader: BaseDatasetLoader,
        format: str = DEFAULT_FORMAT,
        *,
        path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[Union[str, Path]] = None,
        loader_args: Optional[Tuple[Any, ...]] = None,
        loader_kwargs: Optional[Dict[str, Any]] = None,
    ): ...

    def __init__(
        self,
        data_or_loader: Union[List[dict], Dict[str, list], DataFrameType, BaseDatasetLoader] = None,
        format: str = DEFAULT_FORMAT,
        *,
        path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[Union[str, Path]] = None,
        loader_args: Optional[Tuple[Any, ...]] = None,
        loader_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Create a new dataset.

        Parameters
        ----------
        data_or_loader : list of dict, dict of list, DataFrameType, BaseDatasetLoader
            The data to load into the dataset.
        format : str
            The format of the dataset.
        path : str, Path, None
            The path to the dataset.
        cache_dir : str, Path, None
            The directory to use for caching.
        loader_args : tuple, None
            The arguments to pass to the loader function if provided as the first argument.
        loader_kwargs : dict, None
            The keyword arguments to pass to the loader function if provided as the first argument.
        """
        if isinstance(data_or_loader, BaseDatasetLoader):
            if loader_args is None:
                loader_args = ()
            if loader_kwargs is None:
                loader_kwargs = {}
            signature = data_or_loader.get_signature()
            args = signature.bind_partial(
                *loader_args,
                **loader_kwargs,
            )
            args.apply_defaults()
            data_or_loader = functools.partial(
                data_or_loader.func,
                *args.args,
                **args.kwargs,
            )
            data = data_or_loader()
        else:
            data = data_or_loader
        if path is None:
            path = generate_unique_path(
                data_or_loader,
                cache_dir=cache_dir,
            )
        data = create_table(data)
        if format is None:
            format = DEFAULT_FORMAT
        created = write_dataset(
            path,
            data,
            schema=data.schema,
            format=format,
        )
        if not created:
            msg = f"existing dataset found at '{path}', loading existing file(s)"
            logger.warning(msg)
        dataset = read_dataset(
            path,
            format=format,
        )
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

    def head(
        self,
        num_rows: int = 5,
        columns: Union[str, List[str], None] = None,
        filter: Expression = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> DataFrameType:
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

    def take(
        self,
        indices: Union[int, slice, List[int], ArrayLike] = None,
        columns: Union[str, List[str], None] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> DataFrameType:
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
        DataFrameType
            A pandas DataFrame containing the taken rows.
        """
        if indices is None:
            indices = []
        elif isinstance(indices, int):
            indices = [indices]
        elif isinstance(indices, slice):
            step_size = indices.step
            if step_size is None:
                # default step size is 1
                step_size = 1
            elif not isinstance(step_size, int):
                msg = f"expected indices.step to be int, got '{indices.step}'"
                raise ValueError(msg)
            elif step_size < 1:
                msg = f"expected indices.step to be greater than 0, got '{step_size}'"
                raise ValueError(msg)
            start = indices.start or 0
            stop = indices.stop or self.count_rows()
            indices = np.arange(start, stop, step_size)
        if isinstance(columns, str):
            columns = [columns]
        table: pa.Table = self._wrapped.take(
            indices=indices,  # array or array-like
            columns=columns,  # list of str or None
            batch_size=batch_size,  # int
        )
        return table.to_pandas()

    def map(
        self,
        func: Any,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Dataset:
        """
        Map a function over the dataset.

        Parameters
        ----------
        func : Any
            The function to map over the dataset.
        batch_size : int
            Number of rows to map at a time.

        Returns
        -------
        Dataset
            A new dataset containing the mapped rows.
        """
        batch_iter = (
            pa.RecordBatch.from_pandas(
                batch.to_pandas().apply(
                    _map_func_wrapper(func),
                    axis=1,
                ),
            )
            for batch in self._wrapped.to_batches(batch_size=batch_size)
        )
        try:
            first: pa.RecordBatch = next(batch_iter)
        except StopIteration:
            return self
        batch_iter = itertools.chain([first], batch_iter)
        func_hash = hashutils.hash(func)
        path = self.path / f"map-{func_hash}"
        _ = write_dataset(
            path,
            batch_iter,
            schema=first.schema,
            format=self.format,
        )
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
        """
        if expression is None:
            return self
        pyarrow_expression = expression.to_pyarrow()
        dataset = self._wrapped.filter(pyarrow_expression)
        expr_hash = hashutils.hash(pyarrow_expression)
        path = self.path / f"filter-{expr_hash}"
        write_dataset(
            path,
            dataset,
            schema=dataset.schema,
            format=self.format,
        )
        return type(self).load_dataset(path, format=self.format)

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

    def __len__(self) -> int:
        """
        Get the number of rows in the dataset.

        Returns
        -------
        int
            The number of rows in the dataset.
        """
        return self.count_rows()

    def __getitem__(
        self,
        indices: Union[int, slice, List[int]],
    ) -> Dataset:
        """Get rows from the dataset."""
        return self.take(indices)

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
