from __future__ import annotations

import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Generator, Iterable, List, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from octoflow import logging
from octoflow.data.schema import get_schema
from octoflow.utils import hashutils, resources

logger = logging.get_logger(__name__)


def generate_unique_path(
    reference: Any, cache_dir: Optional[Union[str, Path]] = None
) -> Path:
    # create a temporary directory in system temp directory
    if cache_dir is None:
        cache_dir = resources.get_cache_path() / "datasets"
    if cache_dir is not None and not cache_dir.exists():
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
        msg = "failed to create unique path, using temporary directory"
        msg = e.args[0] if e.args else msg
        logger.warning(msg)
        path = Path(tempfile.mkdtemp(dir=cache_dir))
    return path


def get_data_path(path: Union[Path, str]) -> Path:
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir() or not path.exists():
        path.mkdir(
            parents=True,
            exist_ok=True,
        )
        data_path = path / "data"
    else:
        msg = f"expected path to be directory, got '{path}'"
        raise ValueError(msg)
    return data_path


def record_batch(data: Any) -> Union[pa.RecordBatch, pa.RecordBatchReader]:
    # if generator, return a generator
    if isinstance(data, pa.RecordBatch):
        return data
    if isinstance(data, Generator):
        data = (record_batch(d) for d in data)
        data, schema = get_schema(data)
        return pa.RecordBatchReader.from_batches(schema, data)
    if isinstance(data, pa.Table):
        # zero-copy
        return pa.RecordBatchReader.from_batches(
            data.schema, (d for d in data.to_batches())
        )
    if isinstance(data, pd.DataFrame):
        return pa.RecordBatch.from_pandas(data)
    if isinstance(data, Mapping):
        return pa.RecordBatch.from_pydict(data)
    if isinstance(data, Sequence) and not isinstance(data, str):
        return pa.RecordBatch.from_pylist(list(data))
    dtype = data.__class__.__name__
    msg = (
        f"expected data to be of type 'pa.RecordBatch', 'pa.Table', "
        f"'pd.DataFrame', Mapping, or Sequence, got '{dtype}'"
    )
    raise ValueError(msg)


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
    if isinstance(path, str):
        path = Path(path)
    if not path.exists():
        path.mkdir(
            parents=True,
            exist_ok=True,
        )
    out_data_path = get_data_path(path)
    if out_data_path.exists():
        return False
    # first write to temporary directory
    temp_path = Path(
        tempfile.mkdtemp(
            prefix=".temp-",
            dir=path,
        )
    )
    # it might take some time to write the data
    # within that time, another process might try
    # to read the data or write the data so we
    # write to a temporary directory first
    # and then move the data to the desired
    # directory
    ds.write_dataset(
        data if isinstance(data, ds.Dataset) else record_batch(data),
        temp_path,
        schema=schema,
        format=format,
    )
    try:
        os.replace(temp_path, out_data_path)
    except OSError:
        return False
    return True


def read_dataset(
    path: Union[str, Path],
    format: str,
) -> ds.dataset:
    data_path = get_data_path(path)
    return ds.dataset(
        data_path,
        format=format,
    )
