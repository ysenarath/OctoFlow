from __future__ import annotations

import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, List, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from octoflow import logging
from octoflow.utils import hashutils, resources

logger = logging.get_logger(__name__)


def generate_unique_path(
    reference: Any,
    cache_dir: Optional[Union[str, Path]] = None,
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
        reference_hash = hashutils.hash(reference)
        path = cache_dir / f"data-{reference_hash}"
    except Exception as e:
        # an error occurred while hashing data
        msg = "failed to create unique path, using temporary directory"
        msg = e.args[0] if e.args else msg
        logger.warning(msg)
        path = Path(tempfile.mkdtemp(dir=cache_dir))
    return path


def create_table(data: Any) -> pa.Table:
    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pd.DataFrame):
        return pa.Table.from_pandas(data)
    if isinstance(data, Mapping):
        return pa.Table.from_pydict(data)
    if isinstance(data, Sequence) and not isinstance(data, str):
        return pa.Table.from_pylist(list(data))
    dtype = type(data).__name__
    msg = f"expected data to be one of: Mapping, Sequence[Mapping] or pd.DataFrame, got '{dtype}'"
    raise ValueError(msg)


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


def write_dataset(
    path: Union[str, Path],
    data: Union[
        ds.Dataset,
        pa.RecordBatch,
        pa.Table,
        List[pa.RecordBatch],
        List[pa.Table],
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
    ds.write_dataset(
        data,
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
