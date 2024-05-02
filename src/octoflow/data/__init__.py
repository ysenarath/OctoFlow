from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from octoflow.data.dataclass import field
from octoflow.data.dataset import DEFAULT_FORMAT, Dataset
from octoflow.data.expression import Expression, scalar
from octoflow.data.loaders import dataloader
from octoflow.data.loaders import loaders as _loaders

__all__ = [
    "Dataset",
    "Expression",
    "dataloader",
    "field",
    "load_dataset",
    "scalar",
]


def load_dataset(
    __loader: str,
    __path: Optional[str],
    __force: bool = False,
    __dataset_format: str = DEFAULT_FORMAT,
    __dataset_path: Union[Path, str, None] = None,
    /,
    *args,
    **kwargs,
) -> Dataset:
    """
    Load a dataset from a path.

    Parameters
    ----------
    __loader : str
        The name of the loader.
    __path : Optional[str]
        The path to the data (to be passed to the loader).
    __dataset_format : str, optional
        The format of the dataset, by default DEFAULT_FORMAT.
    __dataset_path : Union[Path, str, None], optional
        The path that the dataset will be stored.
    *args : tuple
        The arguments to pass to the loader.
    **kwargs : dict
        The keyword arguments to pass to the loader.

    Returns
    -------
    Dataset
        The loaded dataset.
    """
    loader = _loaders.get(__loader)
    if loader is None:
        # notify the user that the loader is not found
        msg = f"loader '{__loader}' not found"
        raise ValueError(msg)
    if loader.path_arg is not None:
        kwargs[loader.path_arg] = __path
    else:
        # assume the first argument is the path
        args = (__path, *args)
    return Dataset(
        loader,
        __dataset_format,
        path=__dataset_path,
        loader_args=args,
        loader_kwargs=kwargs,
        force=__force,
    )
