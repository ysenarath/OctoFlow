from __future__ import annotations

import functools
import glob
import inspect
import json
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    TypeVar,
    Union,
    overload,
)

import pandas as pd
from typing_extensions import ParamSpec

from octoflow import logging
from octoflow.data.base import BaseDatasetLoader

logger = logging.get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
F = TypeVar("F", bound=Callable[..., Any])

loaders: Dict[str, DatasetLoader] = {}


class DatasetLoader(BaseDatasetLoader):
    def __init__(
        self,
        func: Callable[..., Any],
        name: Optional[str] = None,
        extensions: Optional[list[str]] = None,
        path_arg: Optional[str] = None,
        wraps: Optional[Callable[P, R]] = None,
    ):
        """
        Initialize a dataset loader.

        Parameters
        ----------
        func : Callable[..., Any]
            The function to decorate.
        name : Optional[str], optional
            The name of the loader, by default None.
        extensions : Optional[list[str]], optional
            The extensions that the loader supports, by default None.
        path_arg : Optional[str], optional
            The name of the argument that is the path, by default None.
        wraps : Optional[Callable[..., Any]], optional
            The function to wrap, by default None.
        """
        super().__init__()
        self.func = func
        if extensions is None:
            extensions = []
        elif isinstance(extensions, str):
            extensions = [extensions]
        self.name = name or self.func.__name__
        self.extensions = extensions
        self.path_arg = path_arg
        self.wraps = wraps

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        return self.func(*args, **kwargs)

    def partial(self, *args: P.args, **kwargs: P.kwargs) -> Callable[..., R]:
        """
        Create a partial function with pre-filled arguments and keyword arguments.

        Notes
        -----
        This method is useful for creating a partial function with pre-filled
        arguments and keyword arguments. This helps to improve the uniqueness
        of the fingerprint of the dataset.

        Parameters
        ----------
        args : tuple
            The arguments to pre-fill.
        kwargs : dict
            The keyword arguments to pre-fill.

        Returns
        -------
        Callable[..., R]
            The partial function.
        """  # noqa: E501
        signature = inspect.signature(self.func)
        bound_args = signature.bind_partial(*args, **kwargs)
        bound_args.apply_defaults()
        return functools.partial(self, *bound_args.args, **bound_args.kwargs)


@overload
def dataloader(
    func: F,
    name: Optional[str] = None,
    extensions: Optional[list[str]] = None,
    wraps: Optional[Callable[P, R]] = None,
    path_arg: Optional[str] = None,
) -> F:
    """
    Decorator to register a function as a dataset loader.

    Parameters
    ----------
    func : Callable[..., Any]
        The function to decorate.
    name : Optional[str], optional
        The name of the loader, by default None.
    extensions : Optional[list[str]], optional
        The extensions that the loader supports, by default None.
    wraps : Optional[Callable[..., Any]], optional
        The function to wrap, by default None.
    path_arg : Optional[str], optional
        The name of the argument that is the path, by default None.

    Returns
    -------
    F
        The input function.
    """
    ...


@overload
def dataloader(
    name: str,
    extensions: Optional[list[str]] = None,
    wraps: Optional[Callable[P, R]] = None,
    path_arg: Optional[str] = None,
) -> Callable[[F], F]:
    """
    Decorator to register a function as a dataset loader.

    Parameters
    ----------
    name : str
        The name of the loader.
    extensions : Optional[list[str]], optional
        The extensions that the loader supports, by default None.
    wraps : Optional[Callable[..., Any]], optional
        The function to wrap, by default None.
    path_arg : Optional[str], optional
        The name of the argument that is the path, by default None.

    Returns
    -------
    Callable[[F], F]
        The decorator.
    """
    ...


def dataloader(
    func: Union[F, str, None] = None,
    name: Optional[str] = None,
    extensions: Optional[list[str]] = None,
    wraps: Optional[Callable[..., Any]] = None,
    path_arg: Optional[str] = None,
) -> Union[F, Callable[[F], F]]:
    """
    Decorator to register a function as a dataset loader.

    Parameters
    ----------
    func : Union[Callable[..., Any], str, None], optional
        The function to decorate, by default None.
    name : Optional[str], optional
        The name of the loader, by default None.
    extensions : Optional[list[str]], optional
        The extensions that the loader supports, by default None.
    wraps : Optional[Callable[..., Any]], optional
        The function to wrap, by default None.
    path_arg : Optional[str], optional
        The name of the argument that is the path, by default None.

    Returns
    -------
    DatasetLoader
        The dataset loader.
    """
    if func is None:
        # out type: Callable[[F], F]
        return functools.partial(
            dataloader,
            name=name,
            extensions=extensions,
            wraps=wraps,
            path_arg=path_arg,
        )
    elif isinstance(func, str):
        # out type: Callable[[F], F]
        return functools.partial(
            dataloader,
            name=func,
            extensions=extensions,
            wraps=wraps,
            path_arg=path_arg,
        )
    loader = DatasetLoader(
        func,
        name=name,
        extensions=extensions,
        path_arg=path_arg,
        wraps=wraps,
    )
    if loader.name in loaders:
        msg = f"loader with name '{loader.name}' already exists"
        raise ValueError(msg)
    loaders[loader.name] = loader
    # out type: F
    return func


@dataloader(name="json", extensions=[".json"], path_arg="path")
def load_json(
    path: Union[str, Path], encoding: str = "utf-8"
) -> Generator[List[Dict], None, None]:
    """
    Load a dataset from a JSON file.

    Parameters
    ----------
    path : str, Path
        The path to the file.
    encoding : str, optional
        The encoding of the file, by default "utf-8".

    Returns
    -------
    dict
        The loaded dataset.
    """
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir():
        logger.info("loading all .json files in the directory '%s'", path)
        path = path / "*.json"
    for p in glob.iglob(str(path)):
        with open(p, encoding=encoding) as f:
            yield json.load(f)


@dataloader(
    name="jsonl",
    extensions=[".jsonl", ".ndjson"],
    path_arg="path",
)
def load_jsonl(
    path: Union[str, Path], encoding: str = "utf-8"
) -> Generator[List[Dict], None, None]:
    """
    Load a dataset from a JSONL file.

    Parameters
    ----------
    path : str, Path
        The path to the file.
    encoding : str, optional
        The encoding of the file, by default "utf-8".

    Returns
    -------
    list[dict]
        The loaded dataset.
    """
    if isinstance(path, str):
        path = Path(path)
    logger.debug(
        "Is the provided path '%s' a directory? %s", path, path.is_dir()
    )
    if path.is_dir():
        logger.info("Loading all .jsonl files in the directory '%s'", path)
        path = path / "*.jsonl"
    for p in glob.iglob(str(path)):
        with open(p, encoding=encoding) as f:
            yield [json.loads(line) for line in f]


@dataloader(
    name="csv",
    extensions=[".csv", ".tsv"],
    path_arg="path",
)
def load_csv(
    path: Union[str, Path],
    encoding: str = "utf-8",
) -> Generator[List[Dict], None, None]:
    """
    Load a dataset from a CSV/TSV file.

    Parameters
    ----------
    path : str, Path
        The path to the file.
    encoding : str, optional
        The encoding of the file, by default "utf-8".

    Returns
    -------
    list[dict]
        The loaded dataset.
    """
    if isinstance(path, str):
        path = Path(path)
    if path.is_dir():
        logger.info("loading all .csv files in the directory '%s'", path)
        path = path / "*.csv"
    for p in glob.iglob(str(path)):
        p = Path(p)
        if p.suffix == ".tsv":
            return pd.read_csv(p, sep="\t", encoding=encoding).to_dict(
                orient="records"
            )
        yield pd.read_csv(p, encoding=encoding)
