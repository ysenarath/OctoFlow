from __future__ import annotations

import abc
import shutil
from pathlib import Path
from typing import Any, Dict, List, Type, Union

from octoflow.data.metadata import Metadata

_handler_types: Dict[str, Type[ArtifactHandler]] = {}

METADATA_FILENAME = "metadata.json"
TYPE_FIELD = "type"


def get_handler_type(name: str) -> Type[ArtifactHandler]:
    try:
        return _handler_types[name]
    except KeyError:
        msg = f"handler '{name}' not found"
        raise ValueError(msg) from None


def get_handler_type_by_object(obj: Any) -> Type[ArtifactHandler]:
    handler = None
    for handler_type in _handler_types.values():
        if not handler_type.can_handle(obj):
            continue
        if handler is not None:
            msg = f"multiple handlers found for '{type(obj).__name__}'"
            raise ValueError(msg)
        handler = handler_type
    if handler is not None:
        return handler
    # if we get here, we didn't find an appropriate handler for the object
    msg = f"'{obj.__class__.__name__}' has no handler"
    raise ValueError(msg)


def get_handler_type_by_path(path: Union[str, Path]) -> Type[ArtifactHandler]:
    metadata = Metadata(Path(path) / METADATA_FILENAME)
    if not path.exists():
        msg = f"artifact without metadata at '{path}'"
        raise FileNotFoundError(msg)
    if TYPE_FIELD not in metadata:
        msg = f"artifact metadata missing 'type' at '{path}'"
        raise KeyError(msg)
    handler_type_str = metadata[TYPE_FIELD]
    return get_handler_type(handler_type_str)


def list_handler_types() -> List[str]:
    return list(_handler_types.keys())


class ArtifactHandlerType(abc.ABCMeta):
    def __new__(cls, *args, **kwargs):
        handler_cls = super().__new__(cls, *args)
        base = args[1]  # base class
        if len(base) > 0:
            handler_cls._handler_type_name = kwargs.get("name", args[0])
            _handler_types[handler_cls._handler_type_name] = handler_cls
        return handler_cls

    @property
    def name(cls) -> str:
        return cls._handler_type_name


class ArtifactHandler(metaclass=ArtifactHandlerType):
    def __init__(self, path: Union[str, Path]) -> None:
        """
        Abstract base class for artifact handlers.

        Parameters
        ----------
        path : str
            The path to the artifact
        """
        super().__init__()
        path = Path(path)
        (path / "data").mkdir(parents=True, exist_ok=True)
        self.metadata = Metadata(path / METADATA_FILENAME)
        if TYPE_FIELD in self.metadata:
            if self.metadata[TYPE_FIELD] == self.__class__.name:
                return
            msg = (
                f"metadata type '{self.metadata[TYPE_FIELD]}' does "
                f"not match handler type '{self.__class__.name}'"
            )
            raise ValueError(msg)
        else:
            self.metadata[TYPE_FIELD] = self.__class__.name

    @property
    def path(self) -> Path:
        return self.metadata._path.parent / "data"

    @abc.abstractmethod
    def load(self) -> Any:
        """
        Load the artifact from the path.

        Returns
        -------
        Any
            The loaded artifact.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def save(self, obj: Any, *args, **kwargs):
        """
        Save the given artifact to the path.

        Parameters
        ----------
        obj : Any
            The artifact to save.
        args : tuple
            Additional positional arguments.
        kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        None
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def can_handle(cls, obj: object) -> bool:
        """
        Return True if this handler can handle the given object type.

        Parameters
        ----------
        obj : object
            The object to check.

        Returns
        -------
        bool
            True if this handler can handle the given object type.
        """
        raise NotImplementedError

    def exists(self) -> bool:
        """
        Return True if the artifact exists.

        Returns
        -------
        bool
            True if the artifact exists.
        """
        raise NotImplementedError

    def unlink(self):
        """
        Unlink/delete the artifact.

        Returns
        -------
        None
            None
        """
        path = Path(self.path)
        if not path.exists():
            return
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
