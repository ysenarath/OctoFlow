from __future__ import annotations

import abc
import json
import shutil
import weakref
from pathlib import Path
from typing import Any, Dict, Generator, List, Type, Union

from octoflow.utils.collections import MutableDict

_handler_types: Dict[str, Type[ArtifactHandler]] = {}


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
    msg = f"'{type(obj).__name__}' has no handler"
    raise ValueError(msg)


def list_handler_types() -> List[str]:
    return list(_handler_types.keys())


class ArtifactMetadata(MutableDict[str, Any]):
    def __init__(self, handler: ArtifactHandler) -> None:
        self.handler_ref = weakref.ref(handler)
        super().__init__(self._load_data())
        self.add_event_listener("change", self._on_change)

    @property
    def handler(self) -> ArtifactHandler:
        return self.handler_ref()

    def _on_change(self) -> Generator[Dict[str, Any], None, None]:
        path = self.handler.path / ".metadata.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self._data, f)

    def _load_data(self) -> Dict[str, Any]:
        path = self.handler.path / ".metadata.json"
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return json.load(f)


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
        self.path: Path = Path(path)
        if not self.path.exists():
            self.path.mkdir(parents=True)
        self.metadata = ArtifactMetadata(self)

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
