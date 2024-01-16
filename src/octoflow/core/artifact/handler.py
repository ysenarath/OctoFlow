from __future__ import annotations

import abc
import contextlib
import json
import os
import weakref
from pathlib import Path
from typing import Any, Dict, Generator, List, MutableMapping, Type, Union

from filelock import FileLock

_handler_types: Dict[str, Type[ArtifactHandler]] = {}


def get_handler_type(name: str) -> Type[ArtifactHandler]:
    try:
        return _handler_types[name]
    except KeyError:
        msg = f"handler '{name}' not found"
        raise ValueError(msg) from None


def get_handler_type_by_object(obj: Any) -> Type[ArtifactHandler]:
    for handler in _handler_types.values():
        if handler.can_handle(obj):
            return handler
    # if we get here, we didn't find an appropriate handler for the object
    msg = f"'{type(obj).__name__}' has no handler"
    raise ValueError(msg)


def list_handler_types() -> List[str]:
    return list(_handler_types.keys())


class ArtifactMetadata(MutableMapping[str, Any]):
    def __init__(self, handler: ArtifactHandler) -> None:
        super().__init__()
        self.handler_ref = weakref.ref(handler)

    @property
    def handler(self) -> ArtifactHandler:
        return self.handler_ref()

    def open(self) -> Generator[Dict[str, Any], None, None]:
        lock_path = self.handler.path / ".metadata.json.lock"
        with FileLock(lock_path):
            path = self.handler.path / ".metadata.json"
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                data = yield data
            else:
                data = yield {}
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f)

    def __getitem__(self, key: str) -> Any:
        c = self.open()
        data = c.send(None)
        value = data[key]
        return value

    def __setitem__(self, key: str, value: Any) -> None:
        c = self.open()
        data = c.send(None)
        data[key] = value
        with contextlib.suppress(StopIteration):
            c.send(data)

    def __delitem__(self, key: str) -> None:
        c = self.open()
        data = c.send(None)
        del data[key]
        with contextlib.suppress(StopIteration):
            c.send(data)

    def __iter__(self) -> Generator[str, None, None]:
        c = self.open()
        data = c.send(None)
        yield from data

    def __len__(self):
        c = self.open()
        data = c.send(None)
        return len(data)

    def __repr__(self) -> str:
        c = self.open()
        data = c.send(None)
        return repr(data)


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
        return Path(self.path).exists()

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
            os.rmdir(path)
        else:
            path.unlink()
