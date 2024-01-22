from __future__ import annotations

import functools
import inspect
from collections.abc import Mapping, MutableMapping
from typing import Any, Iterator, Optional, Union

from octoflow.utils.objects import create_object

__all__ = [
    "Config",
]


class ConfigWrapper:
    def __init__(
        self,
        wrapped,
        config: Config,
        name: Optional[str] = None,
    ):
        if name is None:
            name = getattr(wrapped, "__name__", None)
        if name is None:
            name = getattr(wrapped, "__qualname__", None)
        if name is None:
            msg = "builder must have a name"
            raise ValueError(msg)
        self.wrapped = wrapped
        self.signature = inspect.signature(wrapped)
        filter_keys = set()
        for param in self.signature.parameters.values():
            if param.kind == param.POSITIONAL_OR_KEYWORD:
                filter_keys.add(param.name)
        self.filter_keys = filter_keys
        self.name = name
        self.config = config

    def _update_params_from_config(self, args: dict) -> dict:
        config = self.config
        for part in self.name.split("."):
            if part not in config:
                # function or class name or alias is not in config
                return args
            config = config[part]
        # update args
        for k in config:
            if k in args:  # and args[k] is not __placeholder__
                # already set - do not override
                continue
            args[k] = config[k]
        return args

    def get_params(self, *args, **kwargs):
        args = self.signature.bind_partial(*args, **kwargs)
        args = args.arguments
        args = self._update_params_from_config(args)
        args = {k: v for k, v in args.items() if k in self.filter_keys and v is not inspect.Parameter.empty}
        args = self.signature.bind_partial(**args)
        args.apply_defaults()
        args = args.arguments
        return args

    def __call__(self, *args, **kwargs):
        return self.wrapped(**self.get_params(*args, **kwargs))


class FrozenConfig(Mapping):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self._data = dict(*args, **kwargs)
        self._parent = None

    def get_root(self) -> FrozenConfig:
        """
        Get the root config object.

        Returns
        -------
        FrozenConfig
            The root config object.
        """
        if self._parent is None:
            # this is the root
            return self
        # recursively get root
        return self._parent.get_root()

    def set_parent(self, parent: FrozenConfig) -> FrozenConfig:
        """
        Set the parent config object.

        Parameters
        ----------
        parent : FrozenConfig
            The parent config object.

        Returns
        -------
        FrozenConfig
            The current config object.
        """
        self._parent = parent
        return self

    def __getitem__(self, key: str) -> Any:
        value = self._data[key]
        if isinstance(value, dict):
            if "$type" in value:
                # value is an object
                params = dict(value.items())
                cls = params.pop("$type")
                partial = params.pop("$partial", False)
                kwargs = FrozenConfig(params).to_dict()
                return create_object(
                    cls,
                    partial,
                    **kwargs,
                )
            else:
                return FrozenConfig(value).set_parent(self)
        elif isinstance(value, str):
            return value.format(root=self.get_root(), self=self)
        return value

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator:
        yield from self._data

    def to_dict(self) -> dict:
        out = {}
        for k, v in self.items():
            out[k] = v.to_dict() if isinstance(v, FrozenConfig) else v
        return out

    def __reduce__(self) -> tuple:
        return (self.__class__, (self._data,))


class Config(FrozenConfig, MutableMapping):
    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]

    def wraps(
        self,
        wrapped: Union[type, callable, None] = None,
        **kwargs,
    ) -> Union[ConfigWrapper, functools.partial]:
        if isinstance(wrapped, str):
            return functools.partial(
                self.wraps,
                name=wrapped,
            )
        elif wrapped is None:
            # empty builder call
            return functools.partial(
                self.wraps,
                **kwargs,
            )
        wrapped: ConfigWrapper = ConfigWrapper(
            wrapped,
            self,
            name=kwargs.get("name", None),
        )
        if isinstance(wrapped, type):  # if class
            orig_init = wrapped.__init__

            @functools.wraps(wrapped.__init__)
            def __init__(self, *args, **kwargs):  # noqa: N807
                orig_init(self, **wrapped.get_params(*args, **kwargs))

            wrapped.__init__ = __init__
            return wrapped

        @functools.wraps(wrapped)
        def builder_wrapper(*args, **kwargs):
            return wrapped(*args, **kwargs)

        return builder_wrapper


config = Config({
    "resources": {
        "path": "~/.octoflow",
        "cache": {
            "path": "{root.resources.path}/cache",
        },
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
    },
})
