from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass, is_dataclass
from typing import (
    Any,
    MutableMapping,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)

from omegaconf import OmegaConf
from typing_extensions import Self

__all__ = [
    "Config",
]


T = TypeVar("T", bound=dataclass)


class ConfigWrapper:
    def __init__(self, wrapped, config: Config, name: Optional[str] = None):
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
            if k in args:
                # and args[k] is not __placeholder__
                # already set - do not override
                continue
            args[k] = config[k]
        return args

    def get_params(self, *args, **kwargs):
        args = self.signature.bind_partial(*args, **kwargs)
        args = args.arguments
        args = self._update_params_from_config(args)
        args = {
            k: v
            for k, v in args.items()
            if k in self.filter_keys and v is not inspect.Parameter.empty
        }
        args = self.signature.bind_partial(**args)
        args.apply_defaults()
        return args.arguments

    def __call__(self, *args, **kwargs):
        return self.wrapped(**self.get_params(*args, **kwargs))


def configmethod(func: T) -> T:
    @functools.wraps(func)
    def decoratior(cls, *args, **kwargs):
        return cls(func(cls, *args, **kwargs))

    return decoratior


class Config(MutableMapping):
    @overload
    def __new__(cls, config: Type[T]) -> T: ...

    @overload
    def __new__(cls, config: dict[str, Any]) -> Self: ...

    def __new__(cls, config: Union[Type[T], dict[str, Any]]) -> Union[T, Self]:
        return super().__new__(cls)

    def __init__(self, config: Any) -> None:
        self.omconf = (
            config
            if isinstance(config, OmegaConf)
            else OmegaConf.structured(config)
            if is_dataclass(config)
            else OmegaConf.create(config)
        )

    def __getitem__(self, name: str) -> Any:
        return self.omconf[name]

    def __setitem__(self, name: str, value: Any) -> None:
        self.omconf[name] = value

    def __delitem__(self, name: str) -> None:
        del self.omconf[name]

    def __iter__(self) -> Any:
        return iter(self.omconf)

    def __len__(self) -> int:
        return len(self.omconf)

    def __getattr__(self, name: str) -> Any:
        return self.omconf[name]

    @overload
    def wraps(
        self, wrapped: Union[type, callable], **kwargs
    ) -> ConfigWrapper: ...

    @overload
    def wraps(
        self, wrapped: Union[str, None], **kwargs
    ) -> functools.partial: ...

    def wraps(
        self, wrapped: Union[type, callable, str, None] = None, **kwargs: Any
    ) -> Union[ConfigWrapper, functools.partial]:
        if isinstance(wrapped, str):
            return functools.partial(self.wraps, name=wrapped)
        elif wrapped is None:
            # empty builder call
            return functools.partial(self.wraps, **kwargs)
        config_wrapper = ConfigWrapper(
            wrapped, self, name=kwargs.get("name", None)
        )
        if isinstance(config_wrapper, type):  # if class
            orig_init = config_wrapper.__init__

            @functools.wraps(config_wrapper.__init__)
            def __init__(self, *args, **kwargs):  # noqa: N807
                orig_init(self, **config_wrapper.get_params(*args, **kwargs))

            config_wrapper.__init__ = __init__
            return config_wrapper

        @functools.wraps(config_wrapper)
        def builder_wrapper(*args, **kwargs):
            return config_wrapper(*args, **kwargs)

        return builder_wrapper

    @classmethod
    @configmethod
    def load(cls, path: str) -> Self:
        return OmegaConf.load(path)

    @classmethod
    @configmethod
    def from_dotlist(cls, dotlist: str) -> Self:
        return OmegaConf.from_dotlist(dotlist)

    @classmethod
    @configmethod
    def from_cli(cls, args: list[str]) -> Self:
        return OmegaConf.from_cli(args)
