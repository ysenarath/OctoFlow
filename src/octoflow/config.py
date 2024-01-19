from __future__ import annotations

import functools
import importlib
import inspect
from collections.abc import Mapping, MutableMapping
from typing import Any, Dict, Iterator, Optional, Union

__all__ = [
    "Config",
]


def construct(
    cls,
    params: Optional[Dict[str, Any]] = None,
    partial: bool = False,
):
    if isinstance(cls, str):
        module, name = cls.rsplit(".", 1)
        module = importlib.import_module(name=module)
        cls = getattr(module, name)
    try:
        if params is None:
            params = {}
        if not isinstance(params, dict):
            msg = f"params must be a dict, found {type(params).__name__}"
            raise TypeError(msg)
        if partial:
            return functools.partial(cls, **params)
        return cls(**params)
    except Exception as ex:
        raise ex


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
        self.name = name
        self.config = config

    def _update_params_from_config(self, args):
        if self.name not in self.config:
            # function or class name or alias is not in config
            return args
        config = self.config[self.name]
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

    def __getitem__(self, key: str) -> Any:
        value = self._data[key]
        if isinstance(value, dict):
            if "$type" in value:
                params = dict(value.items())
                cls = params.pop("$type")
                partial = params.pop("$partial", False)
                return construct(
                    cls,
                    params=FrozenConfig(params).to_dict(),
                    partial=partial,
                )
            else:
                return FrozenConfig(value)
        return value

    def __getattr__(self, name: str) -> Any:
        if name in self:
            return self[name]
        return None

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
