from typing import Any, List, Union

import dill  # noqa: S403
import xxhash

__all__ = [
    "hash",
    "register",
]

dispatch = {}


def register(type_):
    def decorator(func):
        dispatch[type_] = func
        return func

    return decorator


class Hasher:
    """Hasher that accepts python objets as inputs."""

    def __init__(self):
        self.m = xxhash.xxh64()

    @classmethod
    def hash_bytes(cls, value: Union[bytes, List[bytes]]) -> str:
        value = [value] if isinstance(value, bytes) else value
        m = xxhash.xxh64()
        for x in value:
            m.update(x)
        return m.hexdigest()

    @classmethod
    def hash_default(cls, value: Any) -> str:
        return cls.hash_bytes(dill.dumps(value))

    @classmethod
    def hash(cls, value: Any) -> str:
        if type(value) in dispatch:
            return dispatch[type(value)](cls, value)
        else:
            return cls.hash_default(value)

    def update(self, value: Any) -> None:
        header_for_update = f"=={type(value)}=="
        value_for_update = self.hash(value)
        self.m.update(header_for_update.encode("utf-8"))
        self.m.update(value_for_update.encode("utf-8"))

    def hexdigest(self) -> str:
        return self.m.hexdigest()


def hash(obj: Any) -> str:
    h = Hasher()
    h.update(obj)
    return h.hexdigest()
