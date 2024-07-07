from collections import UserDict
from typing import Any

__all__ = [
    "OctoFlowMeta",
]


class OctoFlowMeta(UserDict):
    def __getattr__(self, name: str) -> Any:
        if name in self:
            return self.data[name]
        msg = f"'{self.__class__.__name__}' object has no attribute '{name}'"
        raise AttributeError(msg)
