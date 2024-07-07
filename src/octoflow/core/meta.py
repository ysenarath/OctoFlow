from collections import UserDict
from typing import Any

__all__ = [
    "OctoFlowMeta",
]


class OctoFlowMeta(UserDict):
    def __getattr__(self, name: str) -> Any:
        if name in self:
            return self.data[name]
        return super().__getattr__(name)
