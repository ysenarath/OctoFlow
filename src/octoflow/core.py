from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from octoflow.data.dataclass import BaseModel
from octoflow.utils import objects as obj

if TYPE_CHECKING:
    from octoflow.tracking.models import Run


class Module(BaseModel):
    def get_params(self, deep: bool = True) -> dict:
        if not deep:
            raise NotImplementedError
        return obj.dump(self)

    def set_params(self, **params: Any) -> Self:
        for key, value in params.items():
            if key not in self.__annotations__:
                msg = f"invalid parameter: {key}"
                raise ValueError(msg)
            value = obj.load(value)
            setattr(self, key, value)


class Task(Module):
    def run(self, run: Run) -> None:
        raise NotImplementedError
