from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from octoflow.tracking.base import Base
from octoflow.tracking.utils import validate_slug

if TYPE_CHECKING:
    from octoflow.tracking.run import Run

ValueType = Union[None, float, int, str, bool, List["ValueType"], Dict[str, "ValueType"]]
VariableType = Literal["unknown", "parameter", "metric"]


class Value(Base):
    run: Run
    key: str
    value: ValueType
    step: Optional[Value] = None
    type: VariableType = "unknown"
    id: Optional[int] = None

    def __post_init__(self) -> None:
        # check if value type is valid
        if self.type not in {"unknown", "parameter", "metric"}:
            msg = "invalid value type"
            raise ValueError(msg)
        # check name is valid
        if not validate_slug(self.key):
            msg = f"invalid key: '{self.key}'"
            raise ValueError(msg)
        super().__post_init__()
