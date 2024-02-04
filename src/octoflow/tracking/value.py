from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from octoflow.tracking.base import Base

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
