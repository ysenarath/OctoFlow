from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, List, Optional, Union

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Integer,
)
from sqlalchemy.orm import Mapped, mapped_column

from octoflow.model.base import Base

ValueType = Union[None, float, int, str, bool, List["ValueType"], Dict[str, "ValueType"]]


class Value(Base):
    __tablename__ = "value"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("run.id"))
    variable_id: Mapped[int] = mapped_column(Integer, ForeignKey("variable.id"))
    value: Mapped[ValueType] = mapped_column(JSON, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    step_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("value.id"), nullable=True)

    def __repr__(self) -> str:
        value = json.dumps(self.value)
        if self.step_id:
            return f"Value({value}, step_id={self.step_id}, run_id={self.run_id}, variable_id={self.variable_id})"
        return f"Value({value}, run_id={self.run_id}, variable_id={self.variable_id})"
