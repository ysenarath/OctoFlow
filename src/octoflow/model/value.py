from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Union

from sqlalchemy import JSON, DateTime, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column

from octoflow.model.base import Base

JSONType = Union[None, float, int, str, bool, List["JSONType"], Dict[str, "JSONType"]]


class Value(Base):
    __tablename__ = "value"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("run.id"))
    variable_id: Mapped[int] = mapped_column(Integer, ForeignKey("variable.id"))
    value: Mapped[JSONType] = mapped_column(JSON, nullable=True)  # Assuming you want to store JSON data
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    step_id: Mapped[int] = mapped_column(Integer, ForeignKey("value.id"), nullable=True)
