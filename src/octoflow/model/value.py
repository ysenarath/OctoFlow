from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Union

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Integer

from octoflow.model.base import Base

JSONType = Union[None, int, str, bool, List["JSONType"], Dict[str, "JSONType"]]


class Value(Base):
    __tablename__ = "value"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    run_id: int = Column(Integer, ForeignKey("run.id"))
    variable_id: int = Column(Integer, ForeignKey("variable.id"))
    value: JSONType = Column(JSON, nullable=True)  # Assuming you want to store JSON data
    timestamp: datetime = Column(DateTime, default=datetime.utcnow)
    step_id: int = Column(Integer, ForeignKey("value.id"), nullable=True)
