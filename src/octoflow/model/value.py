from __future__ import annotations

import enum
from datetime import datetime
from typing import Dict, List, Optional, Union

from sqlalchemy import (
    JSON,
    Boolean,
    Connection,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    case,
    event,
    select,
)
from sqlalchemy.orm import Mapped, mapped_column

from octoflow.model.base import Base

JSONType = Union[None, float, int, str, bool, List["JSONType"], Dict[str, "JSONType"]]


class ValueType(enum.Enum):
    unknown = 0
    parameter = 1
    metric = 2


class Value(Base):
    __tablename__ = "value"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("run.id"))
    key: Mapped[str] = mapped_column(String)
    value: Mapped[JSONType] = mapped_column(JSON, nullable=True)  # Assuming you want to store JSON data
    timestamp: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    step_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("value.id"), nullable=True)
    is_step: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    type: Mapped[ValueType] = mapped_column(Enum(ValueType), nullable=False, default=ValueType.unknown)

    __table_args__ = (
        Index(
            "ix_value_run_id_key_step_id_step",
            run_id,
            key,
            step_id,
            case(
                (is_step, value),
                else_=key,
            ),
            unique=True,
        ),
    )


@event.listens_for(Value, "before_insert")
def check_step_reference_before_insert(mapper, connection: Connection, target: Value):
    step_id = target.step_id
    if step_id is None:
        return
    is_step = connection.execute(select(Value.is_step).where(Value.id == step_id)).scalar()
    if is_step:
        return
    msg = "referenced row (parent row) must have is_step=True when step_id is specified."
    raise ValueError(msg)
