from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from octoflow.model.base import Base
from octoflow.model.value import JSONType, Value, ValueType


class Run(Base):
    __tablename__ = "run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment.id"))
    name: Mapped[Optional[str]] = mapped_column(String, nullable=False)  # there can be multiple runs of same name
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    def _log_value(
        self,
        key: str,
        value: JSONType,
        *,
        step: Union[Value, int, None] = None,
        is_step: bool = False,
        type: Optional[ValueType] = None,
    ) -> Value:
        step_id = step.id if isinstance(step, Value) else step
        with self.session():
            result = Value(
                run_id=self.id,
                key=key,
                value=value,
                step_id=step_id,
                is_step=is_step,
                type=type,
            )
        return result

    def _log_values(
        self,
        *,
        values: Dict[str, JSONType],
        step: Union[Value, int, None] = None,
        type: Optional[ValueType] = None,
    ) -> List[Value]:
        step_id = step.id if isinstance(step, Value) else step
        result = []
        for key, value in flatten_values(values).items():
            value = self._log_value(
                key=key,
                value=value,
                step=step_id,
                type=type,
            )
            result.append(value)
        return result

    def log_param(
        self,
        key: str,
        value: JSONType,
        step: Union[Value, int, None] = None,
        is_step: bool = False,
    ) -> Value:
        return self._log_value(
            key=key,
            value=value,
            step=step,
            is_step=is_step,
            type=ValueType.parameter,
        )

    def log_params(
        self,
        values: Dict[str, JSONType],
        step: Union[Value, int, None] = None,
    ) -> List[Value]:
        return self._log_values(
            values=values,
            step=step,
            type=ValueType.parameter,
        )

    def log_metric(
        self,
        key: str,
        value: JSONType,
        step: Union[Value, int, None] = None,
    ) -> Value:
        return self._log_value(
            key=key,
            value=value,
            step=step,
            type=ValueType.metric,
        )

    def log_metrics(
        self,
        values: Dict[str, JSONType],
        step: Union[Value, int, None] = None,
    ) -> List[Value]:
        return self._log_values(
            values=values,
            step=step,
            type=ValueType.metric,
        )

    def get_logs(self) -> List[Value]:
        with self.session() as session:
            logs = session.query(Value).filter(Value.run_id == self.id).all()
        return logs

    def exists(self) -> List[int]: ...


def flatten_values(
    data: Dict[str, Any],
    parent_key: str = "",
    separator: str = ".",
) -> dict[str, Any]:
    items = []
    for key, value in data.items():
        # escape dots
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, Mapping):
            items.extend(
                flatten_values(
                    value,
                    parent_key=new_key,
                    separator=separator,
                ).items()
            )
        else:
            items.append((new_key, value))
    return dict(items)
