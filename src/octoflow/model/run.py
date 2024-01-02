from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

try:
    import pandas as pd
except ImportError:
    pd = None
from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column

from octoflow.model.base import Base
from octoflow.model.value import Value, ValueType
from octoflow.model.variable import Variable, VariableType


class Run(Base):
    __tablename__ = "run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment.id"))
    name: Mapped[str] = mapped_column(String, nullable=False)  # there can be multiple runs of same name
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    def _log_value(
        self,
        key: str,
        value: ValueType,
        *,
        step: Optional[Value] = None,
        type: Optional[VariableType] = None,
    ) -> Value:
        parent_id = None if step is None else step.variable_id
        step_id = None if step is None else step.id
        with self.session():
            variable = Variable.get(
                key=key,
                experiment_id=self.experiment_id,
                parent_id=parent_id,
                type=type,
            )
            value = Value(
                run_id=self.id,
                variable_id=variable.id,
                value=value,
                step_id=step_id,
            )
        return value

    def _log_values(
        self,
        *,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
        type: Optional[VariableType] = None,
    ) -> List[Value]:
        result = []
        for key, value in flatten_values(values).items():
            value = self._log_value(
                key=key,
                value=value,
                step=step,
                type=type,
            )
            result.append(value)
        return result

    def log_param(
        self,
        key: str,
        value: ValueType,
        step: Optional[Value] = None,
    ) -> Value:
        return self._log_value(
            key=key,
            value=value,
            step=step,
            type=VariableType.parameter,
        )

    def log_params(
        self,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
    ) -> List[Value]:
        return self._log_values(
            values=values,
            step=step,
            type=VariableType.parameter,
        )

    def log_metric(
        self,
        key: str,
        value: ValueType,
        step: Optional[Value] = None,
    ) -> Value:
        return self._log_value(
            key=key,
            value=value,
            step=step,
            type=VariableType.metric,
        )

    def log_metrics(
        self,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
    ) -> List[Value]:
        return self._log_values(
            values=values,
            step=step,
            type=VariableType.metric,
        )

    def get_logs(self) -> List[Dict[str, Any]]: ...

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
