"""Run model.

This module contains the run model.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple

from octoflow.model import value_utils

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
from sqlalchemy.orm import Mapped, aliased, mapped_column

from octoflow.model.base import Base
from octoflow.model.value import Value, ValueType
from octoflow.model.variable import Variable, VariableType

__all__ = [
    "Run",
]


class Run(Base):
    """Run model."""

    __tablename__ = "run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment.id", ondelete="CASCADE"))
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
        """Log a value."""
        parent_id = None if step is None else step.variable_id
        step_id = None if step is None else step.id
        with self.session():
            # get or create variable
            variable = Variable.get(
                key=key,
                experiment_id=self.experiment_id,
                parent_id=parent_id,
                type=type,
                create=True,  # create variable if it does not exist
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
        prefix: Optional[str] = None,
    ) -> List[Value]:
        """Log multiple values."""
        result = []
        prefix = "" if prefix is None else str(prefix).strip()
        for key, value in value_utils.flatten(values).items():
            if len(prefix) > 0:
                d = "." if prefix[-1] != "." else ""
                key = f"{prefix}{d}{key}"
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
        """Log a parameter.

        Parameters
        ----------
        key : str
            Parameter key.
        value : ValueType
            Parameter value.
        step : Optional[Value], optional
            Step value, by default None.

        Returns
        -------
        Value
            Value instance.
        """
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
        prefix: Optional[str] = None,
    ) -> List[Value]:
        """Log multiple parameters.

        Parameters
        ----------
        values : Dict[str, ValueType]
            Dictionary of values.
        step : Optional[Value], optional
            Step value, by default None.

        Returns
        -------
        List[Value]
            List of values.
        """
        return self._log_values(
            values=values,
            step=step,
            type=VariableType.parameter,
            prefix=prefix,
        )

    def log_metric(
        self,
        key: str,
        value: ValueType,
        step: Optional[Value] = None,
    ) -> Value:
        """Log a metric.

        Parameters
        ----------
        key : str
            Metric key.
        value : ValueType
            Metric value.
        step : Optional[Value], optional
            Step value, by default None.

        Returns
        -------
        Value
            Value instance.
        """
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
        prefix: Optional[str] = None,
    ) -> List[Value]:
        """Log multiple metrics.

        Parameters
        ----------
        values : Dict[str, ValueType]
            Dictionary of values.
        """
        return self._log_values(
            values=values,
            step=step,
            type=VariableType.metric,
            prefix=prefix,
        )

    def get_raw_logs(self) -> List[Tuple[int, int, int, str, Any]]:
        """Get all logs of the run.

        Returns
        -------
        List[Tuple[int, int, int, bool, str, Any]]
            List of logs.
        """
        with self.session() as session:
            child_value = aliased(Value)
            # get all values of this run and their corresponding variable keys
            values = (
                session.query(
                    Value.run_id,
                    Value.id,
                    Value.step_id,
                    child_value.id.is_not(None).label("is_step"),
                    Variable.key,
                    Value.value,
                )
                .select_from(Value)
                .join(Variable, Variable.id == Value.variable_id)
                .outerjoin(child_value, child_value.step_id == Value.id)
                .filter(Value.run_id == self.id)
                .distinct(Value.id)
                .all()
            )
        return values

    def get_logs(self) -> value_utils.ValueTree:
        """Get all logs of the run.

        Returns
        -------
        ValueTree
            List of logs.
        """
        values = self.get_raw_logs()
        trees = value_utils.build_trees(values)
        return trees[self.id]

    def match(self, partial: bool = True) -> Generator[int, None, None]:
        """Checks whether the run with same values for parameter typed variables exists in the database.

        Parameters
        ----------
        partial : bool, optional
            Whether to check for partial match, by default True.

        Yields
        ------
        Generator[int, None, None]
            Generator of run IDs.
        """
        with self.session() as session:
            # get all values of this run and their corresponding variable keys
            child_value = aliased(Value)
            values = (
                session.query(
                    Value.run_id,
                    Value.id,
                    Value.step_id,
                    child_value.id.is_not(None).label("is_step"),
                    Variable.key,
                    Value.value,
                )
                .select_from(Value)
                .join(Variable, Variable.id == Value.variable_id)
                .outerjoin(child_value, child_value.step_id == Value.id)
                .filter(
                    Variable.experiment_id == self.experiment_id,
                    Variable.type == VariableType.parameter,
                )
                .distinct(Value.id)
                .all()
            )
            # build up a tree of values
            trees = value_utils.build_trees(values)
        # check whether there is a run with same values
        if self.id not in trees:
            msg = f"run with id '{self.id}' does not exist"
            raise ValueError(msg)
        this = trees.pop(self.id).normalize()
        for run_id, other in trees.items():
            other = other.normalize()
            if value_utils.equals(this, other, partial=partial):
                yield run_id
