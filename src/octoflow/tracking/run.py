from __future__ import annotations

import base64
from dataclasses import field
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, TypedDict, Union

from octoflow.tracking import utils
from octoflow.tracking.base import Base
from octoflow.tracking.value import Value, ValueType
from octoflow.typing import Property

if TYPE_CHECKING:
    from octoflow.tracking.experiment import Experiment
else:
    Experiment = "Experiment"

EMPTY_DICT = {}


class FilterByType(TypedDict):
    parameter: bool
    metric: bool


class FilterExpression(TypedDict):
    type: FilterByType


def get_run_id(expr: Union[Experiment, str]) -> str:
    if isinstance(expr, str):
        return base64.urlsafe_b64encode(expr.encode("utf-8")).decode("utf-8")
    if not hasattr(expr, "_id") or expr._id is None:
        expr._id = get_run_id(expr.name)
    return expr._id


class Run(Base):
    experiment: Experiment
    name: str
    description: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    id: ClassVar[Property[str]] = property(fget=get_run_id)

    def log_param(
        self,
        key: str,
        value: ValueType,
        *,
        step: Optional[Value] = None,
    ) -> Value:
        """Log a value."""
        value = Value(run=self, key=key, value=value, type="parameter", step=step)
        return self.store.log_value(value)

    def log_params(
        self,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        log_vals = []
        for key, value in utils.flatten(values).items():
            key = f"{prefix}.{key}" if prefix else key
            log_val = self.log_param(
                key=key,
                value=value,
                step=step,
            )
            log_vals.append(log_val)
        return log_vals

    def log_metric(
        self,
        key: str,
        value: ValueType,
        *,
        step: Optional[Value] = None,
    ) -> Value:
        """Log a value."""
        value = Value(run=self, key=key, value=value, type="metric", step=step)
        return self.store.log_value(value)

    def log_metrics(
        self,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        log_vals = []
        for key, value in utils.flatten(values).items():
            key = f"{prefix}.{key}" if prefix else key
            log_val = self.log_metric(
                key=key,
                value=value,
                step=step,
            )
            log_vals.append(log_val)
        return log_vals

    def get_logs(self, filters: FilterExpression = EMPTY_DICT) -> dict:
        return self.store.get_logs(run=self, filters=filters)
