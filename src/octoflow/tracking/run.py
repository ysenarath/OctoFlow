from __future__ import annotations

import hashlib
from dataclasses import field
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Union

import octoflow as of
from octoflow.tracking import value_utils
from octoflow.tracking.base import Base
from octoflow.tracking.value import Value, ValueType
from octoflow.typing import Property

if TYPE_CHECKING:
    from octoflow.tracking.experiment import Experiment


def get_run_id(expr: Union[Experiment, str]) -> str:
    if isinstance(expr, str):
        return hashlib.sha256(expr.encode()).hexdigest()
    if not hasattr(expr, "_id") or expr._id is None:
        expr._id = get_run_id(expr.name)
    return expr._id


class Run(Base):
    experiment: of.tracking_fs.experiment.Experiment
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
        self.store.log_value(value)
        return value

    def log_params(
        self,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        for key, value in value_utils.flatten(values).items():
            key = f"{prefix}.{key}" if prefix else key
            self.log_param(
                key=key,
                value=value,
                step=step,
            )

    def log_metric(
        self,
        key: str,
        value: ValueType,
        *,
        step: Optional[Value] = None,
    ) -> Value:
        """Log a value."""
        value = Value(run=self, key=key, value=value, type="metric", step=step)
        self.store.log_value(value)
        return value

    def log_metrics(
        self,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        for key, value in value_utils.flatten(values).items():
            key = f"{prefix}.{key}" if prefix else key
            self.log_metric(
                key=key,
                value=value,
                step=step,
            )
