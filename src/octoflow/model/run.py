from __future__ import annotations

import itertools
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    and_,
    or_,
)
from sqlalchemy.orm import aliased

from octoflow.model.base import Base
from octoflow.model.namespace import Namespace
from octoflow.model.value import JSONType, Value
from octoflow.model.variable import Variable, VariableType

try:
    import pandas as pd
except ImportError:
    pd = None


class Run(Base):
    __tablename__ = "run"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id: int = Column(Integer, ForeignKey("experiment.id"))
    name: Optional[str] = Column(String, nullable=True)  # there can be multiple runs of same name
    description: Optional[str] = Column(Text, nullable=True)
    created_at: datetime = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: datetime = Column(DateTime, nullable=False, default=datetime.utcnow)

    def _log_value(
        self,
        variable: Union[Variable, Namespace, str],
        value: JSONType,
        step: Union[Value, int, None] = None,
        type: Optional[VariableType] = None,
    ) -> Value:
        step_id = step.id if isinstance(step, Value) else step
        if isinstance(variable, str) or variable is None:
            variable = Namespace(variable)
        with self.session():
            if isinstance(variable, Namespace):
                # convert namespace to variable in this experiment
                variable = Variable.get_or_create(
                    experiment_id=self.experiment_id,
                    name=variable.name,
                    type=type,
                )
            result = Value(
                run_id=self.id,
                variable_id=variable.id,
                value=value,
                step_id=step_id,
            )
        return result

    @classmethod
    def _flatten_values(
        cls,
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
                    cls._flatten_values(
                        value,
                        parent_key=new_key,
                        separator=separator,
                    ).items()
                )
            else:
                items.append((new_key, value))
        return dict(items)

    def _log_values(
        self,
        value: JSONType,
        variable: Union[Variable, Namespace, str] = None,
        step: Union[Value, int, None] = None,
        type: Optional[VariableType] = None,
    ) -> Dict[str, Value]:
        step_id = step.id if isinstance(step, Value) else step
        if isinstance(variable, str) or variable is None:
            variable = Namespace(variable)
        with self.session():
            namespace = variable if isinstance(variable, Namespace) else Namespace(variable.name)
            result = {}
            for k, v in self._flatten_values(value).items():
                sub_var = Variable.get_or_create(
                    experiment_id=self.experiment_id,
                    name=namespace.join(k).name,
                    type=type,
                )
                result[k] = Value(
                    run_id=self.id,
                    variable_id=sub_var.id,
                    value=v,
                    step_id=step_id,
                )
        return result

    def log_param(
        self,
        variable: Union[Variable, Namespace, str],
        value: JSONType,
        step: Union[Value, int, None] = None,
    ) -> Value:
        return self._log_value(
            variable=variable,
            value=value,
            step=step,
            type=VariableType.parameter,
        )

    def log_params(
        self,
        value: JSONType,
        variable: Union[Variable, Namespace, str] = None,
        step: Union[Value, int, None] = None,
    ) -> Value:
        return self._log_values(
            variable=variable,
            value=value,
            step=step,
            type=VariableType.parameter,
        )

    def log_metric(
        self,
        variable: Union[Variable, Namespace, str],
        value: JSONType,
        step: Union[Value, int, None] = None,
    ) -> Value:
        return self._log_value(
            variable=variable,
            value=value,
            step=step,
            type=VariableType.metric,
        )

    def log_metrics(
        self,
        value: JSONType,
        variable: Union[Variable, Namespace, str] = None,
        step: Union[Value, int, None] = None,
    ) -> Value:
        return self._log_values(
            variable=variable,
            value=value,
            step=step,
            type=VariableType.metric,
        )

    def get_logs(self) -> List[Dict]:
        with self.session() as session:
            val_0 = aliased(Value)
            steps_tree = (
                session.query(
                    Value.step_id.label("leaf_step_id"),
                    Value.step_id,
                )
                .filter(Value.run_id == self.id)
                .outerjoin(val_0, val_0.step_id == Value.id)
                .filter(val_0.id.is_(None))
                .distinct(Value.step_id)
            )
            steps_tree = steps_tree.cte(recursive=True)
            st_1 = aliased(steps_tree)
            val_1 = aliased(Value)
            steps_tree = steps_tree.union_all(
                session.query(st_1.c.leaf_step_id, val_1.step_id).join(val_1, st_1.c.step_id == val_1.id)
            )
            st_2 = aliased(steps_tree)
            val_2 = aliased(Value)
            val_2_leaf = aliased(Value)
            var_2 = aliased(Variable)
            sq = (
                session.query(st_2.c.leaf_step_id, val_2.id.label("step_id"), var_2.name, val_2.value)
                # include variables that are connected to the path to the leaf_step_id
                .join(
                    # this step may include step variables that are connected to the path to leaf_step_id
                    val_2,
                    or_(
                        and_(
                            val_2.run_id == self.id,
                            val_2.step_id.is_(None),
                            st_2.c.step_id.is_(None),
                        ),
                        val_2.step_id == st_2.c.step_id,
                    ),
                )
                # exclude all step variables (steps in the path to leaf_step_id will be added later)
                .outerjoin(val_2_leaf, val_2_leaf.step_id == val_2.id)
                .filter(val_2_leaf.id.is_(None))
                # for var name
                .join(var_2, var_2.id == val_2.variable_id)
            )
            # include steps in the path to leaf_step_id
            st_3 = aliased(steps_tree)
            val_3 = aliased(Value)
            var_3 = aliased(Variable)
            st = (
                session.query(st_3.c.leaf_step_id, st_3.c.step_id, var_3.name, val_3.value)
                .join(val_3, val_3.id == st_3.c.step_id)
                .join(var_3, var_3.id == val_3.variable_id)
            )
            data = sq.union(st).all()
        result = []
        for _, g in itertools.groupby(sorted(data, key=GroupKey), GroupKey):
            result.append({v.name: v.value for v in g})
        return result


class GroupKey:
    def __init__(self, value):
        self.value = value.leaf_step_id

    def __lt__(self, other):
        if self.value is None:
            return other is None
        elif other.value is None:
            return self.value is None
        return self.value < other.value

    def __eq__(self, other):
        if other is None:
            return self.value is None
        return self.value == other.value

    def __hash__(self):
        return hash(self.value)

    def __repr__(self):
        return f"GroupKey({self.value})"
