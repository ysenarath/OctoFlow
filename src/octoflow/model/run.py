from __future__ import annotations

import itertools
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    and_,
    or_,
)
from sqlalchemy.orm import Mapped, aliased, mapped_column

from octoflow.model.base import Base
from octoflow.model.value import JSONType, Value
from octoflow.model.variable import Variable, VariableType


class Run(Base):
    __tablename__ = "run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment.id"))
    name: Mapped[Optional[str]] = mapped_column(String, nullable=True)  # there can be multiple runs of same name
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    def __post_init__(self):
        self._namespace = None

    def _log_value(
        self,
        name: str,
        value: JSONType,
        *,
        step: Union[Value, int, None] = None,
        namespace: Optional[str] = None,
        type: Optional[VariableType] = None,
    ) -> Value:
        step_id = step.id if isinstance(step, Value) else step
        with self.session():
            variable = Variable.get_or_create(
                experiment_id=self.experiment_id,
                name=name,
                type=type,
                namespace=namespace,
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
        values: Dict[str, JSONType],
        step: Union[Value, int, None] = None,
        type: Optional[VariableType] = None,
        namespace: Optional[str] = None,
    ) -> Dict[str, Value]:
        step_id = step.id if isinstance(step, Value) else step
        with self.session():
            result = {}
            for name, named_value in self._flatten_values(values).items():
                sub_var = Variable.get_or_create(
                    experiment_id=self.experiment_id,
                    name=name,
                    type=type,
                    namespace=namespace,
                )
                result[name] = Value(
                    run_id=self.id,
                    variable_id=sub_var.id,
                    value=named_value,
                    step_id=step_id,
                )
        return result

    def log_param(
        self,
        name: str,
        value: JSONType,
        step: Union[Value, int, None] = None,
        namespace: Optional[str] = None,
    ) -> Value:
        return self._log_value(
            name=name,
            value=value,
            step=step,
            type=VariableType.parameter,
            namespace=namespace,
        )

    def log_params(
        self,
        values: Dict[str, JSONType],
        step: Union[Value, int, None] = None,
        namespace: Optional[str] = None,
    ) -> Value:
        return self._log_values(
            values=values,
            step=step,
            namespace=namespace,
            type=VariableType.parameter,
        )

    def log_metric(
        self,
        name: str,
        value: JSONType,
        step: Union[Value, int, None] = None,
        namespace: Optional[str] = None,
    ) -> Value:
        return self._log_value(
            name=name,
            value=value,
            step=step,
            namespace=namespace,
            type=VariableType.metric,
        )

    def log_metrics(
        self,
        values: Dict[str, JSONType],
        step: Union[Value, int, None] = None,
        namespace: Optional[str] = None,
    ) -> Value:
        return self._log_values(
            values=values,
            step=step,
            namespace=namespace,
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
                session.query(st_2.c.leaf_step_id, val_2.id.label("step_id"), var_2.namespace, var_2.name, val_2.value)
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
                session.query(st_3.c.leaf_step_id, st_3.c.step_id, var_3.namespace, var_3.name, val_3.value)
                .join(val_3, val_3.id == st_3.c.step_id)
                .join(var_3, var_3.id == val_3.variable_id)
            )
            data = sq.union(st).all()
        result = []
        for _, g in itertools.groupby(sorted(data, key=GroupKey), GroupKey):
            result.append({f"{v.namespace}.{v.name}": v.value for v in g})
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
