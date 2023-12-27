from __future__ import annotations

import enum
import re
from typing import Optional, Tuple, Union

from sqlalchemy import Enum, ForeignKey, Integer, Select, String, UniqueConstraint, select
from sqlalchemy.orm import Mapped, aliased, mapped_column, validates
from sqlalchemy.sql import operators

from octoflow.model.base import Base
from octoflow.model.value import Value

ValueType = Union[str, int, float]

name_pattern = r"^[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)*$"

name_re = re.compile(name_pattern)


class VariableType(enum.Enum):
    unknown = 0
    parameter = 1
    metric = 2


class Variable(Base):
    __tablename__ = "variable"
    __table_args__ = (UniqueConstraint("experiment_id", "name", "namespace", name="uc_experiment_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    type: Mapped[VariableType] = mapped_column(Enum(VariableType), nullable=False, default=VariableType.unknown)
    namespace: Mapped[str] = mapped_column(String, nullable=False, default="")
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    @classmethod
    def get(
        cls,
        experiment_id: int,
        name: str,
        namespace: Optional[str] = None,
        type: Optional[VariableType] = None,
    ) -> Optional[Variable]:
        if namespace is None:
            namespace = ""
        with cls.session() as session:
            q = session.query(cls).filter_by(
                experiment_id=experiment_id,
                namespace=namespace,
                name=name,
            )
            if type is not None:
                q = q.filter(cls.type == type)
            obj = q.first()
        return obj

    @classmethod
    def get_or_create(
        cls,
        experiment_id: int,
        name: str,
        namespace: Optional[str] = None,
        type: Optional[VariableType] = None,
    ):
        kwargs = {
            "experiment_id": experiment_id,
            "name": name,
            "type": type,
            "namespace": namespace,
        }
        original_error = None
        try:
            # try creating the object in the db
            var = cls(**kwargs)
        except ValueError as err:
            # unable to persist; get the existing object
            var = cls.get(**kwargs)
            original_error = err
        if var is None:
            msg = f"unable to get or create variable named '{name}'"
            raise ValueError(msg) from original_error
        return var

    @validates("namespace")
    def validate_namespace(self, key, namespace):  # noqa: PLR6301
        if namespace is None:
            namespace = ""
        if len(namespace) == 0:
            return namespace
        if name_re.match(namespace) is None:
            msg = "failed namespace validation"
            raise ValueError(msg)
        return namespace

    @validates("name")
    def validate_name(self, key, name):  # noqa: PLR6301
        if name is None or "." in name or name_re.match(name) is None:
            msg = "failed name validation"
            raise ValueError(msg)
        return name

    def _apply_operator(self, operator, value) -> Expression:
        var = aliased(Variable)
        val = aliased(Value)
        stmt = (
            select(val.run_id)
            .join(var, var.id == val.variable_id)
            .where(var.id == self.id)
            .where(operator(val.value, value))
        )
        return Expression(stmt, self.experiment_id)

    def __eq__(self, value: object) -> Expression:
        return self._apply_operator(operators.eq, value)

    def __ne__(self, value: object) -> Expression:
        return self._apply_operator(operators.ne, value)

    def __lt__(self, value: object) -> Expression:
        return self._apply_operator(operators.lt, value)

    def __gt__(self, value: object) -> Expression:
        return self._apply_operator(operators.gt, value)

    def __hash__(self) -> int:
        return hash(self.as_tuple())

    def as_tuple(self) -> Tuple[int, str, str]:
        return tuple(
            self.experiment_id,
            self.namespace,
            self.name,
        )


class Expression:
    def __init__(
        self,
        stmt: Select,
        experiment_id: int,
    ) -> None:
        self.stmt = stmt
        self.experiment_id = experiment_id

    @staticmethod
    def validate(other: Expression, op_name: str):
        if not isinstance(other, Expression):
            msg = f"only expressions can be used with operator '{op_name}'"
            raise ValueError(msg)
        return True

    def __and__(self, value: Expression):
        self.validate(value, "and")
        stmt = select("*").select_from(self.stmt).intersect(select("*").select_from(value.stmt))
        return Expression(stmt, self.experiment_id)

    def __or__(self, value: Expression):
        self.validate(value, "or")
        # stmt = self.stmt.union(value.stmt)
        stmt = select("*").select_from(self.stmt).union(select("*").select_from(value.stmt))
        return Expression(stmt, self.experiment_id)

    def __invert__(self) -> Expression:
        var = aliased(Variable)
        val = aliased(Value)
        stmt = (
            select(val.run_id)
            .join(var, var.id == val.variable_id)
            .where(var.experiment_id == self.experiment_id)
            .distinct()
        )
        all_stmt = select("*").select_from(stmt)
        this_stmt = select("*").select_from(self.stmt)
        stmt = all_stmt.except_(this_stmt)
        return Expression(stmt, self.experiment_id)
