from __future__ import annotations

import enum
from typing import Optional

from sqlalchemy import (
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from octoflow.model.base import Base


class VariableType(enum.Enum):
    unknown = 0
    parameter = 1
    metric = 2


class Variable(Base):
    __tablename__ = "variable"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String, nullable=False)
    experiment_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment.id"), nullable=False)
    parent_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("variable.id"), nullable=True)
    type: Mapped[VariableType] = mapped_column(Enum(VariableType), nullable=False, default=VariableType.unknown)

    __table_args__ = (
        Index(
            "ix_unique_experiment_id_key",
            key,  # variables with same key should be attached to the same parent
            experiment_id,
            unique=True,
        ),
    )

    @classmethod
    def get(
        cls,
        key: str,
        experiment_id: int,
        parent_id: Optional[int] = None,
        type: VariableType = VariableType.unknown,
        create: bool = True,
    ) -> Variable:
        with cls.session() as session:
            var = (
                session.query(Variable)
                .filter(
                    Variable.key == key,
                    Variable.experiment_id == experiment_id,
                    (Variable.parent_id == parent_id) if parent_id is not None else Variable.parent_id.is_(None),
                )
                .first()
            )
            if var is None and create:
                var = Variable(
                    key=key,
                    experiment_id=experiment_id,
                    parent_id=parent_id,
                    type=type,
                )
        if var is None:
            return ValueError("variable not found")
        return var

    def __repr__(self) -> str:
        return f'Variable(key="{self.key}", experiment_id={self.experiment_id})'

    # Implementing __eq__ method for equality comparison
    def __eq__(self, other):
        if isinstance(other, Variable):
            return self.experiment_id == other.experiment_id and self.key == other.key
        if isinstance(other, tuple):
            return (self.key, self.experiment_id) == other
        return False

    # Implementing __hash__ method for hashability
    def __hash__(self):
        # Use a tuple of relevant attributes for hashing
        return hash((self.key, self.experiment_id))
