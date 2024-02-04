"""Variable model.

This module contains the Variable model.
"""

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

from octoflow.tracking_db.base import Base

__all__ = [
    "Variable",
    "VariableType",
]


class VariableType(enum.Enum):
    """Variable type."""

    unknown = 0
    parameter = 1
    metric = 2


class Variable(Base):
    __tablename__ = "variable"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String, nullable=False)
    experiment_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False)
    parent_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("variable.id", ondelete="CASCADE"), nullable=True
    )
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
        """Get a variable by key and experiment id.

        Parameters
        ----------
        key : str
            Variable key.
        experiment_id : int
            Experiment id.
        parent_id : Optional[int], optional
            Parent variable id, by default None.
        type : VariableType, optional
            Variable type, by default VariableType.unknown.
        create : bool, optional
            Create a new variable if not found, by default True.

        Returns
        -------
        Variable
            Variable instance.
        """
        with cls.session() as session:
            q = session.query(Variable).filter(
                Variable.key == key,
                Variable.experiment_id == experiment_id,
            )
            if parent_id is None:
                q = q.filter(Variable.parent_id.is_(None))
            else:
                q = q.filter(Variable.parent_id == parent_id)
            var = q.first()
            if var is None and create:
                # otherwise create a new variable
                var = Variable(
                    key=key,
                    experiment_id=experiment_id,
                    parent_id=parent_id,
                    type=type,
                )
        if var is None:
            return ValueError(f"unable to find variable with key '{key}'")
        return var

    def __repr__(self) -> str:
        """Override the default representation."""
        return f'Variable(experiment_id={self.experiment_id}, key="{self.key}")'

    def __eq__(self, other) -> bool:
        """Override the default Equals behavior.

        Parameters
        ----------
        other : Variable
            The Variable to compare to.

        Returns
        -------
        bool
            True if equal, False otherwise.
        """
        if isinstance(other, Variable):
            return self.experiment_id == other.experiment_id and self.key == other.key
        if isinstance(other, tuple):
            return (self.key, self.experiment_id) == other
        return False

    def __hash__(self) -> int:
        """Override the default hash behavior (that returns the id or the object).

        Returns
        -------
        int
            hash of the tuple (key, experiment_id)
        """
        return hash((self.key, self.experiment_id))
