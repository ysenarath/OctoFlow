from __future__ import annotations

import enum
import re
from typing import Optional, Union

from sqlalchemy import Column, Enum, ForeignKey, Integer, Text, UniqueConstraint

from octoflow.model.base import Base
from octoflow.model.namespace import NamespaceType

ValueType = Union[str, int, float]

name_pattern = r"^[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)*$"

name_re = re.compile(name_pattern)


class VariableType(enum.Enum):
    unknown = 0
    parameter = 1
    metric = 2


class Variable(Base):
    __tablename__ = "variable"
    __table_args__ = (UniqueConstraint("experiment_id", "name", name="uc_experiment_name"),)

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    experiment_id: int = Column(Integer, ForeignKey("experiment.id"), nullable=False)
    name: str = Column(NamespaceType, nullable=False)
    type: VariableType = Column(Enum(VariableType), nullable=False, default=VariableType.unknown)
    description: Optional[str] = Column(Text, nullable=True)

    @classmethod
    def get(
        cls,
        experiment_id: int,
        name: str,
        type: Optional[VariableType] = None,
    ) -> Optional[Variable]:
        with cls.session() as session:
            q = session.query(cls).filter_by(
                experiment_id=experiment_id,
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
        type: Optional[VariableType] = None,
    ):
        kwargs = {
            "experiment_id": experiment_id,
            "name": name,
            "type": type,
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
