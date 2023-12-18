from __future__ import annotations

import enum
import re
from typing import Optional, Union

from sqlalchemy import Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, validates

from octoflow.model.base import Base

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
