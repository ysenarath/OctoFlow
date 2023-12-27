from __future__ import annotations

import weakref
from typing import List, Optional, Union

import randomname
from sqlalchemy import Integer, String, Text, desc
from sqlalchemy.orm import Mapped, mapped_column

from octoflow import logging
from octoflow.model import namespace as ns
from octoflow.model.base import Base
from octoflow.model.run import Run
from octoflow.model.variable import Expression, Variable

logger = logging.get_logger(__name__)


class Experiment(Base):
    __tablename__ = "experiment"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)

    def start_run(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Run:
        if name is None:
            name = randomname.get_name()
            logger.debug(f"run name generated: {name}")
        with self.session():
            run = Run(
                experiment_id=self.id,
                name=name,
                description=description,
            )
        return run

    def search_runs(
        self,
        name: Optional[str] = None,
        offset: int = 0,
        length: Optional[int] = 1,
        expression: Optional[Expression] = None,
    ) -> List[Run]:
        with self.session() as session:
            q = session.query(Run)
            if expression is not None:
                q = q.filter(Run.id.in_(expression.stmt))
            q = q.order_by(desc(Run.created_at)).offset(offset)
            if name is not None:
                q = q.filter(Run.name == name)
            if length is not None:
                q = q.limit(length)
            result = q.all()
        return result

    @property
    def var(self) -> _VarIndexer:
        return _VarIndexer(self)


class _VarIndexer:
    def __init__(self, expr: Experiment, namespace: str = ""):
        self.get_expr = weakref.ref(expr)
        self.namespace = "" if namespace is None else namespace

    def __getitem__(self, key) -> Union[_VarIndexer, Variable]:
        result = self.get(key)
        if result is None:
            expr = self.get_expr()
            return _VarIndexer(expr, ns.join(self.namespace, key))
        return result

    def get(self, name: str) -> Variable:
        expr = self.get_expr()
        namespace, name = ns.parse(ns.join(self.namespace, name))
        with expr.session():
            result = Variable.get(
                experiment_id=expr.id,
                name=name,
                namespace=namespace,
            )
        return result
