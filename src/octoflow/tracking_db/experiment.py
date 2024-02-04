from __future__ import annotations

import math
from collections import UserList
from typing import List, Optional

import randomname
from sqlalchemy import Integer, String, Text, desc
from sqlalchemy.orm import Mapped, mapped_column

from octoflow import logging
from octoflow.tracking_db.base import Base
from octoflow.tracking_db.run import Run
from octoflow.tracking_db.variable import Variable

logger = logging.get_logger(__name__)


class Experiment(Base):
    __tablename__ = "experiment"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    artifact_uri: Mapped[str] = mapped_column(String(255), nullable=True)

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
        page: int = 1,
        per_page: Optional[int] = 10,
        # expression: Optional[Expression] = None,
    ) -> List[Run]:
        offset = (page - 1) * per_page
        with self.session() as session:
            q = session.query(Run)
            # if expression is not None:
            #     q = q.filter(Run.id.in_(expression.stmt))
            if name is not None:
                q = q.filter(Run.name == name)
            # get number of pages
            total = q.count()
            q = q.order_by(desc(Run.created_at))
            if per_page is not None:
                q = q.offset(offset).limit(per_page)
            result = q.all()
        result = UserList(result)
        result.total = total
        result.num_pages = math.ceil(total / per_page)
        return result

    def get_variables(self) -> List[Variable]:
        with self.session() as session:
            variables = session.query(Variable).filter(Variable.experiment_id == self.id).all()
        return variables
