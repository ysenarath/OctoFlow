from __future__ import annotations

from typing import List, Optional

import randomname
from sqlalchemy import Integer, String, Text, desc
from sqlalchemy.orm import Mapped, mapped_column

from octoflow import logging
from octoflow.model.base import Base
from octoflow.model.run import Run

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
    ) -> List[Run]:
        with self.session() as session:
            q = session.query(Run).order_by(desc(Run.created_at)).offset(offset)
            if name is not None:
                q = q.filter(Run.name == name)
            if length is not None:
                q = q.limit(length)
            result = q.all()
        return result
