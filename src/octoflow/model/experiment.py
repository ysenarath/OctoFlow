from __future__ import annotations

from typing import Optional

from sqlalchemy import Column, Integer, String, Text

from octoflow import logging
from octoflow.model.base import Base
from octoflow.model.run import Run

logger = logging.get_logger(__name__)


class Experiment(Base):
    __tablename__ = "experiment"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    name: str = Column(String(255), nullable=False, unique=True)
    description: str = Column(Text, nullable=True)

    def start_run(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Run:
        with self.session():
            run = Run(
                experiment_id=self.id,
                name=name,
                description=description,
            )
        return run
