from __future__ import annotations

from typing import Optional

from sqlalchemy import Integer, String, Text
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
        with self.session():
            run = Run(
                experiment_id=self.id,
                name=name,
                description=description,
            )
        return run
