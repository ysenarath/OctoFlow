from __future__ import annotations

import hashlib
from typing import ClassVar, List, Optional, Union

import randomname

from octoflow.tracking.base import Base
from octoflow.tracking.run import Run
from octoflow.typing import Property

__all__ = [
    "Experiment",
]


def get_experiment_id(expr: Union[Experiment, str]) -> str:
    if isinstance(expr, str):
        return hashlib.sha256(expr.encode()).hexdigest()
    if not hasattr(expr, "_id") or expr._id is None:
        expr._id = get_experiment_id(expr.name)
    return expr._id


class Experiment(Base):
    name: str
    description: Optional[str] = None
    artifact_uri: Optional[str] = None
    id: ClassVar[Property[str]] = property(fget=get_experiment_id)

    def start_run(self, name: Optional[str] = None, description: Optional[str] = None) -> Run:
        if name is None:
            name = randomname.get_name()
        run = Run(
            experiment=self,
            name=name,
            description=description,
        )
        return self.store.create_run(run)

    def search_runs(
        self,
        name: Optional[str] = None,
        page: int = 1,
        per_page: Optional[int] = 10,
    ) -> List[Run]:
        return self.store.search_runs(
            expr=self,
            name=name,
            page=page,
            per_page=per_page,
        )
