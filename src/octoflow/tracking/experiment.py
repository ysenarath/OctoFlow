from __future__ import annotations

from typing import ClassVar, List, Optional, Union

import randomname

from octoflow.tracking.base import Base
from octoflow.tracking.run import Run
from octoflow.tracking.utils import validate_slug
from octoflow.typing import Property

__all__ = [
    "Experiment",
]


def get_experiment_id(expr: Union[Experiment, str]) -> str:
    if isinstance(expr, str):
        if not validate_slug(expr):
            msg = "invalid experiment name"
            raise ValueError(msg)
        return expr
    if not hasattr(expr, "_id") or expr._id is None:
        expr._id = get_experiment_id(expr.name)
    return expr._id


class Experiment(Base):
    name: str
    description: Optional[str] = None
    artifact_uri: Optional[str] = None
    id: ClassVar[Property[str]] = property(fget=get_experiment_id)

    def __post_init__(self) -> None:
        _ = self.id
        super().__post_init__()

    def start_run(self, name: Optional[str] = None, description: Optional[str] = None) -> Run:
        if name is None:
            name = randomname.get_name(sep="_")
        run = Run(
            experiment=self,
            name=name,
            description=description,
        )
        return self.store.create_run(run)

    def search_runs(self, name: Optional[str] = None) -> List[Run]:
        return self.store.search_runs(expr=self, name=name)

    def delete_run(self, run: Run) -> None:
        self.store.delete_run(run)
