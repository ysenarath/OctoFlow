from __future__ import annotations

from typing import List, Optional

from octoflow.model.base import SessionMixin, create_engine
from octoflow.model.experiment import Experiment
from octoflow.model.run import Run


class Client(SessionMixin):
    def __init__(self, tracking_uri: Optional[str] = None) -> None:
        # if tracking_uri is None -> defaults to 'sqlite:///:memory:'
        self.tracking_uri = tracking_uri
        self.engine = create_engine(tracking_uri)

    def refresh(self) -> Client:
        self.engine = create_engine(self.tracking_uri)
        return self

    def create_experiment(
        self,
        name: str,
        description: Optional[str] = None,
        return_if_exist: bool = True,
    ) -> Experiment:
        with self.session():
            expr = self.get_experiment_by_name(name)
            if expr is not None:
                if return_if_exist:
                    return expr
                msg = f"experiment with name '{name}' already exists"
                raise ValueError(msg)
            expr = Experiment(
                name=name,
                description=description,
            )
        return expr

    def get_experiment_by_name(self, name: str) -> Experiment:
        with self.session() as session:
            expr = session.query(Experiment).filter_by(name=name).first()
        return expr

    def get_experiment(self, id: int) -> Experiment:
        if id is None:
            msg = "experiment id cannot be None"
            raise ValueError(msg)
        with self.session() as session:
            try:
                expr = session.query(Experiment).get(id)
            except ValueError:
                msg = f"experiment with id '{id}' does not exist"
                raise ValueError(msg) from None
        return expr

    def list_experiments(self) -> List[Experiment]:
        with self.session() as session:
            exprs = session.query(Experiment).all()
        return exprs

    def get_run(self, id: int) -> Run:
        with self.session() as session:
            try:
                run = session.query(Run).get(id)
            except ValueError:
                msg = f"run with id '{id}' does not exist"
                raise ValueError(msg) from None
        return run
