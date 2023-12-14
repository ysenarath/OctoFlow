from typing import List, Optional

from octoflow.model.base import SessionMixin, create_engine
from octoflow.model.experiment import Experiment


class Client(SessionMixin):
    def __init__(self, tracking_uri: Optional[str] = None) -> None:
        self.engine = create_engine(tracking_uri)

    def create_experiment(self, name: str, description: Optional[str] = None) -> Experiment:
        with self.session():
            expr = Experiment(
                name=name,
                description=description,
            )
        return expr

    def get_experiment_by_name(self, name: str) -> Experiment:
        with self.session() as session:
            expr = session.query(Experiment).filter_by(name=name).first()
        return expr

    def list_experiments(self) -> List[Experiment]:
        with self.session() as session:
            exprs = session.query(Experiment).all()
        return exprs
