from __future__ import annotations

from typing import List, Optional

from octoflow.tracking.base import Base
from octoflow.tracking.experiment import Experiment
from octoflow.tracking.store import TrackingStore

__all__ = [
    "TrackingClient",
]


class TrackingClient(Base):
    def __init__(self, store: TrackingStore) -> None:
        """
        Client for interacting with the database.

        Parameters
        ----------
        tracking_uri : str, optional
            Database URI, by default None.
        """
        super().__init__()
        self.store = store

    def create_experiment(
        self,
        name: str,
        description: Optional[str] = None,
        artifact_uri: Optional[str] = None,
        return_if_exist: bool = True,
    ) -> Experiment:
        """
        Create an experiment.

        Parameters
        ----------
        name : str
            Experiment name.
        description : str, optional
            Experiment description, by default None.
        return_if_exist : bool, optional
            Return the experiment if it already exists, by default True.

        Returns
        -------
        Experiment
            Experiment object.
        """
        try:
            expr = self.get_experiment_by_name(name)
        except ValueError:
            expr = None
        if expr is not None:
            if return_if_exist:
                return expr
            msg = f"experiment with name '{name}' already exists"
            raise ValueError(msg)
        expr = Experiment(
            name=name,
            description=description,
            artifact_uri=artifact_uri,
        )
        return self.store.create_experiment(expr)

    def get_experiment_by_name(self, name: str) -> Experiment:
        """
        Get an experiment by name.

        Parameters
        ----------
        name : str
            Experiment name.

        Returns
        -------
        Experiment
            Experiment object.
        """
        return self.store.get_experiment_by_name(name)

    def list_experiments(self) -> List[Experiment]:
        """
        List all experiments.

        Returns
        -------
        List[Experiment]
            List of experiments.
        """
        return self.store.list_all_experiments()

    def get_experiment(self, id: str) -> Experiment:
        """
        Get an experiment by id.

        Parameters
        ----------
        id : str
            Experiment id.

        Returns
        -------
        Experiment
            Experiment object.
        """
        if id is None:
            msg = "experiment id cannot be None"
            raise ValueError(msg)
        try:
            expr = self.store.get_experiment(id)
        except ValueError:
            msg = f"experiment with id '{id}' does not exist"
            raise ValueError(msg) from None
        return expr

    def delete_experiment(self, expr: Experiment) -> None:
        """
        Delete an experiment.

        Parameters
        ----------
        expr : Experiment
            Experiment object.
        """
        self.store.delete_experiment(expr)
