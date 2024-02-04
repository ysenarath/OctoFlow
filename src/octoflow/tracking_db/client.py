from __future__ import annotations

from typing import List, Optional

from octoflow.tracking_db.base import SessionMixin, create_engine
from octoflow.tracking_db.experiment import Experiment
from octoflow.tracking_db.run import Run


class Client(SessionMixin):
    def __init__(self, tracking_uri: Optional[str] = None) -> None:
        """
        Client for interacting with the database.

        Parameters
        ----------
        tracking_uri : str, optional
            Database URI, by default None.
        """
        # if tracking_uri is None -> defaults to 'sqlite:///:memory:'
        self.tracking_uri = tracking_uri
        self.engine = create_engine(tracking_uri)

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
                artifact_uri=artifact_uri,
            )
        return expr

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
        with self.session() as session:
            expr = session.query(Experiment).filter_by(name=name).first()
        return expr

    def list_experiments(self) -> List[Experiment]:
        """
        List all experiments.

        Returns
        -------
        List[Experiment]
            List of experiments.
        """
        with self.session() as session:
            exprs = session.query(Experiment).all()
        return exprs

    def get_experiment(self, id: int) -> Experiment:
        """
        Get an experiment by id.

        Parameters
        ----------
        id : int
            Experiment id.

        Returns
        -------
        Experiment
            Experiment object.
        """
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

    def get_run(self, id: int) -> Run:
        """
        Get a run by id.

        Parameters
        ----------
        id : int
            Run id.

        Returns
        -------
        Run
            Run object.
        """
        if id is None:
            msg = "run id cannot be None"
            raise ValueError(msg)
        with self.session() as session:
            try:
                run = session.query(Run).get(id)
            except ValueError:
                msg = f"run with id '{id}' does not exist"
                raise ValueError(msg) from None
        return run
