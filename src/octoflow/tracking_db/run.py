"""Run model.

This module contains the run model.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from packaging.version import Version
from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, aliased, mapped_column
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from octoflow.tracking_db import artifact, value_utils
from octoflow.tracking_db.artifact import Artifact
from octoflow.tracking_db.base import Base
from octoflow.tracking_db.value import Value, ValueType
from octoflow.tracking_db.variable import Variable, VariableType

__all__ = [
    "Run",
]

NOT_SPECIFIED = object()


def generate_uuid() -> str:
    return str(uuid.uuid4())


class Run(Base):
    """Run model."""

    __tablename__ = "run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[int] = mapped_column(Integer, ForeignKey("experiment.id", ondelete="CASCADE"))
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    uuid: Mapped[str] = mapped_column(String(36), nullable=False, unique=True, default=generate_uuid)

    def _log_value(
        self,
        key: str,
        value: ValueType,
        *,
        step: Optional[Value] = None,
        type: Optional[VariableType] = None,
    ) -> Value:
        """Log a value."""
        parent_id = None if step is None else step.variable_id
        step_id = None if step is None else step.id
        with self.session():
            # get or create variable
            variable = Variable.get(
                key=key,
                experiment_id=self.experiment_id,
                parent_id=parent_id,
                type=type,
                create=True,  # create variable if it does not exist
            )
            value = Value(
                run_id=self.id,
                variable_id=variable.id,
                value=value,
                step_id=step_id,
            )
        return value

    def _log_values(
        self,
        *,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
        type: Optional[VariableType] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        """Log multiple values."""
        result = []
        prefix = "" if prefix is None else str(prefix).strip()
        for key, value in value_utils.flatten(values).items():
            if len(prefix) > 0:
                d = "." if prefix[-1] != "." else ""
                key = f"{prefix}{d}{key}"
            value = self._log_value(
                key=key,
                value=value,
                step=step,
                type=type,
            )
            result.append(value)
        return result

    def log_param(
        self,
        key: str,
        value: ValueType,
        step: Optional[Value] = None,
    ) -> Value:
        """Log a parameter.

        Parameters
        ----------
        key : str
            Parameter key.
        value : ValueType
            Parameter value.
        step : Optional[Value], optional
            Step value, by default None.

        Returns
        -------
        Value
            Value instance.
        """
        return self._log_value(
            key=key,
            value=value,
            step=step,
            type=VariableType.parameter,
        )

    def log_params(
        self,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        """Log multiple parameters.

        Parameters
        ----------
        values : Dict[str, ValueType]
            Dictionary of values.
        step : Optional[Value], optional
            Step value, by default None.

        Returns
        -------
        List[Value]
            List of values.
        """
        return self._log_values(
            values=values,
            step=step,
            type=VariableType.parameter,
            prefix=prefix,
        )

    def get_param(
        self,
        key: str,
        default: Optional[ValueType] = NOT_SPECIFIED,
        *,
        step: Optional[Value] = NOT_SPECIFIED,
    ) -> ValueType:
        """Get a parameter.

        Parameters
        ----------
        key : str
            Parameter key.
        default : Optional[ValueType], optional
            Default value, by default NOT_SPECIFIED.
        step : Optional[Value], optional
            Step value, by default NOT_SPECIFIED.

        Returns
        -------
        Value
            Value instance.
        """
        with self.session() as session:
            q = (
                session.query(Value)
                .join(Variable)
                .filter(
                    Value.run_id == self.id,
                    Variable.key == key,
                    Variable.type == VariableType.parameter,
                )
            )
            if step is None:
                q = q.filter(Value.step_id.is_(None))
            if step is not NOT_SPECIFIED:
                q = q.filter(Value.step_id == step.id)
            try:
                value = q.one().value
            except NoResultFound as e:
                if default is NOT_SPECIFIED:
                    msg = f"parameter with key '{key}' does not exist"
                    raise ValueError(msg) from e
                value = default
            except MultipleResultsFound as e:
                msg = f"multiple parameters with key '{key}' exist"
                raise ValueError(msg) from e
        return value

    def log_metric(
        self,
        key: str,
        value: ValueType,
        step: Optional[Value] = None,
    ) -> Value:
        """Log a metric.

        Parameters
        ----------
        key : str
            Metric key.
        value : ValueType
            Metric value.
        step : Optional[Value], optional
            Step value, by default None.

        Returns
        -------
        Value
            Value instance.
        """
        return self._log_value(
            key=key,
            value=value,
            step=step,
            type=VariableType.metric,
        )

    def log_metrics(
        self,
        values: Dict[str, ValueType],
        step: Optional[Value] = None,
        prefix: Optional[str] = None,
    ) -> List[Value]:
        """Log multiple metrics.

        Parameters
        ----------
        values : Dict[str, ValueType]
            Dictionary of values.
        """
        return self._log_values(
            values=values,
            step=step,
            type=VariableType.metric,
            prefix=prefix,
        )

    def log_artifact(
        self,
        key: str,
        value: Any = None,
        *,
        version: Union[str, Version, None] = None,
        handler_type: Optional[str] = None,
        save: bool = True,
        **kwargs: Dict[str, Any],
    ) -> Artifact:
        """Log an artifact.

        Parameters
        ----------
        key : str
            Artifact key.
        value : Optional[Any], optional
            Artifact value, by default None.
        version : Union[str, Version, None], optional
            Artifact version, by default None.
        path : Union[str, Path]
            Artifact path.
        handler_type : Union[str, None]
            Artifact handler (name), by default None.
        save : bool, optional
            Whether to save the artifact, by default True.
        kwargs : Dict[str, Any]
            Additional keyword arguments.

        Returns
        -------
        Value
            Value instance.
        """
        if handler_type is None and value is not None:
            handler_type = artifact.get_handler_type_by_object(value)
        elif isinstance(handler_type, str):
            # validate if a handler for type exists
            try:
                handler_type = artifact.get_handler_type(handler_type)
            except ValueError as e:
                msg = str(e.args[0]) + ", please specify an appropriate handler type manually"
                raise ValueError(msg) from e
        with self.session():
            a = Artifact(
                run_id=self.id,
                key=key,
                handler_type=handler_type,
                version=version,
            )
        if save:
            a.save(value, **kwargs)
        return a

    def get_artifact(
        self,
        key: str,
        version: Union[str, Version, None] = None,
    ) -> Artifact:
        """Load an artifact.

        Parameters
        ----------
        key : str
            Artifact key.
        version : Union[str, Version, None], optional
            Artifact version, by default None.

        Returns
        -------
        Any
            Artifact value.
        """
        with self.session() as session:
            q = session.query(Artifact).filter(
                Artifact.run_id == self.id,
                Artifact.key == key,
            )
            if version is not None and str(version) == "latest":
                a = q.order_by(Artifact.version.desc()).first()
            else:
                if version is not None:
                    q = q.filter(Artifact.version == str(version))
                try:
                    a = q.one_or_none()
                except MultipleResultsFound as e:
                    if version is None:
                        # ask user to specify version to narrow down the search
                        msg = f"multiple artifacts with key '{key}' exist, please specify version"
                        raise ValueError(msg) from e
                    raise e
        if a is None:
            msg = f"artifact with key '{key}' does not exist"
            raise ValueError(msg)
        return a

    def get_raw_logs(self) -> List[Tuple[int, int, int, str, Any]]:
        """Get all logs of the run.

        Returns
        -------
        List[Tuple[int, int, int, bool, str, Any]]
            List of logs.
        """
        with self.session() as session:
            child_value = aliased(Value)
            # get all values of this run and their corresponding variable keys
            values = (
                session.query(
                    Value.run_id,
                    Value.id,
                    Value.step_id,
                    child_value.id.is_not(None).label("is_step"),
                    Variable.key,
                    Value.value,
                )
                .select_from(Value)
                .join(Variable, Variable.id == Value.variable_id)
                .outerjoin(child_value, child_value.step_id == Value.id)
                .filter(Value.run_id == self.id)
                .distinct(Value.id)
                .all()
            )
        return values

    def get_logs(self) -> value_utils.ValueTree:
        """Get all logs of the run.

        Returns
        -------
        ValueTree
            List of logs.
        """
        values = self.get_raw_logs()
        trees = value_utils.build_trees(values)
        return trees[self.id]

    def match(self, partial: bool = True) -> Generator[int, None, None]:
        """Checks whether the run with same values for parameter typed variables exists in the database.

        Parameters
        ----------
        partial : bool, optional
            Whether to check for partial match, by default True.

        Yields
        ------
        Generator[int, None, None]
            Generator of run IDs.
        """
        with self.session() as session:
            # get all values of this run and their corresponding variable keys
            child_value = aliased(Value)
            values = (
                session.query(
                    Value.run_id,
                    Value.id,
                    Value.step_id,
                    child_value.id.is_not(None).label("is_step"),
                    Variable.key,
                    Value.value,
                )
                .select_from(Value)
                .join(Variable, Variable.id == Value.variable_id)
                .outerjoin(child_value, child_value.step_id == Value.id)
                .filter(
                    Variable.experiment_id == self.experiment_id,
                    Variable.type == VariableType.parameter,
                )
                .distinct(Value.id)
                .all()
            )
            # build up a tree of values
            trees = value_utils.build_trees(values)
        # check whether there is a run with same values
        if self.id not in trees:
            msg = f"run with id '{self.id}' does not exist"
            raise ValueError(msg)
        this = trees.pop(self.id).normalize()
        for run_id, other in trees.items():
            other = other.normalize()
            if value_utils.equals(this, other, partial=partial):
                yield run_id
