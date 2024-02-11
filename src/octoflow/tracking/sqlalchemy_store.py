from __future__ import annotations

import datetime as dt
from collections import deque
from contextlib import contextmanager
from typing import ClassVar, List, Literal, NoReturn, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

from filelock import FileLock
from sqlalchemy import (
    JSON,
    URL,
    Boolean,
    Column,
    ColumnExpressionArgument,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    create_engine,
    desc,
)
from sqlalchemy.orm import Session, registry

from octoflow.tracking.models import Experiment, Run, RunTag, Value, Variable
from octoflow.tracking.store import TrackingStore

__all__ = [
    "SQLAlchemyTrackingStore",
]


mapper_registry = registry()


class Experiment(Experiment, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "experiment",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(60), nullable=False, unique=True),
        Column("description", String(60), nullable=True),
        Column("artifact_uri", String(60), nullable=True),
    )


class Run(Run, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "run",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("experiment_id", Integer, ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False),
        Column("name", String(60), nullable=False),
        Column("description", String(60), nullable=True),
        Column("created_at", DateTime, nullable=False, default=dt.datetime.utcnow),
        Column("ruid", String(60), nullable=True, unique=True, default=None),
    )


class RunTag(RunTag, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "tag",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("run_id", Integer, ForeignKey("run.id", ondelete="CASCADE"), nullable=False),
        Column("label", String(60), nullable=False),
        Index("ix_run_id_label", "run_id", "label", unique=True),
    )


class Value(Value, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "value",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("run_id", Integer, ForeignKey("run.id", ondelete="CASCADE"), nullable=False),
        Column("variable_id", Integer, ForeignKey("variable.id", ondelete="CASCADE"), nullable=True),
        Column("value", JSON, nullable=True),
        Column("timestamp", DateTime, nullable=False, default=dt.datetime.utcnow),
        Column("step_id", Integer, ForeignKey("value.id", ondelete="CASCADE"), nullable=True),
    )


class Variable(Variable, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "variable",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("experiment_id", Integer, ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False),
        Column("key", String(60), nullable=False),
        Column("parent_id", Integer, ForeignKey("variable.id", ondelete="CASCADE"), nullable=True),
        Column("type", String(60), nullable=True),
        Column("is_step", Boolean, nullable=True, default=None),
    )


variable_table_indexes = (
    Index(
        "ix_experiment_id_key",
        Variable.experiment_id,
        Variable.key,
        Variable.parent_id.is_(None),
        unique=True,
    ),
    Index(
        "ix_experiment_id_key_parent_id",
        Variable.experiment_id,
        Variable.key,
        Variable.parent_id,
        Variable.parent_id.isnot(None),
        unique=True,
    ),
)


class SQLAlchemyTrackingStore(TrackingStore):
    """SQLAlchemy tracking store.

    This class is used to define the interface for tracking store.
    """

    def __init__(
        self,
        url: Union[str, URL] = "sqlite:///:memory:",
    ):
        parsed_url: ParseResult = urlparse(url)
        lockfile = None
        if parsed_url.scheme in {"sqlite+filelock", "sqlite+lock"}:
            if len(parsed_url.netloc) > 0:
                msg = "SQLite database with filelock does not support netloc"
                raise ValueError(msg)
            if parsed_url.path == "/:memory:":
                msg = "SQLite in-memory database is not supported with filelock"
                raise ValueError(msg)
            lockfile = parsed_url.path + ".lock"
            url = f"sqlite:///{parsed_url.path}"
        if lockfile is not None:
            lockfile = FileLock(lockfile)
        self.lock: Optional[FileLock] = lockfile
        self.engine = create_engine(url)
        self.create_all()

    def create_all(self, checkfirst: bool = True):
        mapper_registry.metadata.create_all(
            self.engine,
            checkfirst=checkfirst,
        )

    @contextmanager
    def session(self):
        if self.lock is not None:
            try:
                self.lock.acquire(timeout=10)
            except TimeoutError as e:
                # another process might be using the lock
                raise e
        try:
            with Session(self.engine) as session:
                try:
                    yield session
                except Exception as e:
                    raise e
                finally:
                    deque(map(session.refresh, iter(session)))
                    session.expunge_all()
        finally:
            if self.lock is not None:
                self.lock.release()

    def create_experiment(
        self,
        name: str,
        description: Optional[str] = None,
        artifact_uri: Optional[str] = None,
    ) -> Experiment:
        expr = Experiment(
            name=name,
            description=description,
            artifact_uri=artifact_uri,
        )
        with self.session() as session:
            session.add(expr)
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
        return expr

    def list_experiments(self) -> List[Experiment]:
        with self.session() as session:
            exprs = session.query(Experiment).all()
        return exprs

    def get_experiment(self, experiment_id: int) -> Experiment:
        with self.session() as session:
            expr = session.query(Experiment).get(experiment_id)
        return expr

    def get_experiment_by_name(self, name: str) -> Experiment:
        with self.session() as session:
            expr = session.query(Experiment).filter(Experiment.name == name).one()
        return expr

    def create_run(
        self,
        experiment_id: int,
        name: str,
        description: Optional[str] = None,
        *,
        ruid: Optional[str] = None,
    ) -> Run:
        run = Run(
            experiment_id=experiment_id,
            name=name,
            description=description,
            ruid=ruid,
        )
        with self.session() as session:
            session.add(run)
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
        return run

    def list_runs(self, experiment_id: int) -> List[Run]:
        with self.session() as session:
            runs = session.query(Run).filter(Run.experiment_id == experiment_id).order_by(Run.id).all()
        return runs

    def search_runs(
        self,
        experiment_id: int,
        expression: ColumnExpressionArgument[bool],
    ) -> List[Run]:
        with self.session() as session:
            stmt = session.query(Run).filter(Run.experiment_id == experiment_id)
            runs = stmt.filter(expression).order_by(desc(Run.id)).all()
        return runs

    def add_tag(self, run_id: int, label: str) -> RunTag:
        tag = RunTag(run_id=run_id, label=label)
        with self.session() as session:
            session.add(tag)
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
        return tag

    def list_tags(self, run_id: int) -> List[str]:
        with self.session() as session:
            tags = session.query(RunTag).filter(RunTag.run_id == run_id).all()
        return [tag.label for tag in tags]

    def remove_tag(self, run_id: int, label: str) -> NoReturn:
        with self.session() as session:
            tag = session.query(RunTag).filter(RunTag.run_id == run_id, RunTag.label == label).one()
            session.delete(tag)
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

    def log_value(
        self,
        run_id: int,
        key: str,
        value: Union[str, int, float, bool],
        *,
        step_id: Optional[int] = None,
        type: Optional[Literal["param", "metric"]] = None,
        value_id: Optional[int] = None,
        is_step: Optional[bool] = None,
    ) -> Value:
        with self.session() as session:
            run: Run = session.query(Run).get(run_id)
        parent_id = None
        if step_id is not None:
            with self.session() as session:
                # return an instance or None if not found
                step: Value = session.query(Value).get(step_id)
            if step is None:
                msg = f"step with key '{key}' does not exist"
                raise ValueError(msg)
            if step.run_id != run_id:
                msg = f"step with key '{key}' does not belong to run with id '{run_id}'"
                raise ValueError(msg)
            parent_id = step.variable_id
            with self.session() as session:
                parent: Variable = session.query(Variable).get(parent_id)
                if parent.is_step is None:
                    parent.is_step = True  # update variable to be a step variable
                    try:
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        raise e
                elif not parent.is_step:
                    msg = f"variable with key '{parent.key}' is not marked as a step variable"
                    raise ValueError(msg)
        variable = Variable(
            experiment_id=run.experiment_id,
            key=key,
            type=type,
            parent_id=parent_id,
            is_step=is_step,
        )
        try:
            with self.session() as session:
                session.add(variable)
                session.commit()
        except Exception as ex:
            try:
                with self.session() as session:
                    variable = (
                        session.query(Variable)
                        .filter(
                            Variable.experiment_id == run.experiment_id,
                            Variable.key == key,
                            Variable.parent_id == parent_id,
                        )
                        .one()
                    )
            except Exception as e:
                raise e from ex
            if type is not None and variable.type != type:
                msg = f"expected type '{variable.type}', got '{type}' for variable with key '{variable.key}'"
                raise ValueError(msg) from ex
            if is_step is not None and variable.is_step is not is_step:
                msg = f"expected is_step '{variable.is_step}', got '{is_step}' for variable with key '{variable.key}'"
                raise ValueError(msg) from ex
        value = Value(
            run_id=run_id,
            variable_id=variable.id,
            value=value,
            step_id=step_id,
        )
        if value_id is not None:
            value.id = value_id
        try:
            with self.session() as session:
                session.add(value)
                session.commit()
        except Exception as e:
            session.rollback()
            raise e
        return value

    def get_values(self, run_id: int) -> List[Tuple[Variable, Value]]:
        with self.session() as session:
            stmt = (
                session.query(Variable, Value)
                .select_from(Value)
                .join(
                    Variable,
                    Value.variable_id == Variable.id,
                )
                .filter(Value.run_id == run_id)
            )
            values = stmt.order_by(Value.id).all()
        return values
