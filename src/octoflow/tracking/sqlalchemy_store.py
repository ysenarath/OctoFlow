from __future__ import annotations

import datetime as dt
from collections import deque
from contextlib import contextmanager
from typing import ClassVar, Dict, List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

import sqlalchemy
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
    case,
    create_engine,
    desc,
)
from sqlalchemy.orm import Session, registry
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from octoflow.tracking.models import Experiment, JSONType, Run, RunTags, Tag, Value, Variable
from octoflow.tracking.store import TrackingStore, ValueType, VariableType

__all__ = [
    "SQLAlchemyTrackingStore",
]


mapper_registry = registry()


class SQLAlchemyModelMixin:
    @sqlalchemy.orm.reconstructor
    def init_on_load(self):
        # when object is constructed via sqlalchemy.orm
        # get a ref to the session maker
        self.__post_init__()


class Experiment(Experiment, SQLAlchemyModelMixin, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "experiment",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(60), nullable=False, unique=True),
        Column("description", String(60), nullable=True),
        Column("artifact_uri", String(60), nullable=True),
    )


class Run(Run, SQLAlchemyModelMixin, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "run",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("experiment_id", Integer, ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False),
        Column("name", String(60), nullable=False),
        Column("description", String(60), nullable=True),
        Column("created_at", DateTime, nullable=False, default=dt.datetime.utcnow),
    )


class Value(Value, SQLAlchemyModelMixin, registry=mapper_registry):
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


class Variable(Variable, SQLAlchemyModelMixin, registry=mapper_registry):
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


variable_constraints = (
    Index(
        "ix_experiment_id_key",
        Variable.experiment_id,
        Variable.key,
        case(
            (Variable.parent_id.is_(None), "<NULL>"),
            else_=Variable.parent_id,
        ),
        unique=True,
    ),
)


class Tag(Tag, SQLAlchemyModelMixin, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "tag",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(60), nullable=False, unique=True),
    )


class RunTags(RunTags, SQLAlchemyModelMixin, registry=mapper_registry):
    __table__: ClassVar[Table] = Table(
        "run_tags",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("run_id", Integer, ForeignKey("run.id", ondelete="CASCADE"), nullable=False),
        Column("tag_id", Integer, ForeignKey("tag.id", ondelete="CASCADE"), nullable=False),
        Column("value", JSON, nullable=True, default=None),
        Index("ix_run_id_tag_id", "run_id", "tag_id", unique=True),
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
            try:
                session.add(expr)
                session.commit()
            except Exception as e:
                session.rollback()
                msg = "experiment with name '{name}' already exists"
                raise ValueError(msg) from e
        return expr

    def list_experiments(self) -> List[Experiment]:
        with self.session() as session:
            exprs = session.query(Experiment).all()
        return exprs

    def get_experiment(self, experiment_id: int) -> Experiment:
        with self.session() as session:
            expr = session.query(Experiment).get(experiment_id)
        if expr is None:
            msg = f"experiment with id '{experiment_id}' does not exist"
            raise ValueError(msg)
        return expr

    def get_experiment_by_name(self, name: str) -> Experiment:
        with self.session() as session:
            try:
                stmt = session.query(Experiment).filter(Experiment.name == name)
                expr = stmt.one()
            except NoResultFound as e:
                msg = f"experiment with name '{name}' does not exist"
                raise ValueError(msg) from e
            except MultipleResultsFound as e:
                msg = f"multiple experiments with name '{name}' found"
                raise ValueError(msg) from e
        return expr

    def create_run(self, experiment_id: int, name: str, description: Optional[str] = None) -> Run:
        run = Run(
            experiment_id=experiment_id,
            name=name,
            description=description,
        )
        with self.session() as session:
            try:
                session.add(run)
                session.commit()
            except Exception as e:
                session.rollback()
                msg = f"could not create run with name '{name}'"
                raise ValueError(msg) from e
        return run

    def set_tag(self, run_id: int, name: str, value: JSONType = None) -> RunTags:
        msg = f"could not set tag '{name}' for run with id '{run_id}'"
        with self.session() as session:
            tag = Tag(name=name)
            try:
                session.add(tag)
                session.commit()
            except Exception:
                session.rollback()
                tag = session.query(Tag).filter(Tag.name == name).one_or_none()
            if tag is None:
                # unale to get or create tag
                raise ValueError(msg)
            run_tag = RunTags(
                run_id=run_id,
                tag_id=tag.id,
                value=value,
            )
            try:
                session.add(run_tag)
                session.commit()
                return run_tag
            except Exception:
                session.rollback()
            run_tag = (
                session.query(RunTags)
                .filter(
                    RunTags.run_id == run_id,
                    RunTags.tag_id == tag.id,
                )
                .first()
            )
            if run_tag is None:
                raise ValueError(msg)
            # try to update the value
            run_tag.value = value
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise ValueError(msg) from e
        return run_tag

    def get_tag(self, run_id: int, name: str) -> JSONType:
        with self.session() as session:
            stmt = (
                session.query(RunTags)
                .select_from(RunTags)
                .join(Tag, RunTags.tag_id == Tag.id)
                .filter(
                    RunTags.run_id == run_id,
                    Tag.name == name,
                )
            )
            run_tag = stmt.one_or_none()
        if run_tag is None:
            return None
        return run_tag.value

    def get_tags(self, run_id: int) -> Dict[str, JSONType]:
        with self.session() as session:
            stmt = (
                session.query(Tag, RunTags)
                .select_from(RunTags)
                .join(Tag, RunTags.tag_id == Tag.id)
                .filter(RunTags.run_id == run_id)
            )
            results: List[Tuple[Tag, RunTags]] = stmt.all()
        tags = []
        for row in results:
            tags.append((row[0].name, row[1].value))
        return dict(tags)

    def count_tags(self, run_id: int) -> int:
        with self.session() as session:
            stmt = (
                session.query(Tag, RunTags)
                .select_from(RunTags)
                .join(Tag, RunTags.tag_id == Tag.id)
                .filter(RunTags.run_id == run_id)
            )
            count = stmt.count()
        return count

    def delete_tag(self, run_id: int, name: str) -> RunTags:
        msg = f"could not delete tag '{name}' for run with id '{run_id}'"
        with self.session() as session:
            tag = session.query(Tag).filter(Tag.name == name).one_or_none()
            if tag is None:
                return None
            run_tag = (
                session.query(RunTags)
                .filter(
                    RunTags.run_id == run_id,
                    RunTags.tag_id == tag.id,
                )
                .one_or_none()
            )
            if run_tag is None:
                return None
            try:
                session.delete(run_tag)
                session.commit()
            except Exception as e:
                session.rollback()
                raise ValueError(msg) from e
        return run_tag

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

    def log_value(
        self,
        run_id: int,
        key: str,
        value: ValueType,
        *,
        step_id: Optional[int] = None,
        type: Optional[VariableType] = None,
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
