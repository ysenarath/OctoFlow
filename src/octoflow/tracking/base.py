import json
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Generator, Optional, Union

import sqlalchemy as sa
from sqlalchemy import Engine, event
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from typing_extensions import Protocol, dataclass_transform, runtime_checkable  # noqa: UP035

from octoflow import logging

_sessionmaker_cv = ContextVar("sessionmaker_cv", default=None)
_session_cv = ContextVar("session_cv", default=None)
_persist_on_init_cv = ContextVar("persist_on_init_cv", default=True)

logger = logging.get_logger(__name__)


class SessionError(ValueError): ...


@contextmanager
def persist_on_init(state: Optional[bool] = None) -> Generator[bool, None, None]:
    if state is None:
        yield _persist_on_init_cv.get()
    else:
        token = _persist_on_init_cv.set(state)
        yield state
        _persist_on_init_cv.reset(token)


@contextmanager
def build_session(
    engine_or_session_factory: Union[Engine, sessionmaker, None] = None,
) -> Generator[Session, None, None]:
    """
    Context manager for managing SQLAlchemy sessions.

    Parameters
    ----------
    engine_or_session_factory : Engine
        SQLAlchemy Engine or SessionFactory.

    Yields
    ------
    Session
        A SQLAlchemy Session.

    Raises
    ------
    ValueError
        If the provided `engine_or_session_factory` is invalid or not in the context.
    """
    # Determine the session factory based on the input
    if isinstance(engine_or_session_factory, Engine):
        session_factory = sessionmaker(
            engine_or_session_factory,
            expire_on_commit=False,
        )
    else:
        session_factory = engine_or_session_factory
    # Set the session factory as the global session maker for this coroutine
    if session_factory is None:
        session_factory = _sessionmaker_cv.get()
    sessionmaker_token = _sessionmaker_cv.set(session_factory)
    # Retrieve the current session from the context
    session: Session = _session_cv.get()
    if session is None:
        # No existing session found in the context
        if session_factory is None:
            msg = "no session object in the context, and session_factory is None"
            raise ValueError(msg)
        # Create a new session and set it in the context
        with session_factory() as session:
            session_token = _session_cv.set(session)
            yield session  # Yield the session
            _session_cv.reset(session_token)
    else:
        # Use the existing session from the context
        yield session
    # Reset the global session maker in the context
    _sessionmaker_cv.reset(sessionmaker_token)


@runtime_checkable
class EngineProtocol(Protocol):
    """
    Protocol defining an object with an optional SQLAlchemy Engine.

    Attributes
    ----------
    engine : Optional[Engine]
        SQLAlchemy Engine. If not provided, it can be None.
    """

    engine: Optional[Engine]


@runtime_checkable
class SessionFactoryProtocol(Protocol):
    """
    Protocol defining an object with an optional SQLAlchemy SessionFactory.

    Attributes
    ----------
    session_factory : Optional[sessionmaker]
        SQLAlchemy SessionFactory. If not provided, it can be None.
    """

    session_factory: Optional[sessionmaker]


class SessionMixin:
    """
    Mixin class providing a context manager for managing SQLAlchemy sessions.

    Methods
    -------
    session() -> Generator[Session, None, None]
        Context manager for accessing an SQLAlchemy Session.

    Raises
    ------
    TypeError
        If the session type is unknown.
    """

    @contextmanager
    def session(self=None) -> Generator[Session, None, None]:
        """
        Context manager for managing SQLAlchemy sessions.

        Yields
        ------
        Session
            A SQLAlchemy Session.

        Raises
        ------
        TypeError
            If the protocol is unknown.
        """
        if self is None:
            # called in class context
            with build_session() as session:
                yield session
        elif isinstance(self, SessionFactoryProtocol):
            with build_session(self.session_factory) as session:
                yield session
        elif isinstance(self, EngineProtocol):
            with build_session(self.engine) as session:
                yield session
        else:
            msg = f"unknown session type: {type(self).__name__}"
            raise TypeError(msg)


@dataclass_transform()
class Base(DeclarativeBase, SessionMixin):
    def __init__(self, *args, **kwargs):
        super(Base, self).__init__()
        self.session_factory: sessionmaker = _sessionmaker_cv.get()
        if self.session_factory is None:
            msg = "no session object in the context, and session_factory is None"
            raise ValueError(msg)
        n_args = len(args)
        cls = type(self)
        for idx, (key, _) in enumerate(cls.__annotations__.items()):
            if idx < n_args:
                val = args[idx]
            elif key in kwargs:
                val = kwargs[key]
            else:
                # if not provided
                continue
            super(Base, self).__setattr__(key, val)
        with persist_on_init() as persist:
            if persist:
                self.create()

    @sa.orm.reconstructor
    def init_on_load(self):
        # when object is constructed via sqlalchemy.orm
        # get a ref to the session maker
        self.session_factory: sessionmaker = _sessionmaker_cv.get()

    def create(self):
        with self.session() as session:
            try:
                logger.debug(f"Persisting '{type(self).__name__}'.")
                session.add(self)
                session.commit()
            except SQLAlchemyError as err:
                logger.debug(f"Failed to persist '{type(self).__name__}'.")
                session.rollback()
                msg = f"unable to persist '{type(self).__name__}' object with attributes: {self.to_dict()}"
                raise ValueError(msg) from err

    def __setattr__(self, name: str, value: Any):
        if hasattr(self, "session_factory") and self.session_factory is not None:
            # update
            revert_if_fails_value = value
            with self.session() as session:
                try:
                    logger.debug(f"Updating attribute '{name}' of '{type(self).__name__}' to '{value}'.")
                    super(Base, self).__setattr__(name, value)
                    session.merge(self)
                    session.commit()
                    logger.debug(f"Updated attribute '{name}' of '{type(self).__name__}' to '{value}'.")
                except SQLAlchemyError as err:
                    logger.debug(f"Failed to update attribute '{name}' of '{type(self).__name__}' to '{value}'.")
                    session.rollback()
                    super(Base, self).__setattr__(name, revert_if_fails_value)
                    msg = f"unable to set '{name}'"
                    raise AttributeError(msg) from err
        else:
            super(Base, self).__setattr__(name, value)

    def delete(self) -> Any:
        with self.session() as session:
            try:
                logger.debug(f"Deleting '{type(self).__name__}'.")
                session.delete(self)
                session.commit()
                logger.debug(f"Deleted '{type(self).__name__}'.")
            except SQLAlchemyError as err:
                logger.error(f"Failed to delete '{type(self).__name__}'.")
                session.rollback()
                msg = f"unable to delete '{type(self).__name__}'"
                raise ValueError(msg) from err

    @classmethod
    def from_dict(cls, data: dict):
        primary_keys = iter(column.name for column in cls.__table__.columns if column.primary_key)
        kwargs = {key: data[key] for key in primary_keys}
        session_factory: sessionmaker = _sessionmaker_cv.get()
        with build_session(session_factory) as session:
            # merge with the current session
            self = session.query(cls).filter_by(**kwargs).first()
        return self

    def to_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in type(self).__annotations__}

    def to_json(self):
        data = self.to_dict()
        return json.dumps(data, sort_keys=True, default=str)


def create_engine(url: Optional[str] = None):
    """
    Create a SQLAlchemy engine with an optional database URL.

    Parameters
    ----------
    url : Optional[str], optional
        Database URL, by default None.
        If None, an SQLite in-memory database is created for testing purposes.

    Returns
    -------
    Engine
        SQLAlchemy Engine instance.

    Notes
    -----
    This function also creates tables defined in the declarative base (Base).

    Examples
    --------
    >>> engine = create_engine("sqlite:///logs/octoflow/database.sqlite")
    >>> engine = create_engine("postgresql://user:password@localhost/mydatabase")
    >>> engine = create_engine()  # Creates an SQLite in-memory database for testing
    """
    if url is None:
        # Create an SQLite in-memory database for testing purposes
        url = "sqlite:///:memory:"
    # Create the SQLAlchemy engine
    engine = sa.create_engine(url, echo=False)
    event.listen(engine, "connect", _fk_pragma_on_connect)
    # Create tables defined in the declarative base
    Base.metadata.create_all(engine)
    return engine


def _fk_pragma_on_connect(dbapi_con, con_record):
    dbapi_con.execute("pragma foreign_keys=ON")
