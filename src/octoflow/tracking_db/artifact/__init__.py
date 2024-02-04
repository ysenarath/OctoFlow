from __future__ import annotations

from datetime import datetime
from distutils.version import Version
from pathlib import Path
from typing import Any, Type, Union

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    text,
    types,
)
from sqlalchemy.orm import Mapped, mapped_column

from octoflow.tracking_db.artifact import handler
from octoflow.tracking_db.artifact.handler import (
    ArtifactHandler,
    get_handler_type,
    get_handler_type_by_object,
    list_handler_types,
)
from octoflow.tracking_db.base import Base

__all__ = [
    "Artifact",
    "handler",
    "get_handler_type",
    "get_handler_type_by_object",
    "list_handler_types",
]


class ArtifactHandlerType(types.TypeDecorator):
    """A SQLAlchemy type decorator for ArtifactHandler."""

    impl = types.Unicode

    cache_ok = True

    def process_bind_param(self, value: Type[ArtifactHandler], dialect) -> str:  # noqa: PLR6301
        """Gets the name from the handler type."""
        if not issubclass(value, ArtifactHandler):
            msg = f"'{value}' is not a subclass of 'ArtifactHandler'"
            raise ValueError(msg)
        return value.name

    def process_result_value(self, value: str, dialect) -> Type[ArtifactHandler]:  # noqa: PLR6301
        """Gets the handler type for the given name."""
        return get_handler_type(value)


class VersionType(types.TypeDecorator):
    """A SQLAlchemy type decorator for Version."""

    impl = types.Unicode

    cache_ok = True

    def process_bind_param(self, value: Union[str, Version], dialect) -> str:  # noqa: PLR6301
        """Converts the value to a string."""
        if not isinstance(value, Version):
            value = Version(value)
        return str(value)

    def process_result_value(self, value: str, dialect) -> Version:  # noqa: PLR6301
        """Converts the value to a Version."""
        return Version(value)


class Artifact(Base):
    __tablename__ = "artifact"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("run.id"), nullable=False)
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[Version] = mapped_column(String(128), nullable=False, default="0.0.0")
    handler_type: Mapped[Type[ArtifactHandler]] = mapped_column(ArtifactHandlerType, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index(
            "ix_artifact_run_id_key",
            "run_id",
            "key",
            "version",
            unique=True,
        ),
    )

    @property
    def path(self) -> Path:
        if hasattr(self, "_path") and self._path is not None:
            return self._path
        with self.session() as session:
            query = text("SELECT r.experiment_id, r.uuid FROM run as r WHERE r.id = :run_id")
            exper_id, run_uuid = session.execute(query, {"run_id": self.run_id}).one()
            query = text("SELECT e.artifact_uri FROM experiment as e WHERE e.id = :experiment_id")
            arifact_uri = session.execute(query, {"experiment_id": exper_id}).scalar()
            path: Path = Path(arifact_uri) / run_uuid / f"{self.key}-v{self.version}"
            if not path.exists():
                path.mkdir(parents=True)
            self._path = path
        return self._path

    @property
    def handler(self) -> ArtifactHandler:
        """Return the handler for this artifact."""
        if not hasattr(self, "_handler") or self._handler is None:
            self._handler = self.handler_type(
                self.path,
            )
        return self._handler

    def load(self) -> Any:
        """
        Load the artifact.

        Returns
        -------
        Any
            The loaded artifact.
        """
        return self.handler.load()

    def save(self, obj: Any, **kwargs) -> None:
        """
        Save an object to the artifact.

        Parameters
        ----------
        obj : Any
            The object to save.
        kwargs : dict
            Additional keyword arguments.
        """
        self.handler.save(obj, **kwargs)

    def exists(self) -> bool:
        """
        Return True if the artifact exists.

        Returns
        -------
        bool
            True if the artifact exists.
        """
        return self.handler.exists()

    def unlink(self) -> None:
        """
        Unlink/delete the artifact.

        Notes
        -----
        This will only delete the artifact, not the database record.

        Returns
        -------
        None
            None
        """
        self.handler.unlink()

    def delete(self, unlink: bool = False) -> None:
        """
        Delete the artifact.

        Parameters
        ----------
        unlink : bool, optional
            If True, unlink the artifact, by default False.

        Notes
        -----
        This will delete the database record and unlink the artifact if `unlink` is True.

        Returns
        -------
        None
            None
        """
        try:
            # delete the database record
            super().delete()
            if unlink:
                # unlink the artifact
                self.handler.unlink()
        except ValueError as ex:
            raise ex
