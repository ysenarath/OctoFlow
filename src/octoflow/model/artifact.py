import json
import os
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from octoflow.model.base import Base

try:
    import pandas as pd
    from pandas import DataFrame
except ImportError:
    pd, DataFrame = None, None

try:
    import numpy as np
    from numpy.typing import NDArray
except ImportError:
    np, NDArray = None, None


class ArtifactType:
    @classmethod
    def check(cls, obj: Any):
        raise NotImplementedError

    @classmethod
    def load(cls, path: str):
        raise NotImplementedError

    @classmethod
    def save(cls, obj: Any, path: str):
        raise NotImplementedError


@ArtifactType.register
class DataFrameType(ArtifactType):
    @classmethod
    def check(cls, obj: Any):
        return isinstance(obj, DataFrame)

    @classmethod
    def load(cls, path: str):
        return pd.read_csv(path)

    @classmethod
    def save(cls, obj: Any, path: str):
        if not isinstance(obj, pd.DataFrame):
            msg = "obj must be a pandas DataFrame"
            raise TypeError(msg)
        obj.to_csv(path)


class Artifact(Base):
    __tablename__ = "artifact"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(Integer, ForeignKey("run.id"), nullable=False)
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    path: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[ArtifactType] = mapped_column(Enum(ArtifactType), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index(
            "ix_artifact_run_id_key",
            "run_id",
            "key",
            unique=True,
        ),
    )

    def unlink(self):
        path = Path(self.path)
        if not path.exists():
            return
        if path.is_dir():
            os.rmdir(path)
        else:
            path.unlink()

    def save(self, obj: Any):
        if self.type == ArtifactType.JSON:
            return self.save_json(obj)
        if self.type == ArtifactType.ARRAY:
            return self.save_array(obj)
        if self.type == ArtifactType.DATAFRAME:
            return self.save_dataframe(obj)
        if self.type == ArtifactType.PYOBJECT:
            return self.save_pickle(obj)

    def load(self):
        if self.type == ArtifactType.JSON:
            return self.load_json()
        if self.type == ArtifactType.ARRAY:
            return self.load_array()
        if self.type == ArtifactType.DATAFRAME:
            return self.load_dataframe()
        if self.type == ArtifactType.PYOBJECT:
            return self.load_pickle()

    def load_dataframe(self) -> DataFrame:
        return pd.read_csv(self.path)

    def save_dataframe(self, obj: DataFrame):
        if not isinstance(obj, pd.DataFrame):
            msg = "obj must be a pandas DataFrame"
            raise TypeError(msg)
        obj.to_csv(self.path)

    def load_pickle(self):
        with open(self.path, "rb") as f:
            return pickle.load(f)  # noqa: S301

    def save_pickle(self, obj: Any):
        with open(self.path, "wb") as f:
            pickle.dump(obj, f)

    def load_array(self) -> NDArray:
        return np.load(self.path)

    def save_array(self, obj: NDArray):
        np.save(self.path, obj)

    def load_json(self) -> Any:
        with open(
            self.path,
            encoding="utf-8",
        ) as f:
            return json.load(f)

    def save_json(self, obj: Any):
        with open(
            self.path,
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(obj, f)
