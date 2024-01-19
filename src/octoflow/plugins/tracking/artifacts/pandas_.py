from typing import Any

from octoflow.tracking.artifact.handler import ArtifactHandler

try:
    import pandas as pd
    from pandas import DataFrame, Series
except ImportError:
    pd, DataFrame, Series = None, None, None


class PadndasDataFrameArtifactHandler(ArtifactHandler, name="pandas.DataFrame"):
    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        return isinstance(obj, pd.DataFrame)

    def exists(self) -> bool:
        return (self.path / "data.csv").exists()

    def load(self) -> DataFrame:
        return pd.read_csv(
            self.path / "data.csv",
            index_col=0,
        )

    def save(self, obj):
        if not isinstance(obj, pd.DataFrame):
            msg = "obj must be a pandas DataFrame"
            raise TypeError(msg)
        obj.to_csv(self.path / "data.csv")


class PandasSeriesArtifactHandler(ArtifactHandler, name="pandas.Series"):
    @classmethod
    def can_handle(cls, obj: Any) -> bool:
        return isinstance(obj, pd.Series)

    def load(self) -> Series:
        return pd.read_csv(
            self.path / "data.csv",
            squeeze=True,
        )

    def save(self, obj):
        if not isinstance(obj, pd.Series):
            msg = "obj must be a pandas Series"
            raise TypeError(msg)
        obj.to_csv(self.path / "data.csv")
