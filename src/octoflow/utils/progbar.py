from __future__ import annotations

import contextlib
import io
import json
import sys
import weakref
from typing import Any, Iterable, List, Optional, Union

import pandas as pd
from IPython.display import display as ipy_display
from ipywidgets import widgets
from tabulate import tabulate
from tqdm import tqdm as std_tqdm
from tqdm.auto import tqdm as auto_tqdm
from tqdm.notebook import tqdm as nb_tqdm

from octoflow.utils import func

__all__ = [
    "ProgressBar",
]


class ProgressBarIO(io.StringIO):
    def __init__(self, pbar: Optional[ProgressBar] = None):
        super().__init__()
        self.get_pbar = weakref.ref(pbar)
        self.current_line = ""

    def write(self, s: str) -> int:
        return super().write(s)

    def flush(self) -> None:
        super().flush()
        self.current_line = self.getvalue().strip()
        self.truncate(0)
        self.seek(0)
        self.get_pbar().refresh()

    def display(self) -> None:
        pbar = self.get_pbar()
        content = self.current_line + "\r"
        if pbar and pbar.table:
            content += "\n\n"
            content += pbar.table.to_string() + "\n"
        sys.stdout.write(content)


class ProgressBarTable:
    def __init__(self, pbar: Optional[ProgressBar] = None):
        self.get_pbar = weakref.ref(pbar)
        self.data = {}

    def to_string(self) -> str:
        return tabulate(self.data, headers="keys", tablefmt="psql")

    def to_html(self) -> str:
        return pd.DataFrame(self.data).to_html(index=False)

    @property
    def container(self) -> str:
        return widgets.HTML(self.to_html())

    def add_row(self, row: dict) -> None:
        prow = {}
        for key, value in row.items():
            # make sure the data is json serializable
            with contextlib.suppress(TypeError):
                prow[key] = json.loads(json.dumps(value))
        num_rows = len(next(iter(self.data.values()))) if self.data else 0
        keys = set(self.data.keys()).union(row.keys())
        table_data: dict[str, List[Optional[Any]]] = {}
        for key in keys:
            value = row.get(key)
            if key in self.data:
                table_data[key] = self.data[key].copy()
            else:
                table_data[key] = [None] * num_rows
            table_data[key].append(value)
        self.data = table_data
        self.get_pbar().refresh()


class ProgressBar:
    def __init__(self, *args, **kwargs):
        nb = nb_tqdm in auto_tqdm.__bases__
        # nb = False
        # self.table: dict[str, List[Optional[Any]]] = {}
        self.table = ProgressBarTable(self)
        if nb:
            bound_tqdm = func.bind(nb_tqdm, *args, **kwargs)
            display = bound_tqdm.keywords.pop("display", False)
            bound_tqdm.keywords["display"] = None
            self.tqdm = bound_tqdm()
            container = widgets.VBox([
                self.tqdm.container,
                widgets.HTML("<hr>"),
                self.table.container,
            ])
            ipy_display(container)
        else:
            container = ProgressBarIO(self)
            display = kwargs.pop("display", False)
            kwargs.update({"file": container, "ascii": False})
            self.tqdm = std_tqdm(*args, **kwargs)
        self.container = container

    def __iter__(self) -> Iterable[Any]:
        return iter(self.tqdm)

    def update(self, n: Union[int, float] = 1) -> None:
        self.tqdm.update(n)

    def refresh(self) -> None:
        if isinstance(self.container, ProgressBarIO):
            self.container.display()
        else:
            self.container.children = [
                self.tqdm.container,
                widgets.HTML("<hr>"),
                self.table.container,
            ]

    def set_postfix(self, *args, **kwargs) -> None:
        self.tqdm.set_postfix(*args, **kwargs)

    def set_description_str(
        self, desc: Optional[str] = None, refresh: Optional[bool] = True
    ) -> None:
        self.tqdm.set_description_str(desc, refresh=refresh)

    def close(self) -> None:
        self.tqdm.close()
