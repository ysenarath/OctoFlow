import re
from pathlib import Path
from typing import Iterable, List, Optional, Union

from octoflow.tracking.run import Run, RunState
from octoflow.utils.validate import validator

__all__ = [
    "Experiment",
]


class Experiment:
    def __init__(self, path: Union[str, Path]) -> None:
        self.path = path

    @validator
    def path(self, value) -> Path:  # noqa: PLR6301
        path = Path(value)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def start_run(self, name: str):
        # creating a new run in the same experiment with same name will
        # increment the version run path will be something like
        # {self.path}/{name}-v{version}
        return Run(self.path, name)

    def get_run(
        self, name: str, version: Optional[int] = None
    ) -> Optional[Run]:
        try:
            return Run.from_existing(self.path, name, version=version)
        except FileNotFoundError:
            return None

    def search_runs(
        self,
        *,
        name_regex: Union[str, re.Pattern, None] = None,
        state: Union[
            RunState, str, Iterable[Union[RunState, str]], None
        ] = None,
        latest_only: bool = False,
    ) -> List[Run]:
        if state is None:
            state = [RunState.COMPLETED]
        if isinstance(state, (str, RunState)):
            state = [state]
        state = [RunState[s] if isinstance(s, str) else s for s in state]
        if name_regex is not None and not isinstance(name_regex, re.Pattern):
            name_regex = re.compile(name_regex)
        runs_args = set()
        for run_path in self.path.iterdir():
            try:
                path, name, version = Run.parse_path(run_path)
            except ValueError:
                continue
            if latest_only:
                version = None
            if name_regex is not None and not name_regex.match(name):
                continue
            runs_args.add((path, name, version))
        runs = []
        for path, name, version in runs_args:
            try:
                run = Run.from_existing(path, name, version=version)
            except FileNotFoundError:
                continue
            if run.state not in state:
                continue
            runs.append(run)
        return runs
