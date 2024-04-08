from pathlib import Path
from typing import Union

from git import Repo

from octoflow.tracking import (
    Experiment,
    Run,
    SQLAlchemyTrackingStore,
    TrackingClient,
)


def _init_path(path: Union[str, Path]) -> Path:
    path = Path(path)
    if not path.exists():
        path.mkdir(parents=True)
    # Create the project structure
    for folder in [
        "data",
        "logs",
        "notebooks",
        "src",
        "scripts",
        "tests",
    ]:
        (path / folder).mkdir(exist_ok=True)
    return path


def _start_run(run_path: Union[Path, str], experiment: Experiment, run: str):
    url = f"sqlite:///{run_path / 'tracking.db'}"
    store = SQLAlchemyTrackingStore(url)
    client = TrackingClient(store)
    experiment: Experiment = client.get_or_create_experiment(experiment)
    run: Run = experiment.start_run(run)
    return run


class Project:
    def __init__(self, path: Union[str, Path]) -> None:
        self.path = _init_path(path)
        repo = Repo.init(self.path, separate_git_dir=".octoflow")
        # change worktree to experiments
        print(repo.working_dir)
