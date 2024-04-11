from __future__ import annotations

import contextvars
import weakref
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Generator, Optional, Set, Union

from git import Repo

from octoflow import logging
from octoflow.tracking import (
    Run,
    SQLAlchemyTrackingStore,
    TrackingClient,
)
from octoflow.utils.rsync import rsync

logger = logging.get_logger(__name__)


run_var = contextvars.ContextVar[Optional[Run]]("run", default=None)


class ProjectExperiment:
    def __init__(self, project: Project, expr_name: str) -> None:
        self.get_project = weakref.ref(project)
        self.expr_name = expr_name

    @property
    def project(self) -> Project:
        return self.get_project()

    @contextmanager
    def start_run(
        self,
        force: bool = False,
        description: Optional[str] = None,
    ) -> Generator[Run, None, None]:
        commit_hash = self.project.sync()
        tracking_uri_path = (
            self.project.base_path
            / "experiments"
            / self.expr_name
            / f"{commit_hash}.db"
        )
        if tracking_uri_path.exists():
            if not force:
                msg = f"experiment {self.expr_name} has already been run"
                raise FileExistsError(msg)
            tracking_uri_path.unlink()
        # create the parent directories
        tracking_uri_path.parent.mkdir(parents=True, exist_ok=True)
        tracking_uri = f"sqlite:///{tracking_uri_path}"
        store = SQLAlchemyTrackingStore(tracking_uri)
        client = TrackingClient(store)
        expr = client.get_or_create_experiment(self.expr_name)
        return expr.start_run(commit_hash, description=description)


class ProjectExperimentDict:
    def __init__(self, project: Project) -> None:
        self.get_project = weakref.ref(project)

    @property
    def project(self) -> Project:
        return self.get_project()

    @property
    def experiments_path(self) -> Path:
        exprs_dir = self.project.base_path / "experiments"
        exprs_dir.mkdir(exist_ok=True)
        return exprs_dir

    @property
    def names(self) -> Set[str]:
        return set(
            self.experiments_path.iterdir()
            if self.experiments_path.exists()
            else []
        )

    def __iter__(self):
        yield from self.names

    def __getitem__(self, key: str) -> ProjectExperiment:
        return ProjectExperiment(self.project, key)

    def __contains__(self, key: str) -> bool:
        return key in self.names

    def __len__(self) -> int:
        return len(self.names)

    def __repr__(self) -> str:
        return f"ExperimentDict({self.project.base_path})"


class Project:
    def __init__(self, path: Union[str, Path]) -> None:
        # Initialize the project path
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        # name of the project - different from name of experiment
        self.name = path.name
        self.base_path = path / ".octoflow"
        # Initialize the git repository
        repo = Repo.init(self.base_path / "project.git")
        # commit the initial changes to main if there are no commits
        has_no_commits = True
        with suppress(StopIteration, ValueError):
            next(repo.iter_commits())
            has_no_commits = False
        if has_no_commits:
            repo.index.add("*")
            repo.index.commit("Initialize project")
        self.repo = repo
        self.sync()

    def sync(self, message: Optional[str] = None) -> str:
        # use rsync to copy the project structure
        for cout in rsync(
            self.base_path.parent,
            self.base_path / "project.git",
            exclude=[
                ".git",
                ".gitignore",
                ".octoflow",
            ],
            append_dir=False,
        ):
            logger.info(cout)
        # Commit the changes to the experiment branch
        if "nothing to commit" in self.repo.git.status():
            return self.repo.git.rev_parse("HEAD")
        self.repo.index.add("*")
        if message is None:
            message = "Update project"
        self.repo.index.commit(message)
        return self.repo.git.rev_parse("HEAD")

    @property
    def experiments(self):
        return ProjectExperimentDict(self)
