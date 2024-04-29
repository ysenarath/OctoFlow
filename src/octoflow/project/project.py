from __future__ import annotations

import weakref
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Generator, Mapping, Optional, Set, Union

from git import Repo

from octoflow import logging
from octoflow.tracking import (
    Run,
    SQLAlchemyTrackingStore,
    TrackingClient,
)
from octoflow.utils.rsync import rsync

logger = logging.get_logger(__name__)


class ProjectExperiment:
    def __init__(self, project: Project, expr_name: str) -> None:
        self.get_project = weakref.ref(project)
        self.expr_name = expr_name

    @property
    def project(self) -> Project:
        return self.get_project()

    def start_run(
        self,
        force: bool = False,
        description: Optional[str] = None,
    ) -> Run:
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


class ProjectExperimentDict(Mapping[str, ProjectExperiment]):
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
        if self.experiments_path.exists():
            set(self.experiments_path.iterdir())
        return set()

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

    def first(self) -> ProjectExperiment:
        try:
            return next(iter(self.values()))
        except StopIteration:
            msg = "no experiments found"
            raise KeyError(msg) from None


def update_project_gitgnore(path: Path) -> None:
    gitignore_path = path / ".gitignore"
    if not gitignore_path.exists():
        gitignore_path.write_text("# octoflow\n.octoflow\n")
    else:
        gitignore_text = gitignore_path.read_text().rstrip()
        if "# octoflow" in gitignore_text:
            return
        gitignore_text += "\n# octoflow\n.octoflow\n"
        gitignore_path.write_text(gitignore_text.strip())


class Project:
    def __init__(self, path: Union[str, Path]) -> None:
        path = Path(path)
        self.name = path.name
        self.base_path = path / ".octoflow"
        # update the gitignore file
        update_project_gitgnore(path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.sync()

    @contextmanager
    def get_repo(self) -> Generator[Repo, None, None]:
        # Initialize the git repository
        repo = Repo.init(self.base_path / "project")
        # commit the initial changes to main if there are no commits
        has_no_commits = True
        with suppress(StopIteration, ValueError):
            next(repo.iter_commits())
            has_no_commits = False
        if has_no_commits:
            repo.index.add("*")
            repo.index.commit("Initialize project")
        # close the repo to avoid memory leaks
        yield repo
        repo.close()

    def sync(self, message: Optional[str] = None) -> str:
        # use rsync to copy the project structure
        for cout in rsync(
            self.base_path.parent,
            self.base_path / "project",
            exclude=[
                ".git",
                ".gitignore",
                ".octoflow",
            ],
            append_dir=False,
        ):
            logger.info(cout)
        with self.get_repo() as repo:
            # Commit the changes to the experiment branch
            if "nothing to commit" in repo.git.status():
                commit_hash = repo.git.rev_parse("HEAD")
            else:
                repo.index.add("*")
                if message is None:
                    message = "Update project"
                repo.index.commit(message)
            commit_hash = repo.git.rev_parse("HEAD")
        return commit_hash

    @property
    def experiments(self):
        return ProjectExperimentDict(self)
