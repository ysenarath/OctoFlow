import os
import shutil
from pathlib import Path

import click

from octoflow.project import Project

curr_path = Path(os.path.dirname(os.path.abspath(__file__)))
project_path = curr_path / "project"


@click.group()
def cli():
    pass


@cli.command("clean")
def cli_clean():
    shutil.rmtree(project_path, ignore_errors=True)


@cli.command()
@click.option("--clean", is_flag=True)
def test(clean: bool):
    if clean:
        shutil.rmtree(project_path, ignore_errors=True)
    Project(project_path)


@cli.command()
@click.option("--clean", is_flag=True)
def start_run(clean: bool):
    if clean:
        shutil.rmtree(project_path, ignore_errors=True)
    project = Project(project_path)
    expr = project.experiments["sentiment-analysis"]
    with expr.start_run() as run:
        print(run)


if __name__ == "__main__":
    # run with python examples/example_project.py start_run
    cli()
