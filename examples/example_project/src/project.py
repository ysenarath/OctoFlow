from pathlib import Path

from octoflow.project import Project

pwd = Path(__file__).parent

proj = Project(pwd)

expr = proj.experiments["experiment#1"]

run = expr.start_run(force=True)
