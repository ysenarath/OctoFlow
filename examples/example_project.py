import shutil

from octoflow.project.base import Project

shutil.rmtree("./examples/example_project_folder", ignore_errors=True)


def main():
    project = Project("./examples/example_project_folder")


if __name__ == "__main__":
    main()
