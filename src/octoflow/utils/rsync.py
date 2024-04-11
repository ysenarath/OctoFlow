import shlex
import shutil
import time
from pathlib import Path
from subprocess import PIPE, Popen  # noqa: S404
from tempfile import NamedTemporaryFile
from typing import Generator, Optional

__all__ = [
    "rsync",
]


class RSyncError(Exception):
    pass


def rsync(
    src: Path,
    dest: Path,
    exclude: Optional[list[str]] = None,
    ignore_errors: Optional[bool] = False,
    append_dir: Optional[bool] = False,
) -> Generator[str, None, None]:
    src = Path(src) if isinstance(src, str) else src
    dest = Path(dest) if isinstance(dest, str) else dest
    exclude = [] if exclude is None else exclude
    exclude = " ".join([f"--exclude={e}" for e in exclude])
    rsync_path = shutil.which("rsync")
    src = src if append_dir else f"{src}/"
    shell_cmd = f"{rsync_path} -av {exclude} {src} {dest}"
    shell_cmd_parts = shlex.split(shell_cmd)
    with NamedTemporaryFile() as out, NamedTemporaryFile() as err:
        proc = Popen(
            shell_cmd_parts,  # noqa: S603
            stdout=out,
            stderr=err,
            stdin=PIPE,
        )
        # block until the process is finished
        stdout, stderr, stdout_lines = "", "", []
        with open(out.name, encoding="utf-8") as fout, open(
            err.name, encoding="utf-8"
        ) as ferr:
            while proc.poll() is None:
                stdout += fout.read()
                stdout_lines_current = stdout.split("\n")
                stdout_lines += stdout_lines_current[:-1]
                stdout = stdout_lines_current[-1]
                stderr += ferr.read()
                if len(stdout_lines_current) > 0:
                    yield from stdout_lines_current[:-1]
                time.sleep(0.01)
            stdout += fout.read()
            stdout_lines_current = stdout.split("\n")
            yield from stdout_lines_current
            stderr += ferr.read()
        if len(stderr.strip()) != 0 and not ignore_errors:
            raise RSyncError(stderr)
