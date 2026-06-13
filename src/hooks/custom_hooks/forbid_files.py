import fnmatch
import sys

import click

from src.hooks.hook_utils import run_hook_on_files

blocked_patters = [
    '.env',
    '.env.*',
    '.db',
    '.sqlite',
    '.sqlite3',
    '*.log',
]


def forbid_files(file_path: str) -> bool:
    for pattern in blocked_patters:
        if fnmatch.fnmatch(file_path, pattern):
            return True
    return False


@click.command()
@click.argument('files', nargs=-1, type=click.Path(), required=False)
def run(files) -> None:
    """Run the forbid_files hook on specified files.

    Args:
        files: File paths to process (from pre-commit or manual invocation)
    """
    return_code = run_hook_on_files(forbid_files, files)
    sys.exit(return_code)
