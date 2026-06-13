import importlib
from typing import Any


def get_hook(hook_name: str) -> Any:
    """Dynamically load a hook module by name.

    Converts hook names (kebab-case) to module names (snake_case) and
    imports the corresponding hook module.

    Args:
        hook_name: Name of the hook
    Returns:
        The imported hook module
    """
    if not hook_name or not isinstance(hook_name, str):
        raise ValueError('Hook name must be a non-empty string')

    # Convert kebab-case to snake_case
    module_name = hook_name.replace('-', '_')

    try:
        hook_module = importlib.import_module(
            f'src.hooks.custom_hooks.{module_name}'
        )
        return hook_module
    except ImportError as e:
        raise ImportError(
            f"Hook '{hook_name}' not found. Could not import "
            f"'src.hooks.custom_hooks.{module_name}': {e}"
        ) from e


def run_hook_on_files(hook: str, files: list[str]) -> None:
    """Run a specified hook on a list of files.

    Args:
        hook: hook object to run
        files: List of file paths to process
    Returns:
        0 if all files passed the hook, 1 if any file failed
    """
    files = list(files)
    if any(hook(file) for file in files):
        return 1
    return 0
