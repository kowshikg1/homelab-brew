"""Dynamic hook execution framework using Click."""

import click

from src.hooks.hook_utils import get_hook


class CommandResolver(click.Group):
    """Resolves and executes hooks by name at runtime.

    This class acts as a command dispatcher that loads hook modules
    on-demand based on the provided hook name. Each hook module must
    export a `run` function decorated with Click.
    """

    def get_command(
        self, ctx: click.Context | None, hook_name: str
    ) -> click.Command | None:
        """Load and return a hook's command handler.

        Args:
            ctx: Click context
            hook_name: Name of the hook to load

        Returns:
            The click.Command from the hook module, or None if not found
        """
        hook_module = get_hook(hook_name)
        if not hasattr(hook_module, 'run'):
            raise ValueError(
                f"Hook '{hook_name}' does not have a 'run' function"
            )
        return hook_module.run


@click.command(cls=CommandResolver, invoke_without_command=True)
@click.pass_context
def run(ctx: click.Context) -> None:
    """Execute repository hooks by name.

    Dynamically loads and invokes custom hooks defined in the hooks directory.
    Each hook is a module with a Click-decorated run() function.
    """
    if ctx.invoked_subcommand is None:
        click.echo('Usage: run-hook HOOK_NAME [ARGS]...')
        click.echo('Use run-hook --help for more information')


if __name__ == '__main__':
    run()
