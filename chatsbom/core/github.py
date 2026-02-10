"""GitHub authentication and connection utilities."""
import typer
from rich.console import Console


def check_github_token(token: str | None, console: Console | None = None) -> str:
    """
    Check if GitHub token is provided.
    If not, print a user-friendly error message and exit.
    """
    console = console or Console()

    if not token:
        console.print('[bold red]Error:[/] GitHub Token is missing!\n')
        console.print(
            'To use GitHub-related commands, you need to provide a GitHub Personal Access Token.\n',
        )
        console.print(
            'You can create one here: [blue]https://github.com/settings/tokens[/]\n',
        )
        console.print('Then, set it as an environment variable:\n')
        console.print('  [bold]export GITHUB_TOKEN=your_token_here[/]\n')
        console.print('Or pass it using the [bold]--token[/] option.\n')
        raise typer.Exit(1)

    return token
