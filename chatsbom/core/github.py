"""GitHub authentication and connection utilities."""
import typer
from rich.console import Console
from rich.panel import Panel


def check_github_token(token: str | None, console: Console | None = None) -> str:
    """
    Check if GitHub token is provided.
    If not, print a user-friendly error message and exit.
    """
    console = console or Console()

    if not token:
        console.print()
        console.print(
            Panel(
                '[bold]GitHub Token Missing[/]\n\n'
                'To use GitHub-related features, please provide a [bold blue]Personal Access Token[/].\n\n'
                '1. Create a token at: [link=https://github.com/settings/personal-access-tokens][blue]github.com/settings/personal-access-tokens[/link]\n'
                '2. Select [italic]Public repositories[/italic] under Repository access (no extra permissions needed).\n'
                '3. Set it as an environment variable:\n'
                '   [bold]export GITHUB_TOKEN=your_token_here[/]\n\n'
                'Alternatively, use the [bold]--token[/] command-line option.',
                title='[bold red]Error[/]',
                title_align='left',
                border_style='red',
                padding=(1, 2),
            ),
        )
        raise typer.Exit(1)

    return token
