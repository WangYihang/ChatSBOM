"""Syft installation and connection utilities."""
import shutil

import typer
from rich.console import Console
from rich.panel import Panel


def check_syft_installed(console: Console | None = None) -> bool:
    """
    Check if the 'syft' command is available in the system PATH.
    If not, print a user-friendly installation guide and exit.
    """
    console = console or Console()

    if shutil.which('syft'):
        return True

    console.print()
    console.print(
        Panel(
            '[bold]Syft Not Found[/]\n\n'
            'This command requires [bold blue]Syft[/] to generate SBOMs.\n'
            'Official Repository: [link=https://github.com/anchore/syft][blue]https://github.com/anchore/syft[/link]\n\n'
            'Please install it using one of the following methods:\n\n'
            '[bold]Option 1: Using Curl (Linux/macOS)[/]\n'
            '  [blue]curl -sSfL https://get.anchore.io/syft | sudo sh -s -- -b /usr/local/bin[/]\n\n'
            '[bold]Option 2: Using Homebrew (macOS)[/]\n'
            '  [blue]brew install syft[/]\n\n'
            '[bold]Option 3: Using Go[/]\n'
            '  [blue]go install github.com/anchore/syft/cmd/syft@latest[/]\n\n'
            'After installation, ensure [bold]syft[/] is in your [bold]PATH[/].',
            title='[bold red]Dependency Missing[/]',
            title_align='left',
            border_style='red',
            padding=(1, 2),
        ),
    )
    raise typer.Exit(1)
