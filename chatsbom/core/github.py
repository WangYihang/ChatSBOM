"""GitHub authentication and connection utilities."""
from collections.abc import Callable

import requests
import structlog
import typer
from rich.console import Console
from rich.panel import Panel

logger = structlog.get_logger('github_auth')

#: Fetches `GET /user` for a token. Injected so tests need no network.
TokenFetcher = Callable[[str], 'requests.Response']


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


def _fetch_user(token: str) -> requests.Response:
    return requests.get(
        'https://api.github.com/user',
        headers={
            'Authorization': f"Bearer {token}",
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'ChatSBOM',
        },
        timeout=10,
    )


def verify_github_token(
    token: str,
    fetch: TokenFetcher = _fetch_user,
    console: Console | None = None,
) -> str | None:
    """Check that a token actually works, returning the login it belongs to.

    `check_github_token` only proves a string is non-empty. An expired
    token passes that and then fails deep inside collection with a bare
    401, which the rate-limit handling does not recognise. Returns None
    when the API could not be reached — that is not evidence the token is
    bad, so it must not stop the run.
    """
    console = console or Console()

    try:
        response = fetch(token)
    except requests.RequestException as e:
        logger.warning('Could not verify GitHub token', error=str(e))
        return None

    if response.status_code == 200:
        login = str(response.json().get('login') or '')
        logger.info('GitHub token verified', login=login)
        return login

    if response.status_code == 401:
        console.print(
            Panel(
                '[bold]GitHub Token Invalid or Expired[/]\n\n'
                'The API rejected this token with [bold]401 Unauthorized[/].\n\n'
                'Create a new one at '
                '[link=https://github.com/settings/personal-access-tokens]'
                '[blue]github.com/settings/personal-access-tokens[/link] '
                'and update [bold]GITHUB_TOKEN[/].',
                title='[bold red]Error[/]',
                title_align='left',
                border_style='red',
                padding=(1, 2),
            ),
        )
        raise typer.Exit(1)

    if response.status_code == 403:
        console.print(
            Panel(
                '[bold]GitHub Token Lacks Required Access[/]\n\n'
                'The API returned [bold]403 Forbidden[/] for [cyan]GET /user[/].\n\n'
                'Grant the token [italic]Public repositories[/italic] read access.',
                title='[bold red]Error[/]',
                title_align='left',
                border_style='red',
                padding=(1, 2),
            ),
        )
        raise typer.Exit(1)

    logger.warning(
        'Unexpected response verifying GitHub token',
        status=response.status_code,
    )
    return None
