"""GitHub authentication and connection utilities."""
import re
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


#: Where more GitHub tokens for the dependency-graph stage come from,
#: beside `GITHUB_TOKEN`: comma- or whitespace-separated. Each token is
#: metered by GitHub on its own, so each one added is another stage
#: worker, paced within the depgraph limit, running in parallel.
DEPGRAPH_TOKENS_ENV = 'CHATSBOM_DEPGRAPH_TOKENS'


def depgraph_tokens(primary: str | None, extra: str | None) -> list[str]:
    """The tokens the dependency-graph stage may use, each once.

    `primary` first — `--token`, or `GITHUB_TOKEN` — then every token in
    `extra`, the value of `CHATSBOM_DEPGRAPH_TOKENS`. A token listed
    twice is one token: GitHub meters the token, not the listing, and
    two workers on one would only be refused twice as fast.
    """
    tokens: list[str] = []
    for token in [primary or '', *re.split(r'[\s,]+', extra or '')]:
        token = token.strip()
        if token and token not in tokens:
            tokens.append(token)
    return tokens


def token_label(position: int, login: str | None) -> str:
    """How a token is named in logs and summaries: never its value."""
    return f'token {position}' + (f' ({login})' if login else '')


def verify_extra_token(
    token: str,
    fetch: TokenFetcher = _fetch_user,
) -> tuple[bool, str | None]:
    """`(usable, login)` for a token beside the primary one.

    Unlike `verify_github_token`, a token GitHub rejects does not stop
    the command: the others can still do the work, and the caller says
    which one was dropped. Unreachable is usable, as there.
    """
    try:
        response = fetch(token)
    except requests.RequestException as e:
        logger.warning('Could not verify a depgraph token', error=str(e))
        return True, None
    if response.status_code == 200:
        return True, str(response.json().get('login') or '') or None
    return False, None
