"""A GitHub token, as the research tools send one (#113).

The core's commands checked their tokens the same way until the
collector replaced them (#171): it cleans its own
(`collector/settings.py`), and this is the research tools' alone.
"""
import unicodedata

import structlog
from rich.console import Console
from rich.console import Group
from rich.panel import Panel
from rich.text import Text

from chatsbom.core.diagnostics import fail

logger = structlog.get_logger('github_auth')


def clean_github_token(
    token: str | None, console: Console | None = None,
) -> str | None:
    """`token` as a header may carry it: without the whitespace around
    it, and None when that leaves nothing.

    A token read from a file ends with the file's line ending when what
    read it kept it: `$(cat token)` of a file saved on Windows keeps its
    carriage return, and a secret file read whole ends with a newline.
    requests refused the header then, with an `InvalidHeader` quoting
    it, token and all, and the log printed that (#113).

    A control character still in it, inside, where stripping does not
    reach, stops the command: no GitHub token holds one, and sent, it is
    refused by requests, by GitHub or by a proxy on the way. What is
    said, on stderr or as an event, names the character and where it
    is, but never the token.
    """
    if token is None:
        return None
    token = token.strip()
    for position, character in enumerate(token, start=1):
        if unicodedata.category(character) == 'Cc':
            # Where the logs go, and one event when they are JSON: stdout
            # is for what a command prints, and this was printed there
            # (#124). After an empty line, as before.
            fail(
                Group(
                    Text(),
                    Panel(
                        '[bold]GitHub Token Malformed[/]\n\n'
                        'The token holds a control character, '
                        f'[bold]U+{ord(character):04X}[/] at character '
                        f'{position}, which no GitHub token holds. The '
                        'whitespace around a token is left out; this is '
                        'inside it.\n\n'
                        'Copy the token again, and set it:\n'
                        '   [bold]export GITHUB_TOKEN=your_token_here[/]',
                        title='[bold red]Error[/]',
                        title_align='left',
                        border_style='red',
                        padding=(1, 2),
                    ),
                ),
                'GitHub token malformed', logger, console=console,
                character=f'U+{ord(character):04X}', position=position,
            )
    return token or None
