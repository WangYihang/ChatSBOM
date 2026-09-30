"""GitHub tokens, as the collector holds them: named, and never shown.

A token is named by its place, `token 1` for GITHUB_TOKEN and `token 2`
and on for the others, as `core/github.token_label` names them. The name
is what a log line, an error or a repr says; the secret is sent in a
header and nowhere else.

What is said of a request can hold its token anyway: an error about a
header quotes the header (#113), and anything else might. `scrub` takes
every token the collector holds out of a text, whatever surrounds it,
before `redact` takes out the credentials and signed URLs it can
recognise.
"""
from collections.abc import Iterable

from chatsbom.core.redact import redact
from chatsbom.core.redact import REDACTED


class Token:
    """A token and its name."""

    __slots__ = ('label', '_secret')

    def __init__(self, label: str, secret: str) -> None:
        self.label = label
        self._secret = secret

    @property
    def secret(self) -> str:
        """What the `Authorization` header carries, and nothing else."""
        return self._secret

    def __repr__(self) -> str:
        return f'Token({self.label!r})'

    def __str__(self) -> str:
        return self.label

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Token) and other.label == self.label
            and other.secret == self.secret
        )

    def __hash__(self) -> int:
        return hash(self.label)


def scrub(text: str, tokens: Iterable[Token]) -> str:
    """`text` without any of `tokens`, and as `redact` leaves it. The
    longest first: one that begins another would leave the rest of it."""
    for secret in sorted((t.secret for t in tokens), key=len, reverse=True):
        if secret:
            text = text.replace(secret, REDACTED)
    return redact(text)
