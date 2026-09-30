"""What can go wrong with a request to GitHub, as the collector acts on
it (#156): one of four.

- `NotFound`: 404. Deleted, made private, or never there.
- `Gone`: gone or moved. A redirect, which a renamed or transferred
  repository is answered with by its old name, says where (`moved_to`);
  410, 451, and a 403 that says the repository is blocked, do not.
- `RateLimited`: no token had room in the bucket within the wait the
  caller allowed. A refusal is not one of these by itself: the client
  backs the bucket off and asks again, with another token or later.
- `Failed`: anything else. A transport that failed, a 5xx, a 4xx no
  other kind covers, a body that is not what was asked for, and
  `Unauthorized`: every token refused, 401.

The text of each names the request, its method and URL without a query
it could be fetched with, and what GitHub said, with no token in it
(`tokens.scrub`).
"""
from datetime import datetime


class GitHubError(Exception):
    """A request to GitHub that did not give what was asked."""

    def __init__(self, message: str, *, status: int = 0, url: str = '') -> None:
        super().__init__(message)
        #: The answer's status; 0 when there was none.
        self.status = status
        #: The request's URL, as a log shows one.
        self.url = url


class NotFound(GitHubError):
    """404."""


class Gone(GitHubError):
    """Gone, or moved, and then `moved_to` says where."""

    def __init__(
        self, message: str, *, status: int = 0, url: str = '',
        moved_to: str | None = None,
    ) -> None:
        super().__init__(message, status=status, url=url)
        self.moved_to = moved_to


class RateLimited(GitHubError):
    """No token had room in `bucket` in time. `until` is when one is
    expected to, when that is known."""

    def __init__(
        self, message: str, *, bucket: str, until: datetime | None,
        status: int = 0, url: str = '',
    ) -> None:
        super().__init__(message, status=status, url=url)
        self.bucket = bucket
        self.until = until


class Failed(GitHubError):
    """Anything else."""


class Unauthorized(Failed):
    """Every token was refused: GitHub answered each with 401."""
