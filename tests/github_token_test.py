"""A token's presence is not its validity."""
import io

import pytest
import requests
import typer
from rich.console import Console

from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


def test_missing_token_is_rejected():
    with pytest.raises(typer.Exit):
        check_github_token(None, console=Console(quiet=True))


def test_present_token_is_returned():
    assert check_github_token('tok', console=Console(quiet=True)) == 'tok'


def test_valid_token_reports_the_login():
    login = verify_github_token(
        'tok',
        fetch=lambda t: FakeResponse(200, {'login': 'octocat'}),
        console=Console(quiet=True),
    )
    assert login == 'octocat'


def test_expired_token_exits_with_a_specific_message():
    # quiet=True suppresses recording, so capture into a buffer instead.
    console = Console(file=io.StringIO(), record=True, width=100)
    with pytest.raises(typer.Exit):
        verify_github_token(
            'tok',
            fetch=lambda t: FakeResponse(401),
            console=console,
        )
    assert 'expired' in console.export_text().lower()


def test_insufficient_scope_exits():
    with pytest.raises(typer.Exit):
        verify_github_token(
            'tok',
            fetch=lambda t: FakeResponse(403),
            console=Console(quiet=True),
        )


def test_network_failure_does_not_block_the_run():
    """An unreachable API is not proof the token is bad."""
    def boom(token: str):
        raise requests.RequestException('no route to host')

    assert verify_github_token(
        'tok', fetch=boom, console=Console(quiet=True),
    ) is None
