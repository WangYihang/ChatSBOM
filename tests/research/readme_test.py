"""`readme`, and the README fetch it shares with `classify` (#167).

The fetch was the pipeline's GitHub client's, and moved with the
research tools with what it needs of that client, so that the rest of it
can go with the pipeline (#155, 6e): a README is asked for raw, kept on
disk, and not asked for again; and a rate limit is waited out. Nothing
here reaches GitHub: the session the client makes is a stand-in.
"""
import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.research.__main__ import app
from chatsbom.research.services import github_service
from chatsbom.research.services.github_service import GitHubService

runner = CliRunner()

API = 'https://api.github.com'
RAW = 'application/vnd.github.v3.raw'


class Response:
    def __init__(
        self, status_code: int, text: str = '',
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class GitHub:
    """GitHub's REST API, as the session the client sends through: each
    repository's README from `readmes`, 404 for any other, and first
    whatever `answers` holds. What it was sent is in `sent`, as
    (method, URL, Accept)."""

    def __init__(self, readmes: dict[str, str]) -> None:
        self.readmes = readmes
        self.answers: list[Response] = []
        self.sent: list[tuple[str, str, str | None]] = []
        self.headers: dict[str, str] = {}

    def request(self, method: str, url: str, **kwargs: Any) -> Response:
        accept = kwargs.get('headers', {}).get('Accept')
        self.sent.append((method, url, accept))
        if self.answers:
            return self.answers.pop(0)
        name = url.removeprefix(f'{API}/repos/').removesuffix('/readme')
        if name in self.readmes:
            return Response(200, self.readmes[name])
        return Response(404, '{"message": "Not Found"}')

    def get(self, url: str, **kwargs: Any) -> Response:
        return self.request('GET', url, **kwargs)


@pytest.fixture
def api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GitHub:
    """The stand-in, as the session every client is made with, in a
    working directory of its own, where `.cache` is."""
    monkeypatch.chdir(tmp_path)
    stand_in = GitHub({'acme/shop': '# Shop\n'})
    monkeypatch.setattr(
        github_service, 'get_http_client', lambda **_: stand_in,
    )
    return stand_in


def kept(root: Path, owner: str, repo: str) -> Path:
    """Where a README is kept, as `readme` and `classify` look for it."""
    return (
        root / '.cache' / 'github-readme' / owner / repo / 'default'
        / 'default' / 'readme.md'
    )


def test_a_readme_is_asked_for_raw_and_kept(api, tmp_path):
    readme = GitHubService(token='ghp_t').get_readme('acme', 'shop')

    assert readme == '# Shop\n'
    assert api.sent == [('GET', f'{API}/repos/acme/shop/readme', RAW)]
    assert api.headers['Authorization'] == 'Bearer ghp_t'
    assert kept(tmp_path, 'acme', 'shop').read_text() == '# Shop\n'


def test_a_kept_readme_is_not_asked_for_again(api):
    service = GitHubService(token='ghp_t')
    service.get_readme('acme', 'shop')

    assert service.get_readme('acme', 'shop') == '# Shop\n'
    assert len(api.sent) == 1


def test_a_repository_without_one_has_none(api, tmp_path):
    assert GitHubService(token='ghp_t').get_readme('acme', 'bare') is None
    assert not kept(tmp_path, 'acme', 'bare').exists()


def test_a_rate_limit_is_waited_out(api, monkeypatch):
    waited: list[float] = []
    monkeypatch.setattr(github_service.time, 'sleep', waited.append)
    api.answers.append(Response(429, headers={'Retry-After': '7'}))

    assert GitHubService(token='ghp_t').get_readme('acme', 'shop') == (
        '# Shop\n'
    )
    assert waited == [8.0]
    assert len(api.sent) == 2


def test_readme_downloads_each_and_skips_what_it_has(api, tmp_path):
    listing = tmp_path / 'repos.jsonl'
    listing.write_text(
        ''.join(
            json.dumps({'id': i, 'owner': 'acme', 'name': name}) + '\n'
            for i, name in enumerate(('shop', 'bare', 'kept'), start=1)
        ),
        encoding='utf-8',
    )
    had = kept(tmp_path, 'acme', 'kept')
    had.parent.mkdir(parents=True)
    had.write_text('# Kept\n')

    result = runner.invoke(app, ['readme', '--input', str(listing)])

    assert result.exit_code == 0, result.output
    said = ' '.join(result.output.split())
    assert 'Newly downloaded: 1' in said
    assert 'Already cached: 1' in said
    assert 'Failed/Missing: 1' in said
    assert [url for _, url, _ in api.sent] == [
        f'{API}/repos/acme/shop/readme', f'{API}/repos/acme/bare/readme',
    ]
    assert kept(tmp_path, 'acme', 'shop').read_text() == '# Shop\n'
    assert had.read_text() == '# Kept\n'
