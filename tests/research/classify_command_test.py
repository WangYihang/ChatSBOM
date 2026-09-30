"""`chatsbom-research classify`, which was `github classify`, run as a
person runs it (#47, #167).

It crashed before classifying anything when `OPENAI_API_KEY` was set and
`OPENAI_BASE_URL` was not: the CLI passed its unset URL on as `None`,
over the service's default, and the service lowercased it. Its default
model, `deepseek-chat`, was asked of OpenAI's API, the default address.
Its prompt offered a category the schema has no value for, so every
answer that took it failed validation and was retried, at up to four
paid calls a repository. And it read `01-github-search/all.jsonl` unless
told otherwise, which `github search` has not written since the search
snapshots were dated.

No request leaves the machine: the classification itself is stubbed,
and every connection refused.
"""
import json
import re
import socket
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.main import get_command
from typer.testing import CliRunner

from chatsbom.core.config import ChatSBOMConfig
from chatsbom.core.config import PathConfig
from chatsbom.models.repository import Repository
from chatsbom.research.__main__ import app
from chatsbom.research.models.analysis import RepoAnalysis
from chatsbom.research.models.analysis import RepoCategory
from chatsbom.research.models.analysis import RepoClassification
from chatsbom.research.services.github_analysis_service import (
    GitHubAnalysisService,
)
from tests.snapshot.conftest import artifact
from tests.snapshot.conftest import at
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import repository
from tests.snapshot.conftest import warehouse

runner = CliRunner()

OPENAI = 'https://api.openai.com/v1'

CLASSIFIED = RepoClassification.model_validate({
    'category': RepoCategory.WEB_APP,
    'description': {'en': 'A shop', 'zh': '商店'},
    'tags': ['shop'],
    'reasoning': '卖东西',
})


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The command's relative paths, `data/`, `.cache/` and the
    requests cache, all land here; and no connection gets anywhere."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    monkeypatch.delenv('OPENAI_BASE_URL', raising=False)
    config = ChatSBOMConfig(paths=PathConfig(base_data_dir=tmp_path / 'data'))
    monkeypatch.setattr(
        'chatsbom.research.commands.classify.get_config', lambda: config,
    )

    def refuse(self: socket.socket, address: object, *args: object) -> None:
        raise OSError(f'no connections in this test: {address}')

    monkeypatch.setattr(socket.socket, 'connect', refuse)
    return tmp_path


@pytest.fixture
def asked(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    """What each classification was asked of: the endpoint and model the
    service was made with. The service itself is real; only the call to
    the model is stubbed."""
    seen: list[tuple[str, str]] = []

    def analyze(self, repo, github_service):
        seen.append((self.base_url, self.model))
        # A copy each: the command sets the framework it read on the
        # classification it is handed, as on one the model returned.
        return RepoAnalysis.from_repository(
            repo, CLASSIFIED.model_copy(deep=True),
        )

    monkeypatch.setattr(GitHubAnalysisService, 'analyze_repo', analyze)
    return seen


def repositories(path: Path, *ids: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        ''.join(
            json.dumps({'id': i, 'owner': 'shop', 'name': f'app{i}'}) + '\n'
            for i in ids
        ),
        encoding='utf-8',
    )
    return path


def classify(*argv: str):
    return runner.invoke(app, ['classify', *argv])


def results(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


# --- the endpoint and the model ----------------------------------------------

def test_an_openai_key_alone_is_enough(workdir, asked, monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')
    listing = repositories(workdir / 'repos.jsonl', 1)
    out = workdir / 'out.jsonl'

    result = classify('--input', str(listing), '--output', str(out))

    assert result.exit_code == 0, result.output
    [(endpoint, model)] = asked
    assert endpoint == OPENAI
    assert [row['id'] for row in results(out)] == [1]


def test_the_default_model_is_one_the_default_endpoint_serves(
    workdir, asked, monkeypatch,
):
    """Asked of OpenAI's API, a DeepSeek model is not found."""
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')
    listing = repositories(workdir / 'repos.jsonl', 1)

    classify('--input', str(listing), '--output', str(workdir / 'o.jsonl'))

    [(endpoint, model)] = asked
    assert endpoint == OPENAI
    assert model.startswith('gpt-'), model


def test_the_command_and_the_service_default_alike():
    """One default each, so the two cannot disagree again."""
    from chatsbom.research.commands import classify as command
    from chatsbom.research.services import github_analysis_service as service

    params = {p.name: p for p in get_command(command.app).params}
    assert params['base_url'].default == service.DEFAULT_BASE_URL == OPENAI
    assert params['model'].default == service.DEFAULT_MODEL


def test_another_endpoint_is_asked_with_the_model_given(
    workdir, asked, monkeypatch,
):
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')
    monkeypatch.setenv('OPENAI_BASE_URL', 'https://api.deepseek.com/v1')
    listing = repositories(workdir / 'repos.jsonl', 1)

    result = classify(
        '--input', str(listing), '--output', str(workdir / 'o.jsonl'),
        '--model', 'deepseek-chat',
    )

    assert result.exit_code == 0, result.output
    assert asked == [('https://api.deepseek.com/v1', 'deepseek-chat')]


def test_openai_is_not_asked_without_a_key(workdir, asked):
    listing = repositories(workdir / 'repos.jsonl', 1)

    result = classify('--input', str(listing))

    assert result.exit_code == 1
    assert 'OPENAI_API_KEY' in result.output
    assert asked == []


def test_a_local_endpoint_needs_no_key(workdir, asked, monkeypatch):
    """Ollama's, for one, as `.env.example` suggests."""
    monkeypatch.setenv('OPENAI_BASE_URL', 'http://localhost:11434/v1')
    listing = repositories(workdir / 'repos.jsonl', 1)

    result = classify(
        '--input', str(listing), '--output', str(workdir / 'o.jsonl'),
        '--model', 'llama3',
    )

    assert result.exit_code == 0, result.output
    assert asked == [('http://localhost:11434/v1', 'llama3')]


# --- the prompt ----------------------------------------------------------------

class Completions:
    """`client.chat.completions` as instructor wraps it: records what it
    is sent, and answers with a classification."""

    def __init__(self) -> None:
        self.sent: list[list[dict]] = []

    def create(self, **request):
        self.sent.append(request['messages'])
        return CLASSIFIED


def test_the_prompt_offers_exactly_the_categories_the_schema_accepts(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    service = GitHubAnalysisService(api_key='sk-test')
    completions = Completions()
    service.client = SimpleNamespace(
        chat=SimpleNamespace(completions=completions),
    )
    github = SimpleNamespace(
        config=ChatSBOMConfig(), get_readme=lambda owner, repo: 'A shop.',
    )
    repo = Repository.model_validate({'id': 1, 'owner': 'shop', 'name': 'app'})

    assert service.analyze_repo(repo, github) is not None

    [[system, _]] = completions.sent
    offered = re.findall(r'^- (.+?): ', system['content'], re.MULTILINE)
    assert offered == [category.value for category in RepoCategory]


# --- the input -----------------------------------------------------------------

def test_the_default_input_is_the_newest_search_snapshot(
    workdir, asked, monkeypatch,
):
    """The collector's universe writes `all-<date>.jsonl`, as `github
    search` did, and the newest is the corpus (`core/catalog.py`)."""
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')
    paths = PathConfig(base_data_dir=workdir / 'data')
    repositories(paths.search_snapshot(date(2026, 3, 9)), 1)
    repositories(paths.search_snapshot(date(2026, 9, 1)), 2, 3)
    # A per-language list, which is not the corpus.
    repositories(paths.search_dir / 'go.jsonl', 4)
    out = workdir / 'out.jsonl'

    result = classify('--output', str(out))

    assert result.exit_code == 0, result.output
    assert sorted(row['id'] for row in results(out)) == [2, 3]


def test_without_a_snapshot_it_says_how_to_make_one(workdir, monkeypatch):
    """The collector's universe writes one: `github search`, which it
    named, went with the old pipeline (#171)."""
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')

    result = classify()

    assert result.exit_code == 1
    said = ' '.join(result.output.split())
    assert '`chatsbom collect`' in said
    assert 'github search' not in said
    assert '--input' in said


# --- the frameworks, from the warehouse ----------------------------------------

def test_the_framework_is_the_current_scans_as_the_warehouse_has_it(
    workdir, asked, monkeypatch,
):
    """A repository's framework, and its version, as its current scan
    has them: an older scan's version is one it moved off. Asked of the
    ClickHouse server until #153, and of the warehouse now,
    data/warehouse.duckdb."""
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')
    listing = repositories(workdir / 'repos.jsonl', 1, 2)
    (workdir / 'data').mkdir(exist_ok=True)
    warehouse(
        workdir / 'data' / 'warehouse.duckdb',
        Corpus(
            repositories=[
                repository(1, 'shop', 'app1', 100, 'Python'),
                repository(2, 'shop', 'app2', 100, 'Python'),
            ],
            artifacts=[
                artifact(
                    1, 'flask', '2.3.0', 'python',
                    observed_at=at(2026, 1, 15), commit='a' * 40,
                ),
                artifact(
                    1, 'flask', '3.0.0', 'python',
                    observed_at=at(2026, 9, 14), commit='b' * 40,
                ),
            ],
        ),
    )
    out = workdir / 'out.jsonl'

    result = classify('--input', str(listing), '--output', str(out))

    assert result.exit_code == 0, result.output
    assert {
        row['id']: (row['framework'], row['framework_version'])
        for row in results(out)
    } == {1: ('flask', '3.0.0'), 2: ('', '')}


def test_without_a_warehouse_each_is_classified_without_its_framework(
    workdir, asked, monkeypatch,
):
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')
    listing = repositories(workdir / 'repos.jsonl', 1)
    out = workdir / 'out.jsonl'

    result = classify('--input', str(listing), '--output', str(out))

    assert result.exit_code == 0, result.output
    assert [
        (row['id'], row['framework']) for row in results(out)
    ] == [(1, '')]
