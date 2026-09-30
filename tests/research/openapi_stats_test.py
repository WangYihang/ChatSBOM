"""`openapi stats` measures repositories against eleven context windows.

It asked litellm for them, and imported it for nothing else: importing
litellm fetched its model price list over the network, unless told not
to, and loaded a `.env` of its own, found by walking up from where it is
installed (#26).
"""
import os
import sys
from pathlib import Path

import pytest

from chatsbom.research.commands.openapi import stats

#: What `get_context_windows` returned from litellm 1.81.16, offline
#: (LITELLM_LOCAL_MODEL_COST_MAP=True): each model's `max_input_tokens`,
#: and nine tenths of it, the most a repository may hold to fit.
LITELLM = {
    'GPT-5': {
        'limit': 115200, 'full_limit': 128000,
        'display': 'GPT-5 (128k)',
    },
    'Opus-4.6': {
        'limit': 900000, 'full_limit': 1000000,
        'display': 'Opus-4.6 (1000k)',
    },
    'Opus-4.5': {
        'limit': 180000, 'full_limit': 200000,
        'display': 'Opus-4.5 (200k)',
    },
    'Gemini-3.1-Pro': {
        'limit': 943718, 'full_limit': 1048576,
        'display': 'Gemini-3.1-Pro (1048k)',
    },
    'Gemini-3.0-Pro': {
        'limit': 943718, 'full_limit': 1048576,
        'display': 'Gemini-3.0-Pro (1048k)',
    },
    'DeepSeek-V3.2': {
        'limit': 147456, 'full_limit': 163840,
        'display': 'DeepSeek-V3.2 (163k)',
    },
    'DeepSeek-R1': {
        'limit': 58982, 'full_limit': 65536,
        'display': 'DeepSeek-R1 (65k)',
    },
    'Llama-4-Scout': {
        'limit': 117964, 'full_limit': 131072,
        'display': 'Llama-4-Scout (131k)',
    },
    'Qwen-3': {
        'limit': 115200, 'full_limit': 128000,
        'display': 'Qwen-3 (128k)',
    },
    'GLM-4.7': {
        'limit': 180000, 'full_limit': 200000,
        'display': 'GLM-4.7 (200k)',
    },
    'Kimi-k2.5': {
        'limit': 235929, 'full_limit': 262144,
        'display': 'Kimi-k2.5 (262k)',
    },
}


def test_the_context_windows_are_the_ones_litellm_gave():
    assert stats.get_context_windows() == LITELLM


def test_the_context_windows_need_no_litellm(monkeypatch):
    """As if it were not installed: nothing else used it."""
    monkeypatch.setitem(sys.modules, 'litellm', None)

    assert stats.get_context_windows() == LITELLM


# --- what is counted (#47) --------------------------------------------------
#
# The directories that are never source (`tests`, `vendor`, `tmp`, ...)
# were looked for in every part of a file's path, the repository's own
# location included: a repository under `/tmp`, or one called `examples`,
# counted nothing at all. Files over a megabyte counted nothing either,
# and so did a file that mentions one of the tokenizer's special tokens,
# which it refused to encode.


class Words:
    """A tokenizer that counts words, and refuses a special token unless
    told to take it as text, as tiktoken's `encode` does by default."""

    SPECIAL = '<|endoftext|>'

    def __init__(self) -> None:
        self.pieces: list[int] = []

    def encode(self, text: str) -> list[str]:
        if self.SPECIAL in text:
            raise ValueError(f'disallowed special token {self.SPECIAL}')
        return self.encode_ordinary(text)

    def encode_ordinary(self, text: str) -> list[str]:
        self.pieces.append(len(text))
        return text.split()


def write(path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')
    return path


APP = 'import os\nprint(os.getcwd())\n'


def test_a_repository_under_tmp_is_counted(tmp_path):
    repo = tmp_path / 'tmp' / 'acme' / 'shop'
    write(repo / 'app.py', APP)

    assert stats.analyze_repo(repo, Words(), ['.py']) == (2, 3)


def test_a_repository_named_like_an_ignored_directory_is_counted(
    tmp_path, monkeypatch,
):
    """As `openapi clone` lays it out: `.workspaces/<owner>/<repo>/...`."""
    monkeypatch.chdir(tmp_path)
    repo = Path('.workspaces/acme/examples/v1.0.0') / ('a' * 40)
    write(repo / 'app.py', APP)

    assert stats.analyze_repo(repo, Words(), ['.py']) == (2, 3)


def test_ignored_directories_inside_the_repository_still_are(tmp_path):
    repo = tmp_path / 'shop'
    write(repo / 'app.py', APP)
    write(repo / 'tests' / 'test_app.py', APP)
    write(repo / 'vendor' / 'lib.py', APP)
    write(repo / 'src' / 'build' / 'gen.py', APP)
    # A file is not a directory, whatever it is called.
    write(repo / 'build.py', APP)

    assert stats.analyze_repo(repo, Words(), ['.py']) == (4, 6)


def test_a_file_over_a_megabyte_is_counted(tmp_path):
    big = write(tmp_path / 'big.py', 'x = 1\n' * 300_000)

    assert big.stat().st_size > 1024 * 1024
    assert stats.count_file_stats(big, Words()) == (300_000, 900_000)


def test_a_special_token_in_the_text_is_counted_as_text(tmp_path):
    """Code that talks to a language model spells them out."""
    source = write(tmp_path / 'prompt.py', f"STOP = '{Words.SPECIAL}'\n")

    assert stats.count_file_stats(source, Words()) == (1, 3)


def test_the_tokenizer_is_given_bounded_pieces(tmp_path):
    """Its work on one piece grows with the square of the piece: a
    megabyte of letters with no break in it never finished. The text
    is handed over a piece at a time, cut after a newline where there
    is one."""
    line = 'a' * 900_000
    source = write(tmp_path / 'minified.js', f'{line}\n{APP}')
    words = Words()

    lines, _ = stats.count_file_stats(source, words)

    assert lines == 3
    assert sum(words.pieces) == len(line) + 1 + len(APP)
    assert max(words.pieces) <= 2 * stats.TOKENIZE_CHARS


def test_lines_are_counted_as_before(tmp_path):
    for text in ('', '\n', 'a', 'a\n', 'a\nb', 'a\r\nb\r\n', 'a\n\n\nb\n'):
        source = write(tmp_path / 'x.py', text)
        assert stats.count_file_stats(source, Words())[0] == len(
            text.splitlines(),
        ), repr(text)


# --- the tokenizer's download (#47) -------------------------------------------
#
# tiktoken downloads an encoding the first time one is asked for, 1.7 MB
# from openaipublic.blob.core.windows.net, and keeps it in the system's
# temporary directory, which a reboot may empty: a download nobody was
# told of, and made again whenever that happens.


@pytest.fixture
def tokenizer(tmp_path, monkeypatch):
    """tiktoken's `get_encoding`, answering at once, with where it was
    told to keep what it downloads."""
    import tiktoken

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.delenv('TIKTOKEN_CACHE_DIR', raising=False)
    asked: list[tuple[str, str | None]] = []

    def get_encoding(name):
        asked.append((name, os.environ.get('TIKTOKEN_CACHE_DIR')))
        return Words()

    monkeypatch.setattr(tiktoken, 'get_encoding', get_encoding)
    return asked


def run_stats():
    from typer.testing import CliRunner

    from chatsbom.research.__main__ import app

    return CliRunner().invoke(app, ['openapi', 'stats', '--input', 'none.csv'])


def test_the_tokenizer_is_kept_under_the_project_cache(tmp_path, tokenizer):
    result = run_stats()

    [(name, cache)] = tokenizer
    assert name == 'cl100k_base'
    assert cache is not None
    expected = tmp_path / '.cache' / 'tiktoken'
    assert (tmp_path / cache).resolve() == expected.resolve()
    # Only while it loads: the variable is tiktoken's, not ours to keep.
    assert 'TIKTOKEN_CACHE_DIR' not in os.environ
    # Said when it is about to be downloaded.
    assert 'cl100k_base' in result.stderr


def test_a_cached_tokenizer_is_not_announced(tmp_path, tokenizer):
    write(tmp_path / '.cache' / 'tiktoken' / 'kept', 'the encoding')

    result = run_stats()

    assert len(tokenizer) == 1
    assert 'cl100k_base' not in result.stderr


def test_a_cache_the_user_chose_is_kept(tmp_path, tokenizer, monkeypatch):
    monkeypatch.setenv('TIKTOKEN_CACHE_DIR', str(tmp_path / 'elsewhere'))

    run_stats()

    assert tokenizer == [('cl100k_base', str(tmp_path / 'elsewhere'))]
    assert os.environ['TIKTOKEN_CACHE_DIR'] == str(tmp_path / 'elsewhere')
