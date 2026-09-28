"""web/README.md names the files it describes, and they exist (#46).

It went on describing the retired design, Parquet in the browser, by way
of its files: the model's tools were in `src/queries.ts`, which had gone
with it.
"""
import re
from pathlib import Path

WEB = Path(__file__).resolve().parent.parent / 'web'

#: A path in backticks, in one of the web project's directories.
PATH = re.compile(r'`((?:src|test|scripts|public)/[\w./-]*\w)`')


def test_every_file_the_web_readme_names_exists():
    named = set(PATH.findall((WEB / 'README.md').read_text(encoding='utf-8')))
    assert named, 'the README names no file of the project'
    assert sorted(path for path in named if not (WEB / path).exists()) == []
