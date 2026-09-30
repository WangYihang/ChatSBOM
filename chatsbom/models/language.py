"""The languages the pipeline's commands take (`--language`), and whose
directories the old layout kept its files in (`core/layout.py`).

What a language means to the research tools, the web frameworks written
in it and the files of its source, is theirs since #167
(chatsbom/research/models/language.py).
"""
from enum import Enum


class Language(str, Enum):
    GO = 'go'
    PYTHON = 'python'
    JAVA = 'java'
    RUST = 'rust'
    RUBY = 'ruby'
    NODE = 'node'
    PHP = 'php'
    JAVASCRIPT = 'javascript'
    TYPESCRIPT = 'typescript'

    def __str__(self) -> str:
        return self.value.lower()

    def __repr__(self) -> str:
        return self.value.lower()
