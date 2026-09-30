"""What a language means to the research tools (#167): the web
frameworks written in it, which `classify` and `openapi candidates` look
for, and the files of its source, which `openapi stats` counts.

The part of chatsbom/models/language.py only they read. `Language`
itself stays there, in the core, whose searches it names.
"""
from abc import ABC
from abc import abstractmethod

from chatsbom.models.language import Language
from chatsbom.research.models.framework import Framework


class BaseLanguage(ABC):
    """What a language means for framework detection and the OpenAPI
    commands. Which files are manifests is not a question for a language
    any more: `core/discovery.py` answers it from a repository's tree,
    for every ecosystem at once."""

    @abstractmethod
    def get_frameworks(self) -> list[Framework]:
        ...

    @abstractmethod
    def get_source_extensions(self) -> list[str]:
        ...


class Go(BaseLanguage):
    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.GIN,
            Framework.ECHO,
            Framework.CHI,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.go']


class Python(BaseLanguage):
    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.FLASK,
            Framework.DJANGO,
            Framework.FASTAPI,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.py']


class Java(BaseLanguage):
    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.SPRINGBOOT,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.java', '.kt', '.scala']


class Rust(BaseLanguage):
    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.ACTIX,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.rs']


class Ruby(BaseLanguage):
    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.RAILS,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.rb']


class Node(BaseLanguage):
    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.EXPRESS,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.js', '.ts', '.jsx', '.tsx', '.mjs', '.cjs']


class JavaScript(Node):
    pass


class TypeScript(Node):
    pass


class PHP(BaseLanguage):
    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.LARAVEL,
            Framework.SYMFONY,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.php']


class LanguageFactory:
    _MAPPING = {
        Language.GO: lambda: Go(),
        Language.PYTHON: lambda: Python(),
        Language.JAVA: lambda: Java(),
        Language.RUST: lambda: Rust(),
        Language.RUBY: lambda: Ruby(),
        Language.NODE: lambda: Node(),
        Language.PHP: lambda: PHP(),
        Language.JAVASCRIPT: lambda: JavaScript(),
        Language.TYPESCRIPT: lambda: TypeScript(),
    }

    @staticmethod
    def get_handler(language: Language) -> BaseLanguage:
        handler_cls = LanguageFactory._MAPPING.get(language)
        if handler_cls:
            return handler_cls()
        else:
            raise ValueError(f"Unsupported language: {language}")
