"""What a language means to the research tools (#167): its frameworks
and its source files, which they alone read. `Language` itself is the
core's (tests/language_test.py)."""
from chatsbom.models.language import Language
from chatsbom.research.models.language import Go
from chatsbom.research.models.language import Java
from chatsbom.research.models.language import LanguageFactory
from chatsbom.research.models.language import Node
from chatsbom.research.models.language import PHP
from chatsbom.research.models.language import Python
from chatsbom.research.models.language import Ruby
from chatsbom.research.models.language import Rust


def test_factory_get_handler_valid():
    assert isinstance(LanguageFactory.get_handler(Language.GO), Go)
    assert isinstance(LanguageFactory.get_handler(Language.PYTHON), Python)
    assert isinstance(LanguageFactory.get_handler(Language.JAVA), Java)
    assert isinstance(LanguageFactory.get_handler(Language.RUST), Rust)
    assert isinstance(LanguageFactory.get_handler(Language.RUBY), Ruby)
    assert isinstance(LanguageFactory.get_handler(Language.NODE), Node)
    assert isinstance(LanguageFactory.get_handler(Language.PHP), PHP)


def test_a_language_no_longer_says_which_files_are_manifests():
    """`core/discovery.py` answers that from the tree, for every
    ecosystem; `tests/discovery_test.py` holds the names."""
    for language in Language:
        assert not hasattr(
            LanguageFactory.get_handler(
                language,
            ), 'get_sbom_paths',
        )
