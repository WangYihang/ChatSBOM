from sbom_insight.models.language import Go
from sbom_insight.models.language import Java
from sbom_insight.models.language import Language
from sbom_insight.models.language import LanguageFactory
from sbom_insight.models.language import Node
from sbom_insight.models.language import PHP
from sbom_insight.models.language import Python
from sbom_insight.models.language import Ruby
from sbom_insight.models.language import Rust


def test_language_enum_values():
    assert Language.GO == 'go'
    assert Language.PYTHON == 'python'
    assert str(Language.GO) == 'go'


def test_factory_get_handler_valid():
    assert isinstance(LanguageFactory.get_handler(Language.GO), Go)
    assert isinstance(LanguageFactory.get_handler(Language.PYTHON), Python)
    assert isinstance(LanguageFactory.get_handler(Language.JAVA), Java)
    assert isinstance(LanguageFactory.get_handler(Language.RUST), Rust)
    assert isinstance(LanguageFactory.get_handler(Language.RUBY), Ruby)
    assert isinstance(LanguageFactory.get_handler(Language.NODE), Node)
    assert isinstance(LanguageFactory.get_handler(Language.PHP), PHP)


def test_go_paths():
    handler = Go()
    paths = handler.get_sbom_paths()
    assert 'go.mod' in paths
    assert 'go.sum' in paths


def test_python_paths():
    handler = Python()
    paths = handler.get_sbom_paths()
    assert 'requirements.txt' in paths
    assert 'pyproject.toml' in paths


def test_java_paths():
    handler = Java()
    paths = handler.get_sbom_paths()
    assert 'pom.xml' in paths


def test_rust_paths():
    handler = Rust()
    paths = handler.get_sbom_paths()
    assert 'Cargo.toml' in paths


def test_ruby_paths():
    handler = Ruby()
    paths = handler.get_sbom_paths()
    assert 'Gemfile' in paths


def test_node_paths():
    handler = Node()
    paths = handler.get_sbom_paths()
    assert 'package.json' in paths


def test_php_paths():
    handler = PHP()
    paths = handler.get_sbom_paths()
    assert 'composer.json' in paths
