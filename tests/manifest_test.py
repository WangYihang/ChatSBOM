"""Tests for direct-dependency extraction from package manifests.

A lockfile enumerates the resolved closure; a manifest declares what the
project actually asked for. Syft gives us the closure, so the direct set
has to come from the manifest to tell the two apart.
"""
import pytest

from chatsbom.core.manifest import DIRECT
from chatsbom.core.manifest import DirectDependencies
from chatsbom.core.manifest import parser_for
from chatsbom.core.manifest import resolve_relationships
from chatsbom.core.manifest import TRANSITIVE
from chatsbom.core.manifest import UNKNOWN
from chatsbom.models.language import Language


# --- ruby ------------------------------------------------------------------

GEMFILE = """
source 'https://rubygems.org'

gem 'rails', '~> 7.1'
gem "mail"
gem 'puma', require: false
group :development, :test do
  gem 'rspec-rails'
end
# gem 'commented-out'
gemspec
"""


def test_gemfile_direct_deps():
    names = parser_for(Language.RUBY).parse('Gemfile', GEMFILE)
    assert names == {'rails', 'mail', 'puma', 'rspec-rails'}


def test_gemfile_ignores_commented_lines():
    assert 'commented-out' not in parser_for(Language.RUBY).parse(
        'Gemfile', GEMFILE,
    )


GEMSPEC = """
Gem::Specification.new do |s|
  s.name = 'mygem'
  s.add_dependency 'mail', '~> 2.8'
  s.add_runtime_dependency "activesupport"
  s.add_development_dependency 'rspec'
end
"""


def test_gemspec_direct_deps():
    names = parser_for(Language.RUBY).parse('mygem.gemspec', GEMSPEC)
    assert names == {'mail', 'activesupport', 'rspec'}


# --- npm -------------------------------------------------------------------

PACKAGE_JSON = """
{
  "name": "app",
  "dependencies": {"express": "^4.18.0", "@scope/pkg": "1.0.0"},
  "devDependencies": {"jest": "^29"},
  "peerDependencies": {"react": ">=18"},
  "optionalDependencies": {"fsevents": "*"},
  "scripts": {"test": "jest"}
}
"""


@pytest.mark.parametrize('language', [Language.JAVASCRIPT, Language.TYPESCRIPT])
def test_package_json_direct_deps(language):
    names = parser_for(language).parse('package.json', PACKAGE_JSON)
    assert names == {
        'express', '@scope/pkg', 'jest', 'react', 'fsevents',
    }
    assert 'test' not in names, 'scripts must not be read as deps'


def test_malformed_package_json_yields_nothing():
    assert parser_for(Language.JAVASCRIPT).parse(
        'package.json', '{oops',
    ) == set()


# --- go --------------------------------------------------------------------

# go.mod indents its require block with tabs; \t keeps that faithful
# without putting literal tabs in this file.
GO_MOD = '\n'.join([
    'module github.com/example/app',
    '',
    'go 1.22',
    '',
    'require (',
    '\tgithub.com/gin-gonic/gin v1.9.1',
    '\tgithub.com/stretchr/testify v1.8.4',
    '\tgolang.org/x/sys v0.15.0 // indirect',
    ')',
    '',
    'require github.com/spf13/cobra v1.8.0',
    '',
    'exclude github.com/bad/pkg v1.0.0',
])


def test_go_mod_direct_deps_exclude_indirect():
    names = parser_for(Language.GO).parse('go.mod', GO_MOD)
    assert names == {
        'github.com/gin-gonic/gin',
        'github.com/stretchr/testify',
        'github.com/spf13/cobra',
    }
    assert 'golang.org/x/sys' not in names, 'go.mod marks indirect explicitly'
    assert 'github.com/bad/pkg' not in names, 'exclude is not a require'


# --- rust ------------------------------------------------------------------

CARGO_TOML = """
[package]
name = "app"

[dependencies]
serde = { version = "1.0", features = ["derive"] }
tokio = "1"

[dev-dependencies]
criterion = "0.5"

[build-dependencies]
cc = "1.0"

[target.'cfg(unix)'.dependencies]
nix = "0.27"
"""


def test_cargo_toml_direct_deps():
    names = parser_for(Language.RUST).parse('Cargo.toml', CARGO_TOML)
    assert names == {'serde', 'tokio', 'criterion', 'cc', 'nix'}
    assert 'app' not in names, 'the package itself is not a dependency'


# --- python ----------------------------------------------------------------

PYPROJECT = """
[project]
name = "app"
dependencies = [
    "requests>=2.32",
    "Typer[all]==0.21.1",
    "structlog ; python_version >= '3.12'",
]

[project.optional-dependencies]
dev = ["pytest>=9"]

[dependency-groups]
lint = ["mypy"]
"""


def test_pyproject_direct_deps_are_normalised():
    names = parser_for(Language.PYTHON).parse('pyproject.toml', PYPROJECT)
    assert names == {'requests', 'typer', 'structlog', 'pytest', 'mypy'}


REQUIREMENTS = """
# comment
requests==2.32.5
Flask_SQLAlchemy>=3.0
-r other.txt
--index-url https://example.com
git+https://github.com/x/y.git#egg=ypkg
"""


def test_requirements_txt_direct_deps():
    names = parser_for(Language.PYTHON).parse('requirements.txt', REQUIREMENTS)
    assert 'requests' in names
    assert 'flask-sqlalchemy' in names, 'PEP 503 normalisation'
    assert 'other.txt' not in names
    assert not any(n.startswith('-') for n in names)


# --- php -------------------------------------------------------------------

COMPOSER = """
{
  "require": {"php": ">=8.1", "laravel/framework": "^11.0"},
  "require-dev": {"phpunit/phpunit": "^11"}
}
"""


def test_composer_json_direct_deps_drop_platform_packages():
    names = parser_for(Language.PHP).parse('composer.json', COMPOSER)
    assert names == {'laravel/framework', 'phpunit/phpunit'}
    assert 'php' not in names, 'platform requirements are not packages'


# --- java ------------------------------------------------------------------

POM = """<?xml version="1.0"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
</project>
"""


def test_pom_direct_deps_use_artifact_id():
    names = parser_for(Language.JAVA).parse('pom.xml', POM)
    assert names == {'spring-boot-starter-web', 'junit'}


# --- classification --------------------------------------------------------

def test_direct_dependencies_classifies_names():
    deps = DirectDependencies(
        names=frozenset({'mail', 'rails'}),
        sources=('Gemfile',),
        normalise=str.lower,
    )
    assert deps.relationship_of('mail') == DIRECT
    assert deps.relationship_of('Rails') == DIRECT, 'normalised comparison'
    assert deps.relationship_of('mini_mime') == TRANSITIVE


def test_no_manifest_means_unknown_not_transitive():
    """Absence of evidence is not evidence of transitivity."""
    deps = DirectDependencies(
        names=frozenset(), sources=(), normalise=str.lower,
    )
    assert deps.relationship_of('mail') == UNKNOWN


# --- filesystem integration ------------------------------------------------

def test_resolve_relationships_reads_ruby_project(tmp_path):
    (tmp_path / 'Gemfile').write_text(GEMFILE)
    deps = resolve_relationships(tmp_path, Language.RUBY)

    assert 'Gemfile' in deps.sources
    assert deps.relationship_of('mail') == DIRECT
    assert deps.relationship_of('mini_mime') == TRANSITIVE


def test_resolve_relationships_merges_gemfile_and_gemspec(tmp_path):
    (tmp_path / 'Gemfile').write_text("gem 'rails'\n")
    (tmp_path / 'mygem.gemspec').write_text(GEMSPEC)
    deps = resolve_relationships(tmp_path, Language.RUBY)

    assert deps.relationship_of('rails') == DIRECT
    assert deps.relationship_of('activesupport') == DIRECT
    assert len(deps.sources) == 2


def test_resolve_relationships_with_no_manifest_is_unknown(tmp_path):
    deps = resolve_relationships(tmp_path, Language.RUBY)
    assert deps.sources == ()
    assert deps.relationship_of('anything') == UNKNOWN


def test_resolve_relationships_ignores_vendored_manifests(tmp_path):
    (tmp_path / 'package.json').write_text(PACKAGE_JSON)
    nested = tmp_path / 'node_modules' / 'dep'
    nested.mkdir(parents=True)
    (nested / 'package.json').write_text(
        '{"dependencies": {"should-not-appear": "1"}}',
    )
    deps = resolve_relationships(tmp_path, Language.JAVASCRIPT)
    assert deps.relationship_of('should-not-appear') == TRANSITIVE


def test_unreadable_manifest_does_not_abort_resolution(tmp_path):
    (tmp_path / 'Gemfile').write_bytes(b'\xff\xfe invalid')
    (tmp_path / 'mygem.gemspec').write_text(GEMSPEC)
    deps = resolve_relationships(tmp_path, Language.RUBY)
    assert deps.relationship_of('mail') == DIRECT


def test_every_language_has_a_parser():
    for language in Language:
        assert parser_for(language) is not None, language
