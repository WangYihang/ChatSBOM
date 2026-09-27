"""Tests for direct-dependency extraction from package manifests.

A lockfile enumerates the resolved closure; a manifest declares what the
project actually asked for. Syft gives us the closure, so the direct set
has to come from the manifest to tell the two apart.

A name no manifest declares is `transitive` only if every manifest was
understood in full. Otherwise it is `unknown`: the manifest that was not
understood may be the one declaring it.
"""
import codecs

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


# What `bundle gem` writes: the gem's own dependencies are in its
# .gemspec, which the content stage does not download.
GEMSPEC_GEMFILE = """
source 'https://rubygems.org'

gemspec

group :test do
  gem 'minitest'
end
"""


def test_gemspec_directive_leaves_the_gemfile_incomplete(tmp_path):
    (tmp_path / 'Gemfile').write_text(GEMSPEC_GEMFILE)
    deps = resolve_relationships(tmp_path, Language.RUBY)

    assert deps.relationship_of('minitest') == DIRECT
    assert deps.relationship_of('rack') == UNKNOWN, (
        'the unread gemspec may declare it'
    )
    assert deps.incomplete == ('Gemfile',)


def test_a_gemspec_beside_the_gemfile_is_what_the_directive_loads(tmp_path):
    """Control: with the gemspec read, the directive hides nothing."""
    (tmp_path / 'Gemfile').write_text(GEMSPEC_GEMFILE)
    (tmp_path / 'mygem.gemspec').write_text(GEMSPEC)
    deps = resolve_relationships(tmp_path, Language.RUBY)

    assert deps.relationship_of('activesupport') == DIRECT
    assert deps.relationship_of('rack') == TRANSITIVE


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


def test_malformed_package_json_is_unknown_not_transitive(tmp_path):
    """A manifest that did not parse declares nothing, and rules nothing out.

    This used to stop at the empty set, and the empty set then made every
    package in the SBOM `transitive`.
    """
    assert parser_for(Language.JAVASCRIPT).parse(
        'package.json', '{oops',
    ) == set()

    (tmp_path / 'package.json').write_text('{oops')
    deps = resolve_relationships(tmp_path, Language.JAVASCRIPT)
    assert deps.relationship_of('react') == UNKNOWN
    assert deps.incomplete == ('package.json',)


def test_package_json_with_a_byte_order_mark_is_read(tmp_path):
    """Windows editors write a BOM, and json.loads rejects one."""
    (tmp_path / 'package.json').write_bytes(
        codecs.BOM_UTF8 + b'{"dependencies": {"react": "^18"}}',
    )
    deps = resolve_relationships(tmp_path, Language.JAVASCRIPT)
    assert deps.relationship_of('react') == DIRECT


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


GO_MOD_WITH_COMMENTS = '\n'.join([
    'module github.com/example/app',
    '',
    'require (',
    '\tgithub.com/gin-gonic/gin v1.9.1 // pinned (see #12)',
    '\t// the next two are (for now) unpinned',
    '\tgithub.com/stretchr/testify v1.8.4',
    '\tgolang.org/x/text v0.14.0 // indirect; via testify',
    '\tgolang.org/x/sys v0.15.0 // indirect',
    ')',
    '',
    'require github.com/spf13/cobra v1.8.0 // cli (for now)',
])


def test_go_mod_parenthesis_in_a_comment_does_not_end_the_block():
    """A `)` in a comment ended the require block, losing what followed."""
    names = parser_for(Language.GO).parse('go.mod', GO_MOD_WITH_COMMENTS)
    assert names == {
        'github.com/gin-gonic/gin',
        'github.com/stretchr/testify',
        'github.com/spf13/cobra',
    }


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


def test_requirements_txt_pep508_url_reference_names_its_package():
    names = parser_for(Language.PYTHON).parse(
        'requirements.txt',
        'mypkg @ https://example.com/mypkg-1.0.tar.gz\n'
        'Other_Pkg[cli] @ git+https://github.com/x/other.git@v2\n'
        'flask\n',
    )
    assert names == {'mypkg', 'other-pkg', 'flask'}


POETRY = """
[tool.poetry.dependencies]
python = "^3.10"
requests = "^2.32"

[tool.poetry.dev-dependencies]
black = "*"

[tool.poetry.group.test.dependencies]
pytest = "^8"

[tool.poetry.group.docs.dependencies]
mkdocs = "*"
"""


def test_pyproject_poetry_groups_are_declared():
    """Poetry 1.2 moved dev-dependencies into named groups."""
    names = parser_for(Language.PYTHON).parse('pyproject.toml', POETRY)
    assert names == {'requests', 'black', 'pytest', 'mkdocs'}


UV_AND_PDM = """
[project]
name = "app"
dependencies = ["httpx"]

[tool.uv]
dev-dependencies = ["ruff>=0.5"]

[tool.pdm.dev-dependencies]
test = ["pytest-cov"]
"""


def test_pyproject_uv_and_pdm_dev_dependencies_are_declared():
    names = parser_for(Language.PYTHON).parse('pyproject.toml', UV_AND_PDM)
    assert names == {'httpx', 'ruff', 'pytest-cov'}


def test_pyproject_dynamic_dependencies_are_unknown(tmp_path):
    """The build backend fills them in from somewhere we did not read."""
    (tmp_path / 'pyproject.toml').write_text(
        '[project]\n'
        'name = "app"\n'
        'dynamic = ["version", "dependencies"]\n'
        '\n'
        '[tool.setuptools.dynamic]\n'
        'dependencies = {file = ["requirements.in"]}\n',
    )
    deps = resolve_relationships(tmp_path, Language.PYTHON)
    assert deps.relationship_of('requests') == UNKNOWN


SETUP_CFG = """
[metadata]
name = mylib
version = 1.0
license = MIT
classifiers =
    License :: OSI Approved :: MIT License

[options]
packages = find:
install_requires =
    requests>=2
    click ; python_version >= "3.8"

[options.extras_require]
test =
    pytest>=7

[options.entry_points]
console_scripts =
    black = mylib.cli:main

[flake8]
max-line-length = 100
"""


def test_setup_cfg_declares_only_install_requires_and_extras():
    """It was read as a requirements file, so every INI key was a package."""
    names = parser_for(Language.PYTHON).parse('setup.cfg', SETUP_CFG)
    assert names == {'requests', 'click', 'pytest'}


@pytest.mark.parametrize(
    'directive', ['file: requirements.in', 'attr: mylib.DEPENDENCIES'],
)
def test_setup_cfg_directive_is_unknown(tmp_path, directive):
    """setuptools resolves these from a file or a module we did not read."""
    (tmp_path / 'setup.cfg').write_text(
        f"[options]\ninstall_requires = {directive}\n",
    )
    deps = resolve_relationships(tmp_path, Language.PYTHON)
    assert deps.relationship_of('requests') == UNKNOWN


TOOL_ONLY_PYPROJECT = """
[build-system]
requires = ["setuptools"]

[tool.black]
line-length = 88
"""

# Syft reads the pinned requirements out of setup.py; we cannot.
SETUP_PY = """
from setuptools import setup

setup(name='mylib', install_requires=['requests==2.32.5'])
"""


@pytest.mark.parametrize(
    'beside',
    [
        {'pyproject.toml': TOOL_ONLY_PYPROJECT},
        {'requirements.txt': 'flask\n'},
        {'setup.cfg': '[flake8]\nmax-line-length = 100\n'},
    ],
    ids=['pyproject-without-dependencies', 'requirements', 'setup-cfg-config'],
)
def test_setup_py_declares_what_nothing_beside_it_does(tmp_path, beside):
    """setup.py is code, so what it declares is unseen and unknown."""
    for name, text in beside.items():
        (tmp_path / name).write_text(text)
    (tmp_path / 'setup.py').write_text(SETUP_PY)
    deps = resolve_relationships(tmp_path, Language.PYTHON)

    assert deps.relationship_of('requests') == UNKNOWN
    assert 'setup.py' in deps.incomplete


@pytest.mark.parametrize(
    'beside',
    [
        {'pyproject.toml': PYPROJECT},
        {'pyproject.toml': POETRY},
        {'pyproject.toml': TOOL_ONLY_PYPROJECT, 'setup.cfg': SETUP_CFG},
    ],
    ids=['pep621', 'poetry', 'setup-cfg-install-requires'],
)
def test_setup_py_is_moot_where_the_build_reads_dependencies_elsewhere(
        tmp_path, beside,
):
    """Control: a build takes these instead of whatever setup.py passes."""
    for name, text in beside.items():
        (tmp_path / name).write_text(text)
    (tmp_path / 'setup.py').write_text(
        'from setuptools import setup\nsetup()\n',
    )
    deps = resolve_relationships(tmp_path, Language.PYTHON)
    assert deps.relationship_of('urllib3') == TRANSITIVE


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


def test_composer_json_with_a_trailing_comma_is_unknown(tmp_path):
    (tmp_path / 'composer.json').write_text(
        '{"require": {"laravel/framework": "^11",}}',
    )
    deps = resolve_relationships(tmp_path, Language.PHP)
    assert deps.relationship_of('laravel/framework') == UNKNOWN


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


# Syft 1.41.2 reports commons-lang3 and micrometer-core from this POM,
# and neither the managed BOM nor the plugin's dependency.
POM_WITH_MANAGEMENT = """<?xml version="1.0"?>
<project{xmlns}>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-dependencies</artifactId>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>org.apache.commons</groupId>
      <artifactId>commons-lang3</artifactId>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <artifactId>maven-surefire-plugin</artifactId>
        <dependencies>
          <dependency>
            <groupId>org.junit.platform</groupId>
            <artifactId>junit-platform-surefire-provider</artifactId>
          </dependency>
        </dependencies>
      </plugin>
    </plugins>
  </build>
  <profiles>
    <profile>
      <id>metrics</id>
      <dependencies>
        <dependency>
          <groupId>io.micrometer</groupId>
          <artifactId>micrometer-core</artifactId>
        </dependency>
      </dependencies>
    </profile>
  </profiles>
</project>
"""


@pytest.mark.parametrize(
    'xmlns', ['', ' xmlns="http://maven.apache.org/POM/4.0.0"'],
    ids=['bare', 'namespaced'],
)
def test_pom_declares_its_own_and_its_profiles_dependencies_only(xmlns):
    """dependencyManagement pins versions; a plugin's classpath is its own."""
    names = parser_for(Language.JAVA).parse(
        'pom.xml', POM_WITH_MANAGEMENT.format(xmlns=xmlns),
    )
    assert names == {'commons-lang3', 'micrometer-core'}


GRADLE = """
buildscript {
    dependencies {
        classpath 'com.android.tools.build:gradle:8.2.0'
    }
}

configurations {
    implementation { exclude group: 'commons-logging' }
    compileOnly { extendsFrom annotationProcessor }
}

description = 'The api and implementation of the app'

dependencies {
    implementation 'org.a:plain:1'
    implementation platform('org.b:bom:1')
    implementation(enforcedPlatform("org.b:enforced-bom:1"))
    implementation group: 'org.c', name: 'map-style', version: '1'
    implementation(group = "org.c", name = "named-arguments", version = "1")
    api "org.d:api-dep:$apiVersion"
    compileOnly 'org.projectlombok:lombok:1.18.30'
    runtimeOnly 'org.postgresql:postgresql:42.7.1'
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.1'
    testImplementation(platform("org.junit:junit-bom:5.10.1"))
    testImplementation(kotlin("test"))
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher:1.10.1'
    annotationProcessor 'org.mapstruct:mapstruct-processor:1.5.5.Final'
    kapt 'com.google.dagger:dagger-compiler:2.50'
    ksp("androidx.room:room-compiler:2.6.1")
    compile 'com.google.guava:guava:20.0'
    testCompile 'junit:junit:4.12'
    implementation project(':core')
    implementation(projects.shared)
    testImplementation(testFixtures(project(":core")))
    implementation fileTree(dir: 'libs', include: ['*.jar'])
    // implementation 'org.old:commented-out:1'
}

kapt {
    correctErrorTypes = true
}
"""


def test_gradle_declaration_styles():
    names = parser_for(Language.JAVA).parse('build.gradle', GRADLE)
    assert names == {
        'gradle', 'plain', 'bom', 'enforced-bom', 'map-style',
        'named-arguments', 'api-dep', 'lombok', 'postgresql',
        'junit-jupiter', 'junit-bom', 'kotlin-test',
        'junit-platform-launcher', 'mapstruct-processor', 'dagger-compiler',
        'room-compiler', 'guava', 'junit',
    }


def test_gradle_without_unresolved_references_is_complete(tmp_path):
    """Control: configuration blocks and project modules hide nothing."""
    (tmp_path / 'build.gradle').write_text(GRADLE)
    deps = resolve_relationships(tmp_path, Language.JAVA)
    assert deps.relationship_of('commons-io') == TRANSITIVE


@pytest.mark.parametrize(
    'declaration',
    [
        'implementation(libs.spring.boot.starter.web)',
        'implementation libs.spring.boot.starter.web',
        'implementation(platform(libs.spring.boot.bom))',
        'implementation Deps.springBootStarterWeb',
        'implementation "org.springframework.boot:$starter:3.2.0"',
    ],
    ids=[
        'catalog', 'catalog-groovy',
        'catalog-platform', 'constant', 'interpolated',
    ],
)
def test_gradle_unresolved_reference_is_unknown(tmp_path, declaration):
    """A version catalog needs gradle/libs.versions.toml, never fetched."""
    (tmp_path / 'build.gradle.kts').write_text(
        'dependencies {\n'
        '    implementation("org.a:plain:1")\n'
        f"    {declaration}\n"
        '}\n',
    )
    deps = resolve_relationships(tmp_path, Language.JAVA)

    assert deps.relationship_of('plain') == DIRECT
    assert deps.relationship_of('spring-boot-starter-web') == UNKNOWN


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
    # GEMFILE ends in `gemspec`, and no gemspec was read to say whether
    # it declares mini_mime.
    assert deps.relationship_of('mini_mime') == UNKNOWN


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


# --- completeness ----------------------------------------------------------

@pytest.mark.parametrize(
    'language, filename, text',
    [
        (Language.JAVASCRIPT, 'package.json', '{"dependencies": {'),
        (Language.PHP, 'composer.json', '["not", "an", "object"]'),
        (Language.RUST, 'Cargo.toml', '[dependencies\nserde = "1"\n'),
        (Language.PYTHON, 'pyproject.toml', '[project\ndependencies = []\n'),
        (Language.PYTHON, 'setup.cfg', 'install_requires = requests\n'),
        (Language.JAVA, 'pom.xml', '<project><dependencies>'),
    ],
    ids=['npm', 'composer', 'cargo', 'pyproject', 'setup-cfg', 'pom'],
)
def test_a_manifest_that_does_not_parse_is_unknown(
        tmp_path, language, filename, text,
):
    (tmp_path / filename).write_text(text)
    deps = resolve_relationships(tmp_path, language)
    assert deps.relationship_of('anything') == UNKNOWN


def test_one_incomplete_manifest_leaves_undeclared_names_unknown(tmp_path):
    """What no manifest names may be declared in the one not understood."""
    (tmp_path / 'package.json').write_text(
        '{"dependencies": {"express": "^4"}}',
    )
    web = tmp_path / 'packages' / 'web'
    web.mkdir(parents=True)
    (web / 'package.json').write_text('{"dependencies": {"react": "^18",}}')
    deps = resolve_relationships(tmp_path, Language.JAVASCRIPT)

    assert deps.relationship_of('express') == DIRECT, 'still declared'
    assert deps.relationship_of('react') == UNKNOWN
    assert deps.incomplete == ('packages/web/package.json',)


def test_an_unreadable_manifest_leaves_undeclared_names_unknown(tmp_path):
    (tmp_path / 'Gemfile').write_bytes(b'\xff\xfe invalid')
    (tmp_path / 'mygem.gemspec').write_text(GEMSPEC)
    deps = resolve_relationships(tmp_path, Language.RUBY)

    assert deps.relationship_of('rails') == UNKNOWN, 'the Gemfile may name it'
    assert deps.incomplete == ('Gemfile',)
    assert deps.sources == ('mygem.gemspec',), 'what was actually read'


def test_an_oversized_manifest_leaves_undeclared_names_unknown(
        tmp_path, monkeypatch,
):
    monkeypatch.setattr('chatsbom.core.manifest.MAX_MANIFEST_BYTES', 64)
    (tmp_path / 'package.json').write_text(
        '{"dependencies": {"express": "^4"}, "description": "' + 'x' * 64 + '"}',
    )
    web = tmp_path / 'web'
    web.mkdir()
    (web / 'package.json').write_text('{"dependencies": {"react": "^18"}}')
    deps = resolve_relationships(tmp_path, Language.JAVASCRIPT)

    assert deps.relationship_of('react') == DIRECT
    assert deps.relationship_of('express') == UNKNOWN
