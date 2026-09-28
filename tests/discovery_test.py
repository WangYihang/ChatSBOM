"""Manifest discovery from a repository's stored tree (#51, design §4.4).

The content stage asked for a fixed list of names at the repository
root, chosen by the repository's language. These trees are the #51
repositories' layouts, as literal lists: every one of them lost its
build files that way.
"""
import pytest

from chatsbom.core import discovery
from chatsbom.core.discovery import content_digest
from chatsbom.core.discovery import discover
from chatsbom.core.discovery import discovery_document
from chatsbom.core.discovery import ecosystem_of
from chatsbom.core.discovery import is_safe_path
from chatsbom.core.discovery import MANIFEST_NAMES
from chatsbom.core.discovery import unquote_git_path
from chatsbom.core.manifest import VENDOR_DIRS
from chatsbom.services import sbom_service


def _selected(tree):
    return discover(tree).paths


def _skipped(tree):
    return dict(discover(tree).skipped)


# --- the #51 repositories ---------------------------------------------------

def test_jeecgboot_maven_modules_below_the_root():
    tree = [
        'README.md', 'jeecg-boot/pom.xml',
        'jeecg-boot/jeecg-module-system/pom.xml',
        'jeecg-boot/jeecg-module-system/src/main/java/App.java',
        'jeecgboot-vue3/package.json', 'jeecgboot-vue3/pnpm-lock.yaml',
    ]
    found = discover(tree)
    assert found.paths == [
        'jeecgboot-vue3/pnpm-lock.yaml', 'jeecgboot-vue3/package.json',
        'jeecg-boot/pom.xml', 'jeecg-boot/jeecg-module-system/pom.xml',
    ]
    assert found.ecosystems == ['maven', 'npm']
    assert found.only_below_root


def test_halo_gradle_build_and_version_catalog():
    tree = [
        'build.gradle', 'settings.gradle', 'gradle.properties',
        'gradle/libs.versions.toml', 'application/build.gradle',
        'api/build.gradle', 'ui/package.json', 'ui/pnpm-lock.yaml',
        'gradle/wrapper/gradle-wrapper.properties',
    ]
    assert set(_selected(tree)) == set(tree) - {
        'gradle/wrapper/gradle-wrapper.properties',
    }


def test_appsmith_client_and_server():
    tree = [
        'app/client/package.json', 'app/client/yarn.lock',
        'app/server/pom.xml', 'app/server/appsmith-server/pom.xml',
        'app/client/cypress/package.json',
    ]
    assert _selected(tree) == [
        'app/client/yarn.lock', 'app/client/package.json',
        'app/server/pom.xml', 'app/client/cypress/package.json',
        'app/server/appsmith-server/pom.xml',
    ]


def test_stirling_pdf_gradle_backend_under_a_typescript_label():
    tree = [
        'build.gradle', 'settings.gradle', 'app/common/build.gradle',
        'app/core/build.gradle', 'frontend/package.json',
        'frontend/package-lock.json',
    ]
    found = discover(tree)
    assert 'app/common/build.gradle' in found.paths
    assert found.ecosystems == ['maven', 'npm']


# --- exclusions -------------------------------------------------------------

@pytest.mark.parametrize(
    'path', [
        'node_modules/left-pad/package.json',
        'web/node_modules/a/package.json',
        'vendor/github.com/x/y/go.mod',
        'third_party/lib/Cargo.toml',
        'third-party/lib/Cargo.toml',
        '.venv/lib/site-packages/x/setup.py',
        'venv/pyproject.toml',
        'bower_components/x/package.json',
        'ios/Pods/Podfile',
        '.yarn/cache/package.json',
        'dist/package.json',
        'target/classes/pom.xml',
        'build/package.json',
        'test/package.json',
        'src/tests/requirements.txt',
        'src/__tests__/package.json',
        'pkg/testdata/go.mod',
        'test-data/pom.xml',
        'spec/fixtures/Gemfile',
        '__fixtures__/package.json',
        'benchmark/Cargo.toml',
        'benchmarks/Cargo.toml',
        'e2e/package.json',
    ],
)
def test_vendored_generated_and_test_trees_are_left_out(path):
    assert _skipped(['go.mod', path]) == {path: 'excluded-dir'}


def test_go_vendor_modules_txt_is_kept():
    """Go writes it there on purpose; it lists the vendored modules."""
    tree = ['go.mod', 'vendor/modules.txt', 'vendor/x/y/go.mod']
    assert _selected(tree) == ['go.mod', 'vendor/modules.txt']
    assert _skipped(tree) == {'vendor/x/y/go.mod': 'excluded-dir'}
    assert ecosystem_of('sub/vendor/modules.txt') == 'go'
    assert ecosystem_of('modules.txt') is None


def test_docs_is_never_left_out():
    """A documentation site is built with the project (owner decision
    D5): its dependencies are the project's."""
    assert _selected(['package.json', 'docs/package.json']) == [
        'package.json', 'docs/package.json',
    ]


def test_matching_is_by_whole_segment_and_case_sensitive():
    tree = ['contest/package.json', 'Test/pom.xml', 'builder/go.mod']
    assert set(_selected(tree)) == set(tree)


def test_a_file_named_like_an_excluded_directory_is_not_excluded():
    assert _selected(['build.gradle', 'test']) == ['build.gradle']


# --- examples (owner decision D5) -------------------------------------------

def test_examples_are_skipped_when_the_repository_has_its_own_manifests():
    tree = [
        'pom.xml', 'examples/a/pom.xml', 'samples/b/build.gradle',
        'demo/package.json', 'x/demos/go.mod', 'sample/Cargo.toml',
        'example/setup.py',
    ]
    assert _selected(tree) == ['pom.xml']
    assert set(_skipped(tree).values()) == {'example-dir'}


def test_a_collection_of_examples_keeps_them():
    """`java-design-patterns`-shaped: all it has are examples."""
    tree = ['examples/a/pom.xml', 'examples/b/pom.xml', 'test/pom.xml']
    assert _selected(tree) == ['examples/a/pom.xml', 'examples/b/pom.xml']
    assert _skipped(tree) == {'test/pom.xml': 'excluded-dir'}


def test_manifests_that_are_only_excluded_do_not_count_as_the_projects_own():
    tree = ['examples/a/package.json', 'node_modules/x/package.json']
    assert _selected(tree) == ['examples/a/package.json']


# --- names ------------------------------------------------------------------

def test_every_ecosystem_is_one_list():
    tree = [
        'Gemfile', 'a/package.json', 'b/go.mod', 'c/Cargo.toml',
        'd/pyproject.toml', 'e/composer.json', 'f/pom.xml',
        'g/mix.exs', 'h/pubspec.yaml', 'i/Package.swift', 'j/Podfile',
        'k/conanfile.txt', 'l/vcpkg.json', 'm/DESCRIPTION',
        'n/stack.yaml.lock', 'o/App.csproj', 'p/x.gemspec',
        'q/dev-requirements.txt', 'r/environment.yml',
    ]
    found = discover(tree)
    assert set(found.paths) == set(tree)
    assert found.ecosystems == sorted({
        'gem', 'npm', 'go', 'cargo', 'pypi', 'composer', 'maven', 'hex',
        'pub', 'swift', 'cocoapods', 'conan', 'vcpkg', 'cran', 'hackage',
        'nuget', 'conda',
    })


#: What `BaseLanguage.get_sbom_paths()` listed before it was deleted:
#: every name must still be discovered.
FORMER_LANGUAGE_NAMES = [
    'go.mod', 'go.sum', 'vendor/modules.txt', 'Gopkg.toml', 'Gopkg.lock',
    'glide.yaml', 'glide.lock',
    'requirements.txt', 'uv.lock', 'poetry.lock', 'pyproject.toml',
    'Pipfile.lock', 'Pipfile', 'environment.yml', 'setup.py', 'setup.cfg',
    'pom.xml', 'build.gradle', 'build.gradle.kts',
    'Cargo.toml', 'Cargo.lock',
    'Gemfile', 'Gemfile.lock',
    'package-lock.json', 'yarn.lock', 'pnpm-lock.yaml', 'package.json',
    'npm-shrinkwrap.json',
    'composer.lock', 'composer.json',
]


@pytest.mark.parametrize('name', FORMER_LANGUAGE_NAMES)
def test_every_name_a_language_asked_for_is_still_discovered(name):
    assert ecosystem_of(name) is not None


@pytest.mark.parametrize(
    'name', [
        'requirements-dev.txt', 'requirements_dev.txt', 'dev-requirements.txt',
        'settings.gradle', 'settings.gradle.kts', 'gradle.properties',
        'gradle/libs.versions.toml', 'gradle/deps.versions.toml',
    ],
)
def test_the_extras_the_design_adds(name):
    assert ecosystem_of(name) is not None


def test_one_registry_for_syft_and_discovery():
    """The Syft cache key reads in full exactly what discovery fetches."""
    assert sbom_service.MANIFEST_NAMES is MANIFEST_NAMES
    assert sbom_service.MANIFEST_SUFFIXES is discovery.MANIFEST_SUFFIXES


def test_the_classifier_skips_what_discovery_calls_vendored():
    assert VENDOR_DIRS == discovery.VENDORED_DIRS
    assert VENDOR_DIRS <= discovery.EXCLUDED_DIRS


def test_source_files_and_near_misses_are_not_manifests():
    for path in [
        'main.go', 'package.json.bak', 'pom.xml.orig', 'Gemfile2',
        'gradle/wrapper/gradle-wrapper.properties', 'LICENSE',
    ]:
        assert ecosystem_of(path) is None, path


# --- order and caps ---------------------------------------------------------

def test_order_is_depth_then_lockfile_then_name():
    tree = [
        'b/package.json', 'package.json', 'yarn.lock', 'a/package.json',
        'a/yarn.lock', 'go.mod', 'go.sum',
    ]
    assert _selected(tree) == [
        'go.sum', 'yarn.lock', 'go.mod', 'package.json',
        'a/yarn.lock', 'a/package.json', 'b/package.json',
    ]


def test_the_order_does_not_depend_on_the_trees_order():
    tree = [f'm{i}/package.json' for i in range(30)] + ['pom.xml']
    assert _selected(tree) == _selected(list(reversed(tree)))


def test_the_file_cap_keeps_the_shallowest_and_records_the_rest():
    tree = [
        f'pkgs/p{i:03d}/package.json' for i in range(250)
    ] + ['package.json']
    found = discover(tree)
    assert len(found.paths) == 200
    assert found.paths[0] == 'package.json'
    assert found.paths[-1] == 'pkgs/p198/package.json'
    capped = found.skipped_for('over-file-cap')
    assert capped == [f'pkgs/p{i:03d}/package.json' for i in range(199, 250)]
    assert found.candidates == 251


def test_the_cap_can_be_set():
    tree = [f'p{i}/go.mod' for i in range(5)]
    assert len(discover(tree, max_files=3).paths) == 3


def test_a_tree_without_manifests_selects_nothing():
    found = discover(['README.md', 'src/main.c'])
    assert found.paths == [] and found.skipped == [] and found.candidates == 0
    assert not found.only_below_root


def test_duplicate_lines_are_one_file():
    assert _selected(['go.mod', 'go.mod']) == ['go.mod']


# --- paths as git lists them ------------------------------------------------

def test_a_quoted_path_is_unquoted():
    assert unquote_git_path(
        '"caf\\303\\251/package.json"',
    ) == 'café/package.json'
    assert unquote_git_path('"a\\"b/go.mod"') == 'a"b/go.mod'
    assert unquote_git_path('"tab\\there/go.mod"') == 'tab\there/go.mod'
    assert unquote_git_path('plain/go.mod') == 'plain/go.mod'
    assert _selected(['"\\344\\270\\255/package.json"']) == ['中/package.json']


@pytest.mark.parametrize(
    'path', [
        '../package.json', 'a/../../go.mod', '/etc/package.json',
        'a//go.mod', '.git/package.json', 'a/./go.mod',
    ],
)
def test_an_unsafe_path_is_refused(path):
    assert not is_safe_path(path)
    assert _skipped([path]) == {path: 'unsafe-path'}


def test_digest_is_of_the_sorted_path_size_list():
    assert content_digest([('b', 2), ('a', 1)]) == content_digest(
        [('a', 1), ('b', 2)],
    )
    assert content_digest([('a', 1)]) != content_digest([('a', 2)])
    assert content_digest([]) == content_digest(())


def test_the_document_says_what_was_left_out_and_why():
    found = discover(['pom.xml', 'test/pom.xml', 'a/pom.xml'], max_files=1)
    document = discovery_document(
        found, repository_id=7, commit_sha='c' * 40,
        fetched={'pom.xml': {'status': 'ok', 'size': 3}},
        extra_skipped=[('x/pom.xml', 'over-byte-cap')],
        max_files=1,
    )
    assert document['selected'] == [{
        'path': 'pom.xml', 'ecosystem': 'maven', 'lockfile': False,
        'status': 'ok', 'size': 3,
    }]
    assert document['skipped_by_reason'] == {
        'excluded-dir': 1, 'over-byte-cap': 1, 'over-file-cap': 1,
    }
    assert document['candidates'] == 3


# --- the #55 pilot ------------------------------------------------------------

def test_a_podspec_is_discovered():
    """jasnig/ZJScrollPageView: `ZJScrollPageView.podspec` was its only
    manifest, and discovery listed `Podfile`/`Podfile.lock` alone."""
    found = discover([
        'ZJScrollPageView.podspec',
        'ZJScrollPageView/Assets.xcassets/Contents.json',
        'Specs/Foo.podspec.json',
    ])
    assert found.paths == [
        'ZJScrollPageView.podspec', 'Specs/Foo.podspec.json',
    ]
    assert found.ecosystems == ['cocoapods']


def test_buildsrc_sources_are_discovered_for_their_constants():
    """ZacSweers/CatchUp names every dependency by a `deps.*` constant in
    `buildSrc/src/main/kotlin/dependencies.kt`."""
    found = discover([
        'build.gradle.kts',
        'app/build.gradle.kts',
        'buildSrc/build.gradle.kts',
        'buildSrc/src/main/kotlin/dependencies.kt',
        'buildSrc/src/test/kotlin/DepsTest.kt',
        'app/src/main/kotlin/Main.kt',
    ])
    assert 'buildSrc/src/main/kotlin/dependencies.kt' in found.paths
    assert 'app/src/main/kotlin/Main.kt' not in found.paths
    assert 'buildSrc/src/test/kotlin/DepsTest.kt' not in found.paths
    assert ecosystem_of('buildSrc/src/main/java/deps/Libs.java') == 'maven'


def test_buildsrc_sources_are_capped_apart_from_manifests():
    plugins = [
        f'buildSrc/src/main/kotlin/plugin{i:03}.kt' for i in range(50)
    ]
    found = discover(['build.gradle', *plugins])
    logic = [p for p in found.paths if p.startswith('buildSrc/')]
    assert len(logic) == discovery.MAX_BUILD_LOGIC_SOURCES
    assert logic == plugins[:discovery.MAX_BUILD_LOGIC_SOURCES]
    assert len(found.skipped_for(discovery.OVER_BUILD_LOGIC_CAP)) == 30
    assert 'build.gradle' in found.paths
