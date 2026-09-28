"""What a Gradle build declares, read from its text (owner decision D1).

Syft 1.41.2 reads no Gradle file, and GitHub's dependency graph for
halo-dev/halo lists no Spring starter (#51), so without reading the
build files a Gradle-only repository never shows `spring-boot-starter*`.
"""
import pytest

from chatsbom.core.gradle import context_from
from chatsbom.core.gradle import Coordinate
from chatsbom.core.gradle import declarations
from chatsbom.core.gradle import parse_catalog
from chatsbom.core.gradle import purl_of
from chatsbom.core.gradle import read_build
from chatsbom.core.gradle import settings_catalogs
from chatsbom.core.gradle import VERSION_CONSTRAINT
from chatsbom.core.gradle import VERSION_DECLARED
from chatsbom.core.gradle import VERSION_NONE
from chatsbom.core.gradle import VERSION_SPRING_BOOT
from chatsbom.core.gradle import VIA_CATALOG
from chatsbom.core.gradle import VIA_LITERAL

WEB = Coordinate('org.springframework.boot', 'spring-boot-starter-web')


def coordinates(files):
    return {
        (path, d.coordinate) for path, d in declarations(files)
    }


def modules(files):
    return {d.coordinate.module for _, d in declarations(files)}


# --- literal declarations --------------------------------------------------

GROOVY = """
plugins {
    id 'java'
    id 'org.springframework.boot' version '3.2.4'
    id 'io.spring.dependency-management' version '1.1.4'
}

dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation "com.google.guava:guava:33.0.0-jre"
    implementation group: 'org.apache.commons', name: 'commons-lang3', version: '3.14.0'
    compileOnly 'org.projectlombok:lombok'
    runtimeOnly('org.postgresql:postgresql:42.7.3') { because 'db: postgres' }
    testImplementation(platform('org.junit:junit-bom:5.10.2'))
    testImplementation 'org.junit.jupiter:junit-jupiter'
    integrationTestImplementation 'org.testcontainers:postgresql:1.19.7'
    implementation project(':core')
    implementation fileTree(dir: 'libs', include: ['*.jar'])
    // implementation 'org.old:commented-out:1'
    /* implementation 'org.old:block-commented:1' */
}
"""


def test_groovy_declarations_are_read():
    found = {
        c.module: c.version
        for _, c in coordinates([('build.gradle', GROOVY)])
    }
    assert found == {
        'org.springframework.boot:spring-boot-starter-web': '3.2.4',
        'com.google.guava:guava': '33.0.0-jre',
        'org.apache.commons:commons-lang3': '3.14.0',
        'org.projectlombok:lombok': '',
        'org.postgresql:postgresql': '42.7.3',
        'org.junit:junit-bom': '5.10.2',
        'org.junit.jupiter:junit-jupiter': '',
        'org.testcontainers:postgresql': '1.19.7',
    }


def test_the_spring_boot_version_dates_only_spring_boot_artifacts():
    """Spring Boot's own artifacts are released together, so the plugin's
    version is theirs. What else its BOM manages is not read."""
    by_module = {
        d.coordinate.module: d for _, d in declarations(
            [('build.gradle', GROOVY)],
        )
    }
    web = by_module['org.springframework.boot:spring-boot-starter-web']
    assert (web.coordinate.version, web.version_source) == (
        '3.2.4', VERSION_SPRING_BOOT,
    )
    lombok = by_module['org.projectlombok:lombok']
    assert (lombok.coordinate.version, lombok.version_source) == (
        '', VERSION_NONE,
    )
    guava = by_module['com.google.guava:guava']
    assert guava.version_source == VERSION_DECLARED
    assert guava.via == VIA_LITERAL


KOTLIN = """
plugins {
    kotlin("jvm") version "1.9.23"
    id("org.springframework.boot") version "3.3.0"
}

val jacksonVersion = "2.17.0"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation(kotlin("reflect"))
    implementation(
        "io.projectreactor.kotlin:reactor-kotlin-extensions:1.2.2"
    )
    testImplementation(group = "io.mockk", name = "mockk", version = "1.13.10")
}
"""


def test_kotlin_declarations_are_read():
    found = {
        c.module: c.version
        for _, c in coordinates([('build.gradle.kts', KOTLIN)])
    }
    assert found == {
        'org.springframework.boot:spring-boot-starter-webflux': '3.3.0',
        'com.fasterxml.jackson.module:jackson-module-kotlin': '2.17.0',
        'org.jetbrains.kotlin:kotlin-reflect': '',
        'io.projectreactor.kotlin:reactor-kotlin-extensions': '1.2.2',
        'io.mockk:mockk': '1.13.10',
    }


def test_properties_come_from_gradle_properties_and_ext_blocks():
    files = [
        ('gradle.properties', 'pdfboxVersion=3.0.6\n# a comment\n'),
        (
            'build.gradle',
            "ext {\n    springBootVersion = '3.5.9'\n}\n",
        ),
        (
            'app/build.gradle',
            'dependencies {\n'
            '    api "org.apache.pdfbox:pdfbox:$pdfboxVersion"\n'
            '    api "org.apache.pdfbox:xmpbox:${pdfboxVersion}"\n'
            '    api "org.springframework.boot:spring-boot-starter-web"\n'
            '}\n',
        ),
    ]
    found = {c.module: c.version for _, c in coordinates(files)}
    assert found == {
        'org.apache.pdfbox:pdfbox': '3.0.6',
        'org.apache.pdfbox:xmpbox': '3.0.6',
        'org.springframework.boot:spring-boot-starter-web': '3.5.9',
    }


# --- version catalogs ------------------------------------------------------

CATALOG = """
[versions]
spring-boot = "3.4.1"
jackson = { strictly = "2.18.2" }

[libraries]
spring-boot-starter-web = { module = "org.springframework.boot:spring-boot-starter-web" }
spring-boot-starter-actuator = { group = "org.springframework.boot", name = "spring-boot-starter-actuator", version.ref = "spring-boot" }
jackson_databind = { module = "com.fasterxml.jackson.core:jackson-databind", version.ref = "jackson" }
guava = "com.google.guava:guava:33.0.0-jre"
lucene-core = { module = "org.apache.lucene:lucene-core", version = "9.10.0" }
lucene-queryparser = { module = "org.apache.lucene:lucene-queryparser", version = "9.10.0" }

[bundles]
lucene = ["lucene-core", "lucene-queryparser"]

[plugins]
spring-boot = { id = "org.springframework.boot", version.ref = "spring-boot" }
"""


def test_a_catalog_is_parsed():
    catalog = parse_catalog(CATALOG)
    assert catalog is not None
    assert catalog.libraries['spring.boot.starter.web'] == WEB
    assert catalog.libraries['spring.boot.starter.actuator'].version == '3.4.1'
    assert catalog.libraries['jackson.databind'].version == '2.18.2'
    assert catalog.libraries['guava'].version == '33.0.0-jre'
    assert catalog.bundles['lucene'] == ['lucene.core', 'lucene.queryparser']
    assert catalog.plugins['spring.boot'] == (
        'org.springframework.boot', '3.4.1',
    )


def test_a_catalog_that_is_not_toml_is_none():
    assert parse_catalog('[libraries\n') is None


@pytest.mark.parametrize(
    'name, build',
    [
        (
            'build.gradle',
            'dependencies {\n'
            '    implementation libs.spring.boot.starter.web\n'
            '    implementation libs.jackson.databind\n'
            '    implementation libs.bundles.lucene\n'
            '    implementation(libs.guava)\n'
            '}\n',
        ),
        (
            'build.gradle.kts',
            'dependencies {\n'
            '    implementation(libs.spring.boot.starter.web)\n'
            '    implementation(libs.jackson.databind)\n'
            '    implementation(libs.bundles.lucene)\n'
            '    implementation(libs.guava.get())\n'
            '}\n',
        ),
    ],
    ids=['groovy', 'kotlin'],
)
def test_catalog_aliases_resolve(name, build):
    files = [
        ('gradle/libs.versions.toml', CATALOG),
        (f'server/{name}', build),
    ]
    found = {
        d.coordinate.module: d for _, d in declarations(files)
    }
    assert set(found) == {
        'org.springframework.boot:spring-boot-starter-web',
        'com.fasterxml.jackson.core:jackson-databind',
        'org.apache.lucene:lucene-core',
        'org.apache.lucene:lucene-queryparser',
        'com.google.guava:guava',
    }
    web = found['org.springframework.boot:spring-boot-starter-web']
    assert web.via == VIA_CATALOG
    # The catalog's Spring Boot plugin dates the starter.
    assert web.coordinate.version == '3.4.1'
    assert web.version_source == VERSION_SPRING_BOOT
    assert read_build(build, context_from(files)).complete


def test_an_alias_is_matched_whatever_its_separators():
    """Gradle turns `-`, `_` and `.` in an alias into `.` in the
    accessor: `jackson_databind` is `libs.jackson.databind`."""
    files = [
        ('gradle/libs.versions.toml', CATALOG),
        (
            'build.gradle',
            'dependencies { implementation libs.jackson.databind }\n',
        ),
    ]
    assert modules(files) == {'com.fasterxml.jackson.core:jackson-databind'}


def test_a_plugin_alias_is_not_a_dependency():
    files = [
        ('gradle/libs.versions.toml', CATALOG),
        (
            'build.gradle',
            'plugins {\n    alias(libs.plugins.spring.boot)\n}\n'
            'dependencies {\n    implementation libs.guava\n}\n',
        ),
    ]
    assert modules(files) == {'com.google.guava:guava'}


def test_a_catalog_is_named_by_its_file():
    files = [
        ('gradle/deps.versions.toml', '[libraries]\nweb = "org.x:web:1"\n'),
        ('build.gradle', 'dependencies { implementation deps.web }\n'),
    ]
    assert modules(files) == {'org.x:web'}


def test_an_unknown_alias_leaves_the_file_incomplete():
    files = [
        ('gradle/libs.versions.toml', CATALOG),
        ('build.gradle', 'dependencies { implementation libs.not.there }\n'),
    ]
    build = read_build(files[1][1], context_from(files))
    assert build.declared == ()
    assert build.unresolved == ('libs.not.there',)
    assert not build.complete


SETTINGS = """
dependencyResolutionManagement {
    versionCatalogs {
        libs {
            version('boot', '3.1.0')
            library('boot-web', 'org.springframework.boot', 'spring-boot-starter-web').versionRef('boot')
            library('guava', 'com.google.guava:guava:32.0.0-jre')
            bundle('all', ['boot-web', 'guava'])
        }
    }
}
rootProject.name = 'demo'
"""


def test_a_catalog_declared_in_settings_resolves():
    catalogs = settings_catalogs(SETTINGS)
    assert catalogs['libs'].libraries['boot.web'] == Coordinate(
        'org.springframework.boot', 'spring-boot-starter-web', '3.1.0',
    )
    files = [
        ('settings.gradle', SETTINGS),
        (
            'app/build.gradle',
            'dependencies { implementation libs.bundles.all }\n',
        ),
    ]
    assert modules(files) == {
        'org.springframework.boot:spring-boot-starter-web',
        'com.google.guava:guava',
    }


# --- pinned versions -------------------------------------------------------

def test_a_platform_constraint_versions_what_it_pins():
    """halo's `platform/application` pins the versions its modules
    declare without one; the pin itself is not a dependency."""
    files = [
        (
            'platform/build.gradle',
            'dependencies {\n'
            '    constraints {\n'
            "        api 'org.jsoup:jsoup:1.17.2'\n"
            '    }\n'
            '}\n',
        ),
        ('api/build.gradle', "dependencies {\n    api 'org.jsoup:jsoup'\n}\n"),
    ]
    [(path, declared)] = declarations(files)
    assert path == 'api/build.gradle'
    assert declared.coordinate.version == '1.17.2'
    assert declared.version_source == VERSION_CONSTRAINT


def test_the_spring_boot_bom_coordinates_are_the_bom():
    files = [
        ('gradle/libs.versions.toml', CATALOG),
        (
            'platform/build.gradle',
            'dependencies {\n    api platform(SpringBootPlugin.BOM_COORDINATES)\n}\n',
        ),
    ]
    [(_, declared)] = declarations(files)
    assert declared.coordinate == Coordinate(
        'org.springframework.boot', 'spring-boot-dependencies', '3.4.1',
    )
    assert declared.platform


def test_a_managed_bom_dates_spring_boot():
    build = (
        'dependencyManagement {\n'
        '    imports {\n'
        '        mavenBom "org.springframework.boot:spring-boot-dependencies:2.7.18"\n'
        '    }\n'
        '}\n'
        'dependencies {\n'
        "    implementation 'org.springframework.boot:spring-boot-starter-web'\n"
        '}\n'
    )
    [(_, declared)] = declarations([('build.gradle', build)])
    assert declared.coordinate.version == '2.7.18'


# --- what is left out ------------------------------------------------------

def test_the_build_classpath_and_build_logic_are_not_the_project():
    files = [
        (
            'build.gradle',
            'buildscript {\n'
            '    dependencies {\n'
            "        classpath 'org.springframework.boot:spring-boot-gradle-plugin:2.7.0'\n"
            '    }\n'
            '}\n'
            "dependencies { implementation 'org.x:app:1' }\n",
        ),
        (
            'buildSrc/build.gradle',
            "dependencies { implementation 'org.x:convention:1' }\n",
        ),
    ]
    assert modules(files) == {'org.x:app'}


def test_a_flat_dir_jar_is_the_builds_own_file():
    build = "dependencies { runtimeOnly ':thymeleaf:3.1.3.RELEASE' }\n"
    parsed = read_build(build)
    assert parsed.declared == () and parsed.complete


def test_dynamic_declarations_are_incomplete_not_guessed():
    build = (
        'dependencies {\n'
        '    implementation "org.springframework.boot:$starter"\n'
        '    implementation Deps.web\n'
        '}\n'
    )
    parsed = read_build(build)
    assert parsed.declared == ()
    assert set(parsed.unresolved) == {
        'org.springframework.boot:$starter', 'Deps.web',
    }


def test_a_declaration_in_a_string_is_not_one():
    build = (
        "description = 'api for implementation details'\n"
        "dependencies { implementation 'org.x:a:1' }\n"
        'def url = "https://example.com/api/x"\n'
    )
    assert {d.coordinate.module for d in read_build(build).declared} == {
        'org.x:a',
    }


def test_one_row_per_coordinate_and_file():
    build = (
        'dependencies {\n'
        "    implementation 'org.x:a:1'\n"
        "    testImplementation 'org.x:a:1'\n"
        '}\n'
    )
    assert len(declarations([('build.gradle', build)])) == 1


def test_purls_are_maven_purls():
    assert purl_of(WEB) == (
        'pkg:maven/org.springframework.boot/spring-boot-starter-web'
    )
    assert purl_of(Coordinate('g', 'a', '1.+')) == 'pkg:maven/g/a@1.%2B'


def test_no_build_file_is_no_declaration():
    assert declarations([('gradle/libs.versions.toml', CATALOG)]) == []
    assert declarations([('pom.xml', '<project/>')]) == []


# --- the #51 repositories, as their files stand at the scanned commits ------

HALO_CATALOG = """
[versions]
lucene = '10.3.2'

[libraries]
lucene-core = { module = 'org.apache.lucene:lucene-core', version.ref = 'lucene' }
jsoup = 'org.jsoup:jsoup:1.22.1'

[bundles]
lucene = ['lucene-core']

[plugins]
spring-boot = 'org.springframework.boot:3.5.9'
lombok = 'io.freefair.lombok:9.2.0'
"""

HALO_API = """
plugins {
    id 'java-library'
    alias(libs.plugins.lombok)
}

dependencies {
    api platform(project(':platform:application'))
    annotationProcessor platform(project(':platform:application'))

    api 'org.springframework.boot:spring-boot-starter-actuator'
    api 'org.springframework.boot:spring-boot-starter-webflux'
    // Cache
    api "org.springframework.boot:spring-boot-starter-cache"
    api "org.apache.lucene:lucene-core"
    api "org.jsoup:jsoup"
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
}
"""

HALO_PLATFORM = """
import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    id 'java-platform'
    alias(libs.plugins.spring.boot) apply false
}

dependencies {
    api platform(SpringBootPlugin.BOM_COORDINATES)

    constraints {
        api libs.bundles.lucene
        api libs.jsoup
    }
}
"""


def test_halo_declares_a_spring_boot_web_starter():
    """halo-dev/halo@1e52f2a: `api/build.gradle` declares the WebFlux
    starter, versioned by the catalog's Spring Boot plugin; its other
    versions are pinned by `platform/application`."""
    files = [
        ('gradle/libs.versions.toml', HALO_CATALOG),
        ('api/build.gradle', HALO_API),
        ('platform/application/build.gradle', HALO_PLATFORM),
    ]
    found = {
        (path, d.coordinate.module): d.coordinate.version
        for path, d in declarations(files)
    }
    assert found[(
        'api/build.gradle',
        'org.springframework.boot:spring-boot-starter-webflux',
    )] == '3.5.9'
    assert found[('api/build.gradle', 'org.apache.lucene:lucene-core')] == (
        '10.3.2'
    )
    assert found[('api/build.gradle', 'org.jsoup:jsoup')] == '1.22.1'
    assert found[(
        'platform/application/build.gradle',
        'org.springframework.boot:spring-boot-dependencies',
    )] == '3.5.9'


STIRLING_ROOT = """
plugins {
    id "java"
    id "org.springframework.boot" version "3.5.9"
}

ext {
    springBootVersion = "3.5.9"
    pdfboxVersion = "3.0.6"
}
"""

STIRLING_COMMON = """
dependencies {
    api 'org.springframework.boot:spring-boot-starter-web'
    api "org.apache.pdfbox:pdfbox:$pdfboxVersion"
    runtimeOnly 'org.eclipse.angus:angus-mail:2.0.5'
}
"""


def test_stirling_pdf_declares_spring_boot_starter_web():
    """Stirling-Tools/Stirling-PDF@00a9174: `app/common/build.gradle`."""
    files = [
        ('build.gradle', STIRLING_ROOT),
        ('app/common/build.gradle', STIRLING_COMMON),
    ]
    found = {
        (path, d.coordinate.module): d.coordinate.version
        for path, d in declarations(files)
    }
    assert found == {
        (
            'app/common/build.gradle',
            'org.springframework.boot:spring-boot-starter-web',
        ): '3.5.9',
        ('app/common/build.gradle', 'org.apache.pdfbox:pdfbox'): '3.0.6',
        ('app/common/build.gradle', 'org.eclipse.angus:angus-mail'): '2.0.5',
    }
