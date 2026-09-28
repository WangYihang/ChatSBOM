"""Container isolation for lockfile generation.

Resolving dependencies means executing project-controlled code: `mvn`
runs build plugins, a Gemfile *is* Ruby, `composer` runs scripts. None of
that may touch the host, so the command is built here and asserted on
without ever running Docker.
"""
import pytest

from chatsbom.core.sandbox import build_docker_command
from chatsbom.core.sandbox import lock_recipe_for
from chatsbom.core.sandbox import LOCK_RECIPES
from chatsbom.core.sandbox import recipes_for
from chatsbom.core.sandbox import SandboxLimits


@pytest.fixture
def command(tmp_path):
    (tmp_path / 'in').mkdir()
    (tmp_path / 'out').mkdir()
    return build_docker_command(
        recipe=lock_recipe_for('gem'),
        project_dir=tmp_path / 'in',
        output_dir=tmp_path / 'out',
        limits=SandboxLimits(),
    )


def joined(command: list[str]) -> str:
    return ' '.join(command)


# --- isolation ------------------------------------------------------------

def test_runs_through_docker(command):
    assert command[:2] == ['docker', 'run']


def test_container_is_removed_after_the_run(command):
    assert '--rm' in command


def test_project_is_mounted_read_only(command, tmp_path):
    mounts = [command[i + 1] for i, a in enumerate(command) if a == '--mount']
    project = next(m for m in mounts if 'target=/project' in m)
    assert 'readonly' in project, 'resolvers must not modify the source tree'


def test_only_the_output_directory_is_writable(command):
    mounts = [command[i + 1] for i, a in enumerate(command) if a == '--mount']
    writable = [m for m in mounts if 'readonly' not in m]
    assert len(writable) == 1
    assert 'target=/out' in writable[0]


def test_no_host_paths_beyond_the_two_mounts(command, tmp_path):
    """Nothing else from the host filesystem may be visible."""
    mounts = [command[i + 1] for i, a in enumerate(command) if a == '--mount']
    assert len(mounts) == 2
    assert all(str(tmp_path) in m for m in mounts)


def test_docker_socket_is_never_mounted(command):
    assert 'docker.sock' not in joined(command)


def test_runs_as_a_non_root_user(command):
    assert '--user' in command
    user = command[command.index('--user') + 1]
    assert not user.startswith(
        '0:',
    ), 'root in the container is root on a mount'


def test_privileges_are_dropped(command):
    text = joined(command)
    assert '--cap-drop ALL' in text
    assert '--security-opt no-new-privileges' in text


def test_root_filesystem_is_read_only_with_a_scratch_tmpfs(command):
    text = joined(command)
    assert '--read-only' in text
    assert '--tmpfs /tmp' in text


def test_resources_are_bounded(command):
    text = joined(command)
    assert '--memory' in text
    assert '--cpus' in text
    assert '--pids-limit' in text


def test_network_is_available_because_resolvers_need_it(command):
    """The one thing we cannot take away: resolution fetches metadata."""
    assert '--network none' not in joined(command)


def test_limits_are_configurable(tmp_path):
    (tmp_path / 'in').mkdir()
    (tmp_path / 'out').mkdir()
    command = build_docker_command(
        recipe=lock_recipe_for('gem'),
        project_dir=tmp_path / 'in',
        output_dir=tmp_path / 'out',
        limits=SandboxLimits(memory='512m', cpus='0.5', pids=64),
    )
    text = joined(command)
    assert '--memory 512m' in text
    assert '--cpus 0.5' in text
    assert '--pids-limit 64' in text


# --- recipes --------------------------------------------------------------

@pytest.mark.parametrize('ecosystem', ['composer', 'gem'])
def test_supported_ecosystems_have_a_recipe(ecosystem):
    recipe = lock_recipe_for(ecosystem)
    assert recipe.image
    assert recipe.produces
    assert recipe.script


def test_unsupported_ecosystem_is_an_error():
    with pytest.raises(ValueError, match='no lockfile recipe'):
        lock_recipe_for('go')


def test_recipes_are_keyed_by_ecosystem_not_language():
    """A recipe runs wherever its manifest is, whatever the repository
    is labelled: the keys are `core/ecosystems.py`'s names."""
    from chatsbom.core.ecosystems import MEMBERS
    assert set(LOCK_RECIPES) <= set(MEMBERS)


def test_every_recipe_writes_a_file_syft_reads():
    """A lockfile Syft does not read changes nothing but the cache key.

    Java wrote `dependency-tree.txt` and Python `requirements.lock`.
    Syft 1.41.2 finds no package in either, and finds them all in the
    same text named `requirements.txt`: its Python cataloger reads
    `*requirements*.txt`, and its Java one `pom.xml`, `gradle.lockfile*`
    and archives.
    """
    from chatsbom.services.sbom_service import MANIFEST_NAMES
    for ecosystem, recipe in LOCK_RECIPES.items():
        unread = sorted(set(recipe.produces) - MANIFEST_NAMES)
        assert not unread, f'{ecosystem}: Syft never reads {unread}'
        assert recipe.manifest in MANIFEST_NAMES, ecosystem


@pytest.mark.parametrize(
    'ecosystem,unread', [
        ('maven', 'dependency-tree.txt'),
        ('pypi', 'requirements.lock'),
    ],
)
def test_maven_and_pypi_have_no_recipe_and_say_why(ecosystem, unread):
    """Withdrawn with a reason rather than dropped without a word:
    whoever runs `sbom lock --ecosystem maven` should learn why nothing
    happens."""
    with pytest.raises(ValueError, match='no lockfile recipe') as error:
        lock_recipe_for(ecosystem)
    assert unread in str(error.value)
    assert ecosystem not in LOCK_RECIPES


def test_a_symlink_the_resolver_leaves_is_not_a_lockfile(tmp_path, monkeypatch):
    """The resolver runs project-controlled code with /out writable, so
    what it leaves there is the project's choice. A link named like the
    lockfile would have `sbom generate` read whatever it points at on
    the host, with the collector's privileges."""
    import subprocess
    from chatsbom.core import sandbox

    elsewhere = tmp_path / 'elsewhere'
    elsewhere.write_text('not a lockfile\n')
    (tmp_path / 'in').mkdir()

    def hostile(command, **kwargs):
        (tmp_path / 'out' / 'Gemfile.lock').symlink_to(elsewhere)
        return subprocess.CompletedProcess(command, 0, stdout='', stderr='')

    monkeypatch.setattr(sandbox, 'daemon_is_rootless', lambda: False)
    monkeypatch.setattr(sandbox.subprocess, 'run', hostile)

    result = sandbox.generate_lockfile(
        'gem', tmp_path / 'in', tmp_path / 'out',
        SandboxLimits(user='1000:1000'),
    )

    assert result.produced == ()
    assert not result.ok


def test_every_recipe_pins_its_image_by_digest_or_tag():
    for recipe in LOCK_RECIPES.values():
        assert ':' in recipe.image, f'{recipe.image} is an unpinned tag'
        assert not recipe.image.endswith(':latest'), recipe.image


def test_recipe_scripts_copy_out_rather_than_writing_to_the_project():
    """/project is read-only, so every recipe must work in /tmp."""
    for recipe in LOCK_RECIPES.values():
        assert '/out' in recipe.script, recipe.image


# --- container identity ---------------------------------------------------

def test_container_runs_as_the_invoking_user_so_it_can_write_output(command):
    """The lockfile lands in a host directory the caller owns."""
    import os
    user = command[command.index('--user') + 1]
    if os.getuid() != 0:
        assert user == f'{os.getuid()}:{os.getgid()}'


def test_a_root_caller_falls_back_to_nobody(tmp_path, monkeypatch):
    from chatsbom.core.sandbox import NOBODY, SandboxLimits
    monkeypatch.setattr('os.getuid', lambda: 0)
    assert SandboxLimits().resolved_user() == NOBODY


def test_explicit_user_wins(tmp_path):
    from chatsbom.core.sandbox import SandboxLimits
    assert SandboxLimits(user='1234:5678').resolved_user() == '1234:5678'


# --- rootless daemons invert the --user decision --------------------------

def test_a_rootless_daemon_omits_the_user_flag(tmp_path):
    """Under rootless Docker, `--user` is what breaks the output write.

    A rootful daemon maps container uid 1000 to host uid 1000, so passing
    the invoking uid is what lets the container write the bind-mounted
    output directory. A rootless daemon maps container *root* to the
    unprivileged host user instead, and an explicit `--user 1000` lands
    on a subuid that owns nothing — the resolver runs, and then
    `cp: /out/Gemfile.lock: Permission denied`.

    Verified against a real `docker:27-dind-rootless`: container-root
    wrote a file owned by the host user, and the hostile Gemfile still
    could not touch /project or /etc.
    """
    (tmp_path / 'in').mkdir()
    (tmp_path / 'out').mkdir()
    command = build_docker_command(
        recipe=lock_recipe_for('gem'),
        project_dir=tmp_path / 'in',
        output_dir=tmp_path / 'out',
        limits=SandboxLimits(),
        rootless_daemon=True,
    )
    assert '--user' not in command


def test_a_rootful_daemon_still_pins_the_user(command):
    assert '--user' in command


def test_rootless_keeps_every_other_restriction(tmp_path):
    """Dropping --user must not quietly drop the rest of the sandbox."""
    (tmp_path / 'in').mkdir()
    (tmp_path / 'out').mkdir()
    text = ' '.join(
        build_docker_command(
            recipe=lock_recipe_for('gem'),
            project_dir=tmp_path / 'in',
            output_dir=tmp_path / 'out',
            limits=SandboxLimits(),
            rootless_daemon=True,
        ),
    )
    assert '--cap-drop ALL' in text
    assert '--security-opt no-new-privileges' in text
    assert '--read-only' in text
    assert 'readonly' in text
    assert '--pids-limit' in text


def test_daemon_rootlessness_is_detected_from_security_options():
    from chatsbom.core.sandbox import is_rootless_daemon_output
    rootless = 'name=seccomp,profile=builtin name=rootless name=cgroupns'
    rootful = 'name=apparmor,profile=default name=seccomp,profile=builtin'
    assert is_rootless_daemon_output(rootless)
    assert not is_rootless_daemon_output(rootful)
    assert not is_rootless_daemon_output('')


# --- choosing where to resolve ------------------------------------------------

def _targets(paths):
    return [(t.directory, t.ecosystem) for t in recipes_for(paths)]


def test_a_recipe_runs_where_its_manifest_is_not_at_the_root_only():
    """`sbom lock` resolved the root of a PHP- or Ruby-labelled
    repository. A Composer project under `backend/` of a repository
    labelled TypeScript was never resolved."""
    assert _targets([
        'package.json', 'package-lock.json',
        'backend/composer.json',
        'tools/docs/Gemfile',
    ]) == [('backend', 'composer'), ('tools/docs', 'gem')]


def test_a_directory_that_ships_its_lockfile_is_not_a_target():
    assert _targets([
        'composer.json', 'composer.lock',
        'api/composer.json',
        'site/Gemfile', 'site/Gemfile.lock',
    ]) == [('api', 'composer')]


def test_a_lockfile_without_its_manifest_is_nothing_to_resolve():
    assert _targets(['composer.lock', 'a/Gemfile.lock', 'go.mod']) == []


def test_targets_are_ordered_shallowest_first_and_capped():
    paths = [f'p{i:02d}/composer.json' for i in range(20)] + ['composer.json']
    targets = _targets(paths)
    assert len(targets) == 10
    assert targets[0] == ('', 'composer')
    assert targets[1:] == [(f'p{i:02d}', 'composer') for i in range(9)]


def test_both_recipes_can_run_in_one_directory():
    assert _targets(['Gemfile', 'composer.json']) == [
        ('', 'composer'), ('', 'gem'),
    ]
