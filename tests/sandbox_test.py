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
from chatsbom.core.sandbox import SandboxLimits
from chatsbom.models.language import Language


@pytest.fixture
def command(tmp_path):
    (tmp_path / 'in').mkdir()
    (tmp_path / 'out').mkdir()
    return build_docker_command(
        recipe=lock_recipe_for(Language.JAVA),
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
        recipe=lock_recipe_for(Language.JAVA),
        project_dir=tmp_path / 'in',
        output_dir=tmp_path / 'out',
        limits=SandboxLimits(memory='512m', cpus='0.5', pids=64),
    )
    text = joined(command)
    assert '--memory 512m' in text
    assert '--cpus 0.5' in text
    assert '--pids-limit 64' in text


# --- recipes --------------------------------------------------------------

@pytest.mark.parametrize(
    'language', [
        Language.JAVA, Language.PHP, Language.RUBY, Language.PYTHON,
    ],
)
def test_supported_languages_have_a_recipe(language):
    recipe = lock_recipe_for(language)
    assert recipe.image
    assert recipe.produces
    assert recipe.script


def test_unsupported_language_is_an_error():
    with pytest.raises(ValueError, match='no lockfile recipe'):
        lock_recipe_for(Language.GO)


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
