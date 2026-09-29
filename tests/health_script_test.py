"""scripts/health.sh, which asks whether the site is serving.

It asked the Worker on 127.0.0.1:8787 every time, and in the tunnel mode
(#130) nothing is published there: the check failed however well the
site served, and the exit status, the count of failed checks, did too.
`--no-local` leaves that check out. The script runs here against a fake
`curl`, which answers as a serving site would and keeps what it was
asked for, so nothing reaches the network.
"""
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEALTH = ROOT / 'scripts' / 'health.sh'

#: Answers the page check with its status and the API check with a body
#: only ClickHouse could give, and keeps each call's arguments, a line
#: apiece.
FAKE_CURL = (
    '#!/bin/sh\n'
    'echo "$*" >> "$RECORD"\n'
    'case " $* " in\n'
    "    *' -w '*) printf 200 ;;\n"
    '    *) printf \'{"repositories":28075}\' ;;\n'
    'esac\n'
)

SITE = 'https://sbom.example.test'


def run(
    tmp_path: Path, *arguments: str,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """The script with these arguments, and the calls curl was given."""
    bin_ = tmp_path / 'bin'
    bin_.mkdir(exist_ok=True)
    (bin_ / 'curl').write_text(FAKE_CURL)
    (bin_ / 'curl').chmod(0o755)
    record = tmp_path / 'calls'
    record.write_text('')
    result = subprocess.run(
        ['/bin/sh', str(HEALTH), *arguments],
        env={
            'PATH': f'{bin_}{os.pathsep}{os.environ["PATH"]}',
            'RECORD': str(record),
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result, record.read_text().splitlines()


def test_the_local_worker_is_asked_by_default(tmp_path):
    """As it always was: the port an external tunnel reaches."""
    result, calls = run(tmp_path, SITE)
    assert result.returncode == 0, result.stdout + result.stderr
    assert [call for call in calls if 'http://127.0.0.1:8787/' in call]
    assert [call for call in calls if f'{SITE}/' in call]


def test_no_local_asks_the_public_side_alone(tmp_path):
    """In the tunnel mode the Worker has no port on this machine: the
    public check is the whole answer, and a healthy site exits 0."""
    result, calls = run(tmp_path, '--no-local', SITE)
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls
    assert [call for call in calls if '127.0.0.1' in call] == []
    assert 'public: ok' in result.stdout
    assert 'local' not in result.stdout


def test_no_local_without_a_site_is_refused(tmp_path):
    """It would check nothing, and exit 0: a monitor reading that as
    healthy would never fire."""
    result, calls = run(tmp_path, '--no-local')
    assert result.returncode == 2
    assert 'usage' in result.stderr
    assert calls == []
