"""scripts/health.sh, which asks whether the site is serving.

It asked the Worker on 127.0.0.1:8787 unless told `--no-local`, and
the Worker's `POST /api/q` for a number only ClickHouse had. The Worker
is gone (#151), and the service that serves the site publishes no port
on this machine: the script asks the addresses it is given, each for
the page, the snapshot `/api/meta` names, and that snapshot's totals.
It runs here against a fake `curl`, which answers as a serving site
would and keeps what it was asked for, so nothing reaches the network.
"""
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEALTH = ROOT / 'scripts' / 'health.sh'

#: The snapshot the fake site serves.
SNAPSHOT = '0123456789abcdef'

#: Answers the page check with its status, `/api/meta` with a snapshot
#: and a read with a body only a dataset could give, and keeps each
#: call's arguments, a line apiece. META is what `/api/meta` answers.
FAKE_CURL = (
    '#!/bin/sh\n'
    'echo "$*" >> "$RECORD"\n'
    'case " $* " in\n'
    "    *' -w '*) printf 200 ;;\n"
    "    */api/meta*) printf '%s' \"$META\" ;;\n"
    '    *) printf \'{"repositories":28075,"tracked":31000}\' ;;\n'
    'esac\n'
)

SITE = 'https://sbom.example.test'
LOCAL = 'http://127.0.0.1:8080'
META = f'{{"snapshot":"{SNAPSHOT}","generator":"chatsbom/0.5.4"}}'


def run(
    tmp_path: Path, *arguments: str, meta: str = META,
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
            'META': meta,
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result, record.read_text().splitlines()


def asked(calls: list[str], url: str) -> list[str]:
    """The calls that asked for `url`, the last word of each."""
    return [call for call in calls if call.split()[-1] == url]


def test_asks_the_page_the_snapshot_and_its_totals(tmp_path):
    """The page renders with nothing to read, so it alone is no
    evidence: `/api/meta` names the snapshot, and its totals are a
    number only the dataset has."""
    result, calls = run(tmp_path, SITE)
    assert result.returncode == 0, result.stdout + result.stderr
    assert asked(calls, f'{SITE}/')
    assert asked(calls, f'{SITE}/api/meta')
    assert asked(calls, f'{SITE}/api/v/{SNAPSHOT}/totals')
    assert f'{SITE}: ok' in result.stdout
    assert SNAPSHOT in result.stdout


def test_asks_nothing_it_was_not_given(tmp_path):
    """Under compose the service publishes no port on this machine:
    there is no address here to ask unless one is given."""
    result, calls = run(tmp_path, SITE)
    assert result.returncode == 0, result.stdout + result.stderr
    assert [call for call in calls if '127.0.0.1' in call] == []
    assert [call for call in calls if SITE not in call] == []


def test_the_public_side_is_resolved_over_doh(tmp_path):
    """As a visitor reaches it, and not through this machine's
    resolver, which has failed to resolve a working tunnel's name."""
    _, calls = run(tmp_path, SITE)
    assert calls
    assert all('--doh-url' in call for call in calls), calls


def test_a_service_on_this_machine_is_asked_as_it_is(tmp_path):
    """`chatsbom web serve` on the host, where it listens unless told
    otherwise: the loopback resolves locally by definition."""
    result, calls = run(tmp_path, LOCAL)
    assert result.returncode == 0, result.stdout + result.stderr
    assert asked(calls, f'{LOCAL}/api/v/{SNAPSHOT}/totals')
    assert not [call for call in calls if '--doh-url' in call]


def test_each_address_is_asked_and_counted(tmp_path):
    result, calls = run(tmp_path, SITE, f'{LOCAL}/')
    assert result.returncode == 0, result.stdout + result.stderr
    assert asked(calls, f'{SITE}/api/meta')
    assert asked(calls, f'{LOCAL}/api/meta')


def test_a_site_with_no_snapshot_fails(tmp_path):
    """A page, and `/api/meta` refusing: the service is up and has
    nothing to serve. The exit status counts the failed checks."""
    result, calls = run(
        tmp_path, SITE, meta='{"error":"No dataset is configured."}',
    )
    assert result.returncode == 1
    assert 'no snapshot' in result.stdout
    assert not [call for call in calls if '/api/v/' in call]


def test_without_an_address_it_is_refused(tmp_path):
    """It would check nothing, and exit 0: a monitor reading that as
    healthy would never fire."""
    result, calls = run(tmp_path)
    assert result.returncode == 2
    assert 'usage' in result.stderr
    assert calls == []
