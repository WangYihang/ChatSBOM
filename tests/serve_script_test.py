"""scripts/serve.sh, the way to serve the dashboard without Docker (#20).

It started `wrangler dev` with miniflare's local explorer on: a UI and
API that read and write every binding, the chat's spend counter
included. The image and compose have switched it off since #18; this
path had not. The script runs here against fakes for everything it
starts, and the fake `npx` keeps the environment wrangler would have
had.
"""
import os
import subprocess
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SERVE = ROOT / 'scripts' / 'serve.sh'

FAKES = {
    # `compose ps` lists nothing running, so the script goes ahead.
    'docker': '#!/bin/sh\nexit 0\n',
    'npm': '#!/bin/sh\nexit 0\n',
    # Keeps its arguments and environment; starts nothing.
    'npx': (
        '#!/bin/sh\n'
        'printf "%s\\n" "$@" > "$RECORD/args"\n'
        'env > "$RECORD/env.new" && mv "$RECORD/env.new" "$RECORD/env"\n'
    ),
    # Answers as a worker that is up would.
    'curl': '#!/bin/sh\necho "<script src=/assets/index-test.js>"\n',
    'cloudflared': '#!/bin/sh\necho https://test.trycloudflare.com\n',
    # Detach nothing, so that nothing outlives the test.
    'setsid': '#!/bin/sh\nexec "$@"\n',
    'nohup': '#!/bin/sh\nexec "$@"\n',
}


def test_wrangler_starts_with_the_local_explorer_off(tmp_path):
    """And with local observability off, which fills the state
    directory with traces nothing reads. wrangler takes exactly `true`
    or `false` for either."""
    bin_ = tmp_path / 'bin'
    bin_.mkdir()
    for name, script in FAKES.items():
        (bin_ / name).write_text(script)
        (bin_ / name).chmod(0o755)
    record = tmp_path / 'record'
    record.mkdir()

    result = subprocess.run(
        ['/bin/sh', str(SERVE)],
        env={
            'PATH': f'{bin_}{os.pathsep}{os.environ["PATH"]}',
            'RECORD': str(record),
            # Where the script writes its logs.
            'TMPDIR': str(tmp_path),
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr

    # wrangler is started in the background; wait for the fake to write.
    deadline = time.monotonic() + 10
    while not (record / 'env').exists():
        if time.monotonic() > deadline:
            pytest.fail('npx never ran')
        time.sleep(0.02)
    args = (record / 'args').read_text().splitlines()
    env = dict(
        line.split('=', 1)
        for line in (record / 'env').read_text().splitlines() if '=' in line
    )
    assert args[:2] == ['wrangler', 'dev']
    assert env.get('X_LOCAL_EXPLORER') == 'false'
    assert env.get('X_LOCAL_OBSERVABILITY') == 'false'
