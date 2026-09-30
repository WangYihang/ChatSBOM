"""`chatsbom-research` starts as `chatsbom` does (#26, #167).

Without importing what only some of its commands use, and without the
network: tests/cli_startup_test.py held the core CLI to both while
these commands were its own, and holds it still. What start-up imports
is measured in a fresh interpreter, since the suite's own imported all
of it long ago.
"""
import json
import socket
import subprocess
import sys

import pytest
from typer.testing import CliRunner

from chatsbom.research.__main__ import app
from tests.cli_startup_test import every_help
from tests.cli_startup_test import HEAVY

#: Imports the research CLI, and prints which of the modules named on
#: its command line that loaded.
PROBE = """
import json
import sys

import chatsbom.research.__main__

print(json.dumps(sorted(set(sys.argv[1:]) & set(sys.modules))))
"""


def test_importing_it_loads_no_heavy_library(tmp_path):
    result = subprocess.run(
        [sys.executable, '-c', PROBE, *HEAVY],
        cwd=tmp_path, capture_output=True, text=True, check=True,
    )

    loaded = json.loads(result.stdout.splitlines()[-1])
    assert loaded == [], (
        f"importing the research CLI loaded {', '.join(loaded)}; "
        "`python -X importtime -c 'import chatsbom.research.__main__'` "
        'shows through which module'
    )


def test_no_help_reaches_the_network(monkeypatch: pytest.MonkeyPatch):
    tried: list[tuple[str, object]] = []
    helping = ''

    def refuse(self: socket.socket, address: object, *args: object) -> None:
        tried.append((helping, address))
        raise OSError('--help has no business on the network')

    monkeypatch.setattr(socket.socket, 'connect', refuse)
    runner = CliRunner()
    for argv in every_help(app):
        helping = ' '.join(argv)
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, f'{helping}: {result.output}'

    # Its own, `openapi`'s, and each of the seven commands'.
    assert len(every_help(app)) == 9
    assert tried == []
