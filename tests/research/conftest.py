"""What the research tools' tests borrow from the core's (#167).

They moved here with the tools, and hold them as they did there: `git`
watched from outside by the harness the collector's git is watched by
(tests/git_subprocess_test.py), and an extra taken away as the core's
commands' are (tests/extras_test.py).
"""
from tests.extras_test import logs_reset  # noqa: F401 - a fixture
from tests.extras_test import offline  # noqa: F401 - a fixture
from tests.extras_test import uninstall  # noqa: F401 - a fixture
from tests.git_subprocess_test import github  # noqa: F401 - a fixture
