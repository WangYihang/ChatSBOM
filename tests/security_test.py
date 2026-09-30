"""A token is never shown: not by the configuration that holds it.

The old pipeline's git service masked a token in a URL, and kept it out
of what its git said; it went with the pipeline (#171), and the
collector's git has it from its environment alone, never shown either
(git_subprocess_test, release_tags_git_test).
"""


def test_github_config_repr():
    from chatsbom.core.config import GitHubConfig
    config = GitHubConfig(token='secret-token')
    assert 'secret-token' not in repr(config)
    assert '*****' in repr(config)
