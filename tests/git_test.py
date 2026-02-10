from chatsbom.services.git_service import GitService


def test_git_service_resolve_ref():
    service = GitService()
    sha, is_cached, num_refs = service.resolve_ref('vuejs', 'core', 'v3.0.0')
    assert sha == 'd8c1536ead56429f21233bf1fe984ceb3e273fe9'
    assert is_cached is False
    assert num_refs > 0


def test_git_service_resolve_branch():
    service = GitService()
    sha, is_cached, num_refs = service.resolve_ref('vuejs', 'core', 'main')
    assert sha is not None
    assert len(sha) == 40
    assert is_cached is False
    assert num_refs > 0


def test_git_service_invalid_repo():
    service = GitService()
    sha, is_cached, num_refs = service.resolve_ref(
        'nonexistent_user_12345', 'nonexistent_repo_12345', 'main',
    )
    assert sha is None
    assert is_cached is False
    assert num_refs == 0
