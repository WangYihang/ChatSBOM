from sbom_insight.core.client import get_http_client


def test_get_http_client_returns_session():
    """Test get_http_client returns a requests Session."""
    session = get_http_client()
    assert hasattr(session, 'get')
    assert hasattr(session, 'post')
    assert callable(session.get)


def test_get_http_client_custom_params():
    """Test get_http_client with custom parameters."""
    session = get_http_client(
        cache_name='test_cache',
        expire_after=3600,
        retries=5,
        pool_size=100,
    )
    assert session is not None


def test_get_http_client_has_adapters():
    """Test session has http and https adapters mounted."""
    session = get_http_client()
    assert 'https://' in session.adapters
    assert 'http://' in session.adapters


def test_get_http_client_retry_on_server_errors():
    """Test retry adapter is configured for server errors."""
    session = get_http_client(retries=3)
    adapter = session.get_adapter('https://example.com')
    # The adapter should have retry configuration
    assert adapter.max_retries is not None
