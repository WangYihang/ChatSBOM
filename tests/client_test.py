from requests.adapters import BaseAdapter
from requests.models import Response

from chatsbom.core.client import get_http_client
from chatsbom.core.client import get_plain_client


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


# --- the plain client, for conditional requests ---------------------------

class CountingAdapter(BaseAdapter):
    """Answers 200 to everything, and counts what reached it."""

    def __init__(self):
        super().__init__()
        self.sent = 0

    def send(self, request, **kwargs):
        self.sent += 1
        response = Response()
        response.status_code = 200
        response.request = request
        response.url = request.url
        response._content = b'{}'
        return response

    def close(self):
        pass


def test_get_plain_client_does_not_cache():
    """Conditional requests are their own cache. A second one in front
    answers from disk, and GitHub never gets to say 304."""
    session = get_plain_client()
    adapter = CountingAdapter()
    session.mount('https://', adapter)

    session.get('https://api.github.com/repos/o/r')
    session.get('https://api.github.com/repos/o/r')
    assert adapter.sent == 2


def test_get_plain_client_retries_server_errors():
    retry = get_plain_client().get_adapter('https://api.github.com').max_retries
    assert retry.is_retry('GET', 503)


def test_get_plain_client_hands_a_rate_limit_straight_back():
    """urllib3 honours Retry-After by sleeping and asking again, three
    times, while the token is refused. The caller has to see the 429 at
    once: a slice would rather stop than keep asking."""
    retry = get_plain_client().get_adapter('https://api.github.com').max_retries
    assert not retry.is_retry('GET', 429, has_retry_after=True)
