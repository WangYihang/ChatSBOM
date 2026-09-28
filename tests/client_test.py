import pytest
from requests.adapters import BaseAdapter
from requests.models import Response

from chatsbom.core.client import get_http_client
from chatsbom.core.client import get_plain_client
from chatsbom.core.logging import setup_logging


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


# --- what the request log shows ------------------------------------------

#: A report's temporary download link, signed as S3 and Azure sign them.
#: Whoever holds it can fetch the report until it expires.
SIGNED = (
    'https://sbom-exports.example/uuid.json'
    '?X-Amz-Credential=AKIAEXAMPLE&X-Amz-Signature=5ec7e75ec7e7'
    '&sig=s3cr3t&jwt=eyJhbGciOiJIUzI1NiJ9'
)


def logged(capsys: pytest.CaptureFixture[str]) -> str:
    """Everything printed, its lines joined back together.

    Rich folds a URL longer than the line, which would hide a secret
    from `in` without hiding it from whoever reads the log.
    """
    captured = capsys.readouterr()
    return ''.join((captured.out + captured.err).splitlines())


def request(url: str, headers: dict[str, str] | None = None) -> None:
    setup_logging('INFO')
    session = get_plain_client()
    session.mount('https://', CountingAdapter())
    session.get(url, headers=headers)


def test_a_signed_url_is_logged_without_its_signature(capsys):
    request(SIGNED)

    log = logged(capsys)
    assert "url='https://sbom-exports.example/uuid.json?*****'" in log
    for secret in ('AKIAEXAMPLE', '5ec7e75ec7e7', 's3cr3t', 'eyJhbGciOi'):
        assert secret not in log


def test_the_token_is_never_logged(capsys):
    request(
        'https://api.github.com/repos/o/r',
        headers={'Authorization': 'Bearer ghp_n0tl0gg3d'},
    )

    log = logged(capsys)
    assert "url='https://api.github.com/repos/o/r'" in log
    assert 'ghp_n0tl0gg3d' not in log


@pytest.mark.parametrize(
    'url, shown',
    [
        # The API's own search and paging, which say what a line is for.
        (
            'https://api.github.com/search/repositories'
            '?q=language%3Ago&sort=stars&order=desc&per_page=100&page=3',
            'https://api.github.com/search/repositories'
            '?q=language%3Ago&sort=stars&order=desc&per_page=100&page=3',
        ),
        (
            'https://api.github.com/repos/o/r/releases?per_page=100&page=2',
            'https://api.github.com/repos/o/r/releases?per_page=100&page=2',
        ),
        # Anything else in a query may be what lets a reader fetch it,
        # under any name at all, beside ours or with no value.
        (
            'https://raw.example/o/r/f?token=GHSAT0AAAA',
            'https://raw.example/o/r/f?*****',
        ),
        (
            'https://api.github.com/repos/o/r?page=2&access_token=abc',
            'https://api.github.com/repos/o/r?*****',
        ),
        ('https://files.example/f?GHSAT0AAAA', 'https://files.example/f?*****'),
        # And a password in the address itself.
        (
            'https://octocat:hunter2@files.example/f',
            'https://files.example/f',
        ),
    ],
)
def test_what_the_request_log_shows_of_a_url(url, shown, capsys):
    request(url)

    assert f'url={shown!r}' in logged(capsys)
