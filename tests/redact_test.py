"""URLs inside text, as errors carry them, without what could fetch them.

requests and urllib3 put the request in their messages: the whole URL
(`404 Client Error: Not Found for url: https://...`), or its path and
query (`Max retries exceeded with url: /a.json?X-Amz-Signature=...`).
For a report's download link, the query is the signature, and the
message was logged and kept as the ledger's `last_error` (#25).
"""
import pytest

from chatsbom.core.redact import redact_urls

#: requests' text for a download that failed to connect, as it reads.
REFUSED = (
    "HTTPSConnectionPool(host='sbom-exports.example', port=443): Max "
    'retries exceeded with url: /a.json?X-Amz-Signature=5ec7e7&'
    "X-Amz-Credential=AKIAEXAMPLE (Caused by NewConnectionError('Failed "
    "to establish a new connection'))"
)


@pytest.mark.parametrize(
    'text, redacted',
    [
        (
            REFUSED,
            "HTTPSConnectionPool(host='sbom-exports.example', port=443): "
            'Max retries exceeded with url: /a.json?***** (Caused by '
            "NewConnectionError('Failed to establish a new connection'))",
        ),
        (
            '403 Client Error: Forbidden for url: '
            'https://sbom-exports.example/a.json?sig=5ec7e7&se=2026',
            '403 Client Error: Forbidden for url: '
            'https://sbom-exports.example/a.json?*****',
        ),
        (
            "Invalid URL 'https://files.example/f?token=GHSAT0A': bad",
            "Invalid URL 'https://files.example/f?*****': bad",
        ),
        (
            'redirected to "https://octocat:hunter2@files.example/f"',
            'redirected to "https://files.example/f"',
        ),
        # The API's search and paging, which the request log shows too.
        (
            'Read timed out: https://api.github.com/search/repositories'
            '?q=language%3Ago&page=3',
            'Read timed out: https://api.github.com/search/repositories'
            '?q=language%3Ago&page=3',
        ),
        # Nothing to take out.
        ('HTTP 404', 'HTTP 404'),
        ('report download: HTTP 403', 'report download: HTTP 403'),
        ('see /app/data/ledger.sqlite3', 'see /app/data/ledger.sqlite3'),
    ],
)
def test_what_an_error_says_of_a_url(text, redacted):
    assert redact_urls(text) == redacted


def test_nothing_signed_is_left():
    for secret in ('5ec7e7', 'AKIAEXAMPLE'):
        assert secret not in redact_urls(REFUSED)


def test_redacting_twice_is_redacting_once():
    once = redact_urls(REFUSED)
    assert redact_urls(once) == once
