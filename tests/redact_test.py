"""URLs inside text, as errors carry them, without what could fetch them.

requests and urllib3 put the request in their messages: the whole URL
(`404 Client Error: Not Found for url: https://...`), or its path and
query (`Max retries exceeded with url: /a.json?X-Amz-Signature=...`).
For a report's download link, the query is the signature, and the
message was logged and kept as the ledger's `last_error` (#25).

Redacting raised, too. The URL pattern ran on to the next whitespace or
quote, so it took in the bracket a text closes a URL with: `[https://
example.com]` gave a host of `example.com]`, and urlsplit refuses a
bracket in a host that is no IPv6 address. Its ValueError came out of
every log call whose event held such text.
"""
import itertools

import pytest

from chatsbom.core.redact import redact_url
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


# --- what surrounds a URL, and URLs urlsplit refuses --------------------------

@pytest.mark.parametrize(
    'text, redacted',
    [
        # Nothing to take out, and nothing to raise over.
        ('see [https://example.com]', 'see [https://example.com]'),
        ('(https://example.com)', '(https://example.com)'),
        ('https://[', 'https://['),
        (']', ']'),
        ('http://[invalid', 'http://[invalid'),
        # A bracket closing around the URL, or a Markdown link's: the
        # text's, not the query's.
        (
            '[https://files.example/a.json?sig=5ec7e7]',
            '[https://files.example/a.json?*****]',
        ),
        (
            '(https://files.example/a.json?sig=5ec7e7)',
            '(https://files.example/a.json?*****)',
        ),
        (
            '[report](https://files.example/a.json?sig=5ec7e7)',
            '[report](https://files.example/a.json?*****)',
        ),
        (
            'Max retries exceeded (url: /a.json?X-Amz-Signature=5ec7e7)',
            'Max retries exceeded (url: /a.json?*****)',
        ),
        # A bracket the URL opens is its own.
        (
            'see https://example.com/wiki/Go_(language)',
            'see https://example.com/wiki/Go_(language)',
        ),
        # After punctuation, and followed by it.
        (
            'url:https://files.example/a.json?sig=5ec7e7',
            'url:https://files.example/a.json?*****',
        ),
        (
            'fetched https://files.example/a.json?sig=5ec7e7.',
            'fetched https://files.example/a.json?*****.',
        ),
        (
            'fetched https://files.example/a.json?sig=5ec7e7, then failed',
            'fetched https://files.example/a.json?*****, then failed',
        ),
        # An IPv6 host, bare and in brackets.
        (
            'https://[::1]:8080/a.json?X-Amz-Signature=5ec7e7',
            'https://[::1]:8080/a.json?*****',
        ),
        (
            '[https://[::1]:8080/a.json?X-Amz-Signature=5ec7e7]',
            '[https://[::1]:8080/a.json?*****]',
        ),
        # Hosts urlsplit refuses: redacted all the same.
        (
            'http://[invalid]/a.json?sig=5ec7e7',
            'http://[invalid]/a.json?*****',
        ),
        (
            'https://[files.example/a.json?sig=5ec7e7',
            'https://[files.example/a.json?*****',
        ),
        (
            'https://octocat:hunter2@[files.example/f',
            'https://[files.example/f',
        ),
        # NFKC reads U+2100 as `a/c`, and urlsplit refuses it in a host.
        (
            'https://files℀example/a.json?sig=5ec7e7#frag',
            'https://files℀example/a.json?*****',
        ),
        (
            'https://[api.example/search?q=go&page=3',
            'https://[api.example/search?q=go&page=3',
        ),
        # One of each, in one message.
        (
            'see [https://example.com] and https://[::1/a.json?sig=5ec7e7',
            'see [https://example.com] and https://[::1/a.json?*****',
        ),
    ],
)
def test_what_surrounds_a_url(text, redacted):
    assert redact_urls(text) == redacted


@pytest.mark.parametrize(
    'url, redacted',
    [
        ('https://[', 'https://['),
        ('http://[invalid/a.json?sig=5ec7e7', 'http://[invalid/a.json?*****'),
        ('https://a:b@[::1/f#token=5ec7e7', 'https://[::1/f'),
    ],
)
def test_a_url_urlsplit_refuses_is_redacted_whole(url, redacted):
    """The request log and `conditional_get` redact the URL they were
    handed, which requests refuses as it prepares the request, before
    anything is sent: the log call raised then, in the `except`
    reporting the failure."""
    assert redact_url(url) == redacted


#: What breaks a URL in running text, or ends one: brackets, what
#: urlsplit splits a URL at, an IPv6 host and an IPv4 one, an IPvFuture,
#: and characters NFKC turns into a delimiter (`℀` is `a/c`, `＠` is
#: `@`), which urlsplit refuses in a host.
PIECES = [
    '[', ']', '(', ')', '{', '}', '@', ':', '/', '?', '#', '.', ',', ' ',
    'v1', '::1', '1.2.3.4', '℀', '＠',
]

#: A signature, where each text below puts one.
SECRET = '5ec7e75ec7e7'


def test_no_text_raises_or_keeps_a_signature():
    """Every arrangement of three pieces: in a host, around a URL and
    after it, in a path and query, and as a URL's whole host. Redaction
    runs on every string of every log event, so what raises here raises
    from the log call."""
    failures: list[str] = []
    for pieces in itertools.product(PIECES, repeat=3):
        piece = ''.join(pieces)
        for text in (
            f'https://{piece}/a.json?sig={SECRET}',
            f'{piece}https://files.example/a.json?sig={SECRET}{piece}',
            f'{piece}/a.json?X-Amz-Signature={SECRET}{piece}',
            f'https://{piece}',
        ):
            try:
                redacted = redact_urls(text)
            except Exception as error:  # noqa: BLE001 - collected
                failures.append(f'{text!r} raised {error!r}')
                continue
            if SECRET in redacted:
                failures.append(f'{text!r} kept it: {redacted!r}')
    assert failures == []
