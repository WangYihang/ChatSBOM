"""URLs as logs and stored errors show them: without what could fetch them.

A finished dependency-graph report is downloaded from a temporary link
GitHub signs, and the signature is in the query: whoever reads a log
line, or an error quoting the link, can fetch the report until the link
expires.
"""
import re
from urllib.parse import parse_qsl
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

#: The query parameters a URL may keep: the search and paging this code
#: sends GitHub's API, which say what a request was for.
LOGGED_PARAMETERS = frozenset({'q', 'sort', 'order', 'per_page', 'page'})

#: In place of what a URL leaves out.
REDACTED = '*****'

#: A URL in running text: a whole one, or the path and query urllib3
#: names a request by (`Max retries exceeded with url: /a.json?...`).
#: It ends at whitespace or a quote, as it does in an error.
_URL = re.compile(r'''https?://[^\s'"<>]+|/[^\s'"<>?]*\?[^\s'"<>]+''')


def redact_url(url: str) -> str:
    """`url` as the request log shows it: without what would let a reader
    fetch it.

    The signature goes by whatever name the store behind the link gives
    it — `X-Amz-Signature`, `sig`, `jwt`, `token` — so a list of names
    to hide would only be the ones thought of so far. A query is shown
    when it is made of `LOGGED_PARAMETERS` alone, and is otherwise
    replaced whole; a user and password in the address are left out too.
    """
    parts = urlsplit(url)
    host = parts.netloc.rpartition('@')[2]
    query = parts.query
    names = {name for name, _ in parse_qsl(query, keep_blank_values=True)}
    if query and not names <= LOGGED_PARAMETERS:
        query = REDACTED
    return urlunsplit((parts.scheme, host, parts.path, query, ''))


def redact_urls(text: str) -> str:
    """`text`, every URL in it as `redact_url` shows one.

    For errors, which quote the request they are about: requests'
    `Forbidden for url: https://...`, urllib3's `with url: /a.json?...`.
    They are logged, and kept as the ledger's `last_error`, which
    `queue status` shows.
    """
    return _URL.sub(lambda match: redact_url(match[0]), text)
