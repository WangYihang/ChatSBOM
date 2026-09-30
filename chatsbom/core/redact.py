"""URLs as logs and stored errors show them: without what could fetch them.
Credentials as logs show them: without the credential.

A finished dependency-graph report is downloaded from a temporary link
GitHub signs, and the signature is in the query: whoever reads a log
line, or an error quoting the link, can fetch the report until the link
expires.

A token is sent in a header, and an error about a header quotes it:
requests refuses one holding a carriage return with `InvalidHeader`,
whose message is the header, token and all (#113).
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

#: What ends a sentence, and so may follow a URL without being part of
#: it. Not `=`, `-`, `_` or `~`, which a signature may end with.
_PUNCTUATION = frozenset('.,;:!?')

#: Each closing bracket, and the one that opens it.
_BRACKETS = {')': '(', ']': '[', '}': '{'}


def _shown(query: str) -> str:
    """`query` as a log may show it: whole when it is made of
    `LOGGED_PARAMETERS` alone, and otherwise `REDACTED`."""
    names = {name for name, _ in parse_qsl(query, keep_blank_values=True)}
    return query if names <= LOGGED_PARAMETERS else REDACTED


def redact_url(url: str) -> str:
    """`url` as the request log shows it: without what would let a reader
    fetch it.

    The signature goes by whatever name the store behind the link gives
    it — `X-Amz-Signature`, `sig`, `jwt`, `token` — so a list of names
    to hide would only be the ones thought of so far. A query is shown
    when it is made of `LOGGED_PARAMETERS` alone, and is otherwise
    replaced whole; a user and password in the address are left out too,
    and so is a fragment.

    Never raises: `redact_urls` runs it on every string of every log
    event, and `conditional_get` in the `except` reporting a request
    that failed.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        # urlsplit refuses a host holding a bracket that is no IPv6
        # address (`https://[`, `http://[invalid]/`), or a character
        # NFKC reads as `/`, `?`, `#`, `@` or `:`. Given back as it came,
        # the URL would keep its query, which is the very signature
        # this is for: it is split by hand instead, at what urlsplit
        # splits at, and redacted all the same.
        return _redact_unsplit(url)
    host = parts.netloc.rpartition('@')[2]
    return urlunsplit(
        (parts.scheme, host, parts.path, _shown(parts.query), ''),
    )


def _redact_unsplit(url: str) -> str:
    """`url` as `redact_url` shows one, split without urlsplit.

    At its first `#` and then its first `?`, as urlsplit splits one. The
    address runs from the first `//` to the next `/`, and a user and
    password are what it holds up to its last `@`. Where that is not
    where urlsplit would have split, more is left out, not less.
    """
    rest, question, query = url.partition('#')[0].partition('?')
    start, slashes, after = rest.partition('//')
    if slashes:
        authority, slash, path = after.partition('/')
        rest = start + slashes + authority.rpartition('@')[2] + slash + path
    query = _shown(query)
    return f'{rest}?{query}' if question and query else rest


def _unenclosed(match: str) -> tuple[str, str]:
    """`match` as the URL it holds, and what follows the URL in it.

    `_URL` runs on to whitespace or a quote, so it takes whatever a text
    ends a URL with: `[https://example.com]`, a Markdown link's `)`, a
    full stop. urlsplit then read `example.com]` as a host, and raised;
    and a bracket closing a signed URL was taken for its query, and left
    out with it. A closing bracket the URL does not open, and punctuation
    at its end, are the text's.
    """
    unopened = {
        closing: match.count(closing) - match.count(opening)
        for closing, opening in _BRACKETS.items()
    }
    end = len(match)
    while end:
        last = match[end - 1]
        if unopened.get(last, 0) > 0:
            unopened[last] -= 1
        elif last not in _PUNCTUATION:
            break
        end -= 1
    return match[:end], match[end:]


def _redact_match(match: re.Match[str]) -> str:
    url, after = _unenclosed(match[0])
    return redact_url(url) + after


def redact_urls(text: str) -> str:
    """`text`, every URL in it as `redact_url` shows one.

    For errors, which quote the request they are about: requests'
    `Forbidden for url: https://...`, urllib3's `with url: /a.json?...`.
    They are logged, and kept in collector.sqlite's outcomes, as they
    were in the ledger's `last_error`, which `queue status` showed.

    A URL is what `_URL` finds, without a bracket that closes around it
    or the punctuation after it. Never raises, whatever the text.
    """
    return _URL.sub(_redact_match, text)


#: A credential in running text, as an `Authorization` header holds one:
#: its scheme, a space, then the credential, which runs to whitespace or
#: a quote. The schemes are the ones a token is sent with: `Bearer`, to
#: the API, `Basic`, which git is given it in (`git_auth_env`), and
#: `token`, GitHub's older one. In any case, as HTTP reads a scheme; and
#: on one line, as a header is.
_CREDENTIAL = re.compile(
    r'''\b(?P<scheme>Bearer|Basic|token)(?P<space>[ \t]+)'''
    r'''(?P<credential>[^\s'"]+)''',
    re.IGNORECASE,
)

#: A word of prose, as a sentence puts one after "token": letters, or
#: words of them joined by hyphens, and what ends a sentence or closes a
#: bracket.
_WORD = re.compile(r'[^\W\d_]+(?:-[^\W\d_]+)*[.,;:!?)\]}]*')

#: Shorter than this, what follows a scheme is not taken for its
#: credential: `token 2 (octocat)` is a token's label (`token_label`).
_SHORTEST_CREDENTIAL = 8


def _redact_credential(match: re.Match[str]) -> str:
    credential = match['credential']
    if len(credential) < _SHORTEST_CREDENTIAL or _WORD.fullmatch(credential):
        return match[0]
    return f"{match['scheme']}{match['space']}{REDACTED}"


def redact_credentials(text: str) -> str:
    """`text`, the credential of each `Authorization` header in it as
    `REDACTED`: `Bearer *****`.

    For errors, which quote the header they are about: requests'
    `InvalidHeader` (`in header value: 'Bearer ghp_...\\r'`), and
    http.client's `Invalid header value b'...'`. A control character
    inside a token, which is what they refuse, is taken out with the
    rest of it.

    "token" is a word as well as a scheme, and the log says a good deal
    about tokens. So what follows a scheme is its credential when it is
    8 characters or more, and no word of prose: `GitHub token verified`,
    `the token expired.` and `token 2 (octocat)` are left as they are.
    Every token GitHub issues is longer, and holds an underscore or a
    digit. Never raises, whatever the text.
    """
    return _CREDENTIAL.sub(_redact_credential, text)


def redact(text: str) -> str:
    """`text` as a log shows it: its URLs as `redact_urls` shows them,
    and without a credential, as `redact_credentials` leaves it."""
    return redact_credentials(redact_urls(text))
