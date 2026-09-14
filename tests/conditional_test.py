"""Conditional requests: the lever the whole continuous design rests on.

GitHub's documentation is explicit — "Making a conditional request does
not count against your primary rate limit if a 304 response is returned
and the request was made while correctly authorized with an Authorization
header." So revalidating the 74.7% of repositories that did not change
in a week can cost nothing, provided we actually send If-None-Match.
"""
import pytest

from chatsbom.core.conditional import conditional_get
from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.conditional import NOT_MODIFIED


class FakeResponse:
    def __init__(self, status_code, headers=None, payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError('no body')
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f'status {self.status_code}')


class FakeSession:
    """Records the headers it was asked to send."""

    def __init__(self, *responses):
        self._responses = list(responses)
        self.requests: list[dict] = []

    def get(self, url, headers=None, **kwargs):
        self.requests.append({'url': url, 'headers': headers or {}})
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


# --- sending the condition ------------------------------------------------

def test_a_stored_etag_is_sent_as_if_none_match():
    session = FakeSession(FakeResponse(NOT_MODIFIED))
    conditional_get(session, 'https://api/x', etag='W/"abc"')
    assert session.requests[0]['headers']['If-None-Match'] == 'W/"abc"'


def test_no_etag_means_an_unconditional_request():
    session = FakeSession(FakeResponse(200, payload={}))
    conditional_get(session, 'https://api/x', etag=None)
    assert 'If-None-Match' not in session.requests[0]['headers']


def test_existing_headers_are_preserved():
    session = FakeSession(FakeResponse(200, payload={}))
    conditional_get(
        session, 'https://api/x', etag='e', headers={'Accept': 'application/json'},
    )
    sent = session.requests[0]['headers']
    assert sent['Accept'] == 'application/json'
    assert sent['If-None-Match'] == 'e'


# --- interpreting the answer ----------------------------------------------

def test_304_reports_unchanged_and_costs_nothing():
    session = FakeSession(FakeResponse(NOT_MODIFIED, {'ETag': 'W/"abc"'}))
    result = conditional_get(session, 'https://api/x', etag='W/"abc"')

    assert result.unchanged
    assert not result.spent_quota, '304 does not count against the rate limit'
    assert result.payload is None


def test_200_reports_changed_and_carries_the_new_etag():
    session = FakeSession(
        FakeResponse(
            200, {'ETag': 'W/"new"'},
            {'pushed_at': '2026-09-14T00:00:00Z'},
        ),
    )
    result = conditional_get(session, 'https://api/x', etag='W/"old"')

    assert not result.unchanged
    assert result.spent_quota
    assert result.etag == 'W/"new"'
    assert result.payload == {'pushed_at': '2026-09-14T00:00:00Z'}


def test_a_missing_etag_header_is_tolerated():
    session = FakeResponse(200, {}, {})
    result = conditional_get(FakeSession(session), 'https://api/x')
    assert result.etag is None
    assert result.payload == {}


def test_404_is_reported_as_absent_rather_than_raised():
    session = FakeSession(FakeResponse(404))
    result = conditional_get(session, 'https://api/x')
    assert result.absent
    assert result.payload is None


@pytest.mark.parametrize('status', [500, 502, 503])
def test_server_errors_are_reported_not_raised(status):
    result = conditional_get(
        FakeSession(
            FakeResponse(status),
        ), 'https://api/x',
    )
    assert result.failed
    assert not result.unchanged


def test_transport_failure_is_reported_not_raised():
    import requests
    result = conditional_get(
        FakeSession(requests.ConnectionError('reset')), 'https://api/x',
    )
    assert result.failed
    assert result.error


def test_unparsable_body_is_a_failure_not_a_change():
    session = FakeSession(FakeResponse(200, {'ETag': 'e'}, payload=None))
    result = conditional_get(session, 'https://api/x')
    assert result.failed


# --- the shape callers rely on --------------------------------------------

def test_a_result_is_exactly_one_of_the_four_outcomes():
    outcomes = [
        conditional_get(FakeSession(FakeResponse(NOT_MODIFIED)), 'u'),
        conditional_get(FakeSession(FakeResponse(200, payload={})), 'u'),
        conditional_get(FakeSession(FakeResponse(404)), 'u'),
        conditional_get(FakeSession(FakeResponse(503)), 'u'),
    ]
    for result in outcomes:
        flags = [
            result.unchanged, result.changed,
            result.absent, result.failed,
        ]
        assert sum(flags) == 1, result


def test_result_is_immutable():
    result = ConditionalResult(status=NOT_MODIFIED)
    with pytest.raises(AttributeError):
        result.status = 200
