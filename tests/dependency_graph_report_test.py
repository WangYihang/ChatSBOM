"""GitHub's asynchronous SBOM report, behind `DependencyGraphService.fetch`.

The synchronous endpoint closes down. From the REST reference
(https://docs.github.com/en/rest/dependency-graph/sboms):

    This operation is closing down and will not be accessible after
    November 13, 2026. Please migrate to the asynchronous flow. Use
    "Request generation of a software bill of materials (SBOM) for a
    repository" to trigger the report, then "Fetch a software bill of
    materials (SBOM) for a repository" to retrieve it.

The pair, as documented there and in GitHub's OpenAPI description:

* `GET .../dependency-graph/sbom/generate-report` answers 201 with
  `sbom_url`, "URL to poll for the SBOM export result", or 403 or 404;
* `GET .../dependency-graph/sbom/fetch-report/{sbom_uuid}` answers 202,
  "SBOM is still being processed, no content is returned", then 302,
  "Redirects to a temporary download URL for the completed SBOM", or
  403 or 404. The changelog of 2026-04-14 calls the first answer 201.

What comes back from that URL is documented as "the SBOM in SPDX JSON
format", and has been seen to be the document itself, not the
`{"sbom": ...}` the synchronous endpoint wrapped it in
(ClickHouse/ClickBOM#119). Every reader of a stored graph expects the
wrapper, so the service puts it back when it is missing, and what is
stored is what the synchronous endpoint would have stored.

Only the transport and the clock are faked here: nothing waits, and
nothing reaches GitHub.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import requests
from requests.exceptions import RetryError

from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import FILES
from chatsbom.core.edges import edges_in
from chatsbom.core.fs import atomic_write_text
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from chatsbom.services.db_service import DbService
from chatsbom.services.db_service import graph_observed_at
from chatsbom.services.dependency_graph_service import ASYNC
from chatsbom.services.dependency_graph_service import AUTO
from chatsbom.services.dependency_graph_service import DependencyGraphService
from chatsbom.services.dependency_graph_service import flows_for
from chatsbom.services.dependency_graph_service import parse_spdx_document
from chatsbom.services.dependency_graph_service import SYNC
from chatsbom.services.dependency_graph_service import SYNC_REMOVAL

EXPORT = 'https://api.github.com/repos/github/example/dependency-graph/sbom'
GENERATE = f'{EXPORT}/generate-report'
#: GitHub's own example of `sbom_url`, from the OpenAPI description.
REPORT = f'{EXPORT}/fetch-report/4bab1a7e-da63-4828-9488-44e0e01a7c1b'
#: "a temporary download URL": signed, and on another host.
DOWNLOAD = 'https://sbom-exports.example/4bab1a7e.spdx.json?signature=s3cr3t'

BEFORE = date(2026, 9, 28)
AFTER = date(2026, 12, 1)

#: A spent token, as GitHub reports one: 2026-09-21 14:13:19 UTC.
SPENT = {'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset': '1789999999'}
RESET = datetime(2026, 9, 21, 14, 13, 19, tzinfo=timezone.utc)


#: What the synchronous endpoint answers: GitHub's own example from the
#: REST reference (`dependency-graph-export-sbom-response`), with what a
#: real repository adds to it — a package reached only through another,
#: a Maven one declared without a version, and a workflow action. The
#: SPDX document is wrapped in `sbom`.
EXPORTED: dict[str, Any] = {
    'sbom': {
        'SPDXID': 'SPDXRef-DOCUMENT',
        'spdxVersion': 'SPDX-2.3',
        'creationInfo': {
            # In another zone, with a fraction of a second, as a document
            # may state it: 03:56:20 UTC, to the second.
            'created': '2026-09-14T11:56:20.924281+08:00',
            'creators': ['Tool: GitHub.com-Dependency-Graph'],
        },
        'name': 'github/example',
        'dataLicense': 'CC0-1.0',
        'documentNamespace': (
            'https://spdx.org/spdxdocs/protobom/'
            '15e41dd2-f961-4f4d-b8dc-f8f57ad70d57'
        ),
        'packages': [
            {
                'name': 'rails',
                'SPDXID': 'SPDXRef-Package',
                'versionInfo': '1.0.0',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'licenseConcluded': 'MIT',
                'licenseDeclared': 'MIT',
                'copyrightText': 'Copyright (c) 1985 GitHub.com',
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:gem/rails@1.0.0',
                }],
            },
            {
                'name': 'github/example',
                'SPDXID': 'SPDXRef-Repository',
                'versionInfo': 'main',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:github/example@main',
                }],
            },
            {
                'name': 'npm:body-parser',
                'SPDXID': 'SPDXRef-npm-body-parser-1.20.2',
                'versionInfo': '1.20.2',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'licenseConcluded': 'MIT',
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:npm/body-parser@1.20.2',
                }],
            },
            {
                'name': 'npm:bytes',
                'SPDXID': 'SPDXRef-npm-bytes-3.1.2',
                'versionInfo': '3.1.2',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'licenseConcluded': 'NOASSERTION',
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:npm/bytes@3.1.2',
                }],
            },
            {
                'name': (
                    'maven:org.springframework.boot:spring-boot-starter-web'
                ),
                'SPDXID': 'SPDXRef-maven-spring-boot-starter-web',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': (
                        'pkg:maven/org.springframework.boot/'
                        'spring-boot-starter-web'
                    ),
                }],
            },
            {
                'name': 'actions:actions/checkout',
                'SPDXID': 'SPDXRef-githubactions-actions-checkout-4',
                'versionInfo': '4',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:githubactions/actions/checkout@4',
                }],
            },
        ],
        'relationships': [
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-Repository',
                'relatedSpdxElement': 'SPDXRef-Package',
            },
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-Repository',
                'relatedSpdxElement': 'SPDXRef-npm-body-parser-1.20.2',
            },
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-npm-body-parser-1.20.2',
                'relatedSpdxElement': 'SPDXRef-npm-bytes-3.1.2',
            },
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-Repository',
                'relatedSpdxElement': 'SPDXRef-maven-spring-boot-starter-web',
            },
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-Repository',
                'relatedSpdxElement': 'SPDXRef-githubactions-actions-checkout-4',
            },
            {
                'relationshipType': 'DESCRIBES',
                'spdxElementId': 'SPDXRef-DOCUMENT',
                'relatedSpdxElement': 'SPDXRef-Repository',
            },
        ],
    },
}

#: The same repository's report, as the asynchronous flow has been seen
#: to download it: "the SBOM in SPDX JSON format", without the wrapper.
DOWNLOADED: dict[str, Any] = EXPORTED['sbom']

#: The instant both state.
CREATED = datetime(2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc)


class Answer:
    """One HTTP answer. The body goes over the wire as text, so what
    `json()` hands back is a parse of it, as a real response's is."""

    def __init__(
        self,
        status_code: int,
        body: Any = None,
        headers: dict[str, str] | None = None,
        text: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self._text = text if text is not None else (
            None if body is None else json.dumps(body)
        )

    def json(self) -> Any:
        if self._text is None:
            raise ValueError('no content')
        return json.loads(self._text)


class Transport:
    """A session answering each URL from its own script, in turn, with
    the last answer repeating. A URL with no script is a test error."""

    def __init__(self, script: dict[str, list[Any]]) -> None:
        self._script = {url: list(answers) for url, answers in script.items()}
        self.asked: list[str] = []
        self.options: list[dict[str, Any]] = []

    def get(self, url: str, headers: Any = None, **options: Any) -> Any:
        self.asked.append(url)
        self.options.append(options)
        answers = self._script[url]
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        if isinstance(answer, BaseException):
            raise answer
        return answer


class Clock:
    """Time that passes only when the service sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


ACCEPTED = Answer(201, {'sbom_url': REPORT})
NOT_YET = Answer(202)
READY = Answer(302, headers={'Location': DOWNLOAD})


@dataclass
class Rig:
    """The service over a scripted GitHub, and what it asked of it."""

    api: Transport
    downloads: Transport
    clock: Clock
    service: DependencyGraphService = field(init=False)
    flow: str | None = ASYNC
    today: date = BEFORE
    max_wait: float | None = None

    def __post_init__(self) -> None:
        github = SimpleNamespace(session=self.api, plain_session=self.api)
        self.service = DependencyGraphService(
            github,  # type: ignore[arg-type]
            self.flow,
            today=self.today,
            downloads=self.downloads,
            max_wait=self.max_wait,
            sleep=self.clock.sleep,
            monotonic=self.clock.monotonic,
        )

    def fetch(self) -> ConditionalResult:
        return self.service.fetch('github', 'example')

    @property
    def polls(self) -> int:
        return self.api.asked.count(REPORT)


def rig(
    *,
    accepted: Any = ACCEPTED,
    polls: list[Any] | None = None,
    download: Any = None,
    exported: Any = None,
    flow: str | None = ASYNC,
    today: date = BEFORE,
    max_wait: float | None = None,
) -> Rig:
    """GitHub, answering each request as scripted: `accepted` for the
    report asked for, `polls` for each look at it in turn, `download`
    for the temporary URL, and `exported` for the synchronous endpoint."""
    script: dict[str, list[Any]] = {
        GENERATE: [accepted],
        REPORT: polls if polls is not None else [READY],
    }
    if exported is not None:
        script[EXPORT] = [exported]
    if download is None:
        download = Answer(200, DOWNLOADED)
    return Rig(
        api=Transport(script),
        downloads=Transport({DOWNLOAD: [download]}),
        clock=Clock(),
        flow=flow,
        today=today,
        max_wait=max_wait,
    )


# --- accepted, pending, ready ------------------------------------------------

def test_a_report_is_asked_for_waited_on_and_downloaded():
    github = rig(polls=[NOT_YET, NOT_YET, READY])

    result = github.fetch()

    assert result.changed
    assert result.payload == EXPORTED, 'as the synchronous endpoint said it'
    assert github.api.asked == [GENERATE, REPORT, REPORT, REPORT]
    assert github.downloads.asked == [DOWNLOAD]


def test_a_report_ready_at_the_first_look_costs_two_requests():
    """Each look at the report is a request against the dependency-graph
    bucket, which is metered at about 100 an hour, so the first is not
    sent the moment the report is asked for."""
    github = rig(polls=[READY])

    result = github.fetch()

    assert result.changed
    assert github.clock.slept == [2.0]
    assert github.service.requests == 2, 'generate, then one look'


def test_every_request_to_the_api_is_counted_and_the_download_is_not():
    """`chatsbom run` bounds a pass by what the services counted. The
    download is from a signed URL off the API, which no rate limit
    meters."""
    github = rig(polls=[NOT_YET, NOT_YET, READY])

    github.fetch()

    assert github.service.requests == 4


def test_the_wait_between_looks_backs_off():
    github = rig(polls=[NOT_YET, NOT_YET, NOT_YET, NOT_YET, READY])

    assert github.fetch().changed
    assert github.clock.slept == [2.0, 4.0, 8.0, 16.0, 30.0]


def test_a_report_never_ready_is_pending_after_a_bounded_wait():
    """Not a failure — nothing went wrong — and not "no graph". The
    caller asks again later."""
    github = rig(polls=[NOT_YET], max_wait=60)

    result = github.fetch()

    assert result.pending
    assert not result.changed and not result.failed and not result.absent
    assert result.payload is None
    assert sum(github.clock.slept) <= 60
    assert github.polls == 5
    assert github.downloads.asked == []


@pytest.mark.parametrize('max_wait', [0, 1, 10, 45, 120])
def test_the_wait_never_passes_its_bound(max_wait):
    github = rig(polls=[NOT_YET], max_wait=max_wait)

    assert github.fetch().pending
    assert sum(github.clock.slept) <= max_wait


def test_the_default_wait_is_bounded():
    github = rig(polls=[NOT_YET])

    assert github.fetch().pending
    assert 0 < sum(github.clock.slept) <= DependencyGraphService.REPORT_WAIT


def test_the_changelogs_201_is_not_ready_either():
    """The REST reference says 202 until the report is ready; the
    changelog that announced the pair says 201. Either is "not yet":
    only the redirect says the report is there."""
    github = rig(polls=[Answer(201), Answer(200), READY])

    assert github.fetch().changed
    assert github.polls == 3


def test_retry_after_on_a_pending_answer_is_honoured():
    github = rig(polls=[Answer(202, headers={'Retry-After': '7'}), READY])

    assert github.fetch().changed
    assert github.clock.slept == [2.0, 7.0]


def test_a_retry_after_shorter_than_the_backoff_does_not_hurry_it():
    github = rig(polls=[Answer(202, headers={'Retry-After': '1'}), READY])

    assert github.fetch().changed
    assert github.clock.slept == [2.0, 4.0]


def test_a_retry_after_beyond_the_bound_is_not_slept_through():
    """Waiting as long as GitHub asks would pass the bound; asking
    sooner would ignore it. So it stops, pending, and says when."""
    github = rig(
        polls=[Answer(202, headers={'Retry-After': '600'})], max_wait=60,
    )

    result = github.fetch()

    assert result.pending
    assert github.clock.slept == [2.0]
    assert result.rate_limit.retry_after == 600


def test_a_retry_after_on_the_acceptance_delays_the_first_look():
    github = rig(
        accepted=Answer(201, {'sbom_url': REPORT}, {'Retry-After': '5'}),
    )

    assert github.fetch().changed
    assert github.clock.slept == [5.0]


# --- the report is GitHub's, and the download is not the API ----------------

def test_the_report_is_looked_at_without_following_its_redirect():
    """The session that looks carries the token. Following the redirect
    itself, it would fetch the document from the temporary URL with the
    API's headers; the document comes through the other session."""
    github = rig(polls=[NOT_YET, READY])

    github.fetch()

    looks = [
        options for url, options in zip(
            github.api.asked, github.api.options,
        )
        if url == REPORT
    ]
    assert looks and all(
        options.get('allow_redirects') is False for options in looks
    )


def test_the_download_goes_through_the_session_without_the_token():
    github = rig()

    github.fetch()

    assert DOWNLOAD not in github.api.asked
    assert github.downloads.asked == [DOWNLOAD]


def test_the_default_download_session_has_no_token():
    github = SimpleNamespace(session=None, plain_session=None)
    service = DependencyGraphService(github, ASYNC)

    assert 'Authorization' not in service.downloads.headers


def test_a_relative_redirect_is_read_against_the_report():
    located = 'https://api.github.com/exports/x.spdx.json'
    github = Rig(
        api=Transport({
            GENERATE: [ACCEPTED],
            REPORT: [Answer(302, headers={'location': '/exports/x.spdx.json'})],
        }),
        downloads=Transport({located: [Answer(200, DOWNLOADED)]}),
        clock=Clock(),
    )

    assert github.fetch().changed
    assert github.downloads.asked == [located]


@pytest.mark.parametrize(
    'sbom_url',
    [
        None,
        '',
        42,
        'https://sbom-exports.example/fetch-report/4bab1a7e',
        'http://api.github.com/repos/github/example/dependency-graph/sbom/'
        'fetch-report/4bab1a7e',
    ],
    ids=['missing', 'empty', 'not-a-string', 'another-host', 'plain-http'],
)
def test_a_report_url_anywhere_but_the_api_is_a_failure(sbom_url):
    """The look carries the token, so it goes to the API or nowhere."""
    body = {} if sbom_url is None else {'sbom_url': sbom_url}
    github = rig(accepted=Answer(201, body))

    result = github.fetch()

    assert result.failed
    assert not result.absent
    assert result.payload is None
    assert github.api.asked == [GENERATE]


# --- the request for a report ---------------------------------------------------

def test_no_graph_is_absent_when_the_report_is_asked_for():
    """404, as the synchronous endpoint answered for a repository without
    a graph — and nothing is waited for."""
    github = rig(accepted=Answer(404, {'message': 'Not Found'}))

    result = github.fetch()

    assert result.absent
    assert github.api.asked == [GENERATE]
    assert github.clock.slept == []


REFUSALS = [
    (429, SPENT),
    (403, SPENT),
    (403, {'X-RateLimit-Remaining': '4000', 'Retry-After': '60'}),
]
REFUSAL_IDS = ['429', '403-no-quota-left', '403-secondary-limit']


@pytest.mark.parametrize('status,headers', REFUSALS, ids=REFUSAL_IDS)
def test_a_refusal_when_the_report_is_asked_for_is_rate_limited(
    status, headers,
):
    github = rig(
        accepted=Answer(status, {'message': 'rate limited'}, headers),
    )

    result = github.fetch()

    assert result.rate_limited
    assert not result.absent and not result.failed
    assert github.api.asked == [GENERATE]


@pytest.mark.parametrize(
    'answer',
    [
        Answer(500, {'message': 'Server Error'}),
        Answer(403, {'message': 'Forbidden'}, {'X-RateLimit-Remaining': '9'}),
        Answer(201, text='<html>'),
        RetryError('max retries'),
        requests.ConnectionError('reset'),
        requests.Timeout('slow'),
    ],
    ids=[
        'server-error', 'forbidden', 'unparsable', 'retries-exhausted',
        'connection', 'timeout',
    ],
)
def test_a_failure_when_the_report_is_asked_for_is_a_failure(answer):
    github = rig(accepted=answer)

    result = github.fetch()

    assert result.failed
    assert not result.absent
    assert github.api.asked == [GENERATE]


# --- looking at the report -------------------------------------------------------

@pytest.mark.parametrize('status,headers', REFUSALS, ids=REFUSAL_IDS)
def test_a_refusal_while_looking_is_rate_limited_not_no_graph(
    status, headers,
):
    github = rig(polls=[NOT_YET, Answer(status, {'message': 'x'}, headers)])

    result = github.fetch()

    assert result.rate_limited
    assert not result.absent and not result.pending
    assert github.polls == 2, 'no look after a refusal'
    assert github.downloads.asked == []


def test_a_refusal_while_looking_says_when_to_come_back():
    github = rig(polls=[Answer(429, {'message': 'x'}, SPENT)])

    result = github.fetch()

    assert result.rate_limit.resumes_at(datetime.now(timezone.utc)) == RESET


@pytest.mark.parametrize(
    'answer',
    [
        # The report GitHub accepted a moment before is not there: an
        # expired or lost report, which says nothing about the graph.
        Answer(404, {'message': 'Not Found'}),
        Answer(500, {'message': 'Server Error'}),
        Answer(403, {'message': 'Forbidden'}, {'X-RateLimit-Remaining': '9'}),
        # Ready, but without saying where.
        Answer(302),
        # Ready, but not over https.
        Answer(302, headers={'Location': 'http://sbom-exports.example/x'}),
        RetryError('max retries'),
        requests.ConnectionError('reset'),
    ],
    ids=[
        'not-found', 'server-error', 'forbidden', 'no-location',
        'plain-http', 'retries-exhausted', 'connection',
    ],
)
def test_a_failure_while_looking_is_a_failure_not_no_graph(answer):
    github = rig(polls=[NOT_YET, answer])

    result = github.fetch()

    assert result.failed
    assert not result.absent and not result.pending
    assert github.downloads.asked == []


# --- the download ------------------------------------------------------------------

@pytest.mark.parametrize(
    'answer',
    [
        # A signed URL that expired, or an object that is gone.
        Answer(403, text='<Error>AuthenticationFailed</Error>'),
        Answer(404, text='<Error>BlobNotFound</Error>'),
        Answer(500, text='<Error>InternalError</Error>'),
        RetryError('max retries'),
        requests.ConnectionError('reset'),
    ],
    ids=['expired', 'gone', 'server-error', 'retries-exhausted', 'connection'],
)
def test_a_failed_download_is_a_failure_not_no_graph(answer):
    github = rig(download=answer)

    result = github.fetch()

    assert result.failed
    assert not result.absent and not result.rate_limited
    assert result.payload is None


@pytest.mark.parametrize(
    'answer',
    [
        Answer(200, text='not json'),
        Answer(200, ['not', 'a', 'document']),
        Answer(200, {'unexpected': True}),
        Answer(200, {'sbom': 'not a document'}),
    ],
    ids=['unparsable', 'a-list', 'no-spdx', 'sbom-not-an-object'],
)
def test_a_download_that_is_not_an_spdx_document_is_a_failure(answer):
    """Nothing to store, and no evidence that there is no graph."""
    github = rig(download=answer)

    result = github.fetch()

    assert result.failed
    assert not result.absent
    assert result.payload is None


def test_a_report_that_comes_wrapped_is_stored_as_it_came():
    github = rig(download=Answer(200, EXPORTED))

    assert github.fetch().payload == EXPORTED


# --- which endpoint ----------------------------------------------------------------

@pytest.mark.parametrize(
    'setting,today,flows',
    [
        ('sync', BEFORE, (SYNC,)),
        ('async', BEFORE, (ASYNC,)),
        ('auto', BEFORE, (SYNC, ASYNC)),
        (None, BEFORE, (SYNC, ASYNC)),
        ('', BEFORE, (SYNC, ASYNC)),
        (' Async ', BEFORE, (ASYNC,)),
        # A setting it cannot read is not a reason to stop collecting.
        ('asynchronous', BEFORE, (SYNC, ASYNC)),
        ('auto', SYNC_REMOVAL, (ASYNC,)),
        ('auto', AFTER, (ASYNC,)),
        (None, AFTER, (ASYNC,)),
        ('async', AFTER, (ASYNC,)),
        # Asked for by name, it is asked; the log says it is closed.
        ('sync', AFTER, (SYNC,)),
    ],
)
def test_which_endpoint_is_asked(setting, today, flows):
    assert flows_for(setting, today) == flows


def test_the_endpoint_is_chosen_from_the_environment(monkeypatch):
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_API', 'async')
    github = rig(flow=None)

    assert github.service.flows == (ASYNC,)


def test_unset_is_auto(monkeypatch):
    monkeypatch.delenv('CHATSBOM_DEPGRAPH_API', raising=False)

    assert rig(flow=None).service.flows == flows_for(AUTO, BEFORE)


def test_auto_falls_back_to_a_report_when_the_synchronous_endpoint_fails():
    """What the pair was built for: the synchronous endpoint gives up
    after ten seconds, and spring-boot answers it 500 "Request timed
    out". A brownout before the closure fails the same way."""
    github = rig(flow=AUTO, exported=Answer(500, {'message': 'timed out'}))

    result = github.fetch()

    assert result.changed
    assert result.payload == EXPORTED
    assert github.api.asked[:2] == [EXPORT, GENERATE]
    assert github.service.requests == 3


@pytest.mark.parametrize(
    'answer,outcome',
    [
        (Answer(200, EXPORTED), 'changed'),
        (Answer(404, {'message': 'Not Found'}), 'absent'),
        (Answer(429, {'message': 'x'}, SPENT), 'rate_limited'),
    ],
    ids=['document', 'no-graph', 'refused'],
)
def test_auto_takes_the_synchronous_answer_when_it_is_one(answer, outcome):
    """Only a failure is worth the report's requests: no graph and a
    refused token are answers, and would be the same from the pair."""
    github = rig(flow=AUTO, exported=answer)

    result = github.fetch()

    assert getattr(result, outcome)
    assert github.api.asked == [EXPORT]


def test_from_the_closure_auto_asks_only_for_reports():
    github = rig(flow=AUTO, today=SYNC_REMOVAL)

    assert github.fetch().changed
    assert EXPORT not in github.api.asked


def test_sync_is_the_synchronous_endpoint_alone():
    github = rig(flow=SYNC, exported=Answer(500, {'message': 'timed out'}))

    assert github.fetch().failed
    assert github.api.asked == [EXPORT]


def test_a_pending_report_is_no_artifacts():
    service = rig(polls=[NOT_YET]).service
    assert service.artifacts_for('github', 'example') is None


# --- the same graph, whichever way it came ---------------------------------------

@pytest.fixture
def both(tmp_path) -> tuple[Path, Path]:
    """The graph stored by each flow, as `github depgraph` and `run`
    store it: `json.dumps` of the payload, written whole."""
    old = rig(flow=SYNC, exported=Answer(200, EXPORTED)).fetch()
    new = rig(flow=ASYNC).fetch()
    assert old.changed and new.changed

    paths = tmp_path / 'sync.spdx.json', tmp_path / 'async.spdx.json'
    for path, result in zip(paths, (old, new)):
        atomic_write_text(path, json.dumps(result.payload, ensure_ascii=False))
    return paths


def test_both_flows_store_the_same_bytes(both):
    old, new = both
    assert new.read_bytes() == old.read_bytes()


def test_both_flows_parse_into_the_same_artifact_rows(both):
    old, new = (FILES.get(DEPGRAPH, 1, str(path)) for path in both)
    assert old is not None and new is not None

    rows = parse_spdx_document(new.body)
    assert rows == parse_spdx_document(old.body)
    # And they are the rows a graph should give: the repository and its
    # workflow action are not dependencies, the root's own are declared,
    # and a package reached through another is inherited.
    assert {row['name']: row['relationship'] for row in rows} == {
        'rails': DIRECT,
        'npm:body-parser': DIRECT,
        'npm:bytes': TRANSITIVE,
        'maven:org.springframework.boot:spring-boot-starter-web': DIRECT,
    }


def test_both_flows_give_the_same_edges(both):
    old, new = (json.loads(path.read_text()) for path in both)
    assert edges_in(new) == edges_in(old) == {('npm:body-parser', 'npm:bytes')}


def test_both_flows_are_observed_at_the_instant_the_document_states(both):
    """#22 keys graph rows by `creationInfo.created`, and the report
    states it just as the export did."""
    old, new = (FILES.get(DEPGRAPH, 1, str(path)) for path in both)
    assert graph_observed_at(new) == graph_observed_at(old) == CREATED


def test_both_flows_index_into_the_same_rows(both):
    old, new = (FILES.get(DEPGRAPH, 1, str(path)) for path in both)
    assert old is not None and new is not None
    repository = {'default_branch': 'main', 'sbom_commit_sha': 'c' * 40}
    service = DbService()

    rows = service.parse_dependency_graph(new, 7, repository)
    assert rows == service.parse_dependency_graph(old, 7, repository)
    assert {row['observed_at'] for row in rows} == {CREATED}
