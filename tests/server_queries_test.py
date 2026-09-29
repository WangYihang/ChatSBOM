"""The page's reads, versioned by snapshot (#144): GET /api/meta, and
GET /api/v/{snapshot}/{method}.

The Worker answered `POST /api/q`, `{method, params}`, with `no-store`,
and kept what it could under the dataset's version in a cache of its own
(web/src/d1/api.ts, cache.ts). Here the version is the snapshot's id
(#128, sections 2.4 and 2.5): the page asks `meta` which snapshot is
current, then each question as a GET under it, so that an answer is one
URL's for good, and anything between may keep it for a year. A snapshot
`CURRENT` no longer lists answers 410, and the page asks `meta` again.

Each of the 21 methods of `Dataset` is a path, by the page's name for
it, with its parameters in the query string, by the page's names for
them. Each value is taken as JSON would have carried it to the method,
which checks it as #142 checks it: the route checks nothing twice.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import re
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlsplit

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from chatsbom.dataset import Dataset
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.server.app import create_app
from chatsbom.server.app import POLICY
from chatsbom.server.settings import settings_from
from tests.dataset_contract_test import CALLS
from tests.dataset_contract_test import CONTRACT
from tests.dataset_contract_test import corpus
from tests.dataset_contract_test import label
from tests.dataset_contract_test import snake
from tests.dataset_open_test import empty
from tests.server_app_test import INDEX
from tests.server_settings_test import published

#: Two snapshots' ids, as `snapshot build` would name them.
FIRST = '0123456789abcdef'
SECOND = 'fedcba9876543210'
THIRD = '00000000000000ff'

#: Where cloudflared is, and two visitors it names.
TUNNEL = '172.30.0.2'
VISITOR = '203.0.113.7'
OTHER = '203.0.113.8'

IMMUTABLE = 'public, max-age=31536000, immutable'

# What the methods refuse values with, in #142's words.
NOT_A_LIMIT = '"limit" must be a whole number, at least 1'
NOT_AN_ECOSYSTEM = '"type" is not an ecosystem'
TOO_LONG = '"name" must be at most 256 characters'


def recorded(method: str, params: list[Any]) -> Any:
    """What D1 answered the contract suite's call, as recorded."""
    [answer] = [
        call['returns'] for call in CALLS
        if call['method'] == method and call['params'] == params
    ]
    return answer


#: What `meta` answers the contract's file with: D1's provenance.
CONTRACT_META = recorded('meta', [])

#: The URL the page asks for each recorded call, in the recording's
#: order, with its snapshot written as SNAPSHOT: what the page's client
#: asks (web/test/contracturls.test.ts).
URLS: list[dict[str, Any]] = json.loads(
    (CONTRACT / 'urls.json').read_text(encoding='utf-8'),
)


@pytest.fixture
def spa(tmp_path: Path) -> Path:
    root = tmp_path / 'client'
    (root / 'assets').mkdir(parents=True)
    (root / 'index.html').write_text(INDEX)
    return root


def service(spa: Path, tmp_path: Path, snapshot: Path | None, **environ: str) -> FastAPI:
    """The service, reading `snapshot`: a file, or the directory
    snapshots are published in."""
    settings = settings_from(
        {
            'ALTCHA_HMAC_KEY': 'k' * 32,
            'EDGE_SUBNET': '172.30.0.0/24',
            'WEB_STATE_DIR': str(tmp_path / 'state'),
            **({} if snapshot is None else {'WEB_SNAPSHOT': str(snapshot)}),
            **environ,
        },
        spa=spa,
    )
    return create_app(settings)


def visit(app: FastAPI, peer: str = '198.51.100.20') -> TestClient:
    return TestClient(app, client=(peer, 50000))


@pytest.fixture
def snapshots(tmp_path: Path) -> Path:
    """The contract's corpus, published as FIRST."""
    directory = tmp_path / 'snapshots'
    published(directory, corpus(tmp_path), FIRST)
    return directory


@pytest.fixture
def client(spa: Path, tmp_path: Path, snapshots: Path) -> Iterator[TestClient]:
    with visit(service(spa, tmp_path, snapshots)) as client:
        yield client


def camel(name: str) -> str:
    """`direct_only` as the page spells it: `directOnly`."""
    return re.sub(r'_([a-z])', lambda match: match.group(1).upper(), name)


def text(value: object) -> str:
    """A value as the page writes it in a query string."""
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)


def path(method: str, params: list[Any]) -> str:
    """A recorded call as the page is to ask it, after its snapshot:
    the method by the page's name, and each parameter by the page's
    name for the method's own, in order of the names. The call's
    positional values are the method's first parameters, and an object
    of options its keywords, as `tests/dataset_contract_test.ask` reads
    a call."""
    names = [
        name for name in inspect.signature(getattr(Dataset, snake(method))).parameters
        if name != 'self'
    ]
    query: dict[str, object] = {}
    positional = [param for param in params if not isinstance(param, dict)]
    for name, value in zip(names, positional):
        query[camel(name)] = value
    for param in params:
        if isinstance(param, dict):
            query.update(param)
    pairs = sorted((key, text(value)) for key, value in query.items())
    return f'{method}?{urlencode(pairs)}' if pairs else method


def get(client: TestClient, url: str, **headers: str) -> Any:
    return client.get(url, headers=headers)


class TestTheMeta:
    """Which snapshot is current, and where it came from: the one
    question the page asks before any other, and again when a snapshot
    it asked under is no longer served."""

    def test_names_the_current_snapshot_and_its_provenance(self, client):
        response = client.get('/api/meta')
        assert response.status_code == 200, response.text
        assert response.json() == {'snapshot': FIRST, **CONTRACT_META}

    def test_may_be_kept_for_a_minute(self, client):
        """So a pass that publishes is seen within a minute, and a page
        view costs the service no more than one ask a minute."""
        response = client.get('/api/meta')
        assert response.headers['cache-control'] == 'max-age=60'
        assert response.headers['content-type'] == 'application/json'

    def test_carries_the_pages_headers(self, client):
        headers = client.get('/api/meta').headers
        assert headers['content-security-policy'] == POLICY
        assert headers['x-content-type-options'] == 'nosniff'

    def test_names_one_published_since_without_a_restart(
        self, spa, tmp_path, snapshots,
    ):
        with visit(service(spa, tmp_path, snapshots)) as client:
            before = client.get('/api/meta').json()
            published(snapshots, empty(tmp_path / 'next.sqlite'), SECOND)
            after = client.get('/api/meta').json()
        assert before['snapshot'] == FIRST
        assert after == {
            'snapshot': SECOND, 'generator': '', 'schemaVersion': '',
            'observedFrom': '', 'observedTo': '',
        }

    def test_names_a_snapshot_files_own_id(self, spa, tmp_path):
        """WEB_SNAPSHOT may name one snapshot: its id is the one its
        `meta` holds, which `snapshot build` named its file by."""
        made = empty(tmp_path / 'made.sqlite')
        with closing(sqlite3.connect(made)) as db:
            db.execute(
                'INSERT INTO meta VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (
                    'chatsbom/9.9.9', '8', '2026-01-01', '2026-09-01',
                    SECOND, '9.9.9', 'all-2026-09-01', '{}',
                ),
            )
            db.commit()
        with visit(service(spa, tmp_path, made)) as client:
            said = client.get('/api/meta').json()
        assert said['snapshot'] == SECOND
        assert said['generator'] == 'chatsbom/9.9.9'

    def test_names_a_file_with_no_id_by_its_content(self, spa, tmp_path):
        """A file of D1's tables and the page table, as the tests make
        one, holds no id of its own: it is known by its bytes, so that
        other bytes are another snapshot to every cache."""
        made = corpus(tmp_path)
        expected = hashlib.sha256(made.read_bytes()).hexdigest()[:16]
        with visit(service(spa, tmp_path, made)) as client:
            said = client.get('/api/meta').json()
        assert said == {'snapshot': expected, **CONTRACT_META}

    def test_with_no_dataset_is_a_503(self, spa, tmp_path):
        with visit(service(spa, tmp_path, None)) as client:
            response = client.get('/api/meta')
        assert response.status_code == 503
        assert response.json() == {
            'error': 'No dataset is configured on this deployment.',
        }
        assert response.headers['cache-control'] == 'no-store'

    def test_with_current_gone_is_a_503(self, spa, tmp_path, snapshots):
        with visit(service(spa, tmp_path, snapshots)) as client:
            (snapshots / 'CURRENT').unlink()
            response = client.get('/api/meta')
        assert response.status_code == 503
        assert response.json() == {
            'error': 'The dataset cannot be read for a moment. Try again '
            'shortly.',
        }
        assert response.headers['cache-control'] == 'no-store'


class TestWhatThePageAsks:
    """The URL the page's client asks for each call the contract suite
    makes of D1, as it recorded them (`urls.json`), held to the service:
    a parameter the page names otherwise than the method's own fails
    here, where the page's own tests could not see it."""

    def test_is_asked_for_every_recorded_call(self):
        assert [(asked['method'], asked['params']) for asked in URLS] == [
            (call['method'], call['params']) for call in CALLS
        ]

    @pytest.mark.parametrize('asked', URLS, ids=label)
    def test_names_each_parameter_as_the_method_does(self, asked):
        """`meta` excepted: the page takes the provenance from
        /api/meta, with the snapshot."""
        url = urlsplit(asked['url'])
        if asked['method'] == 'meta':
            assert (url.path, url.query) == ('/api/meta', '')
            return
        expected = urlsplit(f'/{path(asked["method"], asked["params"])}')
        assert url.path == f'/api/v/SNAPSHOT{expected.path}'
        assert sorted(parse_qsl(url.query, keep_blank_values=True)) == sorted(
            parse_qsl(expected.query, keep_blank_values=True),
        )

    @pytest.mark.parametrize(
        'asked,call', list(zip(URLS, CALLS)),
        ids=[label(call) for call in CALLS],
    )
    def test_is_answered_as_d1_answered(self, client, asked, call):
        response = client.get(asked['url'].replace('/SNAPSHOT/', f'/{FIRST}/'))
        assert response.status_code == 200, response.text
        answer = response.json()
        if asked['url'] == '/api/meta':
            assert answer.pop('snapshot') == FIRST
        assert answer == call['returns']


class TestTheAnswers:
    """Every call the contract suite makes of D1, asked by the method's
    own names for its parameters, answered as D1 answered it (#142's
    `calls.json`)."""

    @pytest.mark.parametrize('call', CALLS, ids=label)
    def test_are_d1s(self, client, call):
        response = client.get(
            f'/api/v/{FIRST}/{path(call["method"], call["params"])}',
        )
        assert response.status_code == 200, response.text
        assert response.json() == call['returns']

    def test_are_kept_for_good(self, client):
        """A snapshot is immutable, so its answer to a URL is too."""
        response = client.get(f'/api/v/{FIRST}/totals')
        assert response.headers['cache-control'] == IMMUTABLE
        assert response.headers['content-type'] == 'application/json'

    def test_carry_the_pages_headers(self, client):
        headers = client.get(f'/api/v/{FIRST}/totals').headers
        assert headers['content-security-policy'] == POLICY
        assert headers['referrer-policy'] == 'strict-origin-when-cross-origin'

    def test_are_json_as_the_page_reads_it(self, client):
        count = client.get(f'/api/v/{FIRST}/countDependents?name=mail')
        nothing = client.get(f'/api/v/{FIRST}/edgeAmbiguity')
        assert count.text == str(
            recorded('countDependents', [{'name': 'mail'}]),
        )
        assert nothing.text == 'null'

    def test_under_a_snapshot_still_kept_are_that_snapshots(
        self, spa, tmp_path, snapshots,
    ):
        """A page that asked `meta` before a pass published goes on
        asking under the id it was told, and gets that snapshot's
        answers, beside a page that asks under the new one."""
        published(snapshots, empty(tmp_path / 'next.sqlite'), SECOND)
        with visit(service(spa, tmp_path, snapshots)) as client:
            kept = client.get(f'/api/v/{FIRST}/totals')
            current = client.get(f'/api/v/{SECOND}/totals')
        assert kept.status_code == current.status_code == 200
        assert kept.json()['tracked'] == 12
        assert current.json()['tracked'] == 0
        assert kept.headers['cache-control'] == IMMUTABLE

    def test_of_a_snapshot_file_are_under_its_id(self, spa, tmp_path):
        made = corpus(tmp_path)
        with visit(service(spa, tmp_path, made)) as client:
            snapshot = client.get('/api/meta').json()['snapshot']
            response = client.get(f'/api/v/{snapshot}/totals')
        assert response.status_code == 200
        with open_dataset(made) as dataset:
            assert response.json() == jsonable(dataset.totals())

    def test_are_asked_of_nothing_but_get(self, client):
        response = client.post(f'/api/v/{FIRST}/totals')
        assert response.status_code == 405
        assert response.headers['cache-control'] == 'no-store'


class TestARetiredSnapshot:
    """One `CURRENT` no longer lists: its answers may be kept anywhere
    already, but it is not asked any more, and the page is told to ask
    `meta` again."""

    def refused(self, response: Any) -> None:
        assert response.status_code == 410, response.text
        assert response.json() == {
            'error': 'This snapshot of the dataset is no longer served. '
            'Reload the page.',
        }
        assert response.headers['cache-control'] == 'no-store'

    def test_is_gone(self, spa, tmp_path, snapshots):
        with visit(service(spa, tmp_path, snapshots)) as client:
            # Published with nothing else kept: FIRST is retired.
            published(snapshots, empty(tmp_path / 'next.sqlite'), SECOND)
            (snapshots / 'CURRENT').write_text(f'{SECOND}\n')
            self.refused(client.get(f'/api/v/{FIRST}/totals'))
            assert client.get(f'/api/v/{SECOND}/totals').status_code == 200

    def test_is_gone_once_three_are_kept_after_it(
        self, spa, tmp_path, snapshots,
    ):
        """As `snapshot build` retires one: the current and two kept."""
        with visit(service(spa, tmp_path, snapshots)) as client:
            for number, id in enumerate([SECOND, THIRD, '1111111111111111']):
                published(
                    snapshots, empty(tmp_path / f'{number}.sqlite'), id,
                )
            self.refused(client.get(f'/api/v/{FIRST}/totals'))
            assert client.get(f'/api/v/{SECOND}/totals').status_code == 200

    @pytest.mark.parametrize(
        'snapshot',
        # `..` as a client that does not tidy the path first sends it.
        ['ffffffffffffffff', 'not-an-id', FIRST.upper(), '%2E%2E'],
    )
    def test_so_is_one_never_served(self, client, snapshot):
        self.refused(client.get(f'/api/v/{snapshot}/totals'))

    def test_so_is_one_listed_whose_file_is_gone(
        self, spa, tmp_path, snapshots,
    ):
        with visit(service(spa, tmp_path, snapshots)) as client:
            (snapshots / 'CURRENT').write_text(f'{FIRST}\n{SECOND}\n')
            self.refused(client.get(f'/api/v/{SECOND}/totals'))

    def test_a_snapshot_file_serves_its_own_id_alone(self, spa, tmp_path):
        made = corpus(tmp_path)
        with visit(service(spa, tmp_path, made)) as client:
            self.refused(client.get(f'/api/v/{FIRST}/totals'))

    def test_is_not_said_with_no_current_to_ask_for(
        self, spa, tmp_path, snapshots,
    ):
        """A 503, not a 410: asking `meta` again would not help."""
        with visit(service(spa, tmp_path, snapshots)) as client:
            (snapshots / 'CURRENT').unlink()
            response = client.get(f'/api/v/{FIRST}/totals')
        assert response.status_code == 503
        assert response.headers['cache-control'] == 'no-store'

    def test_with_no_dataset_is_a_503(self, spa, tmp_path):
        with visit(service(spa, tmp_path, None)) as client:
            response = client.get(f'/api/v/{FIRST}/totals')
        assert response.status_code == 503
        assert response.json() == {
            'error': 'No dataset is configured on this deployment.',
        }


class TestTheParameters:
    """The query string, taken as the method's own parameters: by the
    page's names, each value as JSON would have carried it, and checked
    by the method (#142), never by the route."""

    def refused(self, response: Any, status: int, error: str) -> None:
        assert response.status_code == status, response.text
        assert response.json() == {'error': error}
        assert response.headers['cache-control'] == 'no-store'

    @pytest.mark.parametrize(
        'method',
        [
            'nothing', 'dependents_of', 'DependentsOf', 'constructor',
            '__proto__', 'toString', '_rows', '__init__', 'relationshipByLanguage',
            'query',
        ],
    )
    def test_a_method_the_page_has_not_is_a_404(self, client, method):
        """The page's names for the 21 methods, and nothing else: not
        Python's, nor anything else of `Dataset`'s, nor the name the
        Worker kept for a page of the release before."""
        self.refused(
            client.get(f'/api/v/{FIRST}/{method}?name=mail'), 404,
            f'Unknown method: {method}',
        )

    @pytest.mark.parametrize(
        'url,key',
        [
            ('totals?limit=3', 'limit'),
            # Python's spelling is not the page's.
            ('dependentsOf?name=mail&direct_only=true', 'direct_only'),
            ('dependentsOf?name=mail&signal=1', 'signal'),
            ('topPackages?language=ruby', 'language'),
            ('countDependents?name=mail&limit=3', 'limit'),
        ],
    )
    def test_one_the_method_does_not_take_is_refused(self, client, url, key):
        self.refused(
            client.get(f'/api/v/{FIRST}/{url}'), 400,
            f'Unknown parameter: "{key}"',
        )

    def test_one_given_twice_is_refused(self, client):
        """Which of the two is meant is not the route's to guess."""
        self.refused(
            client.get(f'/api/v/{FIRST}/dependentsOf?name=mail&name=rails'),
            400, '"name" is given twice',
        )

    @pytest.mark.parametrize(
        'url,error',
        [
            *(
                (f'dependentsOf?name=mail&limit={limit}', NOT_A_LIMIT)
                for limit in (
                    '0', '-1', '2.5', 'ten', 'true', 'NaN', '1e400',
                    '9007199254740992', '%2220%22', '',
                )
            ),
            ('versionSpread?name=mail&limit=0', NOT_A_LIMIT),
            ('licenseShares?limit=0.5', NOT_A_LIMIT),
            (
                'dependentsOf?name=mail&offset=-1',
                '"offset" must be a whole number, at least 0',
            ),
            (
                'dependencyTree?name=mail&children=0',
                '"children" must be a whole number, at least 1',
            ),
            (
                'dependencyTree?name=mail&branch=-3',
                '"branch" must be a whole number, at least 1',
            ),
            *(
                (url, '"direct_only" must be true or false')
                for url in (
                    'dependentsOf?name=mail&directOnly=yes',
                    'dependentsOf?name=mail&directOnly=1',
                    'topPackages?directOnly=TRUE',
                )
            ),
            ('dependentsOf?name=mail&type=toString', NOT_AN_ECOSYSTEM),
            ('dependentsOf?name=mail&type=__proto__', NOT_AN_ECOSYSTEM),
            (
                'topPackages?ecosystem=constructor',
                '"ecosystem" is not an ecosystem',
            ),
            (
                'relationshipSplit?ecosystem=valueOf',
                '"ecosystem" is not an ecosystem',
            ),
            (
                'dependentsOf?name=mail&language=' + 'x' * 65,
                '"language" must be at most 64 characters',
            ),
            ('dependentsOf?name=' + 'x' * 257, TOO_LONG),
            # Counted as the page counts it: 129 rockets are 258 units.
            ('ecosystemsFor?name=' + '%F0%9F%9A%80' * 129, TOO_LONG),
            *(
                (url, '"name" must be a non-empty string')
                for url in (
                    'dependentsOf?name=', 'dependentsOf',
                    'dependentsOf?type=gem',
                )
            ),
            ('searchPackages', '"term" must be a non-empty string'),
            ('searchPackages?term=', '"term" must be a non-empty string'),
        ],
    )
    def test_what_the_method_refuses_is_a_400_in_its_words(
        self, client, url, error,
    ):
        self.refused(client.get(f'/api/v/{FIRST}/{url}'), 400, error)

    def test_a_number_is_taken_as_json_writes_it(self, client):
        """`2.0` is the whole number 2, to the method as to the page's
        endpoint before it."""
        whole = client.get(
            f'/api/v/{FIRST}/dependentsOf?name=laravel/framework&limit=2',
        )
        written = client.get(
            f'/api/v/{FIRST}/dependentsOf?name=laravel/framework&limit=2.0',
        )
        assert whole.status_code == written.status_code == 200
        assert len(whole.json()) == 2
        assert written.json() == whole.json()

    @pytest.mark.parametrize('term', ['1', 'true', 'null', '2.0', '[]'])
    def test_text_that_looks_like_a_number_is_still_text(self, client, term):
        """A name is text, whatever it spells: a package may be called
        `true`, and a search may begin with a digit."""
        found = client.get(
            f'/api/v/{FIRST}/searchPackages?{urlencode({"term": term})}',
        )
        looked = client.get(
            f'/api/v/{FIRST}/ecosystemsFor?{urlencode({"name": term})}',
        )
        assert found.status_code == looked.status_code == 200
        assert found.json() == looked.json() == []

    def test_a_flag_is_true_or_false(self, client):
        declared = client.get(
            f'/api/v/{FIRST}/countDependents?name=mail&directOnly=true',
        )
        all_of_them = client.get(
            f'/api/v/{FIRST}/countDependents?name=mail&directOnly=false',
        )
        assert declared.json() == recorded(
            'countDependents', [{'directOnly': True, 'name': 'mail'}],
        )
        assert all_of_them.json() == recorded(
            'countDependents', [{'name': 'mail'}],
        )
        assert declared.json() != all_of_them.json()

    def test_an_empty_word_is_one_left_out(self, client):
        """As the method takes it: an ecosystem of '' is none."""
        given = client.get(f'/api/v/{FIRST}/countDependents?name=mail&type=')
        left = client.get(f'/api/v/{FIRST}/countDependents?name=mail')
        assert given.json() == left.json() == recorded(
            'countDependents', [{'name': 'mail'}],
        )

    def test_the_order_of_the_query_does_not_change_the_answer(self, client):
        one = client.get(
            f'/api/v/{FIRST}/dependentsOf?name=mail&directOnly=true&limit=1',
        )
        other = client.get(
            f'/api/v/{FIRST}/dependentsOf?limit=1&directOnly=true&name=mail',
        )
        assert one.json() == other.json()

    def test_a_refusal_reaches_nothing(self, client, monkeypatch):
        """Refused before the snapshot is opened."""
        opened: list[Path] = []
        from chatsbom.server import queries

        def opening(path: Path) -> Any:
            opened.append(path)
            raise AssertionError('opened')

        monkeypatch.setattr(queries, 'open_dataset', opening)
        client.get(f'/api/v/{FIRST}/nothing')
        client.get(f'/api/v/{FIRST}/totals?limit=3')
        client.get('/api/v/ffffffffffffffff/totals')
        assert opened == []


class TestTheRateLimit:
    """QUERY_RATE_LIMIT, per client, as the Worker's `/api/q` counted
    it: every request, refused or not, and `meta` among them."""

    def test_refuses_a_client_past_it_and_no_other(self, spa, tmp_path, snapshots):
        app = service(spa, tmp_path, snapshots, QUERY_RATE_LIMIT='3/60')
        with visit(app, TUNNEL) as client:
            statuses = [
                get(client, url, **{'cf-connecting-ip': VISITOR}).status_code
                for url in (
                    '/api/meta', f'/api/v/{FIRST}/totals',
                    f'/api/v/{FIRST}/nothing', f'/api/v/{FIRST}/totals',
                )
            ]
            refused = get(client, '/api/meta', **{'cf-connecting-ip': VISITOR})
            other = get(
                client, f'/api/v/{FIRST}/totals', **{'cf-connecting-ip': OTHER},
            )
        assert statuses == [200, 200, 404, 429]
        assert refused.status_code == 429
        assert refused.json() == {'error': 'Too many queries. Wait a moment.'}
        assert refused.headers['cache-control'] == 'no-store'
        assert other.status_code == 200

    def test_is_not_the_chats(self, spa, tmp_path, snapshots):
        """A question and its challenge count against CHAT_RATE_LIMIT,
        and a page's queries against this: reading the page leaves the
        reader their questions."""
        app = service(
            spa, tmp_path, snapshots, QUERY_RATE_LIMIT='2/60',
            CHAT_RATE_LIMIT='50/60',
        )
        with visit(app) as client:
            for _ in range(2):
                assert client.get(f'/api/v/{FIRST}/totals').status_code == 200
            assert client.get(f'/api/v/{FIRST}/totals').status_code == 429
            # The chat is off without a key, and says so, rather than
            # "too many".
            assert client.get('/api/ask/challenge').status_code == 503


class TestAFailure:
    def test_is_a_500_that_says_nothing_of_it(self, client, monkeypatch):
        """A database's error names its tables and its SQL: logged, and
        never answered, as the Worker's were."""

        def broken(self: Dataset) -> None:
            raise sqlite3.OperationalError('no such table: agg_totals')

        monkeypatch.setattr(Dataset, 'totals', broken)
        response = client.get(f'/api/v/{FIRST}/totals')
        assert response.status_code == 500
        assert response.json() == {'error': 'The query could not be answered.'}
        assert response.headers['cache-control'] == 'no-store'
        assert 'agg_totals' not in response.text
