"""GitHub's dependency graph as a second SBOM source.

Syft reads lockfiles, so Maven and Composer projects — which often ship
none — come back nearly empty: 0 packages for spring-boot, 0 for
elasticsearch, 0 for ghidra. GitHub parses the manifests server-side and
reports 303, 107 and 147 for those same repositories.

What it returns is narrower than Syft's output in two ways that must be
carried through rather than glossed over:

* the graph is **flat** — the document DESCRIBES the repository, and the
  repository DEPENDS_ON each package, with no tree — so every row is a
  *declared* dependency, never a transitive one;
* `versionInfo` is the manifest's constraint (`>= 0`) or absent, not a
  resolution, so it is classified rather than stored as if exact.

Two package kinds are dropped: the repository's own `pkg:github/...`
entry, which is the SPDX document subject rather than a dependency, and
`pkg:githubactions/...` entries, which are workflow steps rather than
anything the project ships.

GitHub serves the document two ways, and the first is closing down. The
synchronous endpoint answers with it; from 2026-11-13 there is only the
asynchronous pair, which generates a report and serves it once ready.
`DependencyGraphService.fetch` hides which one answered, and hands back
what the synchronous endpoint would have, so what is stored, landed and
indexed does not change.
"""
import json
import os
import time
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import replace
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from urllib.parse import urlsplit

import requests
import structlog

from chatsbom.core.client import get_plain_client
from chatsbom.core.conditional import conditional_get
from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.conditional import RateLimit
from chatsbom.core.conditional import Session
from chatsbom.models.provenance import classify_version
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('dependency_graph')

#: purl types that describe the repository or its CI, not its dependencies.
EXCLUDED_ECOSYSTEMS = frozenset({'github', 'githubactions'})

PURL_REFERENCE_TYPE = 'purl'

#: The ways `fetch` can ask, as `CHATSBOM_DEPGRAPH_API` names them.
SYNC = 'sync'
ASYNC = 'async'
AUTO = 'auto'

#: The day GitHub stops serving the synchronous endpoint. The REST
#: reference: "This operation is closing down and will not be accessible
#: after November 13, 2026"; the changelog that deprecated it: "slated
#: for removal in six months, on November 13, 2026"; the OpenAPI
#: description: `removalDate: 2026-11-13`. Taken as the day it goes, in
#: UTC: a day of reports too early costs a request per repository, where
#: a day of the closed endpoint too late costs a pass.
SYNC_REMOVAL = date(2026, 11, 13)


def flows_for(setting: str | None, today: date) -> tuple[str, ...]:
    """The flows `fetch` tries, in order, for `CHATSBOM_DEPGRAPH_API`.

    * `sync`: the synchronous endpoint alone, as before. Named, it is
      asked even once closed, and the log says so;
    * `async`: the generate/fetch report pair alone, from now;
    * `auto`, and unset: the synchronous endpoint while GitHub serves it,
      then a report if it failed; reports alone from `SYNC_REMOVAL`.

    `auto` asks the endpoint first because it costs one request, where a
    report costs two or more, against a bucket metered at about 100 an
    hour. It falls back on a failure because that is what the pair was
    built for: the endpoint "had a hard-coded timeout value of ten
    seconds", which large repositories exceed — spring-boot answers it
    500 "Request timed out" — and a brownout before the closure would
    fail the same way. No graph and a refused token are answers, which a
    report would only repeat.

    A setting it cannot read is `auto`, and says so: a typo is not a
    reason to stop collecting.
    """
    choice = (setting or '').strip().lower() or AUTO
    if choice not in (SYNC, ASYNC, AUTO):
        logger.warning(
            'Unknown CHATSBOM_DEPGRAPH_API, using auto',
            setting=setting, choices=[SYNC, ASYNC, AUTO],
        )
        choice = AUTO
    closed = today >= SYNC_REMOVAL
    if choice == SYNC:
        if closed:
            logger.warning(
                'CHATSBOM_DEPGRAPH_API=sync asks a closed endpoint; '
                'unset it to fetch reports instead',
                closed_on=str(SYNC_REMOVAL),
            )
        return (SYNC,)
    if choice == ASYNC or closed:
        return (ASYNC,)
    return (SYNC, ASYNC)


def parse_spdx_document(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Project a dependency-graph SPDX document into artifact row mappings.

    Rows carry the same column names `db_service.parse_artifacts` emits,
    so both sources land in one table.
    """
    sbom = payload.get('sbom')
    if not isinstance(sbom, Mapping):
        raise ValueError('response has no sbom document')

    declared = _root_dependencies(sbom)
    rows: list[dict[str, Any]] = []
    for package in sbom.get('packages') or []:
        if not isinstance(package, Mapping):
            continue

        purl = _purl_of(package)
        if purl is None:
            # No purl means no ecosystem, which makes the row unusable.
            continue

        ecosystem = _ecosystem_of(purl)
        if ecosystem in EXCLUDED_ECOSYSTEMS:
            continue

        name = str(package.get('name') or '').strip()
        if not name:
            continue

        version, version_kind = classify_version(package.get('versionInfo'))

        spdx_id = str(package.get('SPDXID') or '')
        rows.append({
            'artifact_id': spdx_id,
            'name': name,
            'version': version,
            'version_kind': version_kind,
            'type': ecosystem,
            'purl': purl,
            'found_by': 'github-dependency-graph',
            'licenses': _licenses_of(package),
            # Declared if the document says the repository depends on it
            # directly, inherited otherwise. See `_root_dependencies`
            # for why this is not simply DIRECT.
            'relationship': DIRECT if spdx_id in declared else TRANSITIVE,
            'source': DEPGRAPH,
        })

    return rows


def _root_dependencies(sbom: Mapping[str, Any]) -> set[str]:
    """SPDX ids the repository itself depends on.

    Every package in one of these documents used to be recorded as
    `direct`, on the belief — written into the code as "the graph is
    flat, so everything in it was declared" — that GitHub's dependency
    graph reports manifests only. It does not, and the same wrong
    reading had already been corrected once in `core/edges.py`: measured
    across 420 documents, 94.4% of Go edges and 93.4% of JavaScript
    edges run between packages rather than out of the root.

    What that cost is specific. Weighted by package count over 400
    documents, **17.3%** of the packages are root dependencies and 82.7%
    are reached through another package — so about 11 million of the
    13,263,227 stored rows claimed to be declared when they were
    inherited. The dashboard's headline read "70.7% of dependency
    records are declared outright" against a truer 14.0%, and its
    declared-only ranking returned `semver, debug, ms, glob, which` —
    npm plumbing nobody chooses — where the same ranking over resolved
    closures gives `typescript, eslint, prettier, react`.

    The distinction is in the document: the root is whatever `DESCRIBES`
    points at, and a `DEPENDS_ON` leaving the root names a declared
    dependency. A document with no relationships at all yields an empty
    set, and every package in it falls to `transitive` — the
    conservative direction, since claiming a dependency was declared is
    the error that misleads. None of the 250 documents sampled was
    actually relationship-free.
    """
    relationships = sbom.get('relationships') or []
    if not isinstance(relationships, list):
        return set()

    roots = {
        r['relatedSpdxElement']
        for r in relationships
        if isinstance(r, Mapping)
        and r.get('relationshipType') == 'DESCRIBES'
        and r.get('relatedSpdxElement')
    }
    return {
        str(r['relatedSpdxElement'])
        for r in relationships
        if isinstance(r, Mapping)
        and r.get('relationshipType') == 'DEPENDS_ON'
        and r.get('spdxElementId') in roots
        and r.get('relatedSpdxElement')
    }


def _purl_of(package: Mapping[str, Any]) -> str | None:
    for ref in package.get('externalRefs') or []:
        if not isinstance(ref, Mapping):
            continue
        if ref.get('referenceType') == PURL_REFERENCE_TYPE:
            locator = ref.get('referenceLocator')
            if locator:
                return str(locator)
    return None


def _ecosystem_of(purl: str) -> str:
    """`pkg:maven/group/artifact@1.0` -> `maven`."""
    without_scheme = purl.split(':', 1)[-1]
    return without_scheme.split('/', 1)[0].lower()


def _licenses_of(package: Mapping[str, Any]) -> list[str]:
    concluded = package.get('licenseConcluded')
    if not concluded or concluded in {'NOASSERTION', 'NONE'}:
        return []
    return [str(concluded)]


def _as_document(result: ConditionalResult) -> ConditionalResult:
    """`result`, its body the document every reader of a graph expects.

    That is `{"sbom": {...}}`, as the synchronous endpoint answers: what
    `github depgraph` and `run` store, `db raw` lands, `db edges` walks
    and `parse_spdx_document` reads, and what #22 dates by the
    `creationInfo.created` inside it. A report from the asynchronous pair
    is documented only as "the SBOM in SPDX JSON format", and has been
    seen to be the document itself, unwrapped (ClickHouse/ClickBOM#119).
    Either shape is taken: an unwrapped one is wrapped here, and stored
    byte for byte as the synchronous endpoint's answer would have been.

    A body that is neither is a failure: nothing to store, and no
    evidence that there is no graph.
    """
    if not result.changed:
        return result
    body = result.payload
    if isinstance(body, dict):
        if isinstance(body.get('sbom'), dict):
            return result
        if 'sbom' not in body and isinstance(body.get('spdxVersion'), str):
            return replace(result, payload={'sbom': body})
    return replace(
        result,
        payload=None,
        error=f'not an SPDX document: {type(body).__name__}',
    )


def _not_before(seconds: float, rate_limit: RateLimit) -> float:
    """`seconds`, or longer if the answer's `Retry-After` asks it."""
    if rate_limit.retry_after is None:
        return seconds
    return max(seconds, float(rate_limit.retry_after))


class DependencyGraphService:
    """Fetches GitHub dependency-graph SBOMs."""

    #: The synchronous endpoint: the document is the answer. Closing down
    #: on `SYNC_REMOVAL`.
    ENDPOINT = 'https://api.github.com/repos/{owner}/{repo}/dependency-graph/sbom'
    #: The first half of the pair that replaces it. It answers 201 with
    #: `sbom_url`, "URL to poll for the SBOM export result": the second
    #: half, `.../sbom/fetch-report/{sbom_uuid}`, which answers 202, "SBOM
    #: is still being processed", until it answers 302, "Redirects to a
    #: temporary download URL for the completed SBOM".
    REPORT_ENDPOINT = f'{ENDPOINT}/generate-report'

    #: Seconds a report may be waited for, all looks at it together. The
    #: synchronous endpoint gave up after ten, and a report of 25,233
    #: packages has been seen ready in 19 (ClickHouse/ClickBOM#119). A
    #: pass pays at most this per repository, of the order of what a
    #: failing export already costs it: four attempts at ten seconds
    #: each, with six of backoff between.
    REPORT_WAIT = 60.0
    #: Seconds before the first look, doubled after each "not yet" up to
    #: `POLL_CAP`. Each look spends the scarce dependency-graph bucket, so
    #: the first is not sent the moment the report is asked for.
    FIRST_POLL = 2.0
    POLL_CAP = 30.0
    #: Seconds any one request may take.
    TIMEOUT = 60

    def __init__(
        self,
        github: GitHubService,
        api: str | None = None,
        *,
        today: date | None = None,
        downloads: Session | None = None,
        max_wait: float | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.github = github
        #: Rate-limited requests sent. `chatsbom run` bounds a pass by
        #: the sum of the services' own counters rather than keeping a
        #: second tally that would drift from theirs.
        self.requests = 0
        #: What `fetch` asks, in order: `flows_for` the setting, which is
        #: read here rather than at import, because `.env` is loaded by
        #: the root callback after every module has been imported.
        self.flows = flows_for(
            os.getenv('CHATSBOM_DEPGRAPH_API') if api is None else api,
            today or datetime.now(timezone.utc).date(),
        )
        #: Where a finished report is downloaded from: a temporary URL off
        #: the API. A session of its own, which has never held the token:
        #: GitHub's credential is for GitHub's API, not for wherever it
        #: keeps the file.
        self.downloads: Session = (
            downloads if downloads is not None else get_plain_client()
        )
        self.max_wait = self.REPORT_WAIT if max_wait is None else max_wait
        self._sleep = sleep or time.sleep
        self._monotonic = monotonic or time.monotonic

    def fetch(self, owner: str, repo: str) -> ConditionalResult:
        """GitHub's answer for one repository, as the outcome it was.

        This used to return None for everything but a document, so a
        batch recorded a refused token as "no graph": 890 answers of 429
        were counted among "3,133 with no graph published". The outcomes
        a batch has to tell apart:

        * `changed` — the SPDX document, in `payload`, as the synchronous
          endpoint answers it whichever way it was fetched;
        * `absent` — 404, no graph. A repository with the dependency
          graph switched off answers the same way, and a request for a
          report is answered as the endpoint answered;
        * `rate_limited` — the token was refused, on any request, which
          says nothing about the repository;
        * `pending` — GitHub accepted the request for a report and was
          still generating it when `max_wait` ran out. Nothing went
          wrong, and there is a graph: ask again later;
        * `failed` — anything else. Large repositories make the
          synchronous endpoint time out server-side (500 "Request timed
          out" for spring-boot), and the session retries 5xx and then
          raises `RetryError`, so a persistent 500 arrives as a transport
          error. A body that is not an SPDX document is a failure too:
          nothing to store, and no evidence that there is no graph. So is
          a report that is gone, or a download that fails, once GitHub
          has accepted the request.

        Never raises for any of them: none is fatal to a batch over
        thousands of repositories, and the caller decides which one stops
        it. It does wait, for a report, and never longer than `max_wait`.

        Which endpoint is asked is `flows`, from `CHATSBOM_DEPGRAPH_API`
        (see `flows_for`).

        Through `plain_session`. The cached `session` sleeps through a 429
        carrying `Retry-After` and asks again, three times, then raises a
        transport error — a refusal nobody could recognise as one. What
        its cache held was a second copy of every document the command
        stores anyway, and a week's memory of each 404: a re-run now asks
        again about a repository that had no graph, which is also how it
        notices one switched on.
        """
        name = f'{owner}/{repo}'
        first, *rest = self.flows
        result = self._ask(first, owner, repo)
        for flow in rest:
            if not result.failed:
                break
            logger.info(
                'Dependency graph export failed, asking for a report',
                repo=name, status=result.status, error=result.error,
            )
            result = self._ask(flow, owner, repo)

        if result.absent:
            logger.info('No dependency graph', repo=name)
        elif result.rate_limited:
            logger.warning(
                'Dependency graph refused: rate limited',
                repo=name, status=result.status,
                remaining=result.rate_limit.remaining,
            )
        elif result.pending:
            logger.info(
                'Dependency graph report not ready yet',
                repo=name, max_wait=self.max_wait,
            )
        elif not result.changed:
            logger.warning(
                'Dependency graph unavailable',
                repo=name, status=result.status, error=result.error,
            )
        return result

    def _ask(self, flow: str, owner: str, repo: str) -> ConditionalResult:
        if flow == SYNC:
            return self._export(owner, repo)
        return self._report(owner, repo)

    def _export(self, owner: str, repo: str) -> ConditionalResult:
        """The synchronous endpoint's answer."""
        self.requests += 1
        return _as_document(
            conditional_get(
                self.github.plain_session,
                self.ENDPOINT.format(owner=owner, repo=repo),
                timeout=self.TIMEOUT,
            ),
        )

    def _report(self, owner: str, repo: str) -> ConditionalResult:
        """A report from the asynchronous pair: asked for, then collected.

        Asking is answered as the synchronous endpoint was: a 404 is no
        graph, and a refusal is a refusal. The answer names where to look
        for the report, and the look carries the token, so a URL anywhere
        but the API's own is not looked at.
        """
        self.requests += 1
        accepted = conditional_get(
            self.github.plain_session,
            self.REPORT_ENDPOINT.format(owner=owner, repo=repo),
            timeout=self.TIMEOUT,
        )
        if not accepted.changed:
            return accepted
        body = accepted.payload
        url = body.get('sbom_url') if isinstance(body, dict) else None
        if (
            not isinstance(url, str)
            or urlsplit(url)[:2] != urlsplit(self.ENDPOINT)[:2]
        ):
            return replace(
                accepted,
                payload=None,
                error=f'no report URL on the API: {url!r}',
            )
        return self._collect(url, accepted)

    def _collect(
        self,
        url: str,
        accepted: ConditionalResult,
    ) -> ConditionalResult:
        """The report at `url`, if GitHub finishes it within `max_wait`.

        Looked at after `FIRST_POLL` seconds, then twice as long each time
        up to `POLL_CAP`, and never sooner than a `Retry-After` asks. A
        wait that would pass the bound is not begun: the report is
        `pending`, and the caller asks again later. Not waiting it out is
        what keeps one slow report from holding up a pass.
        """
        deadline = self._monotonic() + self.max_wait
        backoff = self.FIRST_POLL
        wait = _not_before(backoff, accepted.rate_limit)
        # Until it is looked at, what GitHub has said is that it accepted.
        latest = replace(accepted, payload=None, pending=True)
        while self._monotonic() + wait <= deadline:
            self._sleep(wait)
            self.requests += 1
            answer = self._look(url)
            if isinstance(answer, str):
                return self._download(answer)
            if not answer.pending:
                return answer
            latest = answer
            backoff = min(backoff * 2, self.POLL_CAP)
            wait = _not_before(backoff, answer.rate_limit)
        return latest

    def _look(self, url: str) -> ConditionalResult | str:
        """One look at a report: where to download it, or why not.

        The redirect is not followed. This session carries the token, and
        the URL it points at is not the API's.
        """
        try:
            response = self.github.plain_session.get(
                url, headers={}, timeout=self.TIMEOUT, allow_redirects=False,
            )
        except requests.RequestException as e:
            return ConditionalResult(status=0, error=f'{type(e).__name__}: {e}')

        status = int(response.status_code)
        rate_limit = RateLimit.from_headers(response.headers)
        if 200 <= status < 300:
            # 202, "SBOM is still being processed, no content is
            # returned" — or 201, as the changelog that announced the
            # pair has it. Only the redirect says the report is ready.
            return ConditionalResult(
                status=status, rate_limit=rate_limit, pending=True,
            )
        location = (
            response.headers.get('Location')
            or response.headers.get('location')
        )
        if 300 <= status < 400 and location:
            target = urljoin(url, location)
            if urlsplit(target).scheme == 'https':
                return target
            return ConditionalResult(
                status=status, rate_limit=rate_limit,
                error='report download is not over https',
            )
        # A 404 is a report gone or never made, which says nothing about
        # whether the repository has a graph: GitHub accepted the request
        # for it. The error makes it a failure, not an absence.
        return ConditionalResult(
            status=status, rate_limit=rate_limit,
            error=f'report: HTTP {status}',
        )

    def _download(self, url: str) -> ConditionalResult:
        """The finished report, from the temporary URL GitHub gave.

        Whatever fails here is about the download — a link that expired,
        a file that is gone — and not whether the repository has a
        graph, so its 404 is a failure too.
        """
        result = conditional_get(self.downloads, url, timeout=self.TIMEOUT)
        if result.absent:
            return replace(result, error='report download: HTTP 404')
        return _as_document(result)

    def artifacts_for(self, owner: str, repo: str) -> list[dict[str, Any]] | None:
        """Artifact rows for a repository, or None without a usable graph.

        None covers every outcome but a document — including a refused
        token and a report not ready yet. A batch has to tell those
        apart, so it uses `fetch`.
        """
        result = self.fetch(owner, repo)
        if not result.changed:
            return None
        try:
            return parse_spdx_document(result.payload)
        except ValueError as e:
            logger.warning(
                'Malformed dependency graph',
                repo=f'{owner}/{repo}', error=str(e),
            )
            return None


def load_artifacts(path: Path) -> Iterator[dict[str, Any]]:
    """Artifact rows from a stored dependency-graph document."""
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as e:
        raise ValueError(f"unreadable dependency graph {path}: {e}") from e
    yield from parse_spdx_document(payload)
