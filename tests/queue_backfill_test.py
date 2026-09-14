"""Teaching the ledger what is already on disk.

The ledger schedules by comparing a stage's watermark against the newest
push it has seen, so a missing watermark means "never collected". Before
this existed, 467 of 24,568 rows carried any watermark and none carried
a `depgraph` one, against 24,936 stored dependency graphs — so the queue
reported every stage 100% outstanding and the next continuous run would
have re-fetched the corpus.

The property that matters is where the timestamp comes from. A watermark
of `now` would be the same lie pointing the other way: February's work
would look current and no future push would overtake it.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from datetime import timezone

from chatsbom.commands.queue.backfill import _completed_at
from chatsbom.commands.queue.backfill import _records
from chatsbom.commands.queue.backfill import _stated_creation
from chatsbom.commands.queue.backfill import STAGE_LEDGERS
from chatsbom.core.ledger import Stage


def _at(path, when: datetime) -> None:
    stamp = when.timestamp()
    os.utime(path, (stamp, stamp))


class TestTimestampProvenance:

    def test_a_dependency_graph_is_dated_by_what_it_states(self, tmp_path):
        """GitHub writes `creationInfo.created`, which is the graph's own
        view of when it was produced — better evidence than anything on
        this side of the wire."""
        doc = tmp_path / 'sbom.spdx.json'
        doc.write_text(
            json.dumps({
                'sbom': {'creationInfo': {'created': '2026-09-13T03:56:20Z'}},
            }),
        )
        _at(doc, datetime(2020, 1, 1, tzinfo=timezone.utc))

        when = _completed_at(doc, Stage.DEPGRAPH)
        assert when is not None
        assert (when.year, when.month, when.day) == (2026, 9, 13)

    def test_a_syft_sbom_is_dated_by_its_file(self, tmp_path):
        """Syft writes no timestamp, so the mtime is all there is."""
        doc = tmp_path / 'sbom.json'
        doc.write_text('{}')
        _at(doc, datetime(2026, 2, 11, 9, 30, tzinfo=timezone.utc))

        when = _completed_at(doc, Stage.SBOM)
        assert when is not None
        assert (when.year, when.month, when.day) == (2026, 2, 11)

    def test_it_never_reads_the_clock(self, tmp_path):
        """The whole point. A watermark of `now` says every stage
        finished when the backfill ran, so work done in February looks
        current and the next push cannot overtake it."""
        doc = tmp_path / 'sbom.json'
        doc.write_text('{}')
        february = datetime(2026, 2, 11, tzinfo=timezone.utc)
        _at(doc, february)

        when = _completed_at(doc, Stage.SBOM)
        assert when is not None
        assert when.year == 2026 and when.month == 2

    def test_a_missing_document_is_not_evidence(self, tmp_path):
        """A listing entry for a file that is gone does not prove the
        stage completed — recording it would skip the repository
        forever."""
        assert _completed_at(tmp_path / 'absent.json', Stage.SBOM) is None

    def test_an_unparsable_creation_falls_back_to_the_file(self, tmp_path):
        doc = tmp_path / 'sbom.spdx.json'
        doc.write_text(
            json.dumps({
                'sbom': {'creationInfo': {'created': 'not a timestamp'}},
            }),
        )
        _at(doc, datetime(2026, 5, 4, tzinfo=timezone.utc))
        when = _completed_at(doc, Stage.DEPGRAPH)
        assert when is not None and when.month == 5

    def test_a_document_without_creation_info_reads_as_none(self, tmp_path):
        doc = tmp_path / 'x.json'
        doc.write_text(json.dumps({'sbom': {'packages': []}}))
        assert _stated_creation(doc) is None


class TestLedgerReading:

    def test_one_bad_line_does_not_lose_the_rest(self, tmp_path):
        """These files are appended to by a long-running collector."""
        listing = tmp_path / 'ruby.jsonl'
        listing.write_text('{"id": 1}\nnot json\n{"id": 2}\n')
        assert [r['id'] for r in _records(listing)] == [1, 2]

    def test_an_unreadable_ledger_yields_nothing(self, tmp_path):
        assert list(_records(tmp_path / 'absent.jsonl')) == []


class TestStageCoverage:

    def test_only_per_repository_stages_are_backfilled(self):
        """`repo`, `release` and `commit` write one ledger per language,
        so nothing in them says when an individual repository was seen.
        Claiming a watermark for those would skip work that never ran.
        """
        stages = {stage for stage, _, _ in STAGE_LEDGERS}
        assert stages == {Stage.SBOM, Stage.DEPGRAPH, Stage.CONTENT}
        assert Stage.REPO not in stages
        assert Stage.RELEASE not in stages
        assert Stage.COMMIT not in stages

    def test_each_stage_names_the_field_holding_its_evidence(self):
        for stage, directory, field in STAGE_LEDGERS:
            assert directory.startswith(('07-', '09-')), stage
            assert field.endswith('_path'), stage
