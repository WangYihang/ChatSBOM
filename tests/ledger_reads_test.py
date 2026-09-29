"""What the ledger answers without being written to (#100).

`queue due` compares the due set derived from the store with the
ledger's, while the collector runs and writes the same ledger. So its
due sets are read by the same statements the workers claim by, and
without claiming.
"""
from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone

from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import StageState

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


# --- the depgraph due set, as `claim_stage` takes it ------------------------

def _varied(ledger: Ledger) -> None:
    """One repository for each way the depgraph stage can be due or not."""
    def seed(repository_id: int, stars: int | None) -> None:
        ledger.seed(
            repository_id, 'o', f'r{repository_id}', snapshot='all-x',
            stars=stars,
        )

    for repository_id, stars in (
        (1, 10), (2, 500), (3, 50), (4, 60), (5, 5), (6, 70), (7, 80),
        (8, 90), (9, 20), (10, 30), (11, None), (12, 500),
    ):
        seed(repository_id, stars)
    # 1, 2, 11: never asked; 12 as well, with more stars than 2 by id.
    old = NOW - timedelta(days=31)
    records = {
        3: StageState(
            3, Stage.DEPGRAPH, outcome='ok', done_at=old,
            next_attempt_at=NOW - timedelta(days=1),
        ),
        4: StageState(
            4, Stage.DEPGRAPH, outcome='absent',
            next_attempt_at=NOW - timedelta(days=1),
        ),
        5: StageState(
            5, Stage.DEPGRAPH, outcome='failed', failure_count=1,
            next_attempt_at=NOW - timedelta(minutes=1),
        ),
        6: StageState(
            6, Stage.DEPGRAPH, outcome='absent',
            next_attempt_at=NOW + timedelta(days=1),
        ),
        8: StageState(
            8, Stage.DEPGRAPH, claimed_by='elsewhere',
            claim_expires_at=NOW + timedelta(minutes=5),
        ),
    }
    for record in records.values():
        ledger.record_stage(record)
    # `record_stage` drops a lease: 8's is put back as a claim leaves it.
    ledger._db.execute(
        "UPDATE stage_state SET claimed_by = 'elsewhere', "
        'claim_expires_at = ? WHERE repository_id = 8',
        ((NOW + timedelta(minutes=5)).isoformat(),),
    )
    ledger.record_absent(7, NOW, retry_at=NOW - timedelta(days=1))
    # 9 and 10: a graph fetched before `stage_state`, only a watermark;
    # 9's is past the refresh, 10's is not.
    for repository_id, fetched in (
        (9, NOW - timedelta(days=40)), (10, NOW - timedelta(days=3)),
    ):
        state = ledger.get(repository_id)
        assert state is not None
        state.stage_watermarks[Stage.DEPGRAPH] = fetched
        ledger.upsert(state)


def test_the_depgraph_due_set_is_what_claim_stage_takes_in_its_order(
    tmp_path,
):
    with Ledger(tmp_path / 'ledger.sqlite3') as ledger:
        _varied(ledger)

        due = ledger.depgraph_due_ids(NOW)
        claimed = ledger.claim_stage(Stage.DEPGRAPH, NOW, None, 'w')

        assert due == [work.repository_id for work in claimed]
        # Never asked first, the most starred first, then the refreshes
        # (a graph and a watermark past it), then the expired negative
        # cache; not the backoff, the absent, the leased or the fresh.
        assert due == [2, 12, 1, 5, 11, 3, 9, 4]


def test_the_depgraph_due_set_takes_nothing(tmp_path):
    """Read, not claimed: no lease is taken and no row is written, so
    it can be asked of a ledger the workers are using."""
    with Ledger(tmp_path / 'ledger.sqlite3') as ledger:
        _varied(ledger)
        before = ledger._db.execute(
            'SELECT * FROM stage_state ORDER BY repository_id, stage',
        ).fetchall()

        first = ledger.depgraph_due_ids(NOW)
        again = ledger.depgraph_due_ids(NOW)

        after = ledger._db.execute(
            'SELECT * FROM stage_state ORDER BY repository_id, stage',
        ).fetchall()
        assert [tuple(r) for r in after] == [tuple(r) for r in before]
        assert first == again


def test_the_depgraph_due_set_narrows_and_limits_as_the_claim_does(
    tmp_path,
):
    with Ledger(tmp_path / 'ledger.sqlite3') as ledger:
        _varied(ledger)

        assert ledger.depgraph_due_ids(NOW, repos={1, 3, 6}) == [1, 3]
        assert ledger.depgraph_due_ids(NOW, limit=3) == [2, 12, 1]
        assert [
            work.repository_id
            for work in ledger.claim_stage(
                Stage.DEPGRAPH, NOW, 2, 'w', repos={1, 3, 6},
            )
        ] == [1, 3]


def test_a_claimed_repository_leaves_the_depgraph_due_set(tmp_path):
    with Ledger(tmp_path / 'ledger.sqlite3') as ledger:
        _varied(ledger)
        [work] = ledger.claim_stage(Stage.DEPGRAPH, NOW, 1, 'w')

        assert work.repository_id not in ledger.depgraph_due_ids(NOW)
