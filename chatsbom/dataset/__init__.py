"""The dashboard's questions, answered in Python (#128 §2.5, #138).

`web/src/backend.ts` declares what the dashboard asks, `DatasetQueries`,
and the Worker answers it from D1 or ClickHouse. This answers it once
more, from a snapshot: a read-only SQLite file of the D1 schema
(`D1_SCHEMA` in `chatsbom/export/d1.py`), which the indexer will publish
(#132). One implementation, for the web routes, the chat's tools and
the CLI alike, where there are three today.

    with open_dataset('snapshots/<id>.sqlite') as dataset:
        dataset.dependents_of('mail', type='gem', limit=20)
        jsonable(dataset.totals())  # {'repositories': ..., 'tracked': ...}

Each method is one of `DatasetQueries`, in snake_case, answering as
`web/src/d1/queries.ts` does; each answer is JSON the page already
reads, under the TypeScript names (`jsonable`). Each method checks its
own arguments as the page's endpoint checks them, and a value it does
not take is an `InvalidParameter`. None takes SQL.

`web/test/contract.test.ts` records what D1 answers every call it makes
(`web/test/fixtures/contract/calls.json`), and
`tests/dataset_contract_test.py` holds this to each answer.
"""
from chatsbom.dataset.open import open_dataset
from chatsbom.dataset.params import InvalidParameter
from chatsbom.dataset.queries import Dataset
from chatsbom.dataset.types import jsonable

__all__ = ['Dataset', 'InvalidParameter', 'jsonable', 'open_dataset']
