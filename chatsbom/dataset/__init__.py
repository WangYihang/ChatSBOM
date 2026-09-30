"""The dashboard's questions, answered in Python (#128 §2.5, #138).

The page asks its questions of the web service, by name
(`DatasetClient`, `web/src/dataset/client.ts`), and this answers them,
from a snapshot: a read-only SQLite file of the tables D1 had and a page
table, which `snapshot build` publishes (#132;
`chatsbom/snapshot/schema.py`). One implementation, for the web routes,
the chat's tools and the CLI alike, where the Worker had two, for D1
and ClickHouse, until #151 deleted it.

    with open_dataset(current('data/snapshots')) as dataset:
        dataset.dependents_of('mail', type='gem', limit=20)
        jsonable(dataset.totals())  # {'repositories': ..., 'tracked': ...}

Each method is one the page asks, in snake_case, answering as the
Worker's D1 store did; each answer is JSON the page reads, under the
TypeScript names (`jsonable`). Each method checks its own arguments as
the Worker's endpoint checked them, and a value it does not take is an
`InvalidParameter`. None takes SQL.

What D1 answered every call of the Worker's contract suite was recorded
(`web/test/fixtures/contract/calls.json`), and
`tests/dataset_contract_test.py` holds this to each answer, as
`tests/snapshot/parity_test.py` holds a snapshot of the same seed.
"""
from chatsbom.dataset.open import current
from chatsbom.dataset.open import open_dataset
from chatsbom.dataset.open import served
from chatsbom.dataset.params import InvalidParameter
from chatsbom.dataset.queries import Dataset
from chatsbom.dataset.types import jsonable

__all__ = [
    'Dataset', 'InvalidParameter', 'current', 'jsonable', 'open_dataset',
    'served',
]
