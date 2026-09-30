"""An ecosystem's alias finds what its canonical name finds (#165).

The collectors spell some registries two ways
(`chatsbom/core/ecosystems.py`): Syft's `php-composer` is Composer,
`java-archive` Maven and `python` PyPI. A snapshot keys every row by
the canonical name, which the page shows and sends back; the chat's
tools, and anyone who asks the service by URL, may send either. The
dependants' scope took either, and the ranking and the relationship
split did not: they lowercased the ecosystem and stopped there, so
`php-composer` found nothing where `composer` found the ranking.

So every method that takes an ecosystem is asked here under each alias
and under the name it stands for, of the contract's corpus, and has to
answer alike.
"""
from __future__ import annotations

import inspect
from collections.abc import Callable
from collections.abc import Iterator

import pytest

from chatsbom.core.ecosystems import RENAMES
from chatsbom.dataset import Dataset
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from tests.dataset_contract_test import corpus

#: A package of each ecosystem the corpus has under an alias: Composer's
#: rows are Syft's `php-composer` and the graph's `composer`, `mail` is
#: a `java-archive` and a `python` package besides a gem.
PACKAGE = {'composer': 'laravel/framework', 'maven': 'mail', 'pypi': 'mail'}

#: How each method that takes an ecosystem is asked with one.
ASK: dict[str, Callable[[Dataset, str, str], object]] = {
    'dependents_of': lambda dataset, name, ecosystem: dataset.dependents_of(
        name, type=ecosystem,
    ),
    'count_dependents': lambda dataset, name, ecosystem: (
        dataset.count_dependents(name, type=ecosystem)
    ),
    'count_dependent_rows': lambda dataset, name, ecosystem: (
        dataset.count_dependent_rows(name, type=ecosystem)
    ),
    'relationship_split': lambda dataset, _, ecosystem: (
        dataset.relationship_split(ecosystem)
    ),
    'top_packages': lambda dataset, _, ecosystem: dataset.top_packages(
        ecosystem=ecosystem,
    ),
}

#: What a method's answer is when it found nothing.
NOTHING: tuple[object, ...] = (
    [], 0, {'direct': 0, 'transitive': 0, 'unknown': 0},
)


@pytest.fixture(scope='module')
def dataset(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Dataset]:
    with open_dataset(corpus(tmp_path_factory.mktemp('aliases'))) as opened:
        yield opened


def answer(dataset: Dataset, method: str, ecosystem: str) -> object:
    canonical = RENAMES.get(ecosystem, ecosystem)
    return jsonable(
        ASK[method](dataset, PACKAGE.get(canonical, 'mail'), ecosystem),
    )


def test_these_are_every_method_that_takes_an_ecosystem() -> None:
    """A method that comes to take one is asked here too."""
    taking = {
        name
        for name, method in inspect.getmembers(Dataset, inspect.isfunction)
        if not name.startswith('_')
        and {'type', 'ecosystem'} & set(inspect.signature(method).parameters)
    }
    assert taking == set(ASK)


@pytest.mark.parametrize('method', sorted(ASK))
@pytest.mark.parametrize('alias', sorted(RENAMES))
def test_an_alias_is_answered_as_its_canonical_name(
    dataset: Dataset, method: str, alias: str,
) -> None:
    assert answer(dataset, method, alias) == answer(
        dataset, method, RENAMES[alias],
    )


@pytest.mark.parametrize('method', sorted(ASK))
@pytest.mark.parametrize('alias', ['php-composer', 'java-archive', 'python'])
def test_an_alias_the_corpus_has_finds_something(
    dataset: Dataset, method: str, alias: str,
) -> None:
    """Not agreement on nothing: the corpus has rows under each of these
    aliases, and each method finds them."""
    assert answer(dataset, method, alias) not in NOTHING


@pytest.mark.parametrize('method', ['relationship_split', 'top_packages'])
@pytest.mark.parametrize('spelled', ['PHP-Composer', 'Java-Archive'])
def test_the_aggregates_read_an_alias_in_any_case(
    dataset: Dataset, method: str, spelled: str,
) -> None:
    """The ranking and the split read an ecosystem in any case, as D1's
    did (`calls.json` asks them for `Maven` and `Composer`), and an
    alias no less."""
    assert answer(dataset, method, spelled) == answer(
        dataset, method, RENAMES[spelled.lower()],
    )
    assert answer(dataset, method, spelled) not in NOTHING
