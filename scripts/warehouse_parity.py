#!/usr/bin/env python3
"""The warehouse against ClickHouse, rollup by rollup (#131).

Run after `chatsbom db index` and `chatsbom warehouse build` have read
the same store, beside each other until the cutover (#128, Appendix B,
phase 4). Every rollup ClickHouse has, and the corpus, its language
buckets and the current facts they are built on, is asked of both, and
the rows compared: `chatsbom/warehouse/parity.py` says how, and where
the two are meant to differ.

Usage:

    uv run python scripts/warehouse_parity.py [WAREHOUSE]

WAREHOUSE is `data/warehouse.duckdb` unless given; ClickHouse is the
database the configuration names, read with the export account, as
`scripts/verify_rollups.py` reads it. Exit status is the number of
relations that differ, so a shell `&&` can use it.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from chatsbom.core.container import get_container
from chatsbom.warehouse import connect
from chatsbom.warehouse.parity import compare
from chatsbom.warehouse.parity import report


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(__doc__ or '').split('\n')[0],
    )
    parser.add_argument(
        'warehouse', nargs='?', type=Path, default=None,
        help='the warehouse file; data/warehouse.duckdb by default',
    )
    args = parser.parse_args()
    container = get_container()
    path = args.warehouse or container.config.paths.warehouse_path
    # The export account: the guest profile caps result rows and breaks
    # off silently, which would read as a difference.
    clickhouse = container.get_export_repository()
    with connect(path, read_only=True) as warehouse:
        verdicts = compare(warehouse, clickhouse.client)
    print(report(verdicts))
    differ = sum(not verdict.agrees for verdict in verdicts)
    print(
        f'{differ} of {len(verdicts)} differ.' if differ
        else f'All {len(verdicts)} agree.',
    )
    return differ


if __name__ == '__main__':
    sys.exit(main())
