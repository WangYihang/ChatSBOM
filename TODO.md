# Outstanding work

Measured 2026-09-14 against `data/` and a local D1 export of the current
corpus. Every number here was counted, not estimated, unless it says
otherwise.

## Where the data stands

| | |
|---|---|
| Repositories in the dataset | 28,075 |
| …with any dependency row in `artifacts` | **16,235** (57.8%) |
| …with no dependency row at all | **11,840** |
| `artifacts` rows | 6,062,896 — 6,015,567 syft, 47,329 depgraph |
| Repositories with depgraph rows | **85** |
| depgraph SPDX documents on disk | **24,936** |
| `agg_edges` | 455,281 |
| Dependencies observed | 2026-09-13 |
| Repository metadata (stars, `pushed_at`) | ≤ **2026-02-09** |

The gap between rows 3, 5 and 6 is the single most consequential item
below.

---

## C. `db index --rebuild`, then re-export — **the blocker**

24,936 dependency-graph documents are on disk and 85 repositories' worth
of them are in the fact table. Everything the dashboard answers from
`artifacts` — dependants, counts, version spread, top packages — is
therefore syft-only in practice, and the "Where the data came from"
panel reads `depgraph = 0` for every language except Java, which is not
what the corpus contains.

The ingestion path already supports it: `ingest_from_list()` takes a
`depgraph_index`, and `db/index.py` passes one per language. This is a
command to run, not code to write.

A comment in `db/index.py` records the bug that probably produced the
current state — "treating the latter as the input list once cut Java
from 1,215 repositories to 87" — and the fact table today holds exactly
85. Worth confirming that is fixed before trusting a rebuild.

**Sizing, because it decides whether this fits D1.** Sampling 300 of the
24,936 documents: median 48 dependencies, mean 515, p90 1,341, max
15,222. The two sources coexist per repository (65 repositories have
both today), so ingestion is additive:

- `artifacts` 6.06M → **~18.9M rows**, about 3.1x
- at the measured 294.7 MB for 6.06M normalised rows, **~0.9–1.1 GB**
- fits D1's 10 GB paid limit; roughly 2x the 500 MB free tier

Every `agg_*` table has to be recomputed after this, and the overview's
numbers will move substantially.

---

## A. Ledger backfill from disk

Worse than a gap — the ledger holds almost no collection state:

- 24,568 rows, of which **467** carry any stage watermark, all `repo`
- **zero** carry a `depgraph` watermark, against 24,936 documents fetched
- 3,507 of the dataset's 28,075 repositories are not in it at all

Already established: depgraph ETags are unstable — the same 946,601-byte
body returns a different ETag on every request — so conditional requests
never hit and the ledger cannot save a re-fetch. Its value is knowing
*what is stale*, which is what D+H needs.

---

## D+H. Repository metadata refresh, and a collection timestamp

Dependencies were observed 2026-09-13; `pushed_at` tops out at
2026-02-09. The star counts the UI sorts by are seven months old.

Of the 389 repositories that were actually re-checked, **264 (67.9%)**
had pushed since that snapshot. Extrapolated, roughly 19,000 of 28,075
repositories have moved on.

Two parts, as decided: refresh the metadata, and record when each row
was collected so the UI can say which number is from when.

---

## E. `sbom lock` — full Java and PHP

`data/10-generated-lock/` is empty. To run after C so the two do not
contend for quota and disk.

Constraint, restated because it is load-bearing: lockfile generation
runs **in a container**, never on the development machine. The host
Docker socket is never mounted; the Docker CLI appears only in
`Dockerfile.lock`; `sbom lock` stays out of the collector loop.

---

## F. Deploy — blocked on the account

Needs `wrangler d1 create chatsbom`, then the returned id into
`wrangler.jsonc`. See `DEPLOY.md`.

Note `wrangler dev` here previews the **build**, not the sources: the
Worker is bundled by `@cloudflare/vite-plugin` into `dist/chatsbom/`, so
a source edit needs `npm run build` before it is served. Two
measurements in this project have already been taken against a stale
bundle for want of that.

---

## G. The search box cannot find a package

Typing `laravel` answers "No repository in the dataset depends on
laravel." while 98 repositories depend on `laravel/framework`. Literally
true — no package is named exactly `laravel` — and it reads as "nobody
uses Laravel".

Two separate faults:

1. **The box is not wired to the search.** `searchPackages` is reachable
   only from the model's tools (`web/src/tools.ts:216`); the input
   (`web/src/components/QueryView.tsx:95`) passes what was typed
   straight to `dependentsOf` as an exact name. There is no
   autocomplete and no candidate list.

2. **The search ranks alphabetically.** `ORDER BY p.name` with
   `LIMIT 40` — so `laravel` returns forty `laravel-enso/*` packages
   (`-` is 0x2D, `/` is 0x2F) and never reaches `laravel/framework`,
   which has 98 dependants against their 1 each. It should rank by
   repository count.

Both are small. (2) is a line in `web/src/d1/queries.ts`; (1) is a
candidate list under the input, reusing the row shape the ranked-bar
panels already use.

---

## H. Unresolved version constraints in `versions`

`OpenAPITools/openapi-generator` is recorded against `laravel/framework`
at version `>= 13.0,< 14.0` — a constraint, not a resolved version. So
the "Versions in use" panel counts constraint strings alongside real
versions and its denominator is dirty. A collection-side problem; the
size of it is unmeasured.

---

## I. Extract `QUERIES` from `parquet.py`

Still at `chatsbom/export/parquet.py:135`, imported by `export/d1.py` so
that the two exports cannot describe different data. Its own module,
since neither export owns it. No behaviour change.

---

## Done

- **B. Dependency-graph collection.** All eight languages at 100%:
  28,069 repositories attempted, **24,936** graphs stored (88.8%), 3,133
  with no graph published. Disk, the per-language `.jsonl` indexes and
  the run log all agree at 24,936. Transport: 2,202 × 404, 890 × 429,
  2 × 451, 1 × 403.

  | language | attempted | stored | none |
  |---|---:|---:|---:|
  | python | 7,391 | 6,156 | 1,235 |
  | javascript | 5,834 | 5,561 | 273 |
  | typescript | 4,817 | 4,379 | 438 |
  | go | 3,103 | 2,941 | 162 |
  | java | 3,079 | 2,445 | 634 |
  | rust | 1,701 | 1,584 | 117 |
  | php | 1,281 | 1,068 | 213 |
  | ruby | 863 | 802 | 61 |

- **The edge table, in both directions** — `5878cb3`.
- **Long chart labels trimmed rather than head-cut** — `36db5ef`.
- **The tests are type-checked** — `bc25061`.
