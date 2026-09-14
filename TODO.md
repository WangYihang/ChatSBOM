# Outstanding work

Every number here was counted against `data/` and a rebuilt ClickHouse
on 2026-09-14, not estimated, unless it says otherwise.

## Where the data stands

| | |
|---|---|
| Repositories | 28,075 |
| …with any dependency row | **24,339** (86.7%) |
| …with none | 3,736 |
| `artifacts` rows | **19,361,638** — 13,263,227 depgraph, 6,098,411 syft |
| Repositories with depgraph rows | **22,392** |
| `edges` rows | 614,221 |
| Dependencies observed | depgraph 2026-09-13, syft **2026-02-11** |
| Repository metadata (stars, `pushed_at`) | ≤ **2026-02-09** |

The dependency data is now two-sourced and correctly dated. What is
still seven months stale is the *repository* metadata — see D.

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

## D. Repository metadata refresh

Dependencies are now dated correctly (see H, done). What is still stale
is the *repository* row: `pushed_at` tops out at **2026-02-09**, so the
star counts the UI sorts by are seven months old.

Of the 389 repositories that were actually re-checked, **264 (67.9%)**
had pushed since that snapshot. Extrapolated, roughly 19,000 of 28,075
have moved on.

Read-only collection, so `gh auth token` covers it — `.env`'s
`GITHUB_TOKEN` is expired (401). 28,075 repositories against the REST
rate limit is the cost to plan for; the ledger would say which are
stale if it held anything (see A).

---

## E. `sbom lock` — full Java and PHP

`data/10-generated-lock/` is empty. To run after C so the two do not
contend for quota and disk.

Constraint, restated because it is load-bearing: lockfile generation
runs **in a container**, never on the development machine. The host
Docker socket is never mounted; the Docker CLI appears only in
`Dockerfile.lock`; `sbom lock` stays out of the collector loop.

---

## F. Deploy — local ClickHouse behind a tunnel

Decided: no D1 on the serving path. The whole app runs locally and
`cloudflared` exposes it, reading the live ClickHouse rather than an
831 MB snapshot — so `db index` takes effect immediately and the 500 MB
free-tier limit stops mattering.

Measured on the real 19,361,638 rows, with **no precomputed
aggregates**:

| panel | SQLite, computed live | ClickHouse, native |
|---|---:|---:|
| relationshipSplit | 1,082 ms | **12.0 ms** |
| sourceComparison | 3,122 ms | **86.7 ms** |
| topPackages | — | 176.0 ms |
| licenseShares | — | 98.2 ms |
| totals | — | 66.7 ms |
| languageCoverage | — | 22.1 ms |
| dependencyDistribution | — | 18.9 ms |

Per-package lookups are 2.4–14.4 ms, reading 41k–166k rows of
19,361,638 — the sparse index doing what `backend.ts` predicted it
would. Edge lookups: 2.6 ms reverse (the primary-key prefix), 4.8 ms
forward (a full scan of 614,221 rows, which needs no second index).

So the `agg_*` tables are D1's compromise and this backend skips them,
which is the argument `backend.ts` was written around, now measured at
scale.

**Remaining:**

1. `ClickHouseDataset implements DatasetQueries` — the SQL for all 17
   methods is written and timed above. Server-side parameter binding is
   verified: `{name:Type}` with `param_name=`, and an injection attempt
   passed as a parameter comes back as data (`n: 0`, table intact).
   Never string interpolation — this page is public.
2. Backend selection in the Worker, and where the ClickHouse URL and
   credentials come from.
3. The tunnel, and a look at the rendered site.

`wrangler dev` here previews the **build**, not the sources: the Worker
is bundled by `@cloudflare/vite-plugin` into `dist/chatsbom/`, so a
source edit needs `npm run build` first. Two measurements in this
project have already been taken against a stale bundle for want of
that.

`guest` is read-only with cost caps (30 s, 4 GB, 2e9 rows, 16
concurrent, no DDL) and the server now binds to 127.0.0.1 only, so it
is not the thing being exposed — the tunnel carries the Worker's port,
not the database's.

---

## G. Unresolved version constraints

`OpenAPITools/openapi-generator` is recorded against `laravel/framework`
at version `>= 13.0,< 14.0` — a constraint, not a resolution. GitHub's
dependency graph reports manifest constraints, and `version_kind`
distinguishes them, but the "Versions in use" panel counts all of them
together so its denominator mixes two kinds of thing. Now that depgraph
supplies 13,263,227 of 19,361,638 rows this matters more than it did.
Unmeasured.

---

## H. Extract `QUERIES` from `parquet.py`

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

### C. The dependency-graph rebuild

24,936 dependency-graph documents were on disk and 85 repositories'
worth were in the fact table. Rebuilt:

| | before | after |
|---|---:|---:|
| depgraph rows | 58,566 | **13,263,227** |
| depgraph repositories | 85 | **22,392** |
| syft rows | 6,098,411 | 6,098,411 |
| repositories with any row | 16,235 | **24,339** |

19,361,638 rows in seven minutes, `failed=2` — two 0-byte SBOM files
(`btmills/geopattern`, `layerJS/layerJS`), logged and skipped. The
estimate before running it was ~18.9M from a 300-document sample; the
outcome was 2.4% above it, and the two sources do coexist per
repository (14,268 have both) as that estimate assumed.

The visible effect: `laravel/framework` went from 98 dependants to
**198**.

The D1 export still works and now produces **831.3 MB** (16,839,566
rows after the export's dedup and commit-sha filter — verified that the
filter drops no repository's depgraph data, only duplicate
manifest entries). Over D1's 500 MB free tier, inside the 10 GB paid
one. It is no longer on the serving path — see F.

- **H. Observations dated by the document, not the indexing run** —
  `19e0df8`. `observed_at` defaulted to `now()`, so a rebuild restamped
  all 19,361,638 rows with the moment it ran and the dashboard's
  "SCANNED" column claimed every dependency was seen today. It now
  reads GitHub's `creationInfo.created` for dependency graphs and the
  file mtime for syft output, which has no timestamp of its own.
  Measured after: depgraph 2026-09-13, syft 2026-02-11 — 6,098,411 rows
  correctly showing February instead of today.

- **Edges are a stored table** — `11038fa`. Moved out of the D1 export
  into `core/edges.py`, with a ClickHouse `edges` table and
  `chatsbom db edges`. 614,221 pairs from 24,936 documents, matching
  the export exactly.

- **ClickHouse binds to loopback** — `e15efc4`. `"8123:8123"` published
  on 0.0.0.0, so `admin`/`admin` — which carries
  `access_management=1` — answered on the LAN address.

- **The search box can find a package** — `03f732c`, `fc0a17c`,
  `bf91531`. Typing `laravel` said nothing depended on it while 98
  repositories depended on `laravel/framework`. The input was never
  wired to `searchPackages`, and that query ranked alphabetically so it
  would have missed it anyway.

- **The D1 export applies** — `dc5c4e6`. Two defects found by applying
  the SQL rather than reading it: the `packages` INSERT supplied two
  values for three columns so the table came out empty, and the new
  aggregate was a correlated subquery that had not finished in 110
  seconds. There is now a test that applies all four scripts and
  compares the counts that land against the counts reported.

- **The edge table, in both directions** — `5878cb3`.
- **Long chart labels trimmed rather than head-cut** — `36db5ef`.
- **The tests are type-checked** — `bc25061`.
