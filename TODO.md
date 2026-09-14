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

## F. Deploy — a stable hostname

Running, on a quick tunnel: the whole app is local, `cloudflared`
forwards the Worker's port, and the dashboard reads the live ClickHouse.
`scripts/serve.sh` does build, worker, tunnel.

Verified from the public side: the tunnel forwards `127.0.0.1:8787`
alone, ClickHouse binds loopback only, `/?query=SELECT 1` and `/ping`
reach the SPA rather than the database, a forged method is refused by
the allow-list, and a package name of `x'; DROP TABLE artifacts;--`
returns 0 with the table intact.

What is left is only the address. A quick tunnel's hostname changes on
every restart; a stable one needs `cloudflared tunnel create` against
a Cloudflare-managed domain, which needs the account.

---

## G. The adoption series compares two instruments

`adoptionOverTime` draws a line from 2026-02 to 2026-09 — for `mail`,
124 to 149 — which reads as adoption growing. The two points are
different tools:

    2026-02   syft              124
    2026-09   github-depgraph   149

There are exactly two observation dates in the corpus and they
correspond one-to-one with the two sources, so the slope is an
instrument change. The panel's note — "it accumulates as the collection
queue runs" — now reads as though the points were comparable.

Same class as the headline defect that was just fixed. Three options,
none of them chosen yet:

1. connect points only within one source, which today leaves a single
   point and an honest empty panel;
2. one line per source, so two collections read as two collections;
3. keep the line and say what it is, which is the lightest and changes
   no geometry.

---

---

## H. Unresolved version constraints

`OpenAPITools/openapi-generator` is recorded against `laravel/framework`
at version `>= 13.0,< 14.0` — a constraint, not a resolution. GitHub's
dependency graph reports manifest constraints, and `version_kind`
distinguishes them, but the "Versions in use" panel counts all of them
together so its denominator mixes two kinds of thing. Now that depgraph
supplies 13,263,227 of 19,361,638 rows this matters more than it did.
Unmeasured.

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

- **ClickHouse is the serving store** — `ebae3a2`. The second
  implementation of `DatasetQueries`, which is what that interface was
  written for; nothing in the browser changed.

- **The overview stopped scanning the corpus** — `6159516`, `2c2cdee`.
  Twelve refreshable rollups and a repository dictionary took the
  dashboard's 21 queries from **836.0 ms to 24.2 ms**, nothing over
  4 ms, with every answer checked against the same question asked of
  the base tables — 28 comparisons, including `dictGet` row-for-row
  against the join it replaced. A `PROJECTION` was tried first and was
  the wrong tool: rows read fell 31x and the time did not move, because
  the cost was merging `uniqExact` states rather than I/O.

- **`index_granularity` 8192 → 1024** on `artifacts`. Every point
  lookup reads a multiple of the granularity and most of it was waste:
  `laravel/framework` has 299 rows and the dependants query read
  148,740. Now 9,216. Costs 10% disk and 4.6% on a rollup refresh's
  full scan.

- **The headline was an artifact of the instrument** — `18e9e95`. Every
  dependency-graph row was recorded as `direct` on a belief the
  codebase had already corrected once elsewhere, so the page claimed
  "70.7% of dependency records are declared outright" against a truer
  16.3%, and its declared-only ranking returned npm plumbing where the
  resolved closures give `typescript, eslint, prettier, react`.

- **The edge table, in both directions** — `5878cb3`.
- **Long chart labels trimmed rather than head-cut** — `36db5ef`.
- **The tests are type-checked** — `bc25061`.
