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

---

## D. Repository metadata refresh — running

`queue sync` re-checks the repository resource conditionally, so a 304
costs no quota. In progress; `pushed_at` still tops out at 2026-02-09
for whatever has not been reached yet.

**A trap worth keeping.** `GET /rate_limit` reported 5,000/5,000 while a
real request's own `x-ratelimit-remaining` header said 3,156 with 1,844
used — the endpoint is not the bucket the requests are drawn from. A
budget check that reads it never fires. Read the header off an actual
response.

---

---

## E. `sbom lock` — PHP running, Java not possible

**PHP resolves at 80%** measured over a real slice: 62 resolved, 18
already cached, 20 failed of 100. The failures are genuine dependency
conflicts — `orchestra/testbench-core 10.x-dev conflicts with
laravel/framework <12.63.0|>=13.0.0` — not sandbox problems. Running
over all 1,281.

**Java cannot work on this corpus, and the reason is upstream of the
sandbox.** `06-github-content` stores manifests rather than source trees
— by design, since that is all Syft needs to tell declared from
inherited. Measured over 60 sampled Java projects: 43 have no `pom.xml`
at all, the stored tree's median size is 1 KB, and 10 of the 17 that do
have one declare `<modules>`. Maven cannot resolve a multi-module POM
without its children:

    [ERROR] Child module /tmp/p/mall-common of /tmp/p/pom.xml
            does not exist

Making it work means having `github content` store the module POMs,
which is a collection change with its own storage cost. Recorded beside
the recipe in `core/sandbox.py` so the next person does not re-derive
it.

Two layers had to be peeled off first, both recorded in `5478b10`: the
image bakes `MAVEN_CONFIG=/root/.m2` and runs its entrypoint before the
recipe's script, so an `export HOME` came too late — and that failure
prints "Carrying on ..." while being the only thing on stderr, so it
masked the real error for two rounds.

---

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

---

---

---

## Done

- **A. Ledger backfill** — `800e8d2`. The queue believed nothing had
  ever been collected: 467 of 24,568 rows carried any stage watermark
  and none carried a `depgraph` one, against 24,936 stored graphs. So
  every stage reported 100% outstanding. 81,074 watermarks recorded
  from the documents' own timestamps — never the clock — taking
  `content` and `sbom` to 13% outstanding and `depgraph` to 22%.

- **G. The adoption series** — `3875617`. It drew a line from
  February's 124 to September's 149 for `mail` and read as growth; the
  two points are syft and GitHub's graph, seven months apart. One line
  per source now.

- **H. Version constraints** — `ea99b0e`. "Repositories on each
  resolved version" was counting manifest constraints: for
  `laravel/framework`, `>= 13.0,< 14.0` topped the panel with 11
  against the real leading version's 7.

- **The metadata panel claimed complete coverage** — `1f4a50d`. 99.951%
  rendered as 100 in both the tile and the panel, hiding 9,469
  unclassified records.

- **I. `QUERIES` is its own module** — `2e568a6`. `d1.py` imported them
  from `parquet.py`, which made one format's module own the other's
  contract.

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
