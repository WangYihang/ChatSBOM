# Outstanding work

Every number here was counted against `data/` and a rebuilt ClickHouse
on 2026-09-15, not estimated, unless it says otherwise.

## Where the data stands

| | |
|---|---|
| Repositories | 28,075 |
| …with any dependency row | **24,449** (87.1%) |
| …with none | 3,626 |
| `artifacts` rows | **19,384,165** — 13,263,227 depgraph, 6,120,938 syft |
| Duplicate rows | **0** (was 140,792) |
| Repositories with depgraph rows | **22,392** |
| `edges` rows | 614,221 |
| Dependencies observed | depgraph 2026-09-13, syft 2026-02-11 → **2026-09-14** |
| Repository metadata (stars, `pushed_at`) | **2026-09-14**, all eight languages |

Both halves are now current. The rebuild of 2026-09-15 applied the
metadata refresh (D), the resolved PHP lockfiles (E), and cleared the
140,792 duplicate rows two probe ingests had appended.

Verified rather than assumed, with the checks now in `scripts/`:

    verify_rollups.py      14 of 14 rollups agree with the base tables
    benchmark_queries.py   43.9 ms across 22 queries, slowest 5.1 ms
    health.sh              page and API answer, local and public

PHP coverage went 1,089 → **1,199 of 1,281 (85% → 94%)**, which is the
figure predicted from the SBOMs before the rebuild ran.

---

## G. Resolved — `records` counts facts, not rows

`totals().dependencies` read 19,384,165 from ClickHouse and 16,905,915
from D1, under one label. Settled on the deduplicated figure, which is
what the export had always produced.

Every one of the 2,478,250 was a multi-manifest repeat — GitHub's
dependency graph reports per manifest, so a package declared in both
`package.json` and `packages/x/package.json` is two `artifacts` rows
differing only in `artifact_id`. It is a dependency-graph artefact
rather than a Syft one:

    depgraph   13,263,227 -> 10,867,821   -18.1%
    syft        6,120,938 ->  6,038,094    -1.4%

`mv_package_language` and `mv_repository_deps` now deduplicate on the
same key `export/queries.py` groups by — `artifact_id` deliberately
absent, since it is the per-manifest discriminator. Everything else
sums from those two, so `mv_totals` and `mv_language_totals` inherited
it. `packages` and every repository count were already distinct and did
not move.

The front page changed as expected: 83.6% → **84.3% inherited**, and
the classified share 99.95% → 99.94%. Refreshing the affected rollups
took 16.4s; the 22 queries still total under 50 ms.

`verify_rollups.py` had to change with them — its ground-truth queries
counted rows, so leaving them would have made the checker itself the
thing that was wrong. 14 of 14 agree, and `totals().dependencies` now
matches the D1 export exactly.

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

## E. `sbom lock` — PHP done, Java not possible

**Done, and it beat the slice.** `resolved=1046 cached=80 failed=155`
over all 1,281 in 1h54m — 1,127 lockfiles on disk, 88%, against the
80% the first hundred predicted. The 155 failures are genuine
dependency conflicts (`orchestra/testbench-core 10.x-dev conflicts
with laravel/framework <12.63.0|>=13.0.0`), not sandbox problems.

`sbom generate --force` then re-scanned all 1,281 in 480s:
`generated=1281 failed=0`.

**What it actually bought, measured from the SBOMs rather than
assumed.** Repositories with a resolved lockfile now carry a median of
11 dependencies against 0 for those without, and 35,607 dependency
entries against 5,408:

    with a generated lockfile   1,127 repos   35,607 deps   median 11
    without                       154 repos    5,408 deps   median  0

289 of the 1,127 still scan empty, and the breakdown matters because
it says the mechanism is fine:

    runtime dependencies > 0, SBOM empty       0   <- would be a bug
    lockfile holds only `packages-dev`       202
    lockfile genuinely empty                  87

Not one repository with a runtime dependency produced an empty SBOM.
The 202 are libraries whose whole dependency set is `require-dev` —
`cocur/slugify` has 79 of them and zero runtime — and Syft excludes
dev dependencies by design, consistently across ecosystems
(`include-dev-dependencies: false`). Syft says so itself rather than
failing silently: `php-composer-lock-cataloger: unable to determine
packages`.

So the honest figure is **838 repositories gained dependency data**,
not 1,127. For coverage specifically, 110 of the 192 PHP repositories
that have no dependency row at all now have a non-empty SBOM, which
should take PHP from 1,089/1,281 to **1,199/1,281 — 85% to 94%** at
the next rebuild.

**How much this is worth, measured rather than assumed.** The
dependency-graph ingest already covers most of what `sbom lock` was
meant to reach: of the 237 repositories resolved so far, **221 already
had dependency data** (167.8 packages on average) and 16 did not. Across
all PHP, 192 of 1,281 repositories (15%) have no dependency row at all,
so at 80% resolution this can add about **153 repositories** — worth
doing, and an order of magnitude less than "Composer coverage is 22%"
suggested before depgraph landed.

A resolved lockfile is not in the dataset until `db index` runs; the
`sbom generate` half is done.

**That generate must carry `--force`, and it took a probe to find out.**
`--use-generated-locks` is on by default, but three separate gates skip
a repository whose SBOM already exists — `repo.id in visited_ids`,
`output_file.exists()`, and the content-hash cache — and every one of
them is spelled `if not force`. So the obvious command reports success
having done nothing at all, for exactly the repositories this is meant
to fix: they are the ones that already have an SBOM, just a dependency-
free one.

Measured on `sebastianbergmann/phploc`, which has a resolved lockfile
and no dependency row:

    syft on the stored tree          1 package   (the root, alone)
    with composer.lock merged in     7 packages  (root + all 6)

The seven match the lockfile's six exactly. `--force` also bypasses the
content-hash cache, so all 1,281 PHP repositories re-scan rather than
just the ~470 with lockfiles — 1.2s each over 5 workers, about 5
minutes. Cheap enough not to optimise.

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

**And "only the address" understates it, measured rather than
predicted.** The quick tunnel serving this stopped on its own and left
its process running:

    ERR no more connections active and exiting
    INF Tunnel server stopped

`ps` reported 2h21m of uptime for a tunnel that had been dead for two
hours and ten minutes, so neither a process check nor a port check
would have caught it — only a request does. Its hostname is
unrecoverable: a quick tunnel's name is gone once it stops.

**A request from this machine is not enough either.** Three tunnels
after that one were also reported dead, and all three were serving
normally: `systemd-resolved` here does not resolve
`*.trycloudflare.com`, so `curl` returned `000` for a hostname that
answered on both 1.1.1.1 and 8.8.8.8 and returned HTTP 200 with real
row counts when the resolver was bypassed. `scripts/health.sh` now
resolves public hostnames over DoH. An instrument that cries outage
costs more than no instrument, and this one did it three times before
the difference was checked.

QUIC is the other half of the flapping: `failed to dial to edge with
quic: timeout` appears 8-11 times per tunnel log, and one tunnel died
of it. `--protocol http2` is the usual remedy and is worse here — this
cloudflared is 2024.6.1 and http2 never completes a handshake
(`TLS handshake with edge error: EOF`), so it never registers at all.
Left on the default.

The backend takes the site down the same way and needs less to do it.
`npm run build` while `wrangler dev` is running removes the
content-hashed chunk the live runtime already resolved:

    ✘ No such module "assets/node-CfGHaKin.js"
    ✘ The Workers runtime failed to start.

It exits, and the port keeps listening for a moment afterwards, so a
check run right after the build sees 8787 and reports healthy. Both are
in memory now; `scripts/tunnel-named.sh` is the fix for the first half
and a restart-after-build for the second.

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
