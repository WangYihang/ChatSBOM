# ChatSBOM from first principles

Status: **parked**, a direction for the next version, not a plan to execute
now. Written 2026-09-29, during the first full collection after the
repository-centric redesign (#51, #55). Nothing here should change while
that collection runs.

The question behind it: ignoring what exists, what is the least machinery
that does what ChatSBOM does? The answer is less about which database to
use than about how little state needs to be mutable at all.

## 1. What the system actually does

| Workload | Nature | Scale today |
|---|---|---|
| Collection | For each repository at a commit, a chain of pure, idempotent steps: pick a release, list the tree, fetch manifests, run Syft, fetch the dependency graph. Same inputs, same outputs. Paced by external rate limits. | ~65k repositories × ~9 stages |
| Artefacts | Trees, manifests, SBOMs, graphs: written once, never edited. | tens of GB |
| Analysis | Distinct counts, group-bys and rankings over every dependency row. | tens of millions of rows, growing |
| Serving | A public, read-only dashboard over mostly precomputed answers. Low traffic. | small |

## 2. The design those needs imply

### 2.1 One source of truth: an immutable, content-addressed object store

Every artefact lives at a key determined by what produced it:
`<stage>/<repository id>/<commit>/<tool@version>/…` (plus the input key
where one stage has several inputs). Written once, never overwritten. A
filesystem today; S3/R2 later without a change of model.

Everything else — tables, rollups, dashboards, exports — is **derived** from
it and can be rebuilt from it at any time.

### 2.2 Scheduling is derived, not stored

This is the build-system model (make, Bazel): a stage is done for a
repository exactly when its output object exists for the current input key.

    due = { (repository, stage) : repository ∈ snapshot,
                                  output(stage, input_key) ∉ store }

The input key already captures what should trigger work: the commit (a
push), the stage and tool version (a code change), the upstream stage's
output (a new release chosen). No watermark has to be kept in step with the
files, because the files are the watermark.

What remains genuinely mutable is small and append-only:

- conditional-request state (ETags, last seen push) per repository;
- negative results and backoff (404 for 30 days, repeated 5xx);
- the search snapshots themselves (dated, immutable once written).

Each can be a dated record in the same store or a small append-only table.

**Concurrency without locks.** Workers take a deterministic shard,
`repository_id mod N == k`. No two workers ever want the same repository,
so there is nothing to claim, lease or lock. Changing N means restarting
the workers with the new N; idempotent stages make any overlap during the
switch harmless. This removes the class of failure behind #98 (SQLite
writers timing out on each other) rather than tuning around it.

### 2.3 Analysis is a disposable, columnar derivative

An OLAP engine reads the store (or a normalised copy of it, e.g. Parquet
files of artifact rows per repository and commit) and computes the rollups.
Because it is derived, it can be dropped and rebuilt, and schema changes
become rebuilds rather than migrations.

- At single-machine scale (tens to hundreds of millions of rows),
  **DuckDB over Parquet** is enough and needs no running service.
- **ClickHouse** earns its place when many concurrent live queries hit
  the raw rows, or when the data outgrows one machine's comfortable DuckDB
  range.

Either way, the analytical database is never the only copy of anything.

### 2.4 Serving is decoupled from the collection host

Publish precomputed answers (rollup tables, top lists, per-package pages)
and the Parquet files to the edge — R2 + a Worker, or D1 — and serve from
there. The collection machine can be down, rebuilding, or saturated without
the site noticing. A tunnel (e.g. Cloudflare Tunnel) is the right tool for
internal/admin access to the live system, not for the public entry point.

#128 went another way for now: each pass publishes a read-only SQLite
snapshot, and one Python service on the same machine serves it, behind a
Cloudflare tunnel that is its only way in (#145). Serving never waits on
collection, but it shares the machine. The Worker and D1 are gone (#151).

### 2.5 The minimum

1. an object store as the single source of truth;
2. one columnar analytical engine, derived and disposable;
3. edge publishing for the public, read-only side.

No transactional database (neither Postgres nor SQLite) is required.

## 3. Where the current system stands

Already close:

- Repository-id-keyed paths plus the per-stage input keys (PR B, #59)
  are most of a content-addressed layout.
- The analytical database is derived: the warehouse, a DuckDB file, is
  rebuilt from the store by every pass (#131), and is the only index
  since the ClickHouse server went (#153).
- The collection stages are idempotent and cached by input.
- Serving is decoupled from collection: each pass publishes a read-only
  snapshot, which the web service reads and nothing else writes (#132,
  #145).

The gaps:

1. **Scheduling state is a mutable ledger** (`data/ledger.sqlite3`,
   `stage_state` with leases) kept alongside the files, rather than derived
   from which outputs exist. Leases are needed only because workers share
   one queue.
2. **History is what retention keeps.** The warehouse has the scans the
   store keeps, the newest `data prune --keep` of each repository.
   ClickHouse kept every scan it was given, and went unmigrated (#153);
   keeping every scan's documents, and pruning the trees alone, is
   #128's Q10.
3. **The search snapshot and the ledger overlap** as descriptions of
   "which repositories exist and what we know about them".

## 4. A path there, in steps that each stand alone

1. **Sharded workers + derived due-sets.** Compute "due" from the snapshot
   and the store's keys; give each worker a shard; keep the ETag/backoff
   records append-only. The ledger shrinks to those records or disappears.
   Fixes #98 as a side effect.
2. **Make every table rebuildable.** Done: the warehouse is rebuilt from
   the store by every pass (#131), and held to what ClickHouse answered
   of the same inputs (`tests/golden/`). What existed only in
   ClickHouse, the records in `raw_documents`, was let go rather than
   moved back (#153).
3. **Normalise to Parquet** (artifact rows per repository@commit, source,
   ecosystem) as an intermediate layer, which the engine reads.
4. **Decide the engine by measurement.** Decided: DuckDB (#128, decision
   Q2), and the ClickHouse server is gone (#153).
5. **Publish** Parquet + precomputed rollups; admin access through a
   tunnel. A pass publishes a snapshot, its rollups precomputed, which one
   Python service serves through a tunnel, and the Parquet export runs
   weekly (#150, #151); the edge is still open.

## 5. Open questions

- How much history to keep per repository (every scanned commit, or the
  current scan plus N previous)?
- Where the object store should live long-term (local disk, R2, S3), given
  ~40 GB now and growth with every rescan.
- Whether the dependency-graph source is still worth keeping after GitHub
  retires the synchronous endpoint (2026-11-13) and the report endpoint's
  cost is known.
