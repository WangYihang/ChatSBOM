<h1 align="center">ChatSBOM</h1>

<p align="center">
  <strong>Talk to your Supply Chain. Chat with SBOMs.</strong>
</p>


ChatSBOM is a CLI tool for indexing and querying Software Bill of Materials (SBOM) data, providing deep insights into project dependencies.

<p align="center">
  <img src="figures/use-cases/gin/03.png" alt="Gin">
</p>

<p align="center">
  <img src="figures/demo.gif" alt="Demo">
</p>

## Features

- **Discover**: Find high-quality repositories on GitHub by stars and language.
- **Collect**: Enrich metadata and fetch dependency files (`go.mod`, `package.json`, etc.).
- **Generate**: Transform files into standard SBOM format using [Syft](https://github.com/anchore/syft).
- **Index**: Load SBOM data into [ClickHouse](https://clickhouse.com/) for high-performance queries.
- **Attribute**: Tell **direct** dependencies from **transitive** ones by parsing manifests.
- **Query**: Use the CLI for stats/searches to get insights into project dependencies.
- **Chat**: Use the AI-powered natural language chat to chat with SBOM data.
- **Publish**: Load the dataset into D1 and serve an interactive dashboard from the edge.

## Deployment

See [DEPLOY.md](DEPLOY.md). The short version: collection runs wherever
you keep it (it needs Syft and Docker, which Cloudflare Workers does not
have), and only the exported dataset reaches the edge — so the serving
side has no running cost beyond bandwidth.

## Getting Started

### 1. Prerequisites

- [Docker](https://www.docker.com/) (for ClickHouse)
- [Syft](https://github.com/anchore/syft) (for SBOM generation)
- [uv](https://github.com/astral-sh/uv) (for AI-powered chat feature)

### 2. Installation

```bash
# Via pip
pip install chatsbom

# Via pipx
pipx install chatsbom

# Or run directly via uvx
uvx chatsbom
```

### 3. Setup

#### Start Database

Option 1: Using docker compose

```bash
docker compose up -d
```

Option 2: Using docker run

```bash
docker run -d --name clickhouse -p 8123:8123 --ulimit nofile=262144:262144 clickhouse/clickhouse-server:25.12-alpine
docker exec clickhouse clickhouse-client -q "CREATE DATABASE IF NOT EXISTS chatsbom"
docker exec clickhouse clickhouse-client -q "CREATE USER IF NOT EXISTS admin IDENTIFIED BY 'admin'"
docker exec clickhouse clickhouse-client -q "GRANT ALL ON *.* TO admin WITH GRANT OPTION"
docker exec clickhouse clickhouse-client -q "CREATE USER IF NOT EXISTS guest IDENTIFIED BY 'guest'"
docker exec clickhouse clickhouse-client -q "GRANT SELECT ON chatsbom.* TO guest"
docker exec clickhouse clickhouse-client -q "ALTER USER guest SET PROFILE readonly"
```

#### Configure Environment: Set your API keys

```bash
export GITHUB_TOKEN="your_github_token"
export ANTHROPIC_AUTH_TOKEN="your_anthropic_token"
```

### 4. Basic Workflow

```bash
# 1. Search and collect data
chatsbom github search --language ruby --min-stars 1000
chatsbom github repo --language ruby
chatsbom github release --language ruby
chatsbom github commit --language ruby
chatsbom github content --language ruby

# 2. Generate and index SBOMs
chatsbom sbom generate --language ruby
chatsbom db index --language ruby

# 3. Count package-to-package edges
chatsbom db edges --rebuild

# 4. Query insights
chatsbom db status
chatsbom db query mail --direct-only
chatsbom chat

# 5. Serve the dashboard, and expose it
./scripts/serve.sh
```

`serve.sh` builds the SPA, starts the Worker against the local
ClickHouse and opens a quick `cloudflared` tunnel. Only the Worker's
port is forwarded — ClickHouse binds the loopback interface and is not
reachable through it.

A quick tunnel's hostname changes every restart, and this machine has
seen new ones fail to resolve for several minutes after creation. For an
address that stays put, once:

```sh
cloudflared tunnel login              # interactive, picks the zone
./scripts/tunnel-named.sh sbom.example.com
```

See `DEPLOY.md` for the other serving model, a D1 snapshot at the
edge.

## Command Reference

### `chatsbom github` — collection

| Command | Purpose |
| --- | --- |
| `search` | Find repositories by language and star count |
| `repo` | Enrich each repository with full GitHub metadata |
| `release` | Collect releases and tags |
| `commit` | Resolve the commit SHA for each download target |
| `tree` | Fetch the file tree for a commit |
| `content` | Download the dependency manifests and lockfiles |
| `depgraph` | Download GitHub's own dependency graph as a second SBOM source |
| `readme` | Download README content |
| `classify` | Classify repositories and extract metadata using an LLM |

### `chatsbom sbom` — generation

| Command | Purpose |
| --- | --- |
| `generate` | Run Syft over the downloaded content to produce SBOMs |
| `lock` | Resolve a lockfile for projects that ship none, inside a container |

### `chatsbom db` — indexing and querying

| Command | Purpose |
| --- | --- |
| `index` | Load repositories, releases and SBOM artifacts into ClickHouse |
| | `--rebuild` discards rows written under an older schema |
| `edges` | Count package-to-package dependency edges and store them |
| | `--rebuild` recounts rather than adding to the stored counts |
| `status` | Row counts, per-language totals, framework adoption |
| `query` | Find the repositories that depend on a package |
| `export` | Export projects and their detected frameworks to CSV |

`db edges` reads the stored dependency-graph documents rather than the
`artifacts` table, because the edges are not in it: `artifacts` records
what a repository depends on, not what one package pulls another in by.
It is separate from `db index` because the two cost differently —
rebuilding `artifacts` is minutes over 28,000 repositories, and
recounting edges is a walk of the stored graphs.

`db query` takes `--direct-only` to restrict results to repositories that
declare the package in their own manifest, rather than inheriting it
through another dependency.

### `chatsbom queue` — continuous collection

| Command | Purpose |
| --- | --- |
| `track` | Register collected repositories in the work queue (idempotent) |
| `backfill` | Record stage watermarks for work already on disk |
| | Reports by default; `--apply` writes |
| `sync` | Re-check the stalest repositories and record what changed |
| `status` | Queue health: tracked, outstanding, stale and stuck |

`queue backfill` exists because the queue schedules by comparing each
stage's watermark against the newest push it has seen, and a stage that
ran before the ledger did has no watermark — so the queue reads it as
never collected. Measured before it was run: 467 of 24,568 rows carried
any watermark and none carried a `depgraph` one, against 24,936 stored
dependency graphs, so every stage reported 100% outstanding. After:

| stage | outstanding before | after |
| --- | --- | --- |
| `content` | 100% | **13%** |
| `sbom` | 100% | **13%** |
| `depgraph` | 100% | **22%** |

Nothing is re-fetched: the stored documents are the evidence and their
own timestamps are the watermark — `creationInfo.created` for a
dependency graph, the file's mtime for a syft SBOM. Never the clock,
which would say every stage finished when the backfill ran.

`release`, `commit` and `tree` stay at 100% because they write one
ledger per language rather than per repository, so nothing in them says
when an individual repository was seen.

The dataset is meant to stay fresh rather than be re-collected. Measured
on the corpus itself, **25.3% of repositories are pushed in a given week
and 41.4% have not been pushed in a year**, so a full weekly re-collection
would spend three quarters of the rate budget reproducing identical
results.

`queue sync` instead re-checks only the repository resource, and does it
**conditionally**. Verified against the live API by reading
`X-RateLimit-Remaining` off the responses:

```
10 unconditional 200s     remaining 5000 -> 4990   spent 10
the same 10 with ETags    remaining 4990 -> 4990   spent  0
```

So revalidating an unchanged repository is free. A repository whose
`pushed_at` moves becomes due for every later stage; one that did not
change costs nothing and no stage advances.

Every slice is bounded by both a row limit and a **quota budget**, because
304s are free but 200s are not:

```bash
chatsbom queue track                        # once, after discovery
chatsbom queue sync --slice 500 --quota 250 # what a timer runs
chatsbom queue status                       # what to alarm on
```

`repo` is the change detector, so it polls on a clock (`--recheck-hours`,
default 6). Every other stage is derived: due only once a newly observed
push overtakes its watermark — re-running Syft on an unchanged tree is
waste.

Slices are safe to interrupt. Outcomes are written as they happen and
claims are leased, so killing the process loses at most the repository in
flight.

#### Running it continuously

Containerised, so it leaves nothing behind on a machine you also use for
other things:

```bash
export GITHUB_TOKEN=ghp_...
export UID=$(id -u) GID=$(id -g)   # see below
docker compose --profile collect up -d --build
docker compose logs -f collector
docker compose down          # gone: no units, no host Python, no host syft
```

`UID`/`GID` are not optional. `data/` and `.cache/` are bind mounts owned
by whoever cloned the repo, so a container running as its own baked-in
uid cannot write them — the first symptom is
`sqlite3.OperationalError: attempt to write a readonly database` from the
ledger. Putting them in a `.env` beside the compose file works too.

The collector is behind a profile, so a bare `docker compose up` still
starts only ClickHouse — spending GitHub rate budget should be a decision
rather than a side effect. `docker compose run --rm cli <args>` runs any
stage by hand in the same image, against the same mounted `data/`, so a
manual run and the loop share state.

Continuous trickle rather than a nightly batch, for a reason that is
arithmetic rather than taste: the ~6,200 repositories pushed in a week
cost roughly 62,000 requests, which is 369/hour spread across the week —
7.4% of one token's allowance. Run as a batch and it saturates a token
for 12 hours.

The scheduler is a `sleep` loop, not cron-in-a-container: the interval is
the only schedule there is, `docker compose logs -f` is the whole
observability story, and Docker's restart policy already covers the crash
case a supervisor would.

**`sbom lock` gets its own nested daemon**, so it needs nothing on the
host either:

```bash
docker compose --profile lock run --rm lock sbom lock --language java
```

The question that shapes this is *where an escape lands*. `sbom lock`
runs an ecosystem's own resolver — a Gemfile is Ruby, a POM runs build
plugins — and mounting the host Docker socket into the collector would
put an escape on the host daemon, which is host root. Instead a
`docker:27-dind-rootless` sidecar provides the daemon: its own root maps
to an unprivileged host uid, it publishes no port, and `compose down`
destroys it.

Two things that took measuring rather than reasoning:

- Under a rootless daemon, `--user` is what *breaks* the output write.
  A rootful daemon maps container uid 1000 to host uid 1000; a rootless
  one maps container *root* to the unprivileged host user, so an explicit
  uid lands on a subuid owning nothing and the resolver fails with
  `cp: /out/Gemfile.lock: Permission denied` after doing all the work.
  The sandbox now probes `docker info` and drops only that flag.
- `./data` is mounted on the daemon as well as on `lock`, at the same
  path. A container the daemon starts resolves a bind mount against
  *its own* filesystem, so a path only `lock` could see would mount
  nothing, silently.

Verified end to end: a hostile Gemfile writing to `/project` and `/etc`
was stopped at both, and discourse's `Gemfile.lock` came out resolved and
owned by the invoking user.

`sbom lock` stays out of the collector loop regardless — it is expensive
and runs project-controlled code, so it should be a decision each time
rather than a background habit.

The Docker client lives only in the `lock` image, never the collector's.
An image with a Docker client and a reachable socket is one mistake away
from being an escape; splitting the images makes that a property of the
build rather than a rule someone has to remember.

For a dedicated server rather than a dev machine, `deploy/systemd/` has
units for the same two schedules, hardened with `ProtectSystem=strict`
and `ReadWritePaths` limited to `data/` and `.cache/`.

#### Why there is no message broker

The ledger *is* the queue, and a better fit than a broker. Its items are
durable per-repository state — the ETag held, how far each stage has got,
how many times it has failed — not messages. A broker gives at-least-once
delivery of ephemeral tasks; lose the message and you lose that unit of
work. A killed process loses nothing here, because progress is a
watermark and claims are leased rather than held.

A broker earns its place with many independent producers and tasks cheap
to retry from scratch. Here there is one producer (the clock) and work
that is expensive and idempotent per repository.

`queue status --metrics` emits Prometheus text format for a textfile
collector. Ages are exported as seconds-since, so an alert is a threshold
rather than arithmetic in the rule:

```
chatsbom_queue_tracked                24568
chatsbom_queue_never_checked          24118
chatsbom_queue_failing                    3
chatsbom_queue_oldest_check_seconds  1016.56
chatsbom_queue_due{stage="repo"}      24118
```

The two to alarm on: `chatsbom_queue_due` growing steadily means the
slice size or cadence is too low, and `chatsbom_queue_failing` growing
means something is wrong that backoff is quietly hiding.

Never-checked repositories sort first, so during the initial sweep every
check is unconditional and `sync` reports a 0% free ratio. That figure
only becomes meaningful once `queue status` shows nothing never-checked.
Verified on 60 repositories that had been checked once:

```
with stored ETags     59x 304, 1x 200   spent  1
the same 60, no ETag  60x 200           spent 60
```

The single 200 is a repository that genuinely received a push between the
two checks — which is the signal the whole mechanism exists to detect.

### `chatsbom data` — housekeeping

| Command | Purpose |
| --- | --- |
| `prune` | Keep the newest N scans per repository; discard older ones |

Retention is not optional once collection is continuous. A single
snapshot already occupies 46 GB under `data/` — 16 GB of SBOMs, 9.8 GB of
downloaded content, 8.3 GB of file trees — and every new commit adds
another content tree and another SBOM. Without pruning the disk fills and
collection stops silently, which is the worst failure mode available.

What is removed are *inputs*: recomputable from GitHub and keyed by
commit. The history that matters has already been appended to ClickHouse,
so nothing analytical is lost.

```bash
chatsbom data prune --keep 2          # reports only
chatsbom data prune --keep 2 --apply  # deletes
```

Known gap: `03-github-release` and `04-github-commit` hold one JSONL
ledger per language rather than per-scan directories, so scan retention
does not reach them (5.7 GB each). They are also deduplicated by
repository id, which means a re-collected repository's *new* releases are
never appended — that needs fixing separately before continuous
collection can keep release data fresh.

### `chatsbom export` — portable artefacts

| Command | Purpose |
| --- | --- |
| `parquet` | Write the dataset as Parquet plus a checksummed manifest |
| `d1` | Write SQL that loads the dataset into Cloudflare D1 |
| `schema` | Emit the export contract as JSON and/or TypeScript types |

The dataset reaches the edge as a database. `web/` is a Cloudflare
Worker serving a dashboard that asks it by method name — a visitor
downloads about 110 KB and every answer is one request. See
`web/README.md`.

`web/` also serves an AI question box. The agent loop runs **in the
browser**, one model turn per request, and the model's only tools are
the same typed queries the dashboard's own controls use — it cannot pass
SQL. The queries themselves run in the Worker.

An earlier design shipped 6.1M rows as 20.6 MB of Parquet for a query
engine in the browser. It worked, but a first load cost 28 MB: 7.7 MB of
WebAssembly plus the whole dataset, because the engine downloaded each
file rather than reading ranges of it. `export parquet` still produces
those files — a self-describing copy that DuckDB or pandas reads
directly, worth attaching to a release — but nothing serves them.

`export d1` targets a serving model with a real database behind it,
for the case where shipping the data to the browser is the wrong
trade-off. It writes four scripts applied in order — schema, data,
aggregates, indexes — and normalises the artifact rows on the way out.
That normalisation is not cosmetic: a direct translation of the Parquet
schema measures 762.6 MB in SQLite once the indexes the queries need are
present, which is over D1's 500 MB free tier, while interning the
repeated strings brings it to 294.7 MB with no rows lost. Most of the
saving is one table — the five low-cardinality columns take only 45
distinct combinations across 6,062,896 rows, and were stored as five
strings on every one of them.

The aggregates are precomputed because no index can help them. The
overview's panels read every artifact row by definition; measured on the
real corpus they cost 3,122 ms for the source comparison and 1,082 ms
for the relationship split, and on D1 that is the bill as well as the
latency, since it charges for rows read. Precomputed they answer in
3-4 ms from tables totalling 44 KB. The point lookups are left alone —
`dependentsOf` already answers in 4 ms straight off the indexes, and it
takes an arbitrary package name, so there is nothing finite to
precompute.

`export schema` is the seam between the two languages. `src/schema.ts` in
the web project is generated from `chatsbom/export/schema.py`, so a
renamed column is a TypeScript compile error rather than an `undefined`
at runtime — and a test fails if the checked-in copy goes stale.

### `chatsbom openapi` — OpenAPI specification analysis

| Command | Purpose |
| --- | --- |
| `candidates` | Find repositories that ship an OpenAPI specification |
| `clone` | Clone candidate repositories for version-by-version analysis |
| `list-paths` | Export the API paths declared in each specification |
| `drift` | Measure how API paths change across releases |
| `plot-drift` | Render the drift data as a figure |
| `stats` | Summarise specification counts and sizes |

### `chatsbom chat` — AI querying

Starts a terminal UI that answers natural-language questions by querying
ClickHouse. Requires `ANTHROPIC_API_KEY` or `ANTHROPIC_AUTH_TOKEN`.

## Direct vs Transitive Dependencies

Syft reads lockfiles, so an SBOM is the *resolved closure* of a project's
dependencies. Of the 118 Ruby repositories in our dataset whose SBOM lists
`mail`, only 17 actually declare it — the rest inherit it through
`actionmailer` or `devise`.

ChatSBOM parses the manifests alongside the lockfiles and records how each
dependency arrived:

| Value | Meaning |
| --- | --- |
| `direct` | The project's own manifest declares the package |
| `transitive` | Another dependency pulled it in |
| `unknown` | No manifest could be read, so the question is unanswered |

Supported manifests: `Gemfile`/`*.gemspec`, `package.json`, `go.mod` (honouring
`// indirect`), `Cargo.toml`, `pyproject.toml`/`requirements*.txt`,
`composer.json`, `pom.xml`/`build.gradle`.

### Two SBOM sources

Syft only sees what a lockfile tells it, which is why Maven and Composer
projects come back nearly empty. `github depgraph` adds GitHub's own
dependency graph, which parses manifests server-side:

| Repository | Syft | Dependency graph |
| --- | --- | --- |
| `spring-projects/spring-boot` | 0 | 303 |
| `elastic/elasticsearch` | 0 | 107 |
| `NationalSecurityAgency/ghidra` | 0 | 147 |

The two are complementary, not interchangeable, so every artifact row
records which produced it:

| Column | Meaning |
| --- | --- |
| `source` | `syft` (lockfile, resolved closure) or `github-depgraph` (manifest, declared only) |
| `version_kind` | `resolved` (exact), `constraint` (`>= 0`, `^4.18`) or `unversioned` |

GitHub's graph is flat — the repository `DEPENDS_ON` each package with no
tree — so its rows are always `direct`. Its versions are the manifest's
constraints, which `version_kind` marks so a range is never charted as if
it were a resolution.

`repositories.manifest_sources` records which manifest files were read, so
a `transitive` verdict can be told apart from an unexamined one.

## The dataset keeps history

`artifacts` is **append-only**. Each row is an observation — this package,
at this version, in this repository, as seen in this scan — so an update
adds rows rather than replacing them.

That is a deliberate choice, and it decides what the project can answer.
Overwriting the current state destroys information on every refresh:
"how long did projects take to move off `mail` 2.7" is unanswerable once
the rows that knew are gone. Storage is no argument against it — 6.1M
rows compress to 17 MB, and a year of weekly deltas to roughly 220 MB.

| | |
| --- | --- |
| Engine | `MergeTree`, partitioned by `toYYYYMM(observed_at)` |
| Current state | derived by joining on the repository's recorded `sbom_commit_sha` |
| History | the whole table, pruned by partition for a bounded window |

Counting is always `count(DISTINCT repository_id)`, which is also what
makes re-indexing the same scan harmless: a plain `MergeTree` does not
deduplicate, so the queries must.

Two queries exist only because of this: `get_version_history` (every
version of a package, with when it first appeared) and
`get_adoption_over_time` (monthly repository counts, split by direct and
transitive). `export parquet` writes them to a separate `history.parquet`
so the dashboard's current-state payload stays small.

### Resolving missing lockfiles

`sbom lock` closes the remaining gap: where a project ships no lockfile,
it runs the ecosystem's own resolver to produce one, which `sbom generate`
then folds into the scan.

Resolving dependencies means **executing project-controlled code** — a
`Gemfile` is Ruby evaluated on load, a POM runs whatever build plugins it
declares, `composer` runs `scripts` hooks. Doing that on the host across
thousands of unvetted repositories is not acceptable, so every resolution
runs in a container with:

- the project mounted **read-only**, and exactly one writable path (the
  output directory) — no other host path is visible
- `--user` set to the invoking user, never root: root in the container is
  root on a bind mount
- `--cap-drop ALL`, `--security-opt no-new-privileges`, `--read-only`
  root filesystem with a `tmpfs` scratch
- bounded memory, CPU, process count and wall-clock time
- images pinned to explicit versions, so generated lockfiles are
  reproducible

Network access is the one thing that cannot be removed — resolution *is*
fetching metadata from a registry. That is the residual risk, and it is
why nothing else is granted. Requires Docker.

Recipes exist for Java, PHP, Ruby and Python. Go, Rust and npm are absent
on purpose: those ecosystems commit lockfiles as a matter of course, so
Syft already reads them (Go coverage is 90%, Rust 69%).

## Development

```bash
uv sync
uv run pytest                  # unit tests
docker compose up -d           # start ClickHouse for integration tests
uv run pytest                  # now includes the query-layer integration tests
uv run pre-commit run -a       # lint, format, type-check
```

Query-layer tests run against a real ClickHouse and are skipped when one is
not reachable on `localhost:8123`.

### Database accounts

`docker compose` creates two accounts. `admin` owns the schema and is used
by `db index`; `guest` is read-only and is what `db query`, `db status`,
`db export` and `chat` connect as.

The `guest` profile bounds query *cost*, not just privileges — execution
time, memory, rows read and result size — because `readonly` alone does not
stop one expensive join from exhausting the server. See
`database/config/users.d/guest.xml`.

Grants for `guest` live in that same file: a user defined in `users.xml`
is read-only storage, so `GRANT` at runtime fails with
`ACCESS_STORAGE_READONLY`. Pointing `CLICKHOUSE_DB` at a different
database means adding a matching `<query>GRANT SELECT ON ...</query>`
line there.

The committed passwords are development defaults. For any deployment
reachable from outside localhost, replace `<password>` with
`<password_sha256_hex>` and supply `CLICKHOUSE_ADMIN_PASSWORD` /
`CLICKHOUSE_GUEST_PASSWORD` from the environment.

## Use Case: Analyzing Framework Adoption

Find the most popular projects depending on a specific library (e.g., `gin`) using natural language.

<p align="center">
  <img src="figures/use-cases/gin/01.png" alt="Query">
</p>

<p align="center">
  <img src="figures/use-cases/gin/02.png" alt="Result">
</p>
