<h1 align="center">ChatSBOM</h1>

<p align="center">
  <strong>Talk to your Supply Chain. Chat with SBOMs.</strong>
</p>


ChatSBOM is a CLI tool for indexing and querying Software Bill of Materials (SBOM) data, providing deep insights into project dependencies.

<p align="center">
  <img src="https://raw.githubusercontent.com/WangYihang/ChatSBOM/main/figures/use-cases/gin/03.png" alt="Gin">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/WangYihang/ChatSBOM/main/figures/demo.gif" alt="Demo">
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
- [uv](https://github.com/astral-sh/uv) (optional: `uvx` runs chatsbom without installing it)

### 2. Installation

```bash
# Via pip
pip install chatsbom

# Via pipx
pipx install chatsbom

# Or run directly via uvx
uvx chatsbom
```

That installs everything the collection pipeline runs, from `github
search` to `db index`, `queue` and `run`, and every other command that
needs nothing more. The few that need a large library of their own take
an extra: without it, such a command stops and says which one to
install, and its `--help` works either way.

| Extra | For | Installs |
| --- | --- | --- |
| `chat` | `chat` | the Claude Agent SDK, textual |
| `classify` | `github classify` | instructor, openai |
| `openapi` | `openapi drift`, `list-paths` and `stats` | pandas, tiktoken |
| `export` | `export parquet` | pyarrow |
| `web` | `web serve` | FastAPI, uvicorn, ALTCHA, the OpenAI SDK |
| `all` | all of the above | |

```bash
pip install 'chatsbom[chat]'              # one
pip install 'chatsbom[chat,export]'       # several
uv tool install 'chatsbom[all]'           # all of them
uvx --from 'chatsbom[chat]' chatsbom chat
```

The quotes keep a shell from reading the brackets as a pattern. The
extras are extras for their size: the Claude Agent SDK alone is 218 MB,
and pyarrow 152 MB, where the rest of chatsbom is under 80 MB. The
collector's image has none of them ([Running it
continuously](#running-it-continuously)).

### 3. Setup

#### Start Database

Option 1: Using docker compose

```bash
docker compose up -d clickhouse
```

Option 2: Using docker run, from the repository root

```bash
docker run -d --name clickhouse \
  -p 127.0.0.1:8123:8123 --ulimit nofile=262144:262144 \
  -v "$PWD/database/data:/var/lib/clickhouse" \
  -v "$PWD/database/config/users.d:/etc/clickhouse-server/users.d" \
  -v "$PWD/database/config/config.d/logs.xml:/etc/clickhouse-server/config.d/logs.xml" \
  clickhouse/clickhouse-server:26.8-alpine
```

The accounts come from `database/config/users.d`, as they do under
compose: `admin`, and a read-only `guest` with its grants and the limits
on what one query may cost. The port is published on the loopback
interface alone, as compose publishes it, since `admin` can create users
and grant anything. `logs.xml` bounds the server's own logs, as it does
under compose; it is mounted as one file, because mounting `config.d`
would hide the image's `listen_host` setting. `chatsbom db index` creates
the database the first time it runs.

#### Configure Environment: Set your API keys

```bash
export GITHUB_TOKEN="your_github_token"
export ANTHROPIC_AUTH_TOKEN="your_anthropic_token"
```

Or keep them in a `.env` file. `chatsbom` reads the one in its working
directory, or in the nearest parent directory that has one, and a
variable already set in the environment wins over the file. Compose
reads the `.env` beside `docker-compose.yaml`, so from the repository
root the two are the same file. `.env.example` lists every setting with
its default commented out, so a copy of it changes nothing until you
edit it. Leave `ANTHROPIC_BASE_URL` unset unless you mean it: `chat`
sends your token to whatever endpoint it names.

### 4. Basic Workflow

```bash
# 1. Search every language, and queue what it found. The search writes a
#    dated snapshot, data/01-github-search/all-<YYYY-MM-DD>.jsonl
chatsbom github search --min-stars 1000
chatsbom queue track --snapshot data/01-github-search/all-<YYYY-MM-DD>.jsonl
chatsbom queue sync

# 2. Collect: release, commit, tree, every manifest the tree lists (any
#    depth, every ecosystem), and an SBOM of them
chatsbom run --limit 50

# 3. Land and index (every repository the ledger tracks)
chatsbom db raw --apply
chatsbom db index

# 4. Count package-to-package edges
chatsbom db edges

# 5. Query insights
chatsbom db status
chatsbom db query mail --direct-only
chatsbom chat                    # with the `chat` extra

# 6. Serve the dashboard
docker compose up -d
```

That starts two services and nothing else: ClickHouse, and the
dashboard; the tunnel mode, below, adds a third, the tunnel. The
dashboard reaches the database by service name over the compose
network, so nothing about the page depends on a host port, and
`docker compose down` removes them.

`wrangler dev` is a development server and a container does not make it
a production one — see the note at the top of `Dockerfile.web`. The
mitigation is that the only intended path in is a tunnel, and that the
image switches off what a development server offers and a public one
must not: wrangler's local explorer, which reads and writes every
binding (`X_LOCAL_EXPLORER=false`), and secrets on the command line. The
ClickHouse password, `ANTHROPIC_API_KEY`, `TURNSTILE_SECRET` and
`EDGE_SECRET` reach the Worker through a `.dev.vars` the entrypoint
writes at each start, readable by the container's own user alone.

The chat's daily spend counter lives in the `web-state` volume, so a
rebuild or a `docker compose down` no longer resets the day's cap;
`docker compose down -v` does.

### Putting it on the internet

Through a Cloudflare tunnel, one of two ways.

**The tunnel mode** runs `cloudflared` as a service beside the
dashboard, on a network the two share with nothing else, and publishes
the dashboard's port nowhere, so nothing off the machine reaches it but
through the tunnel. Create a tunnel in the Cloudflare dashboard, route
the site's hostname to `http://web:8787`, and put two lines in the
`.env` beside `docker-compose.yaml`:

    COMPOSE_FILE=docker-compose.yaml:docker-compose.tunnel.yaml
    TUNNEL_TOKEN=<the tunnel's token>

`docker compose up -d` then starts it, from that directory. DEPLOY.md
has the steps, and how to check that the port is closed.

**A `cloudflared` outside this compose project** reaches the port the
dashboard publishes, `8787` on all interfaces by default. Point the
tunnel's public hostname at:

    http://host.docker.internal:8787

Two things about that address are easy to get wrong, and both were
measured here rather than assumed:

  - **On Linux the name does not exist by default.** Docker Engine
    29.8.0 does not resolve `host.docker.internal` in a plain
    container, with or without an explicit `--network bridge`. The
    tunnel container needs
    `--add-host host.docker.internal:host-gateway`; it is Docker
    Desktop that provides the name for free.
  - **It is the host gateway, not loopback.** A `127.0.0.1:8787`
    publish is invisible from there, which is why the default is all
    interfaces. That includes the LAN, and a client that reaches the
    port directly rather than through the tunnel sets the headers the
    tunnel would have — `CF-Connecting-IP`, which the rate limiters
    key on, among them. `WEB_BIND` publishes it on the docker bridge
    alone:

        WEB_BIND=172.17.0.1 docker compose up -d

    That is the bridge's address on a default install; `ip -4 addr
    show docker0` says for certain. It can live in the `.env` beside
    the compose file like any other setting. On a named tunnel, the
    Worker can also check for itself: with `EDGE_SECRET` set and a
    Cloudflare Transform Rule adding it to every request, a request
    without it shares one rate-limit bucket whatever address it
    claims (DEPLOY.md).

Never point a tunnel at `8123`. That is ClickHouse itself, and the
compose file binds it to the loopback interface precisely so it cannot
be reached from anywhere else.

`scripts/health.sh` answers whether the site is actually serving.
Liveness is a request, never a process or a port: a `cloudflared`
quick tunnel has stopped here while its process kept running and `ps`
kept reporting uptime, and `wrangler dev` killed by a concurrent build
left the port listening for a moment after it exited. The script also
resolves public hostnames over DoH, because `systemd-resolved` on this
machine does not resolve `*.trycloudflare.com` and a plain `curl`
therefore reports a working tunnel as dead. In the tunnel mode,
`--no-local` leaves out its local check, since the dashboard has no
port on the machine to ask.

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
| `tree` | Fetch the file tree for a commit (`run --stage tree`) |
| `content` | Download every manifest and lockfile the tree lists, at any depth and of every ecosystem (`run --stage content`; see below) |
| `depgraph` | Download GitHub's own dependency graph as a second SBOM source, for every repository the queue tracks (`run --stage depgraph`) |
| `readme` | Download README content |
| `classify` | Classify repositories and extract metadata using an LLM (the `classify` extra) |

`classify` asks an OpenAI-compatible API: OpenAI's,
`https://api.openai.com/v1`, for `gpt-4o-mini`, with `OPENAI_API_KEY`,
unless `OPENAI_BASE_URL` and `--model` name another endpoint and one of
its models. A server of your own, Ollama's for one, needs no key. It
classifies the repositories of the newest search snapshot,
`01-github-search/all-<date>.jsonl`, unless `--input` names a list.

### `chatsbom sbom` — generation

| Command | Purpose |
| --- | --- |
| `generate` | Run Syft over every stored content root, every ecosystem at once |
| `lock` | Resolve a lockfile, per directory, for projects that ship none, inside a container |

`generate` skips a content root while its SBOM is whole, was written by
the Syft installed now (as the SBOM's own `descriptor` says), and is
newer than every file under the root and its generated lockfiles. So an
upgrade of Syft regenerates every stored SBOM, once, and `generate` says
how many before it starts. The collector loop runs it in its daily index
pass, so that happens within a day of deploying a new Syft, and
`chatsbom run` does the same for each repository it walks (DEPLOY.md,
"Upgrading Syft"). `--force` scans every root regardless, bypassing the
Syft cache.

#### Which files are fetched

The content stage reads each repository's stored tree
(`05-github-tree/<id>/<sha>/tree.txt`) and fetches every manifest and
lockfile it lists, **at any depth and of every ecosystem**
(`chatsbom/core/discovery.py`). It used to ask for a fixed list of names
at the root only, chosen by the repository's language, so
`jeecg-boot/pom.xml`, halo's `application/build.gradle` and appsmith's
`app/server/pom.xml` were never fetched, and a repository labelled
TypeScript was never searched for its Java backend (#51).

- **One list of names** for every ecosystem, shared with the Syft cache
  key, plus the Gradle build-logic files (`settings.gradle`,
  `gradle.properties`, `*.versions.toml`).
- **Left out**: vendored and generated trees (`node_modules/`,
  `vendor/` but not Go's `vendor/modules.txt`, `third_party/`, `dist/`,
  `target/`, `build/`, …), and tests, fixtures and benchmarks.
  `examples/`, `samples/` and `demo/` are left out only when the
  repository has manifests elsewhere as well; `docs/` never is.
- **Caps**: 200 files and 64 MiB a repository (16 MiB a file). Files
  are taken shallowest first, lockfiles before manifests, then by name,
  so the same tree always keeps the same files.
- Each file is stored at its own path, `06-github-content/<id>/<sha>/
  <path in the repository>`, and `manifests.json` beside the tree says
  what was selected, fetched and left out, and why.

### `chatsbom db` — indexing and querying

| Command | Purpose |
| --- | --- |
| `index` | Load every repository the ledger tracks, its releases and its artifacts (Syft, dependency graph, Gradle manifests) into ClickHouse |
| | `--repos-file PATH` narrows to some repositories (one `owner/repo` or id per line) |
| | `--rebuild` builds the table again beside the one in use, keeps older scans, and swaps it in |
| | `--from-files` reads the `data/` ledgers instead of `raw_documents` |
| `edges` | Count package-to-package dependency edges and store them |
| | Every run replaces the stored counts; `--rebuild` is still accepted |
| `raw` | Land the collectors' documents in the database, unchanged |
| | Reports by default; `--apply` writes |
| `status` | The corpus and its coverage, repositories per ecosystem and per GitHub language (top 12 + other), framework adoption by ecosystem |
| `query` | Find the repositories that depend on a package; `--ecosystem maven` scopes to one registry, `--language` to a repository's GitHub language |
| `export` | Export projects and their detected frameworks to CSV |

`db index` masters on the ledger (`data/ledger.sqlite3`, read-only):
every tracked repository gets a `repositories` row, whether or not it
has a scan. One with a record is indexed from its newest record; one
without — seeded from a search snapshot, or whose scan failed — from
the repository resource `github repo` last fetched, else from the
ledger's own row, and still gets its dependency graph. A record filed
under `07-sbom/index.jsonl` (a repository tracked with no language) is
read like any other: no list is chosen by language. `--language` is
gone; `--repos-file` narrows instead, and is refused with `--rebuild`
as `--limit` is.

`repositories` also records `github_language` (verbatim, an attribute
only), `ecosystems` (canonical, from the current scan's artifacts and
manifests), and the dependency graph's own stamp, `depgraph_ref` and
`depgraph_commit_sha`.

`db raw` copies the Syft and dependency-graph documents into
`raw_documents` verbatim. `db index` reads about 80 bytes out of each
820-byte package entry those tools write; the rest — `cpes`,
`locations`, `metadata`, Syft's `artifactRelationships` — was on disk
and not queryable. This project has paid for that twice:
`06-github-content` stores manifests rather than sources, so PHP
lockfiles could be resolved after the fact and Java's could not, and
Java's coverage is still 46%.

It costs less than the files it copies, not more — 19.7 GiB of JSON
lands in 1.92 GiB under `ZSTD(3)`, measured. Keyed on the content hash,
so re-running it inserts nothing and the same document twice is one
row.

It is a landing zone, not a serving path: no request reads it. It is
there so a transform can be re-run without re-fetching, and so the next
person who wants a field nobody extracted does not spend a day of
GitHub quota to get it.

It holds three kinds. `syft` and `github-depgraph` are one document
per repository; `content` is one row per **manifest file**, because
`local_content_path` is a directory and those 46,335 files are the sole
evidence behind every direct/transitive verdict. They were left out of
the first pass on the grounds that the content directory holds source
files rather than JSON to query — the wrong test, since while they
lived only on disk the transform could not be re-run from the database
at all.

| kind | rows | source text |
| --- | --- | --- |
| `syft` | 28,069 | 9.86 GiB |
| `github-depgraph` | 24,936 | 9.80 GiB |
| `content` | 46,335 | 3.99 GiB |
| | **99,340** | **23.65 GiB → 2.90 GiB on disk** (8.2x) |

Nothing is lost in the copy, and the arithmetic closes: 46,433 files
found, minus 8 language ledgers, minus 81 empty and 9 whitespace-only
files, is the 46,335 stored. An empty manifest is skipped for the same
reason an empty SBOM is — a landing zone that preserves it faithfully
preserves nothing.

`db index` is the other half of that: the transform reads the
documents *and the manifests* out of `raw_documents` rather than off
disk, and since the ledgers were slimmed that is the default. Same rows either way — `observed_at`
included, because `db raw` copied each file's mtime into `fetched_at`
for exactly this reason. Verified by reading 100 documents across four
ecosystems both ways and comparing the projected rows field by field:
all 100 identical. The manifests likewise: 120 repositories across four
ecosystems, the declared set and the `sources` audit trail compared
both ways, all 120 identical — which is the check that matters, because
a different declared set means different direct/transitive labels and
that is the one thing in the table a reader cannot verify.

The repository records moved too, so `db index` reads its list, its
metadata and its releases from the database as well —
verified across all nine languages by projecting every repository both
ways and comparing the `repositories` row field by field: **28,069 of
28,069 identical**. One ledger is still read, `09-github-depgraph`'s,
and only because it names *extra* documents for repositories the graph
happens to cover.

What is in those ledgers is the remaining problem. A record in
`07-sbom/ruby.jsonl` is 63.1 KiB, of which **98% is `all_releases`**
and the stage's own contribution — one path — is 0.4 KiB. The same
record is appended again by each of `05-github-tree`,
`06-github-content`, `07-sbom` and `09-github-depgraph`, so the release
list is stored four times on disk:

| ledger | size |
| --- | --- |
| `05-github-tree` | 5.7 GB |
| `06-github-content` | 5.2 GB |
| `07-sbom` | 5.2 GB |
| `09-github-depgraph` | 5.2 GB |
| `01-github-search`, `02-github-repo` | 545 MB |

Roughly 21 of those 22 GB are the same release data repeated — data
that is already in ClickHouse as 1,154,743 `releases` rows, and now in
`raw_documents` as well. Slimming them is a separate change, because
the stage-major commands read each other's ledgers: `sbom generate`
takes the record from `06-github-content`'s and `github depgraph` from
`07-sbom`'s, so the fat record is what carries a repository from one
stage to the next. `chatsbom run` does not need it — it threads the
record itself — which is what makes the ledgers removable rather than
load-bearing.

### Reading only what changed

`db raw --apply` re-read every byte on every run: 23.65 GiB from disk,
all of it hashed, 99,340 rows inserted to net-add 46,335. The other
53,005 were byte-identical re-inserts that a merge then collapsed.
Idempotent, wasteful, and now running daily from the collector loop.

It compares first. A file whose mtime is no newer than the stored
`fetched_at` cannot have changed, so it is skipped by a `stat` rather
than opened; the ledger-derived records have no per-record file to stat
— one ledger holds 28,069 of them — so those are skipped by content
hash instead. The next full pass read **5.3 GiB instead of 23.65**,
skipped 53,005 documents, and finished in 2 minutes 10 seconds.

The skip is conservative in the one direction that matters: an
unreadable `stat` or a missing row means "read it", because a wrong
*unchanged* would freeze a document at an old version while a wrong
*changed* only costs a read.

That comparison is also how a real bug surfaced. Every `DateTime`
column in the database was eight hours early, because the insert path
called `.replace(tzinfo=None)` and `clickhouse_connect` reads a naive
datetime as *local* time:

```
inserted naive  2026-02-11 11:14:39  ->  stored 2026-02-11 03:14:39
inserted aware  2026-02-11 11:14:39  ->  stored 2026-02-11 11:14:39
```

Nothing in the read path corrected it, so the error was silent and
plausible — `artifacts.observed_at` bottomed out at `03:04:04` against
a true mtime of `11:04:04`, and the dashboard's "SCANNED" column, the
freshness panel and the export all repeated it. `chatsbom/core/instants.py`
now owns every timestamp that reaches an insert, and a test fails if
any module strips a timezone again.

`db edges` reads the stored dependency-graph documents rather than the
`artifacts` table, because the edges are not in it: `artifacts` records
what a repository depends on, not what one package pulls another in by.
It is separate from `db index` because the two cost differently —
rebuilding `artifacts` is minutes over 28,000 repositories, and
recounting edges is a walk of the stored graphs.

`db query` takes `--direct-only` to restrict results to repositories that
declare the package in their own manifest, rather than inheriting it
through another dependency.

It asks which of the packages matching the name is meant. The
candidates and the question go to stderr, and stdout holds the answer
alone, the dependents: `chatsbom db query mail > dependents.txt` still
shows what is being chosen from. 0, or no choice, cancels; an answer
that names no candidate is an error.

The `db` commands keep stdout for what they report — a table, a count,
a summary — and say anything else on stderr: an error, a warning, that
nothing was found. A failure exits 1, and a usage error, such as a
`--limit` below 1, exits 2. With `CHATSBOM_LOG_FORMAT=json`, each error
and warning they report is one JSON event, as the logs are.

### `chatsbom warehouse` — the DuckDB warehouse, beside ClickHouse

| Command | Purpose |
| --- | --- |
| `build` | Build `data/warehouse.duckdb` from the store alone: every scan, the current facts and the rollups |
| | `--output PATH` writes it elsewhere |

The warehouse of #128 (decision Q2): an embedded DuckDB file, rebuilt
from `data/` by each pass and never backed up, which is to replace the
ClickHouse server. Until that cutover ClickHouse is what the dashboard
reads. The collector's loop builds the warehouse in each index pass,
after `db index`, unless `WAREHOUSE=off` (DEPLOY.md, "The warehouse,
the snapshots and the export").

It reads the store with the parsers `db index` uses, and reads all of
it: every commit's Syft document and manifests, and every fetch of the
dependency graph, where `db index` reads the one commit a record names.
Each is a `scans` row, keyed by its input and tool@version, and what it
saw is `observations`, append-only: what `artifacts` is in ClickHouse.
`repositories` has the metadata, `repository_history` what each dated
search snapshot said of each repository, and `releases` and `edges` are
`db index`'s and `db edges`'. A repository's releases, and each scan's
ref, are its release and commit decisions' where the store has them
(the repository-keyed layout, below): the releases of the newest push
whose commit the store has a scan of, as `db index` reads the record
the last walk to reach a scan landed, and the ref each commit was
resolved from. Where it has none they are its record's: so a
repository whose record is only in `raw_documents`, as `chatsbom run`
files it, has them too.

What is current is one rule: each repository's newest scan of each
source, of the corpus, the newest complete search snapshot. The
rollups are ClickHouse's, by the same names, and a parity check holds
every one to ClickHouse's on the same input, and the releases and refs
beside them; beside a deployment,
`uv run python scripts/warehouse_parity.py` compares the warehouse with
the ClickHouse database `db index` fills, and says where the two are
meant to differ. Adoption over time,
`mv_package_month_intervals`, counts a repository in every month
between two scans that both show the package; `mv_package_month`, the
months of the scans alone, stays for that check.

A pass writes `warehouse.duckdb.building` and renames it into place when
it has finished, so `duckdb data/warehouse.duckdb` can read the last
one throughout; a second pass while one runs is refused. What it built
is printed on stdout, anything else on stderr.

DuckDB runs within limits, which fit the collector's container (4 GiB
and 2 CPUs, `docker-compose.yaml`): at most `CHATSBOM_DUCKDB_MEMORY_LIMIT`
of memory, 2GiB unless set, and `CHATSBOM_DUCKDB_THREADS` threads, 2
unless set (`.env.example`); compose gives the collector both. Every
command that opens DuckDB takes them, `snapshot build` and `export
parquet --from warehouse` too. Its own defaults are 80% of the machine's
memory and a thread per core. At the documented shape, 19.4M
observations on a 4-vCPU, 15 GB machine, deriving took 62 s and held
5.5 GB at its peak with those, and 109 s and 2.4 GB within the limits.
What does not fit is spilled to disk, 1.9 GB of it there, into a
directory of the process's own beside the file DuckDB opened,
`<file>.tmp-<id>`: two processes spilling into DuckDB's shared
`<file>.tmp` crashed each other. DuckDB removes the directory when it
closes the file. A pass removes the ones a killed pass left, and the
ones killed readers left once no process has the warehouse open, which
DuckDB's lock on the file says.

Nothing is fetched at run time. A connection is in UTC, and the zone is
ICU's, which DuckDB's wheel links in; given as a setting when the
database opened, DuckDB looked for ICU in `~/.duckdb` first, fetched
20.7 MB of it from its servers where it could write there, and failed
where it could not, as in the collector's container, whose uid has no
home. It is set once the connection is made, and DuckDB may neither
fetch an extension nor load one from disk.

### `chatsbom snapshot` — the serving snapshot, from the warehouse

| Command | Purpose |
| --- | --- |
| `build` | Publish `data/snapshots/<id>.sqlite` from `data/warehouse.duckdb`, unless the data has not changed |
| | `--warehouse PATH` reads another warehouse, `--output DIR` publishes elsewhere |

The snapshot of #128 (decisions Q3 and Q11): one read-only SQLite file
a pass publishes, which the Python web service reads. Its chat does
already, with `WEB_SNAPSHOT=data/snapshots` (`chatsbom web`, below),
and its dataset routes are to (phase 3); D1 and ClickHouse stay what
the dashboard reads until the cutover. The collector's loop runs it in
each index pass, after `warehouse build`, unless `WAREHOUSE=off`.
What it publishes is anyone's to read, whatever the umask: `site`
reads it as a uid of its own, through a read-only mount. The directory
is `0755`, `CURRENT` `0644` and each snapshot `0444`, and a directory
made by hand is opened to all by the first pass.

Its schema is `export d1`'s, so the D1 backend's statements answer from
it as they answer from D1, and its rows are `export d1`'s of the same
data, id for id. Two things differ by design: adoption over time counts
a repository in every month between two scans that both show the
package (Q9), and a repository with no dependency is dated by its
newest scan rather than by the day `db index` wrote its row. `meta`
also says which snapshot the file is, the version that wrote it, the
corpus, and each table's rows.

It adds one table, `dependants`: the rows of a package's dependants
table, stored in the order the page shows them, which the Python
dataset API (`chatsbom/dataset/`) reads a range of where D1 groups and
sorts every artifact of the package, with the same answers. At the
documented shape (16.1M facts) the most used package's page and its
counts took 171 ms from D1's tables and 14 ms from it; it costs 956 MB
of the file (1.75 GB in all) and 80 s of the build (160 s in all).

It opens the warehouse within DuckDB's limits, as `warehouse build`
does: at the documented shape, 192 s and 2.6 GB at the peak within
them, against 176 s and 3.5 GB with DuckDB's own defaults, for the same
snapshot.

The id is the hash of what the file serves, table by table and row by
row: the same content is the same id, and when `CURRENT` names it
already, nothing is published. Otherwise the file, written under a
hidden name in `data/snapshots/` with no journal, indexed, analysed,
made read-only and synced, is renamed to `<id>.sqlite`; then `CURRENT`
is replaced by a rename. Its first line names the current snapshot and
the lines after it the two published before, which are kept; a
snapshot it does not list is removed only after it has moved. Readers
open the file `CURRENT` names read-only and immutable
(`chatsbom/dataset/open.py`), so they take no lock and make no file
beside it, and a file one has open stays readable when it is removed.
A second pass while one runs is refused; what a pass that stopped left
is cleared by the next.

    sqlite3 "data/snapshots/$(head -1 data/snapshots/CURRENT).sqlite" \
        'SELECT * FROM meta'

### `chatsbom queue` — continuous collection

| Command | Purpose |
| --- | --- |
| `track` | Register collected repositories in the work queue (idempotent) |
| `backfill` | Record stage watermarks for work already on disk |
| | Reports by default; `--apply` writes |
| `sync` | Re-check the stalest repositories and record what changed |
| `status` | Queue health: tracked, outstanding, stale and stuck |
| `due` | What is due, derived from the store; `--compare`: how the ledger's due set differs, and why. Reads only |

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

#### `queue due`: the due set derived from the store

The next version schedules the way a build system does
([`docs/design/first-principles.md`](docs/design/first-principles.md),
#100): a stage is due for a repository exactly when its output for the
current input is not in the store, so no watermark has to be kept in
step with the files. `queue due` computes that set on the corpus as it
stands, before anything is scheduled from it, and `--compare` holds it
beside the ledger's.

```bash
chatsbom queue due                                  # where each stage stands
chatsbom queue due --compare                        # and why the ledger differs
chatsbom queue due --compare --shard 0/16 --json due.json
```

It reads and never writes. The ledger is opened read-only
(`Ledger.open_readonly`), and its bytes, times and WAL are as they were
afterwards; nothing is made beside it, so it works where the ledger's
directory is not writable. It is safe beside a running collector, and
across its redeploys: what the collector wrote while it read is caught
by a second look at every repository the two disagreed on (`timing`).
The report is on stdout, anything else on stderr.

**The universe** is the newest complete unfiltered search snapshot,
`01-github-search/all-<date>.jsonl`: one dated before today (UTC), or
with `all-<date>.jsonl.complete` beside it, since today's may still be
being written. `--universe ledger` takes the ledger's own set instead,
to compare stage by stage without the differences between the lists.

**Each stage** of each repository is walked in chain order and is:

- *present*: its output for the current input is in the store;
- *due*: its input is there and its output is not;
- *waiting*: a stage before it is not present, so its input does not
  exist yet. The ledger counts it as due, and the walk would find nothing
  to run: it is counted apart;
- *blocked*: due, but its own backoff after a failure still runs;
- *deferred*: `queue sync` holds the repository (a 404, its backoff), or,
  for the dependency graph, its negative cache.

Release and commit have no output files yet, so what they last produced
is read from the ledger. The tree is present when `tree.txt` is whole
(or empty where the ledger recorded it); the content when
`manifests.json` is for that commit, under the discovery limits in
force, with every selected file settled (no error, 5xx or 429 left to
retry), and written by the content stage version in force: stamped in
the document where it says (nothing writes that yet), else vouched for
by the ledger's row for that commit, or with `--rediscover` found by
discovering the tree again to select exactly its files. The SBOM is
present while `sbom generate` would skip it: whole, written by the Syft
in force (`--syft-version` names it; by default, the one installed
where this runs) and newer than its content. The dependency graph is
due on the ledger's clock and present when the store keeps it.

| Option | |
| --- | --- |
| `--compare` | Hold each stage beside the ledger's due set, with counts and, per reason, up to `--samples` ids (default 10) |
| `--stage` | One stage: `release`, `commit`, `tree`, `content`, `sbom` or `depgraph`. The chain before it is walked for its keys, nothing after it is read |
| `--shard K/N` | Only the repositories whose id is K modulo N: one worker's share, and a sample that is the same run after run |
| `--universe` | `snapshot` (default) or `ledger` |
| `--syft-version` | The Syft an SBOM must record to be current |
| `--rediscover` | Discover the tree again for a content root no version vouches for. Reads each such tree whole |
| `--inventory` | Count the scans nothing points to: a commit no current key names, or a repository outside the universe |
| `--json FILE` | The report, for a machine |

Where the two disagree, each difference is given one reason, from the
evidence on either side (`core/due.py`, `REASONS`):

| Reason | Why the two differ |
| --- | --- |
| `universe:snapshot-only` | The snapshot lists the repository and the ledger does not track it: `queue track` has not seeded it |
| `universe:ledger-only:absent` | The ledger tracks it, the snapshot does not list it, and GitHub answered 404 |
| `universe:ledger-only:unlisted` | No snapshot lists it: `queue track` unlisted it, or it came from a language list |
| `universe:ledger-only:older-snapshot` | Only an older snapshot listed it, and `queue track` has not read the newest |
| `universe:ledger-only:newer-snapshot` | A newer snapshot, not complete yet, lists it |
| `universe:ledger-only:snapshot-differs` | The ledger has this snapshot listing it, and the file does not now |
| `universe:ledger-only:other-snapshot` | A snapshot that is not an unfiltered one listed it |
| `upstream-not-run` | The ledger has it due, and it is waiting: its input is not produced yet |
| `head-moved` | The snapshot saw a push the ledger has not (release); or the ledger recorded the stage for another commit than its commit row produced, as a walk for one stage records it (tree, content) |
| `output-unknown` | The ledger has the commit current, but not what it produced: a row adopted from a watermark |
| `file-missing` | The ledger has the stage done, and its file is not in the store, or is cut short or unreadable |
| `lost-record` | The store has the output, and the ledger has no row for it, or one for another input |
| `stage-version` | The ledger has the stage due for its code version; the store's output cannot say which version wrote it |
| `selection-unchanged` | Content due for its version in the ledger, whose tree, discovered again, selects exactly its files |
| `content-version` | Content whose stamp is older, or that no version vouches for, while the ledger has it current |
| `selection-changed` | Discovering the tree again selects other files than the content root holds |
| `limits-changed` | Content fetched under other discovery limits than the ones in force |
| `unsettled` | Content with a file that failed in a way that may pass: an error, a 5xx or a 429 |
| `another-syft` | An SBOM written by another Syft than the one in force |
| `input-changed` | An SBOM older than a file it was made from |
| `leased` | A dependency graph a worker holds right now |
| `timing` | They differed, and agreed on a second look: the collector wrote meanwhile |
| `unexplained` | No rule explains it; worth reading the samples |

It reads about six files a repository (the tree's last byte,
`manifests.json`, the content root, the SBOM's two ends, one listing of
the graphs) and walks the content root for the SBOM's age, and it stops
at the first stage not present. Measured warm on a synthetic corpus of
65,000 repositories (4 vCPUs): 35 s for `--compare` (27 s of it the
store), 4.4 s for `--compare --shard 0/16`, and 8 s more for
`--inventory`. It says how long each part took. On the collection host,
run it gently and a shard at a time: see
[DEPLOY.md](DEPLOY.md#comparing-the-derived-due-set-with-the-ledger).

### `chatsbom run` — collect what the queue says is due

`queue sync` closed half the loop: it notices a push, and a repository
whose `pushed_at` moved becomes due for every later stage. Nothing
consumed that. A repository could be due for six stages and then wait
for someone to run six commands by hand.

`chatsbom run` is the other half — one repository, all its due stages,
in order:

```bash
chatsbom queue sync --slice 500 --quota 250   # notice what changed
chatsbom run --limit 50 --quota 500           # collect what that made due
chatsbom db raw --apply                       # land the documents
chatsbom db index                             # project them
```

The two are separate because they cost differently. A revalidation is
conditional and usually free, so a pass can check thousands of
repositories; collecting one spends several rate-limited requests.
Running them together would size both to the expensive one.

It is **repository-major**, which is the part that needed a decision.
Each stage needs what the one before it produced — `content` needs the
`download_target` that `commit` resolved, `sbom` needs the directory
`content` wrote — and those hand-offs live in the language-major JSONL
ledgers, which are 5.2 GB for `07-sbom` alone because each record
embeds the repository *and every one of its releases*. Indexing them by
repository id is minutes and gigabytes, not a lookup.

It is also unnecessary, because every path is a pure function of the
repository and its download target:

```
content_dir / repository_id / commit_sha
```

and each service checks its own per-repository cache before reaching
for the network. So the worker walks the whole chain for a claimed
repository and lets those caches make the not-due stages nearly free,
rather than storing the hand-offs a second time.

The ledger schedules **each stage separately**, in its `stage_state`
table: when it last ran, what it consumed (`input_key`) and produced
(`output_key`), at which `STAGE_VERSION`, and its own lease and
backoff. A stage is due when it never ran, ran at an older version,
failed and its backoff ran out, or consumed something other than what
its upstream produces now — `release` against the push `queue sync`
saw, `commit` against the tag `release` chose, `tree` and `content`
against the commit, `sbom` against the digest of the files `content`
stored. Bumping a stage's version makes it due everywhere with no push:
`content`, `lock` and `sbom` are at 2 since manifests are discovered from
the tree, so every content root is filled out and scanned again. Every
tracked repository is walked, whatever its language, including those a
search snapshot seeded with none. A stage that fails backs off alone; the walk stops there
for that repository and the other stages keep their schedule.

```bash
chatsbom run --stage tree --limit 200        # one stage: its own claims
chatsbom run --repos-file pilot.txt          # only these (owner/repo per line)
```

`--stage` takes `release`, `commit`, `tree`, `content`, `sbom` or
`depgraph`. It claims only what that stage is due for and records only
that stage; the stages before it are walked for their hand-off, from
their caches.

**The release stage** lists a repository's GitHub releases (about 1.2
REST pages each) and its tags (`git ls-remote`, `refs/tags/*` only).
A tag with no release is dated by its commit over the git protocol — a
shallow, tree-less fetch of the tags into a scratch repository — not
with one `/commits/{sha}` call per tag, which averaged 47 calls a
repository. Only a tag git cannot date (a tag of a tree, or one that
moved between the two calls) is asked of the API, at most 20 a
repository, newest version first. Dates are kept in the release cache,
so a fresh cache costs nothing and a refresh dates only new or moved
tags. `--quota` counts the REST requests that reached GitHub (cache hits
are free); `git` costs no quota.

The latest stable release is the newest candidate that is not a
pre-release or a draft. A GitHub release says so itself (its
`prerelease` flag wins); a bare tag is judged by its name: SemVer
suffixes (`-rc.1`, `-rc5`, `-beta2`, `-alpha`, `-pre`, `-preview`,
`-dev`, `-snapshot`, `-nightly`, `-canary`, `-next`), PEP 440 forms
(`1.2.0a1`, `1.2.0b2`, `1.2.0rc1`, `.dev0`; `.post1` is a release) and
Maven qualifiers (`-M1`, `.RC1`, `-SNAPSHOT`), case-insensitively.
With no stable candidate, the default branch is scanned.

Two stages are deliberately absent. `repo` belongs to `queue sync` —
that is the conditional request whose 304 is free, and repeating it
here would spend rate limit to learn what sync already knows. `lock`
runs a package manager over untrusted source, so it stays in a
container (compose's `lock` service) rather than in a loop that also
holds a GitHub token.

Verified against the live API: a two-repository pass advanced 8 stages
for 4 core requests, `failed=0`, both repositories left with four
watermarks and their claims released, and the outstanding counts for
those stages each fell by exactly two.

`--quota` counts core API requests. The dependency graph is metered
separately and far more tightly — 100 to 200 requests an hour per token,
against the core 5,000 — and its synchronous endpoint closes after
2026-11-13, so it is **a stage of its own**, not part of the walk:

```bash
chatsbom queue track --snapshot data/01-github-search/all.jsonl  # seed
chatsbom run --stage depgraph --limit 200 --rate 90  # = github depgraph
chatsbom queue status                                 # its table
```

- **Independent.** Due for every repository the queue tracks, whatever
  its language and whether or not its SBOM succeeded; it needs only
  `owner/repo`. `queue track --snapshot` seeds repositories no language
  list has (tracked with no `language`; the walk takes them too). Order: never asked, then graphs older than 30
  days, then expired negative caches; most stars first.
- **Scheduled per stage** in the ledger's `stage_state` table, with its
  own outcome, lease and backoff: a depgraph failure never backs off
  Syft. A 404 (`absent`) is not asked again for 30 days, then 60, then
  90. A 5xx or timeout backs off from 15 minutes, doubling, up to 30
  days; five in a row is `too_large`, asked monthly. A refused token
  records nothing, and only that token stops.
- **Kept for good**, keyed by repository id:
  `09-github-depgraph/<id>/<YYYYMMDDTHHMMSSZ>-<head sha>/sbom.spdx.json`
  with a `meta.json` holding the default branch and the HEAD sha `git
  ls-remote` read just before the fetch. Never overwritten, never
  pruned; a byte-identical document is not stored twice. Each fetch is
  logged in `09-github-depgraph/index.jsonl`; `db raw` lands every
  fetch, and `db index` prefers the newest over the legacy document
  (`<id>/legacy/` since `data migrate-layout`), and
  its artifact rows carry the graph's own ref and sha.
- **Several tokens.** `CHATSBOM_DEPGRAPH_TOKENS` (comma-separated) adds
  tokens beside `GITHUB_TOKEN`. Each is a worker paced to `--rate`
  requests an hour, in parallel; values are never logged.
- **Closing.** With `CHATSBOM_DEPGRAPH_API=sync` the stage turns itself
  off on 2026-11-13 and says so; with the default `auto` it asks for
  GitHub's asynchronous report from that day. `off` turns it off now.
  Nothing else depends on it.

A plain `chatsbom run` runs the stage after its walk, for up to
`--limit` repositories; `--no-depgraph` leaves it to the compose
`depgraph` service, which runs `collector-loop.sh depgraph`.

#### When the dashboard wedges

The container watches itself, because Docker will not.

Measured on a live outage: ClickHouse slowed under a concurrent
`db index --rebuild` — `/api/q` went from 100ms to 10,278ms — and the
Workers runtime crashed. It came back **wedged**: wrangler printed

```
Updated and ready on http://0.0.0.0:8787
```

while `GET /` and `POST /api/q` accepted the connection and never
answered, for 60 seconds and counting. The healthcheck noticed and the
container went `unhealthy`. Nothing acted on that — `restart:
unless-stopped` fires when a process *exits*, and a wedged one does
not — so the site stayed down until someone restarted it by hand.

So `deploy/web-entrypoint.sh` runs wrangler as a child and probes it,
exiting when it is broken, which is the state the restart policy
already knows how to handle. The probe is the healthcheck's assertion —
*which* backend answered, not merely that something did — because a
Worker serving a stale D1 snapshot is the other failure this
deployment has actually had.

Four consecutive failures at 30s apart, so a slow minute restarts
nothing; the outage was two minutes of no answer at all. Verified by
running the image against a dead backend: `probe failed (1/3)`,
`(2/3)`, `(3/3)`, `wedged — exiting so the container restarts`.
`WATCHDOG_DISABLED=1` goes back to a bare `wrangler dev`.

A note on what this does *not* cover. The same outage also had the
tunnel flapping, and that is a separate fault with a separate
signature: `failed to dial to edge with quic: timeout: no recent
network activity` in the `cloudflared` log, while the origin answers
`host.docker.internal:8787` in 3ms. The public hostname returns
nothing and the dashboard is fine — check the origin before touching
anything.

#### Running it continuously

Containerised, so it leaves nothing behind on a machine you also use for
other things. Set `GITHUB_TOKEN`, `UID` and `GID` in the `.env` beside
`docker-compose.yaml` (copy `.env.example` if you have none yet), then:

```bash
mkdir -p data .cache .requests-cache   # once, before the first `up`
docker compose --profile collect up -d --build
docker compose logs -f collector
docker compose down          # gone: no units, no host Python, no host syft
```

Until the cutover (#128), name what it is to build, `docker compose
--profile collect up -d --build collector depgraph`: a bare `--build`
builds the dashboard's image, `web`, again as well, from a page that no
longer asks the Worker (`chatsbom web`, below).

`UID`/`GID` are not optional. `data/` and `.cache/` are bind mounts owned
by whoever cloned the repo, so a container running as its own baked-in
uid cannot write them — the first symptom is
`sqlite3.OperationalError: attempt to write a readonly database` from the
ledger. `id -u` and `id -g` print them. They go in `.env` rather than an
`export`: bash holds `UID` read-only, so `export UID=$(id -u)` fails, and
stops a `set -e` script there. Without a token the collector refuses to
start, and says so in its log.

The `mkdir` is for the same reason. None of the three directories is in
a fresh clone, and Docker creates a missing bind-mount source owned by
root, which the containers, running as you, cannot write. Make them
before the first `up` or `run` of the `collect`, `lock` or `tools`
profile, all of which mount them. The collector checks, and refuses to
start on one it cannot write, with the `sudo chown` that fixes it in
its log.

The collector is behind a profile, so a bare `docker compose up` still
starts only ClickHouse and the dashboard — spending GitHub rate budget
should be a decision rather than a side effect.
`docker compose run --rm cli <args>` runs any stage by hand in the same
image, against the same mounted `data/`, so a manual run and the loop
share state.

What the loop runs: a slice, `queue sync`, then a `run` pass for what
it made due, every `SYNC_INTERVAL_SECONDS`; every `INDEX_EVERY_SLICES`
an index pass, `sbom generate` for the SBOMs no longer current, `db raw
--apply` and `db index`, then `warehouse build` and `snapshot build`
for the Python web service; every `EXPORT_EVERY_SLICES` the public
Parquet export, into `data/export`; and every `PRUNE_EVERY_SLICES` the
retention pass. A step that fails is logged and stepped over, and the
next slice starts. `WAREHOUSE=off` leaves out the warehouse, the
snapshot and the export, for a host without the 10 GB they want
(DEPLOY.md, "The warehouse, the snapshots and the export").

The image has chatsbom with the one extra the loop needs, `export`,
for the Parquet export, byte-compiled: what the loop runs, and nothing
it does not. `chat`, `github classify` and the `openapi` analyses stop
in it with the extra to install; run them from a checkout or an
install that has it. Its virtualenv is 273 MB, about 150 MB of it
pyarrow; the clickhouse-connect `uv.lock` pins imports pyarrow only for
a query asked for as Arrow, so the loop's other commands do not load
it, where an older one imported pandas and pyarrow at every command's
first connection.

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
docker compose --profile lock run --rm lock sbom lock --ecosystem composer
```

The question that shapes this is *where an escape lands*. `sbom lock`
runs an ecosystem's own resolver — a Gemfile is Ruby, a POM runs build
plugins — and mounting the host Docker socket into the collector would
put an escape on the host daemon, which is host root. Instead a
`docker:29-dind-rootless` sidecar, pinned by digest, provides the
daemon: its own root maps to an unprivileged host uid, it publishes no
port, and `compose down` destroys it.

Only `lock` can reach it. The two share a network, `sandbox`, that
nothing else is on — not ClickHouse, not `web`, not the collector — and
the API is TLS on 2376, verified both ways. The image's entrypoint makes
a CA and certificates at every start; the client certificate reaches
`lock` alone, read-only, through the `dind-certs` volume, and the CA's
key never leaves the daemon's container. It used to serve plain TCP on
2375 on the default network, where every service, `web` included, could
start containers on it, and a resolver could reach ClickHouse through
it. `sandbox` is not `internal`: the daemon pulls the recipes' images,
and a resolver fetches from its registry. Limiting that egress to the
package registries is not done.

Two things that took measuring rather than reasoning:

- Under a rootless daemon, `--user` is what *broke* the output write,
  when the lockfile was written to a mounted directory. A rootful
  daemon maps container uid 1000 to host uid 1000; a rootless one maps
  container *root* to the unprivileged host user, so an explicit uid
  lands on a subuid owning nothing and the resolver failed with
  `cp: /out/Gemfile.lock: Permission denied` after doing all the work.
  The sandbox probes `docker info` and drops only that flag.
- `./data` is mounted on the daemon as well as on `lock`, at the same
  path. A container the daemon starts resolves a bind mount against
  *its own* filesystem, so a path only `lock` could see would mount
  nothing, silently. The daemon's is read-only: a resolver only reads
  the project, and its lockfile comes back on stdout for `lock` to
  write.

Verified end to end, before the lockfile came back on stdout: a hostile
Gemfile writing to `/project` and `/etc` was stopped at both, and
discourse's `Gemfile.lock` came out resolved and owned by the invoking
user.

`sbom lock` stays out of the collector loop regardless — it is expensive
and runs project-controlled code, so it should be a decision each time
rather than a background habit. `--workers N` resolves N directories at
once, each a container of up to `--memory` and `--cpus`; the default is
one at a time.

The Docker client lives only in the `lock` image, never the collector's.
An image with a Docker client and a reachable socket is one mistake away
from being an escape; splitting the images makes that a property of the
build rather than a rule someone has to remember. Both are stages of the
one `Dockerfile`, and the collector's never reaches the `lock` stage.

For a dedicated server rather than a dev machine, `deploy/systemd/` has
units for the same two schedules, hardened with `ProtectSystem=strict`
and `ReadWritePaths` limited to `data/`, `.cache/` and
`.requests-cache/`. They are templates whose instance is the checkout's
path, so they run wherever it is without editing; DEPLOY.md has the
commands to install them.

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
means something is wrong that backoff is quietly hiding. Two answers
that mean nothing is broken stay out of it: a repository GitHub answers
404 for is counted in `chatsbom_queue_absent` and re-checked a fortnight
later, and a refused token (429, or 403 with no quota left) ends the
slice and hands the rest back untouched.

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
| `migrate-layout` | Move every stage artefact under its repository's id, journaled, with verify and rollback |
| `prune` | Keep the newest N scans and release decisions per repository, and whatever the current scan descends from; discard older ones |
| `slim` | Drop from a stage ledger the fields nothing reads |
| `backfill-decisions` | Write the release and commit decisions from `raw_documents`' records, once, before ClickHouse goes (DEPLOY.md) |
| | Reports by default; `--apply` rewrites |

#### The repository-keyed layout

Every stage artefact is keyed by the repository's numeric id and the
commit (#55, owner decision D3): an id does not move when a repository
is renamed or transferred, a repository needs no language to have a
path, and two refs at one commit are one scan.

| Artefact | Before | Now |
| --- | --- | --- |
| Tree | `05-github-tree/<lang>/<o>/<r>/<ref>/<sha>/tree.txt` | `05-github-tree/<id>/<sha>/tree.txt` |
| Content | `06-github-content/<lang>/<o>/<r>/<ref>/<sha>/<path>` | `06-github-content/<id>/<sha>/<path>` |
| SBOM | `07-sbom/<lang>/<o>/<r>/<ref>/<sha>/sbom.json` | `07-sbom/<id>/<sha>/sbom.json` |
| Dependency graph | `09-github-depgraph/<lang>/<o>/<r>/sbom.spdx.json` | `09-github-depgraph/<id>/legacy/` (+ `meta.json`), beside every kept fetch |
| Generated lock | `10-generated-lock/<lang>/<o>/<r>/<sha>/` | `10-generated-lock/<id>/<sha>/` |
| Syft cache | `.cache/syft/<ver>/<o>/<r>/<ref>/<hash>.json` | `.cache/syft/<ver>/<id>/<hash>.json` |
| Tree cache | `.cache/git-tree/<o>/<r>/<ref>/<sha>/` | `.cache/git-tree/<id>/<sha>/` |
| Release decision | in `raw_documents` only | `03-github-release/<id>/<P>/release@2.json` |
| Release list | in `raw_documents` only | `03-github-release/<id>/releases/<sha256>.json` |
| Commit decision | in `raw_documents` only | `04-github-commit/<id>/<K>/commit@1.json`, a later one `<K>/<P>/commit@1.json` |

**The release and commit decisions** (#147, owner decision Q3 on #100).
Those two stages make no scan: what each produces is a decision, which
`chatsbom run`, `github release` and `github commit` keep as they make
it, beside the record `RecordStore` lands in `raw_documents` as before.

- **The release decision** for the push `P` (`pushed_at`) says the tag
  of the latest stable release it chose, or none, and names the release
  list it chose from:
  `{"id": 42, "key": "2026-09-29T12:28:14Z", "out": "v2.0.0", "releases": "<sha256>", "stage": "release", "sv": 2}`.
- **The release list** is the releases as the model holds them, each
  asset trimmed to what `db index` keeps of it less its download count,
  which moves on every fetch: the same releases are the same bytes, so a
  push that decides them again writes only its decision. The file is
  named by the sha256 of its bytes.
- **The commit decision** for the key `K`, `tag:T` or `head:P` when the
  push has no release, says the commit, the ref it was resolved from,
  and the push it was resolved for:
  `{"id": 42, "key": "tag:v2.0.0", "out": "<sha>", "push": "2026-09-29T12:28:14Z", "ref": "v2.0.0", "ref_type": "release", "stage": "commit", "sv": 1}`.
  A key is resolved again when a later push decides it, and may resolve
  to another commit: a tag can be moved, as a `latest` tag is at every
  build, and a tag that is gone is resolved to the default branch's
  head. `<K>/commit@1.json` is the key's first resolution; a later one
  that says another commit or ref is kept beside it, under its push,
  `<K>/<P>/commit@1.json`. A push reads the newest resolution made for
  it or a push before it, else the earliest: the first, where they were
  written in their pushes' order.

`P` is spelled as a fetch of the dependency graph is,
`YYYYMMDDTHHMMSSZ` in UTC (`20260929T122814Z`): fixed width, so names
sort as the instants, and parse back. `K` is `head-<P>`, or `tag-<T>`
with every byte of the tag outside `a-z 0-9 . _ - @` as `%xx` in
lower-case hex: `v1.2.3` is `tag-v1.2.3`, `release/1.4.0`
`tag-release%2f1.4.0`, and `V1.0` `tag-%561.0`, which a file system that
ignores case (APFS and NTFS by default) keeps apart from `tag-v1.0`. No
name has a capital, a trailing dot Windows would drop, or a character a
shell needs quoted, and none is longer than 128 bytes (a name may hold
255 on ext4, APFS and NTFS, 143 under eCryptfs); a longer tag is named
`tag~<sha256>`, and its key is read from the file. The bytes are the
tag's as git keeps them, UTF-8 or not, and the files are ASCII JSON,
anything else escaped; the warehouse, which holds text, has U+FFFD for
each byte that is not UTF-8. A file is written through a temporary one,
fsynced and linked into place, never over a file that is there: the
same content twice is one file, and another release decision for a push
already decided leaves the first. The warehouse reads the decisions in
place of a repository's record (`warehouse build`, above). What was
decided before the stages kept their decisions is in `raw_documents`
alone: `data backfill-decisions` writes it, once (see DEPLOY.md).

**What they cost**, measured on a synthetic corpus of 1,000
repositories shaped like this one (41 releases each on average, heavy
tailed, a list of 39 KB; 25% pushed in a given week, 41% not in a year),
written by the stages' own code for a year of pushes, on ext4 with 4 KiB
blocks. A decision is two inodes, its directory and its file, and 8 KiB
of blocks for about 200 bytes; the lists are most of the bytes.

| Per repository, and for 65,000 | Inodes | Bytes | Blocks |
| --- | ---: | ---: | ---: |
| The backfill | 8 · 0.52 M | 35 KB · 2.3 GB | 66 KB · 4.3 GB |
| A year of pushes, not pruned | 83 · 5.4 M | 173 KB · 11 GB | 499 KB · 32 GB |
| The same, `data prune --keep 2` | 10 · 0.68 M | 43 KB · 2.8 GB | 83 KB · 5.4 GB |

The year saw 28 pushes a repository (1.8 M for the corpus, two a pushed
week), 30% of them a new key (the head of a repository with no stable
release, or a new release), and 2.6 new lists. Not pruned, the decisions
grow by two inodes a push seen: to 5.4 M in a year at today's cadence,
and to some 16 M if an hourly change detector (#128 §2.1) sees seven
pushes in each week a repository is pushed, about as many inodes as an
ext4 file system of 250 GB has at its default ratio (16.7 M). Pruned,
they stay at about ten inodes a repository, bounded by `--keep` however
often it is pushed: so it is `data prune` that keeps up with the
inodes. Phase 7's recompression cannot: it makes the lists about eight
times smaller (the bytes), but a decision stays a file in a directory.
Half the blocks kept are that per-file overhead, 2.7 GB for the corpus
pruned, which a file system that keeps a small file in its inode
(ext4's `inline_data`) does not pay.

`raw_documents.path` is relative to the data directory
(`07-sbom/<id>/<sha>/sbom.json`) and carries `ref`/`commit_sha`
columns. Paths recorded before the move (the per-language lists, older
records) are translated by `core/layout.py` wherever they are read.

`data migrate-layout` moves an existing corpus with `rename(2)` on one
filesystem — nothing copied, fetched or deleted:

```bash
chatsbom data migrate-layout --inventory     # pre.tsv: every file, 1% hashed
chatsbom data migrate-layout                 # dry run: plan.tsv, conflicts
chatsbom data migrate-layout --apply         # move, rewrite raw_documents, adopt the ledger
chatsbom data migrate-layout --verify        # counts, bytes, sample hashes, paths
chatsbom data migrate-layout --rollback      # undo it all
```

The dry run writes only its report and plan (`--workdir`, by default
`data/_migration`); it reads the ledger read-only and asks the database
only read-only questions. The apply refuses a plan with a conflict, logs
each batch of renames to an fsynced journal before making them, resumes
from it after a kill, and sets identical copies aside in
`_migration/dedup/` rather than deleting them. See DEPLOY.md for the
operator runbook.

`data slim` exists because the stage ledgers were 22 GB of which 21 was
the same data four times. Each stage appends its own copy of the whole
repository record to carry it to the next stage, and a record in
`07-sbom/ruby.jsonl` is 63.1 KiB of which **98% is `all_releases`** —
against 0.4 KiB for the one path the stage actually contributed.

What each ledger is read for was measured, not assumed:

| ledger | read by | after |
| --- | --- | --- |
| `05-github-tree` | nothing — written and never read | 8.6 MiB |
| `06-github-content` | `sbom generate`, `sbom lock` | 10.3 MiB |
| `09-github-depgraph` | `db index`, for `depgraph_path` alone | 5.4 MiB |
| `07-sbom` | `db raw`, `github depgraph`, `sbom generate` | 13.9 MiB |

**All four: 22 GB of ledgers → 585 MB**, of which 545 MB is
`01-github-search` and `02-github-repo`, which are left alone. The
stage ledgers themselves are about 40 MB.

`07-sbom` was refused at first, and the reason it stopped being
refused is the interesting part. `db raw` derived the repository
record from that ledger, so slimming it would have produced a record
with no `all_releases` — and because that row would be the *newest*,
`RawRecords` would serve it in preference to the complete one. A 5 GB
reclaim that silently empties the releases table.

So the record moved out first. `RecordStore` writes it, called by
`chatsbom run` and `sbom generate` **once per repository at the end of
the chain** rather than once per stage: written per stage it would be
seven rows a repository, six of them describing states nothing reads.
Then `db raw` stopped deriving it, and only then could the ledger
slim. Verified in that order — the count stayed at 28,072 repositories
through the slimming rather than being quietly replaced.

The first version of this kept `name`, which is not the key the model
dumps — it dumps `repo` — so every slimmed line failed validation.
Nothing said so: `load_jsonl` catches the error per line and returns
what it could parse, which was none of them, and the reader then
reported an empty language and carried on. Five gigabytes becoming
unusable with no error message is why `data slim` now validates each
line against `Repository` *before* replacing anything, and refuses the
whole file if one would not load.

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

**What the current scan descends from is never removed** (#100 Q13): the
scan the newest resolved commit decision points to, in every scan root
whatever its age, beside the `--keep` newest rather than in place of
one, and the release decision, commit decision and release list it
descends from. Of the decisions in `03-github-release` and
`04-github-commit` (below), each repository also keeps the `--keep`
newest release decisions, one older than the current by default, as for
scans: it shows what the last push changed, a new release or none. An
older one says nothing its release list does not, and one per observed
push, never pruned, is what would outgrow the store's inodes. A key's
resolution is kept while a kept release decision stands on it or its
scan is kept: a later one goes with the pushes it stood for, and a
key's directory, with its first, when none of its resolutions is kept.
A list is kept while a kept release decision names it, or for a day
after it was written: a list is written before the decision that names
it. A directory holding a decision this code cannot read, a later
version of the stage's among them, is left whole, and so are its
repository's lists; one with no decision in it, a killed writer's
leftover, is left as it is, and keeps nothing.

Known gap: `03-github-release` and `04-github-commit` also hold one
JSONL ledger per language (5.7 GB each), which retention does not
reach. `github release` and `github commit`, which write them, skip a
repository their ledger has, deduplicated by repository id, so a
repository they collect again keeps its first push's releases there,
and decides no new push in the store; `chatsbom run` decides every push
it walks.

### `chatsbom export` — portable artefacts

| Command | Purpose |
| --- | --- |
| `parquet` | Write the dataset as Parquet plus a checksummed manifest (the `export` extra) |
| | `--from warehouse` reads the warehouse instead of ClickHouse, `--warehouse PATH` another one |
| `d1` | Write SQL that loads the dataset into Cloudflare D1 |
| `schema` | Emit the export contract as JSON and/or TypeScript types |

The dataset reaches the edge as a database. `web/` is a Cloudflare
Worker serving a dashboard that asks it by method name — a visitor
downloads about 250 KB, fonts included, and every answer is one
request. See `web/README.md`.

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

`export parquet --from warehouse` writes the same files from the
warehouse `warehouse build` makes, and reaches no server: the same four
tables, contract (`EXPORT_SCHEMA`, version 8), content-addressed names
and checksummed manifest, from `export parquet`'s queries ported to
DuckDB (`chatsbom/export/warehouse.py`), within DuckDB's limits
(`CHATSBOM_DUCKDB_*`, above). It is the weekly public export of #128
(decision Q11), and from the cutover the only one: the ClickHouse
source goes with the server. The collector's loop runs it every
`EXPORT_EVERY_SLICES`, a week at the defaults and every seventh index
pass, into `data/export`, which holds the last export alone: exported
into again, a table that has not changed keeps its file, and the last
export's others go once the new manifest is written. Where it is
published, as a release's assets or as files the site serves, is the
owner's decision.

The same rows make the same files, whichever engine gave them: on the
contract seed, a synthetic corpus and a store indexed by both engines,
the export from the warehouse is `export parquet`'s table by table, row
by row and byte by byte, but for three things that differ by design, as
they do in the snapshot. Adoption over time counts a repository in every
month between two scans that both show the package (Q9), where
ClickHouse's counts the months of the scans. A repository with no
dependency is dated by its newest scan, or not at all, rather than by
the day `db index` wrote its row. And a scan is dated by when the store
first had its commit, which can be the day before Syft made the
document. At the documented shape (16.1M facts, 60,000 repositories) it
wrote 99.9 MB in 37 s and held 3.0 GB at its peak, most of it DuckDB
sorting the facts; with DuckDB's own defaults, 33 s and 3.1 GB, and the
same files. `export parquet` from ClickHouse took 50 s over the same
rows, the sort on the server, and wrote the same bytes for three of the
four tables; the fourth differed only in the dates of the 31,930
repositories never scanned.

`export d1` targets a serving model with a real database behind it,
for the case where shipping the data to the browser is the wrong
trade-off. It writes SQL files applied in the order of their names —
the schema, the data in numbered parts of at most 50 MB
(`02-<table>-0001.sql` onwards), the aggregates, the indexes — and
normalises the artifact rows on the way out. Each file can be applied
again without changing the result, so an import that fails partway
goes on from the file that failed rather than from the start. The
package-to-package edges are the ones `db edges` stored in ClickHouse,
and the export refuses to run without them.

The normalisation is not cosmetic: at 6,062,896 artifact rows a direct
translation of the Parquet schema measured 762.6 MB in SQLite once the
indexes the queries need were present, while interning the repeated
strings brought it to 294.7 MB with no rows lost; at 16.8 million rows
the normalised database is 831 MB. Most of the saving is one table —
the five low-cardinality columns take only 45 distinct combinations
across six million rows, and were stored as five strings on every one
of them.

Both exports stream. `export parquet` reads each table as Arrow record
batches, from ClickHouse or from DuckDB, and writes a row group at a
time, and `export d1` writes each
artifact row as soon as it has been turned into references, so neither
holds a table in memory. Both held the artifacts, 16.8 million rows, as
Python objects: about 3.8 GiB for Parquet and 2.3 GiB for D1. Their
queries go out with every overflow mode set to `throw`, so a result cap
on the connecting account fails an export rather than truncating it;
the Parquet export used to run each query a second time to count its
rows, and D1 did not check at all.

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
| `drift` | Measure how far each specification is from the endpoints its code implements |
| `stats` | Count each cloned repository's lines and tokens, and the LLM context windows it fits |

`list-paths`, `drift` and `stats` need the `openapi` extra;
`candidates` and `clone` need nothing more.

`clone` keeps a bare, blobless clone of each repository in
`~/.repositories`, never checked out, and cuts each snapshot from it
with `git archive`: the repositories are untrusted, and a checkout runs
whatever filters git is configured with, git-lfs's among them.
`stats` downloads its tokenizer on its first run, 1.7 MB from
`openaipublic.blob.core.windows.net`, into `.cache/tiktoken`, or
wherever `TIKTOKEN_CACHE_DIR` says.

`plot-drift` is gone: it charted a series across releases, from columns
`drift` never wrote, where `drift` measures one snapshot per candidate,
and it failed on every run. matplotlib, which only it used, went with
it.

### `chatsbom chat` — AI querying

Starts a terminal UI that answers natural-language questions by querying
ClickHouse. Needs the `chat` extra, and `ANTHROPIC_API_KEY` or
`ANTHROPIC_AUTH_TOKEN`.
`ANTHROPIC_BASE_URL` points it at an Anthropic-compatible endpoint other
than Anthropic's, and that endpoint receives the key or token — set it
only for one you mean to give it to.

The Claude CLI it starts is given what it needs of the environment and
the value of nothing else: `PATH`, `HOME`, the locale, a proxy and the
certificates it is trusted by (`HTTPS_PROXY`, `NO_PROXY`,
`NODE_EXTRA_CA_CERTS`, `SSL_CERT_FILE`), and the `ANTHROPIC_*`,
`CLAUDE_*` and `DISABLE_*` settings. Any other variable reaches it
empty, `GITHUB_TOKEN`, `OPENAI_API_KEY` and the ClickHouse passwords
among them: it has no tool that could use one.
`chatsbom/commands/chat_agent.py` lists what it is given.

### `chatsbom web` — the Python web service (opt-in)

| Command | Purpose |
| --- | --- |
| `serve` | Serve the dashboard's page, its reads of the dataset, an ALTCHA challenge, the chat and `/healthz` from one FastAPI process on uvicorn |

It is to replace the Worker, which serves the site until the cutover
(#128). Compose runs it only when asked, beside the Worker (below). The
page this tree builds asks it, and no longer the Worker (#144): its
reads are GETs under a snapshot, and its questions carry an ALTCHA
proof of work. The Worker answers none of those paths, so its
deployment stays on the build it has until the cutover.

  - The built page, `web/dist/client` unless `--spa` names another:
    `/assets/*` cached for good, since they are named by their content,
    and `index.html`, which every other path answers, never cached
    without asking.
  - `GET /api/meta`: the current snapshot's id, and its provenance, as
    `meta` answers it. Kept a minute (`max-age=60`).
  - `GET /api/v/<snapshot>/<method>?...`: one of the dataset's 21
    questions (`chatsbom/dataset/`), by the page's name for it, asked of
    that snapshot, with its parameters in the query string by the
    page's names for them: `dependentsOf?directOnly=true&name=mail`.
    The method checks them, as it does for every caller. An answer is
    kept for good (`public, max-age=31536000, immutable`), since a
    snapshot never changes, and a refusal not at all. A snapshot no
    longer served answers 410 (#144).
  - `GET /api/ask/challenge`: an ALTCHA proof of work, which the chat
    requires of each question, signed for the client that asked.
  - `POST /api/ask`: a question, `{question, prior, altcha}`, answered
    by DeepSeek's `deepseek-flash` as server-sent events (#140), which
    the page reads as they come, in its Ask panel (#144). The
    loop runs here, at most 8 model turns, and its tools are the dataset
    API, run against the snapshot the question pinned as it started
    (`WEB_SNAPSHOT`, below). The events are `tool`, `text` (the answer
    as it is written), `done` (the usage and its cost) and `error` (a
    code for the page to say in its reader's words).
    Anything else under `/api/` is a JSON 404.
  - `GET /healthz`, for a peer outside the edge network alone.

The chat's checks come cheapest first: the page's own origin, the
client's rate, the questions in flight, the proof of work, then the
day's cap, which each turn's worst case is held against before its call
and settled at its cost after, at DeepSeek's price for the hour. A
client that goes away stops the loop after the turn in flight, which is
settled. Without `DEEPSEEK_API_KEY` the chat is off, and both of its
routes answer 503.

Every response carries the page's headers from `web/public/_headers`,
less Turnstile's origin, and `Cross-Origin-Opener-Policy`. It listens
on `127.0.0.1:8080` unless `--host` and `--port` say otherwise, and
logs to stderr.

It needs the `web` extra, and `ALTCHA_HMAC_KEY`. Without the key, or
with a setting it cannot read, it does not start, and says which. The
settings, which `.env.example` describes:

| Setting | Default | What it is |
| --- | --- | --- |
| `ALTCHA_HMAC_KEY` | none | What signs the challenges: `openssl rand -hex 32` |
| `EDGE_SUBNET` | none | The subnets of the network only cloudflared is on. `CF-Connecting-IP` is believed from a peer there, and from no one when it is unset |
| `WEB_STATE_DIR` | `data` | Where `web.sqlite` is: the daily spend ledger, and the challenges used |
| `CHAT_RATE_LIMIT` | `20/60` | At most 20 challenges and questions from a client in any 60 s: a question counts twice |
| `QUERY_RATE_LIMIT` | `100/10` | The same for the page's reads, `/api/meta` and `/api/v/*` |
| `DAILY_SPEND_CAP_USD` | `5` | The chat's cap a UTC day; `0` is none |
| `WEB_SNAPSHOT` | none | The dataset the page and the chat's tools read: the directory `snapshot build` publishes in, `data/snapshots`, or one snapshot's file. Unset, the page's reads answer 503 |
| `DEEPSEEK_API_KEY` | none | The chat's key; unset, the chat is off |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | Where DeepSeek's OpenAI-format API is |
| `CHAT_MODEL` | `deepseek-flash` | The model |
| `CHAT_MAX_IN_FLIGHT` | `3` | The most questions answered at once, whoever asks |
| `CHAT_INPUT_USD_PER_MTOK` | `0.30` | Dollars per million prompt tokens the cache missed, at peak |
| `CHAT_CACHED_INPUT_USD_PER_MTOK` | `0.006` | Per million it served, at peak |
| `CHAT_OUTPUT_USD_PER_MTOK` | `1.20` | Per million generated, reasoning included, at peak |
| `CHAT_OFF_PEAK_INPUT_USD_PER_MTOK` | `0.15` | The first, off peak |
| `CHAT_OFF_PEAK_CACHED_INPUT_USD_PER_MTOK` | `0.003` | The second, off peak |
| `CHAT_OFF_PEAK_OUTPUT_USD_PER_MTOK` | `0.60` | The third, off peak |
| `CHAT_PEAK_HOURS` | `01:00-04:00,06:00-10:00` | DeepSeek's peak hours, UTC, Monday to Friday |

The prices and hours are DeepSeek's pricing page as read on 2026-09-29
(https://api-docs.deepseek.com/quick_start/pricing/). A turn that
touches peak hours is settled at peak, as is one on a Chinese public
holiday, which DeepSeek prices off peak: an overcount, which the cap
can afford.

The dataset is a snapshot (`chatsbom snapshot`, above), published from
the warehouse into `data/snapshots`, which `WEB_SNAPSHOT=data/snapshots`
names:

    uv run chatsbom warehouse build
    uv run chatsbom snapshot build

Named by its directory, each question reads the snapshot `CURRENT`
names as it starts, and that one to its end: one a later pass publishes
is served from the next question on, without a restart, and changes no
answer in flight. The page's reads name their snapshot: each `CURRENT`
lists, the current one and the two kept, is answered, and one it no
longer lists answers 410, which sends the page to `/api/meta` again.
Named by its file, the snapshot's id is the one its `meta` holds, or,
for a file of D1's tables with none, the hash of its bytes, read as the
service starts. A question that finds none there is refused,
`unavailable`, before any of the day is held for it. A D1 export
applied with `sqlite3` is not a snapshot: it lacks the table a
package's dependants are read from, and the service does not start
with one.

A client is an IPv4 address or an IPv6 /64. The rate limits are the
Worker's, over a window that slides, counted in memory: a restart
forgets them. A watchdog in the process exits it when its event loop
has not ticked for a minute, so that the restart policy starts it
again.

Under compose it is the `site` service, behind the `site` profile, in
the image `Dockerfile.site` builds: Python, the package with this
extra, and the page, which Node builds in a stage of its own, with no
Node, `node_modules` or uv in the image. It runs as a uid of its own,
on a read-only root with no capabilities, and checks itself by asking
`/healthz` from inside.

    docker compose --profile site up -d

It serves on 8080, on the `edge` network, where the tunnel reaches it,
and publishes no port. Compose hands it the settings above from `.env`,
but for three it sets itself: `WEB_STATE_DIR`, the `site-state` volume;
`WEB_SNAPSHOT`, `data/snapshots`, mounted read-only; and `EDGE_SUBNET`,
the subnet compose gives `edge`, `172.16.128.0/24` unless `.env` says
otherwise. DEPLOY.md has how to route a second hostname to it, beside
the Worker's.

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
| `transitive` | Another dependency pulled it in: every manifest was understood, and none declares it |
| `unknown` | The manifests cannot say: none could be read, or one was not understood in full and may declare it |

Supported manifests: `Gemfile`/`*.gemspec`, `package.json`, `go.mod` (honouring
`// indirect`), `Cargo.toml`, `pyproject.toml`/`setup.cfg`/`requirements*.txt`,
`composer.json`, `pom.xml`/`build.gradle(.kts)`. A manifest is not understood in full
when it does not parse, or declares dependencies somewhere else: a Gemfile's
`gemspec`, dynamic dependencies in `pyproject.toml`, `file:`/`attr:` in
`setup.cfg`, a Gradle reference nothing in the repository resolves.
`setup.py` is code, so a project whose build takes its dependencies from it
is never understood in full.

Each artifact is judged **in its own ecosystem** — its type, canonicalised
(`java-archive` is `maven`), else its purl's — against that ecosystem's
manifests, anywhere in the repository. The repository's language plays no
part: a repository GitHub calls TypeScript with a Maven backend gets Maven
verdicts for its Maven artifacts and npm verdicts for its npm ones. An
ecosystem with no parser (NuGet, Swift, pub, conan, …) stays `unknown`.

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
| `source` | `syft` (lockfile, resolved closure), `github-depgraph` (manifest, declared only) or `manifest` (Gradle build files, declared only; below) |
| `version_kind` | `resolved` (exact), `constraint` (`>= 0`, `^4.18`) or `unversioned` |

GitHub's graph is flat — the repository `DEPENDS_ON` each package with no
tree — so its rows are always `direct`. Its versions are the manifest's
constraints, which `version_kind` marks so a range is never charted as if
it were a resolution.

`repositories.manifest_sources` records which manifest files were read, so
a `transitive` verdict can be told apart from an unexamined one.

### The third source: Gradle build files

Syft reads no Gradle file — on 1.41.2, 40 of 40 sampled Gradle-only projects
had an empty SBOM, and 1.52.0 reads none either — and GitHub's graph is
partial for Gradle (halo-dev/halo: 105 packages, no Spring starter). So
`db index` reads the build files itself (owner decision D1 on #55) and
stores what they declare as rows with
`source = 'manifest'`, `type = 'maven'`, `relationship = 'direct'`, stamped
with the Syft scan's commit (they are read from the content root it scanned).

What is read (`chatsbom/core/gradle.py`):

- declarations in `build.gradle` and `build.gradle.kts`, Groovy or Kotlin,
  in any configuration: string coordinates (with `$x`/`${x}` from
  `gradle.properties` and literal `ext`/`val`/`extra` assignments), map
  notation, `kotlin("x")`, and `platform(…)`/`enforcedPlatform(…)` around
  them;
- **version-catalog references**, `libs.spring.boot.starter.web` and
  `libs.bundles.x`, against every `*.versions.toml` (`gradle/libs.versions.toml`
  is `libs`, others are named by their stem) and catalogs declared in
  `settings.gradle(.kts)`;
- versions a declaration leaves out, from a `constraints { }` block or a Spring
  `dependencyManagement` entry elsewhere in the build, and — for
  `org.springframework.boot` artifacts only — from the Spring Boot plugin, a
  `spring-boot-dependencies` BOM or `SpringBootPlugin.BOM_COORDINATES`.

What is not: plugins (`plugins { }`, `alias(libs.plugins.x)`), the buildscript
`classpath`, `buildSrc/`/`build-logic/`, and anything built by code — loops,
convention plugins, `apply from:`, a coordinate held in a variable. Such a
reference makes the file incomplete for the classifier rather than guessed at.
A version a BOM manages is not read out of the BOM.

Every such row is a **declared** version, never a resolved one:
`version_kind` is `constraint` when the build states (or pins) a version and
`unversioned` when not. `found_by` is `chatsbom-gradle` for a literal and
`chatsbom-gradle-catalog` for a catalog entry. `pom.xml` gives no such row:
Syft's `java-pom-cataloger` already reports it.

## What is counted: the corpus, by ecosystem

Every current-state number — the rollups, the dashboard, the exports and
`db query`/`db status` — counts **the corpus**: the repositories of the
current search snapshot (owner decision D2 on #55). That is the
newest-dated `all-*` snapshot the ledger records (`queue track
--snapshot`), stamped on each `repositories` row by `db index`. A
repository the snapshot no longer lists (below the star cut, deleted,
private) keeps every row it has in `repositories` and `artifacts`, and
is simply not counted. A database with no snapshot recorded counts
every repository, as before.

Numbers are keyed by **ecosystem** — npm, Maven, PyPI, Go, Composer,
Cargo, RubyGems and whatever else a collector reports, under the
canonical names of `core/ecosystems.py` — not by the repository's
language. A repository with an npm front end and a Maven back end is an
npm dependant of `react` and a Maven dependant of
`spring-boot-starter-web`. So a whole-corpus count is never the sum of
per-ecosystem counts: it is its own distinct count.

Coverage is measured against the whole corpus, collected or not:
`db status` and the dashboard give the snapshot's size, and how many of
its repositories have dependency data from any source, from Syft, from
the dependency graph and from Gradle declarations. GitHub's language is
kept as an attribute and shown folded to the twelve most common and
`other` (owner decision D7), with `none` for a repository GitHub names
no language for.

## Which languages are worth collecting

Nine, and the tenth was measured rather than argued about.
`scripts/probe_language.py` answers "can this pipeline extract
dependencies from this language" for about one request per repository,
and C++ is the case it was written for — no single package manager, so
the answer had a real chance of being no.

It was, but not for the reason the first measurement suggested. Across
the 200 most-starred C++ repositories:

| what the repository declares | share |
| --- | --- |
| `CMakeLists.txt` | 80.5% |
| git submodules | 41.5% |
| nothing machine-readable | 12.0% |
| `vcpkg.json` | 8.0% |
| `meson.build` | 8.0% |
| `conanfile.*` | 5.0% |

Only the last two rows are manifests. `CMakeLists.txt` declares
dependencies in imperative CMake, so reading it means evaluating CMake;
`.gitmodules` names dependencies by repository URL with a commit sha
for a version, which is not a package. **A manifest a parser could
read: 13%**, and 5% without writing a vcpkg parser from scratch —
against 87% of the existing corpus yielding dependencies.

Then GitHub's own dependency graph answered for **88%** of a sample,
median 26 packages, which reads like the manifest number being beside
the point. It is not. Per repository, what those graphs are *in*:

| ecosystem | share of repositories |
| --- | --- |
| `githubactions` | 81% |
| `pypi` | 44% |
| `npm` | 26% |
| `nuget` | 19% |
| `conan` / `vcpkg` | **11%** |

The graph is reporting each project's CI workflows, its docs site's
`package.json` and its build scripts' `requirements.txt` — not what the
C++ code depends on. Two independent measurements land on the same
11–13%, and the naive coverage number gets it backwards.

So C++ stays out, and the cost of adding it would have been worse than
zero: those repositories would have contributed `npm` and
`githubactions` rows attributed to a C++ project, diluting the
per-ecosystem figures that already work.

```bash
GITHUB_TOKEN=... python scripts/probe_language.py 'C++' --repos 200
```

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
| Current state | derived by joining on what the repository records: its `sbom_commit_sha` for a Syft row, and for a dependency-graph row the graph document it was indexed with, by the instant the document states (`depgraph_observed_at`). The `current_artifacts` view, deduplicated by the `facts` view, which the rollups and exports read |
| History | the whole table, pruned by partition for a bounded window: `mv_package_month` and the exported `history` |

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

- the project mounted **read-only**, and no other host path: the
  lockfile comes back as a tar on the container's stdout, capped at 32
  MiB, and only regular files named as the recipe's lockfiles are kept
  from it — no links, no paths, no other names
- `--user` set to the invoking user, never root: root in the container is
  root on a bind mount
- `--cap-drop ALL`, `--security-opt no-new-privileges`, `--read-only`
  root filesystem with a `tmpfs` scratch
- bounded memory, CPU, process count and wall-clock time. The deadline
  is kept by removing the container, which is named for that: killing
  the `docker` client leaves its container running. The container kills
  its own command at the same deadline too, should nothing be left to
  remove it
- a network of its own, `chatsbom-lock`, made on first use with traffic
  between its containers off, so that resolutions running at once
  (`--workers`) cannot reach each other
- images pinned by digest, so that every run resolves with the same
  composer and Ruby: a tag moves with each rebuild of its image

Network access is the one thing that cannot be removed — resolution *is*
fetching metadata from a registry. That is the residual risk, and it is
why nothing else is granted. Requires Docker.

Recipes exist for Composer and Bundler, and are chosen per *directory*
from the manifests there, not per repository from its language: a
directory with `composer.json` and no `composer.lock` is resolved
wherever it is in the repository (at most 10 directories a repository),
and `sbom generate` merges the result back at that directory. A
directory that already ships its lockfile is left alone: that lockfile
is what the project pins, so `sbom lock` does not resolve it again and
`sbom generate` never merges a resolved one over it. Go, Rust and npm are absent on purpose: those ecosystems commit
lockfiles as a matter of course, so Syft already reads them (Go coverage
is 90%, Rust 69%). Java and Python had recipes, withdrawn because Syft
reads neither file they wrote (`dependency-tree.txt`,
`requirements.lock`); Java also cannot resolve a multi-module POM from
the manifests `github content` stores (TODO.md, section E).

## Development

```bash
uv sync
uv run pytest                  # unit tests
docker compose up -d clickhouse  # start ClickHouse for integration tests
uv run pytest                  # now includes the query-layer integration tests
uv run pytest --cov            # with coverage, held to the floor in pyproject.toml
uv run pre-commit run -a       # lint, format, type-check
```

`uv sync` installs every extra, since the tests cover every command: the
`dev` group includes `chatsbom[all]`. `uv sync --no-dev` is chatsbom
without them, as the collector's image and the systemd units have it.

Dependabot moves the packages pyproject.toml names, and nothing moves
what they pull in until a relock does. `python scripts/audit_lock.py`
asks OSV about every package uv.lock pins, whatever pulls it in, lists
each advisory with the versions that fix it, and exits 1 if there is
one; `uv lock --upgrade-package <name>` moves that package.

Query-layer tests run against a real ClickHouse and are skipped when one is
not reachable on `localhost:8123`. With `CI` set, as GitHub Actions sets it,
they fail instead, and so does a run in which any test skips: CI provides
everything the suite needs, ClickHouse with the repository's users.d and
syft among it.

A release is `uvx bump-my-version bump patch` (or `minor`, `major`) on a
clean tree. It rewrites the version wherever it is written, then runs
`uv lock` for the lockfile's copy; commit that, tag the commit
`v<version>` and push the tag, and the release workflow publishes it
once the tests pass. `[tool.bumpversion]` in pyproject.toml has the
rest, including why README's images stay on `main`.

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
  <img src="https://raw.githubusercontent.com/WangYihang/ChatSBOM/main/figures/use-cases/gin/01.png" alt="Query">
</p>

<p align="center">
  <img src="https://raw.githubusercontent.com/WangYihang/ChatSBOM/main/figures/use-cases/gin/02.png" alt="Result">
</p>
