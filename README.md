<h1 align="center">ChatSBOM</h1>

<p align="center">
  <strong>Talk to your Supply Chain. Chat with SBOMs.</strong>
</p>


ChatSBOM is a CLI tool for indexing and querying Software Bill of Materials (SBOM) data, providing deep insights into project dependencies.

<p align="center">
  <img src="https://raw.githubusercontent.com/WangYihang/ChatSBOM/main/figures/use-cases/gin/03.png" alt="Gin">
</p>

## Features

- **Discover**: Every repository on GitHub with 1,000 stars or more, searched weekly, and swept hourly for what changed.
- **Collect**: Fetch each one's dependency files (`go.mod`, `package.json`, etc.), and GitHub's own dependency graph.
- **Generate**: Transform files into standard SBOM format using [Syft](https://github.com/anchore/syft).
- **Index**: Build a [DuckDB](https://duckdb.org/) warehouse of every scan, a file each pass rebuilds from the store.
- **Attribute**: Tell **direct** dependencies from **transitive** ones by parsing manifests.
- **Query**: Ask the warehouse anything in SQL, from the DuckDB CLI.
- **Chat**: Ask the site's chat about the dataset, in natural language.
- **Publish**: Publish a snapshot of the dataset, and serve an interactive dashboard, with AI answers, from one Python process.

## Deployment

See [DEPLOY.md](DEPLOY.md). The short version: everything runs on one
machine under compose. The collector fills the store, a pass publishes
a read-only snapshot of the dataset, and one web service serves the
page, its reads of the snapshot and the chat. A Cloudflare tunnel is
the way in: the web service publishes no port.

## Getting Started

### 1. Prerequisites

- [Docker](https://www.docker.com/) (for compose, and for `sbom lock`'s sandbox)
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

That installs everything the collector runs, `collect` and its index
pass's `warehouse build`, `snapshot build` and `data prune`, and every
other command that needs nothing more; and a second command,
`chatsbom-research`, for the research tools
([below](#the-research-tools-chatsbom-research)). The few commands that
need a large library of their own take an extra: without it, such a
command stops and says which one to install, and its `--help` works
either way.

| Extra | For | Installs |
| --- | --- | --- |
| `research` | `chatsbom-research`: `classify`, and `openapi drift`, `list-paths` and `stats` | instructor, openai, pandas, tiktoken |
| `export` | `export parquet` | pyarrow |
| `web` | `web serve` | FastAPI, uvicorn, ALTCHA, the OpenAI SDK |
| `all` | all of the above | |

```bash
pip install 'chatsbom[web]'               # one
pip install 'chatsbom[web,export]'        # several
uv tool install 'chatsbom[all]'           # all of them
uvx --from 'chatsbom[web]' chatsbom web serve
```

The quotes keep a shell from reading the brackets as a pattern. The
extras are extras for their size: pyarrow alone is 152 MB, where the
rest of chatsbom is under 80 MB. The collector's image has one of them,
`export` ([Running it continuously](#running-it-continuously)).

### 3. Setup

#### Configure Environment: Set your API keys

```bash
export GITHUB_TOKEN="your_github_token"
```

Or keep them in a `.env` file. `chatsbom` reads the one in its working
directory, or in the nearest parent directory that has one, and a
variable already set in the environment wins over the file. Compose
reads the `.env` beside `docker-compose.yaml`, so from the repository
root the two are the same file. `.env.example` lists every setting with
its default commented out, so a copy of it changes nothing until you
edit it. Leave a base URL, `DEEPSEEK_BASE_URL` or `OPENAI_BASE_URL`,
unset unless you mean it: the key beside it goes to whatever endpoint
it names.

### 4. Basic Workflow

```bash
# 1. Collect, until Ctrl-C: the universe, every repository with 1,000
#    stars or more, swept hourly for what changed; each collected, its
#    release, commit, tree, every manifest the tree lists (any depth,
#    every ecosystem) and an SBOM of them, and its dependency graph
#    kept; and the warehouse and a snapshot of it made, daily
chatsbom collect

#    Or one repository's stages, now
chatsbom collect repo octocat/hello-world

# 2. Index what the store holds, now: the warehouse, every scan and the
#    package-to-package edges, rebuilt from data/ alone
chatsbom warehouse build

# 3. Ask it anything, in SQL (DEPLOY.md, "Asking the warehouse by hand")
duckdb -readonly data/warehouse.duckdb 'SELECT * FROM build'

# 4. Publish a snapshot of it, and serve it
chatsbom snapshot build
docker compose up -d
```

That starts the web service, `web`: `chatsbom web serve`, which serves
the page, its reads of the snapshot `data/snapshots/CURRENT` names, the
chat, the weekly Parquet export in `data/export` and `/healthz`
(`chatsbom web`, below). It needs `ALTCHA_HMAC_KEY` in the `.env`
beside `docker-compose.yaml`, and publishes no port: the tunnel, below,
is the way in. `docker compose down` removes it.

### Putting it on the internet

Through a Cloudflare tunnel: `cloudflared` runs as a service beside the
web service, on a network the two share with nothing else, and the web
service publishes no port, so nothing off the machine reaches it but
through the tunnel. Create a tunnel in the Cloudflare dashboard, route
the site's hostname to `http://web:8080`, and put two lines in the
`.env` beside `docker-compose.yaml`:

    COMPOSE_FILE=docker-compose.yaml:docker-compose.tunnel.yaml
    TUNNEL_TOKEN=<the tunnel's token>

`docker compose up -d` then starts it, from that directory. DEPLOY.md
has the steps, and how to check that the tunnel is the way in.

`scripts/health.sh https://<the site>` answers whether the site is
actually serving: the page, the snapshot `/api/meta` names, and that
snapshot's totals, a number only the dataset has. Liveness is a
request, never a process or a port: a `cloudflared` quick tunnel has
stopped here while its process kept running and `ps` kept reporting
uptime. The script resolves public hostnames over DoH, because
`systemd-resolved` on this machine does not resolve
`*.trycloudflare.com` and a plain `curl` therefore reports a working
tunnel as dead. It asks only the addresses it is given: the web
service has no port on the machine to ask, and `docker compose ps` has
the containers' own checks.

## Command Reference

### `chatsbom collect` — the collector

| Command | Purpose |
| --- | --- |
| `collect` | The collector: the universe, its sweep, every repository's stages, the dependency graph and the index pass, until SIGTERM or SIGINT |
| `repo` | One repository's due stages, now, and what each did |

One long-running process owns every GitHub token's budget and schedules
every stage (#128, section 2.1; #155): `chatsbom collect` (#171), in
place of the ledger, `queue`, `run`, the stage-major `github` commands
and the `depgraph` worker, the old pipeline. Its foundations (#156),
what it detects with them (#160), its stages (#161), the dependency
graph (#162) and the process that runs them all (#171) are in
`chatsbom/collector/`. It runs until SIGTERM or SIGINT, and `chatsbom
collect repo` runs one repository's stages by hand.

#### Running it continuously

Containerised, so it leaves nothing behind on a machine you also use for
other things. Set `GITHUB_TOKEN`, `UID` and `GID` in the `.env` beside
`docker-compose.yaml` (copy `.env.example` if you have none yet), then:

```bash
mkdir -p data/snapshots data/export .cache   # once, before the first `up`
docker compose --profile collect up -d --build
docker compose logs -f collector
docker compose down          # gone: no units, no host Python, no host syft
```

DEPLOY.md, "Continuous collection", has the rest: what healthy looks
like, how it stops, what tunes it, and the cutover from the old
pipeline, once. In short, each of its parts is a task of the one
process, on one budget:

- **detection:** the universe, loaded from the newest complete search
  snapshot and searched again weekly; and the sweep, hourly, of every
  repository in it by node id;
- **the collections,** four repositories at once, one task each, its
  stages one after another: what changed since it was collected, then
  what never was, the most stars first, then what a new Syft or content
  stage makes due again, found by walking the universe in the store;
- **the dependency graph,** a step when one is due and after every
  sweep;
- **the index pass,** once something was collected since the last and
  at most daily: `warehouse build`, `snapshot build`, the weekly `export
  parquet` and `data prune`, each a child process.

Where requests wait for the same room in a bucket, detection's go
first, then the collections' by their priority, then the graph's: a
backlog of collections never holds up the next sweep, and a bucket
GitHub refuses holds back only what needs it. On SIGTERM it takes no
more work, gives a collection in flight ten seconds, interrupts an index
step and kills it ten seconds later if it has not gone, and exits within
compose's 30 s grace; each stage writes whole or not at all, and what
was given up is due again at the next start. A heartbeat in
`data/collector.heartbeat`, every 30 s, says what each part is doing,
and compose's healthcheck, `python -m chatsbom.collector.health`, fails
once one stops moving. It logs a line per sweep, per search of the
universe, per index pass and per repository collected, and never a
token, only its label.

`UID`/`GID` are not optional. `data/` and `.cache/` are bind mounts owned
by whoever cloned the repo, so a container running as its own baked-in
uid cannot write them — the first symptom is
`sqlite3.OperationalError: attempt to write a readonly database` from
collector.sqlite. `id -u` and `id -g` print them. They go in `.env`
rather than an `export`: bash holds `UID` read-only, so `export
UID=$(id -u)` fails, and stops a `set -e` script there. Without a token
the collector refuses to start, and says so in its log.

The `mkdir` is for the same reason. None of these directories is in a
fresh clone, and Docker creates a missing bind-mount source owned by
root, which the containers, running as you, cannot write. Make them
before the first `up` or `run` of the `collect`, `lock` or `tools`
profile, all of which mount them. The collector checks, and refuses to
start on one it cannot write, with the `mkdir` and the `sudo chown`
that fix it in its log. `data/snapshots` and `data/export` are the web
service's, which every `up` starts: compose refuses to make them, and
stops, rather than leave them root's (`chatsbom web`, below). The
collector makes `data/export` as it starts.

The collector is behind a profile, so a bare `docker compose up` still
starts only the web service — spending GitHub rate budget should be a
decision rather than a side effect. `docker compose --profile tools run
--rm cli <args>` runs any command by hand in the same image, against the
same mounted `data/`, so a command run by hand and the collector share
state.

The image has chatsbom with the one extra the collector needs,
`export`, for the Parquet export, byte-compiled: what it runs, and
nothing it does not. The research tools, `chatsbom-research`, need the
`research` extra it lacks, and say so; run them from a checkout or an
install that has it. Its virtualenv is 263 MB, 161 MB of it pyarrow,
which only the export loads; clickhouse-connect and the two compression
libraries it brought were 15 MB more, until #153.

For a dedicated server rather than a dev machine,
`deploy/systemd/chatsbom-collect@.service` runs it as a user service,
hardened with `ProtectSystem=strict` and `ReadWritePaths` limited to
`data/` and `.cache/`. It is a template whose instance is the
checkout's path, so it runs wherever that is without editing; DEPLOY.md
has the commands to install it.

#### How it works

- **`data/collector.sqlite`** is what the process keeps between runs:
  each repository as last observed (node id, full name, stars, archived,
  `pushedAt`, default branch and HEAD, latest release), REST validators,
  `nothing` and failure outcomes with their backoff, and the dependency
  graph's pending reports. Never what is done, which the store says:
  deleting it costs requests, not results. One process writes it, in
  WAL. A second is refused, by a lock on `collector.sqlite.lock` that the
  kernel lets go when its holder dies. A later schema brings an older
  file forward, and a file from a later one is refused, untouched.
- **The GitHub client** is async, on httpx2: REST, conditional where a
  validator is kept, so that an unchanged document costs nothing (304);
  GraphQL; and search. What goes wrong is one of four errors: not found,
  gone or moved, rate limited, and failed. A token goes to the API
  alone, and into no log line or error.
- **The budget manager** keeps each token's buckets (`core`, `graphql`,
  `search`, the dependency graph's, and whatever else
  `X-RateLimit-Resource` names) where each answer's `X-RateLimit-*`
  headers say they stand, never `GET /rate_limit`. A request takes the
  token with the most left in its bucket, with at most four in flight per
  token, and leaves each bucket's reserve for work run by hand. A 403 or
  429 backs the bucket off: until its reset for a primary limit, by
  `Retry-After` for a secondary one, and otherwise a minute, doubling.
- **The universe** is the newest complete, unfiltered search snapshot of
  the repositories with at least 1,000 stars, searched again weekly:
  about 700 search requests, split past GitHub's 1,000 results a query
  by star counts, and then by creation date, as `github search` split
  it. It is written where `github search` wrote one,
  `01-github-search/all-<date>.jsonl`, and only once whole: a refresh
  that fails, or lists fewer than three quarters of the last universe,
  leaves the last one standing, and the next waits an hour.
  `collector.sqlite` keeps each repository's node id.
- **The sweep** asks after every repository of the universe by its node
  id, 100 a GraphQL `nodes(ids:)` call, hourly: about 650 of a token's
  5,000 points. A push, HEAD or latest release other than the last
  observed is a change, which the stages read; a rename costs nothing;
  a node that comes back null is gone until the next universe. A
  refusal backs off, and a sweep cut short goes on where it was. Each
  sweep logs what it cost, as GraphQL's `rateLimit { cost }` says and
  as the rate-limit headers do, and warns where they disagree: the cost
  model #128 asks to be verified on a live token before it is relied on.
- **What is due** is derived from the store, per repository, along the
  chain (#100 §2): the push P, last observed; the release decision for P,
  which gives the tag T; K, `tag:T`, or `head:P` with no release; the
  commit decision for K, which gives the commit S; then the tree of S,
  its content, stamped with the content stage's version, and its SBOM, by
  the Syft now running. A stage is due when its output for its key is not
  in the store, and every stage after it waits for it; so a push that
  comes to a commit collected already has nothing after it due (the early
  cutoff). A content root without the stamp, as the old pipeline left
  every one, is fetched again (#100 Q4), and an SBOM another Syft wrote is
  made again. A stage that found nothing or failed is kept in
  `collector.sqlite` and backs off, 15 minutes doubling to a week, before
  it is due again (#100 Q5).
- **The stages** write what today's write: the release and commit
  decisions (`03`, `04`), the tree (`05`), the content with
  `manifests.json` (`06`) and the SBOM (`07`), by today's rules. The API is
  asked on the async client, git (`ls-remote`, the tag fetch, the tree's
  clone) spends no quota, and raw content is downloaded by a client of
  its own that carries no token.
- **Priority**, highest first: a repository pushed and changed since it
  was collected, the longest changed first; one never collected, the
  most stars first; and a rescan for a new version of a tool (Syft, or
  the content stage's stamp), which costs CPU and downloads and no
  quota. The first two are `collector.sqlite`'s to say, from what the
  sweep observed and what was collected: a repository is marked
  collected as of the observation its stages ran for, whatever became
  of them. The rescans are the store's, found by walking the universe's
  repositories in it, and with them a stage due again once its backoff
  has passed.
- **Syft runs in a pool** of `cores - 1` subprocesses, each with a timeout
  and a limit on the memory it holds (`RLIMIT_DATA`: Syft is Go, which
  reserves far more address space than it uses). Scans waiting for a slot
  take it by the same priority.
- **The dependency graph** (`chatsbom/collector/depgraph.py`, #162) is
  fetched through GitHub's report flow (#50): a report asked for, looked
  at until GitHub has made it, and the graph downloaded from the signed
  link its 302 points to, with no token. That link is never logged or
  kept. Every request draws from the graph's own bucket,
  `dependency_sbom`.
  - A graph is fetched again once its repository is pushed after the
    graph was last learned (fetched or found unchanged, as of when its
    report was asked for), as the sweep observed `pushedAt`, but never
    within the minimum of that; or, pushed or not, once that is older
    than the backstop. Never asked about first; then the pushed, each
    waiting from the push it was first found with, the longest waiting
    first; then the oldest. Which graph is kept, the store says.
  - A push makes a graph due only once it has settled, so that GitHub
    has had time to update the graph, and a graph learns only the pushes
    that had settled when its report was asked for. Nothing else waits
    for a push to settle.
  - A repository GitHub has no graph of is asked again after the
    negative cache's delay, and a failure backs off from 15 minutes,
    doubling, up to a week.
  - What it costs, at 65,000 repositories pushed as the synthetic
    corpus below is (a quarter in any week, 41% not in a year), with a
    graph fetched at most once in 21 days: after pushes, from 34 graphs
    an hour, if the same repositories are pushed week after week, to
    76, if none is pushed two weeks running, where the minimum holds
    each to 17 fetches a year of the 22 weeks it is pushed in; about 66
    if one week's push says nothing of the next. At the backstop, 6. At
    about 2.2 requests a graph, asked for and looked at once or twice,
    that is 88 to 181 requests an hour, about 159 in between: of one
    token's 200, 112 left at best, 41 in between, and 19 at worst. The
    first pass over them all, some 143,000 requests, takes about a month
    on one token. The estimate is weakest where the minimum does its
    work, how a repository's pushes follow one another from week to
    week, which the corpus's shares do not say; then in the requests a
    graph takes, which no live token has measured, each tenth more about
    7 an hour; the backstop's share is the least it can be, and asking
    again where there is no graph is not counted.
  - At most ten reports are pending at once, kept in `collector.sqlite`:
    a restart looks at them again rather than asking anew.
  - Graphs are kept where the `depgraph` worker kept them,
    `09-github-depgraph/<id>/<fetched>-<head>/`. One the same as the
    last kept, byte for byte but for what GitHub makes anew for each
    report (when it made it, `creationInfo.created`, and the document's
    `documentNamespace`), is not stored again: when it was found so is
    kept in `collector.sqlite`, and it is due again at the next push, or
    the backstop.

| Setting | Default | Meaning |
| --- | --- | --- |
| `GITHUB_TOKEN` | | `token 1` |
| `CHATSBOM_GITHUB_TOKENS` | | More tokens, comma-separated: `token 2` on. Each serves every bucket; GitHub meters an account, so a token adds to the budget only when it is another account's |
| `CHATSBOM_GITHUB_RESERVE` | `core=500,graphql=500,search=5` | What the collector leaves of each token's buckets, as `bucket=count`; a bucket it names is set, and the others keep these |
| `CHATSBOM_SWEEP_INTERVAL` | `1h` | How often the sweep asks after the universe: a whole number and a unit, `s`, `m`, `h`, `d` or `w` |
| `CHATSBOM_UNIVERSE_INTERVAL` | `7d` | How often the universe is searched again, in the same form |
| `CHATSBOM_REPOSITORIES_AT_ONCE` | `4` | Repositories collected at once, one task each |
| `CHATSBOM_INDEX_INTERVAL` | `1d` | How often at most the index pass runs, once something was collected since the last, in the same form |
| `CHATSBOM_RECOLLECT_INTERVAL` | `7d` | How long at least between two collections of a repository for a change, in the same form: one collected since waits, its change kept |
| `CHATSBOM_SYFT_SLOTS` | cores − 1; `1` in compose | Syft scans at once |
| `CHATSBOM_SYFT_TIMEOUT` | `10m` | How long a scan may run before it is killed and failed, in the same form as the sweep's |
| `CHATSBOM_SYFT_MEMORY` | `2GiB` | How much a scan may hold, as `2GiB`, `1500MB` or bytes; `0` is no limit |
| `CHATSBOM_DEPGRAPH_MAX_AGE` | `180d` | How long a repository's dependency graph stands, unpushed, before it is fetched again anyway, in the same form, `3650d` at most |
| `CHATSBOM_DEPGRAPH_MIN_INTERVAL` | `21d` | The least time between two fetches of a repository's dependency graph: a push within it waits for it to end, keeping its place. In the same form, no longer than `CHATSBOM_DEPGRAPH_MAX_AGE` |
| `CHATSBOM_DEPGRAPH_SETTLE` | `1h` | How old a push is before it makes a repository's dependency graph due, so that GitHub has had time to update the graph. In the same form |
| `CHATSBOM_DEPGRAPH_NO_GRAPH` | `30d` | How long a repository GitHub has no dependency graph of is left before it is asked again, in the same form, `3650d` at most |

`chatsbom collect repo <owner/name | id>` runs one repository's due
stages now, as the process would, and says what each did. It asks
GitHub how the repository stands first, and keeps that, as the sweep
does; then it runs the stages due for that push, and marks the
repository collected as of what it saw. It writes the store and
`collector.sqlite`, so it is refused while another process holds them.
A stage that fails exits 1, and says when it is due again; `--retry`
runs a stage that is backing off now.

```
$ chatsbom collect repo octocat/hello-world
octocat/hello-world (1296269): pushed 2026-09-02 00:00:00 UTC
  release  done     decided v1.0.0 of 1 release
  commit   done     resolved tag:v1.0.0 to 0d504bc (release v1.0.0)
  tree     done     listed 6 paths at 0d504bc
  content  done     4 of 4 manifests stored, 32 bytes (4 fetched)
  sbom     done     scanned by Syft 1.52.0: 4 packages
Current: every stage is done for this push.
Asked: 2 core requests, 1 graphql point, 4 raw files, 1 Syft scan.
```

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

### `chatsbom sbom` — lockfiles

| Command | Purpose |
| --- | --- |
| `lock` | The resolver: a lockfile, per directory, for what ships none at each repository's current commit, in a container whose one way out is its registries; a service unless `--once` |

What it resolves, how, and what it may reach are "Resolving missing
lockfiles", below; the collector's SBOM stage merges what it resolves
into the next scan of that commit. `sbom generate`, which ran Syft over
every stored content root, went with the old pipeline (#171): the
collector scans each root as its SBOM stage comes due, a new Syft's
included (DEPLOY.md, "Upgrading Syft").

### `chatsbom warehouse` — the index

| Command | Purpose |
| --- | --- |
| `build` | Build `data/warehouse.duckdb` from the store alone: every scan, the current facts and the rollups |
| | `--output PATH` writes it elsewhere |

The warehouse of #128 (decision Q2): an embedded DuckDB file, rebuilt
from `data/` by each pass and never backed up, and the only index since
the ClickHouse server went (#153). The collector builds it in each index
pass (DEPLOY.md, "The warehouse, the snapshots and the export"), and the
snapshot the site serves, the Parquet export and the research tools are
made from it.

It reads the store with the parsers `db index` used, and reads all of
it: every commit's Syft document and manifests, and every fetch of the
dependency graph, where `db index` read the one commit a record named.
Each is a `scans` row, keyed by its input and tool@version, and what it
saw is `observations`, append-only: what `artifacts` was in ClickHouse.
`repositories` has the metadata, `repository_history` what each dated
search snapshot said of each repository, and `releases` and `edges` are
what `db index` and `db edges` made. A repository's releases, and each
scan's ref, are its release and commit decisions' where the store has
them (the repository-keyed layout, below): the releases of the newest
push whose commit the store has a scan of, and the ref each commit was
resolved from. Where it has none they are its record's. A repository
the old pipeline's `chatsbom run` collected has no record in the store:
`run` kept them in ClickHouse's `raw_documents`, which went with the
server unmigrated (#153), so its description, licence and topics are
gone, and its releases are its decisions'. The collector does not fetch
them: a repository it alone collected has what the search snapshots say
of it.

What is current is one rule: each repository's newest scan of each
source, of the corpus, the newest complete search snapshot. The
rollups are ClickHouse's, by the same names. What ClickHouse answered
of three inputs, every rollup and the releases and refs beside them,
was recorded before the server went, and the tests hold the warehouse
to it (`tests/golden/`). Adoption over time,
`mv_package_month_intervals`, counts a repository in every month
between two scans that both show the package; `mv_package_month`, the
months of the scans alone, stays for that check.

The `db` commands, which filled and asked the ClickHouse server, went
with it (#153), with no command in their place: `warehouse build` is
the index, and `db edges`' count is in it. What they asked is SQL for
the DuckDB CLI, on the warehouse (DEPLOY.md, "Asking the warehouse by
hand"): the corpus and its coverage, which `db status` gave, is
`build` and the `mv_*` tables, and a package's dependants, which `db
query` gave, are `facts`, the site's package page, or its API. `db
export`'s CSV of projects and their frameworks has none: the research
tools' `classify` and `openapi candidates` read the frameworks from the
warehouse themselves. Nor has a partial index (`--repos-file`,
`--limit`): a pass reads the whole store, in minutes.

A pass writes `warehouse.duckdb.building` and renames it into place when
it has finished, so `duckdb data/warehouse.duckdb` can read the last
one throughout; a second pass while one runs is refused. What it built
is printed on stdout, anything else on stderr.

DuckDB runs within limits, which fit the collector's container (4 GiB
and 2 CPUs, `docker-compose.yaml`): at most `CHATSBOM_DUCKDB_MEMORY_LIMIT`
of memory, 2GiB unless set, and `CHATSBOM_DUCKDB_THREADS` threads, 2
unless set (`.env.example`); compose gives the collector both. Every
command that opens DuckDB takes them, `snapshot build` and `export
parquet` too. Its own defaults are 80% of the machine's
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
a pass publishes, which the web service serves, to the page and to the
chat's tools alike (`WEB_SNAPSHOT=data/snapshots`, `chatsbom web`,
below). The collector runs it in each index pass, after `warehouse
build`. What it publishes is
anyone's to read, whatever the umask: `web` reads it as a uid of its
own, through a read-only mount. The directory is `0755`, `CURRENT`
`0644` and each snapshot `0444`, and a directory made by hand is
opened to all by the first pass.

Its tables, and the dataset API's answers from them, are those of the
Cloudflare D1 store the site read until #151, whose recorded answers
are the contract the API is held to (`web/test/fixtures/contract/`):
its rows are what D1 held of the same data, id for id. Two things
differ by design: adoption over time counts a repository in every
month between two scans that both show the package (Q9), and a
repository with no dependency is dated by its newest scan rather than
by the day `db index` wrote its row. `meta` also says which snapshot
the file is, the version that wrote it, the corpus, and each table's
rows. And since D1 went, two answers say what D1's could not (#165):
the contract is `v8`, as the Parquet export's manifest numbers it,
where D1's was `d1 v8`, and the edges' ambiguity is measured, where D1
answered none.

It adds two tables. `dependants`: the rows of a package's dependants
table, stored in the order the page shows them, which the Python
dataset API (`chatsbom/dataset/`) reads a range of where D1 grouped and
sorted every artifact of the package, with the same answers. At the
documented shape (16.1M facts) the most used package's page and its
counts took 171 ms from D1's tables and 14 ms from it; it costs 956 MB
of the file (1.75 GB in all) and 80 s of the build (160 s in all). And
`agg_edge_ambiguity`, one row: how far the edges, keyed by package
name, merge ecosystems, which the warehouse measures on each pass
(`mv_edge_ambiguity`) and the page's caveat on its edge panels quotes.
It adds an index too, the package names in the order SQLite's `LIKE`
matches them in, without regard to case: the search box's anchored
`LIKE` reads a range of it, where it read every name.

The overview's aggregates are precomputed, because no index can help
them: its panels read every artifact row by definition, and measured on
the real corpus they took 3,122 ms for the source comparison and 1,082
ms for the relationship split. Precomputed they answer in 3-4 ms from
tables totalling 44 KB. The point lookups are left alone:
`dependentsOf` answers in 4 ms straight off the indexes, and it takes
an arbitrary package name, so there is nothing finite to precompute.

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

### `chatsbom data` — housekeeping

| Command | Purpose |
| --- | --- |
| `prune` | Keep the newest N scans and release decisions per repository, and whatever the current scan descends from; discard older ones |
| | Reports by default; `--apply` deletes |

The collector's index pass runs it daily, `--keep 2 --apply`.
`data migrate-layout`, which moved a corpus to the layout below, and
`data slim`, which slimmed the old pipeline's per-language lists, went
with that pipeline (#171).

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
| Tree cache | `.cache/git-tree/<o>/<r>/<ref>/<sha>/` | none: the old pipeline's, which went with it (#171); the tree is the store's |
| Release decision | in `raw_documents` only | `03-github-release/<id>/<P>/release@2.json` |
| Release list | in `raw_documents` only | `03-github-release/<id>/releases/<sha256>.json` |
| Commit decision | in `raw_documents` only | `04-github-commit/<id>/<K>/commit@1.json`, a later one `<K>/<P>/commit@1.json` |

**The release and commit decisions** (#147, owner decision Q3 on #100).
Those two stages make no scan: what each produces is a decision, which
the collector's release and commit stages keep as they make it, as the
old pipeline's did before them.

- **The release decision** for the push `P` (`pushed_at`) says the tag
  of the latest stable release it chose, or none, and names the release
  list it chose from:
  `{"id": 42, "key": "2026-09-29T12:28:14Z", "out": "v2.0.0", "releases": "<sha256>", "stage": "release", "sv": 2}`.
- **The release list** is the releases as the model holds them, each
  asset trimmed to what `db index` kept of it less its download count,
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
decided before the stages kept their decisions was in ClickHouse's
`raw_documents` alone, which went with the server unmigrated (#153):
the collector decides it again, as it walks each repository.

**What they cost**, measured on a synthetic corpus of 1,000
repositories shaped like this one (41 releases each on average, heavy
tailed, a list of 39 KB; 25% pushed in a given week, 41% not in a year),
written by the stages' own code for a year of pushes, on ext4 with 4 KiB
blocks. A decision is two inodes, its directory and its file, and 8 KiB
of blocks for about 200 bytes; the lists are most of the bytes.

| Per repository, and for 65,000 | Inodes | Bytes | Blocks |
| --- | ---: | ---: | ---: |
| One push decided | 8 · 0.52 M | 35 KB · 2.3 GB | 66 KB · 4.3 GB |
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

Paths recorded before the move (the per-language lists, older
records) are translated by `core/layout.py` wherever they are read.

Retention is not optional once collection is continuous. A single
snapshot already occupies 46 GB under `data/` — 16 GB of SBOMs, 9.8 GB of
downloaded content, 8.3 GB of file trees — and every new commit adds
another content tree and another SBOM. Without pruning the disk fills and
collection stops silently, which is the worst failure mode available.

What is removed are *inputs*: recomputable from GitHub and keyed by
commit. The warehouse is rebuilt from what is kept, so its history is
the `--keep` newest scans of each repository. ClickHouse kept every
scan it was given, and that history went with the server (#153);
keeping every scan's documents, and pruning the trees alone, is #128's
Q10.

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

`03-github-release` and `04-github-commit` also hold one JSONL list
per language (5.7 GB each), which the old pipeline's `github release`
and `github commit` wrote and nothing reads since it went (#171), and
which retention does not reach: DEPLOY.md's cutover says what may
become of them.

### `chatsbom export` — portable artefacts

| Command | Purpose |
| --- | --- |
| `parquet` | Write the warehouse as Parquet plus a checksummed manifest (the `export` extra) |
| | `--warehouse PATH` reads another warehouse than `data/warehouse.duckdb`, and `--output DIR` writes elsewhere than `data/export`, which the site serves |
| `schema` | Emit the export contract as JSON and/or TypeScript types |

`export parquet` writes a self-describing copy of the dataset, a file a
table, that DuckDB or pandas reads directly, worth attaching to a
release. The page does not read it: it asks the web service by method
name, and the service answers from a snapshot (`chatsbom snapshot`,
above). The site serves it, as it is, for anyone to download or query
in place (`chatsbom web`, below). An earlier design shipped
6.1M rows as 20.6 MB of Parquet for a query engine in the browser. It
worked, but a first load cost 28 MB: 7.7 MB of WebAssembly plus the
whole dataset, because the engine downloaded each file rather than
reading ranges of it.

`export parquet` writes them from the warehouse `warehouse build`
makes, and reaches no server: four tables, one contract
(`EXPORT_SCHEMA`, version 8), content-addressed names and a checksummed
manifest, from the queries in `chatsbom/export/warehouse.py`, within
DuckDB's limits (`CHATSBOM_DUCKDB_*`, above). The warehouse is all it
reads since the ClickHouse server went (#153), and its `--from`, which
chose between the two, went with it. It is the weekly public export of
#128 (decision Q11). The collector's index pass runs it when the last
export is a week old, by its manifest's age, into `data/export`, which
holds the last export alone: exported into again,
a table that has not changed keeps its file, and the last export's
others go once the new manifest is written. The site serves it from
there, at `/export/` (#154, the owner's decision of 2026-09-30).

The same rows make the same files, whichever engine gave them. What
`export parquet` wrote from ClickHouse, of the contract seed, a
synthetic corpus and a store indexed by both engines, was recorded
before the server went (#153), and the export from the warehouse is
that, table by table, row by row and byte by byte, but for three things
that differ by design, as they do in the snapshot. Adoption over time
counts a repository in every month between two scans that both show
the package (Q9), where ClickHouse's counted the months of the scans. A
repository with no dependency is dated by its newest scan, or not at
all, rather than by the day `db index` wrote its row. And a scan is
dated by when the store first had its commit, which can be the day
before Syft made the document. At the documented shape (16.1M facts,
60,000 repositories) it wrote 99.9 MB in 37 s and held 3.0 GB at its
peak, most of it DuckDB sorting the facts; with DuckDB's own defaults,
33 s and 3.1 GB, and the same files. `export parquet` from ClickHouse
took 50 s over the same rows, the sort on the server, and wrote the
same bytes for three of the four tables; the fourth differed only in
the dates of the 31,930 repositories never scanned.

`export parquet` streams. It reads each table from DuckDB as Arrow
record batches and writes a row group at a time, so it never holds a
table in memory: it held the artifacts, 16.8 million rows, as Python
objects, about 3.8 GiB. A query that stops before its last row fails
the export, naming the table, rather than leaving it short; it used to
run each query a second time to count its rows.

`export schema` is the seam between the two languages. `src/schema.ts` in
the web project is generated from `chatsbom/export/schema.py`, so a
renamed column is a TypeScript compile error rather than an `undefined`
at runtime — and a test fails if the checked-in copy goes stale.

### `chatsbom web` — the web service

| Command | Purpose |
| --- | --- |
| `serve` | Serve the dashboard's page, its reads of the dataset, an ALTCHA challenge, the chat, the weekly Parquet export and `/healthz` from one FastAPI process on uvicorn |

The site (#128): one Python process. The page's reads are GETs under a
snapshot of the dataset, and its questions carry an ALTCHA proof of
work (#144).

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
  - `GET /export/manifest.json`: the weekly Parquet export's manifest
    (`chatsbom export`, above), from `WEB_EXPORT_DIR`, kept five minutes
    (`max-age=300`), and `GET /export/<file>`: each file it names now,
    kept for good, since a file's name is its content's, its ETag its
    SHA-256. A file answers `Range`, so DuckDB reads a table over HTTP
    without fetching all of it: `SELECT * FROM
    'https://<the site>/export/<file>'`. Anything else under `/export/`
    is a JSON 404: nothing is listed, and nothing the manifest does not
    name is served, nor anything through a link (#154).
  - `GET /healthz`, for a peer outside the edge network alone.

The chat's checks come cheapest first: the page's own origin, the
client's rate, the questions in flight, the proof of work, then the
day's cap, which each turn's worst case is held against before its call
and settled at its cost after, at DeepSeek's price for the hour. A
client that goes away stops the loop after the turn in flight, which is
settled. Without `DEEPSEEK_API_KEY` the chat is off, and both of its
routes answer 503.

Every response carries the page's headers: a content security policy
of this origin alone, with nothing inline or evaluated, `nosniff`,
`Referrer-Policy` and `Cross-Origin-Opener-Policy`. It listens on
`127.0.0.1:8080` unless `--host` and `--port` say otherwise, and logs
to stderr.

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
| `EXPORT_RATE_LIMIT` | `600/60` | The same for the export, `/export/*`, each range a request: DuckDB reads a table a row group a range |
| `DAILY_SPEND_CAP_USD` | `5` | The chat's cap a UTC day; `0` is none |
| `WEB_SNAPSHOT` | none | The dataset the page and the chat's tools read: the directory `snapshot build` publishes in, `data/snapshots`, or one snapshot's file. Unset, the page's reads answer 503 |
| `WEB_EXPORT_DIR` | `data/export` | The weekly Parquet export it serves at `/export/`: the directory the collector exports into. Until an export is written there, `/export/` answers 404 |
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
`unavailable`, before any of the day is held for it. A file without
the table a package's dependants are read from is not a snapshot, and
the service does not start with one.

A client is an IPv4 address or an IPv6 /64. The rate limits count over
a window that slides, in memory: a restart forgets them. A watchdog in
the process exits it when its event loop has not ticked for a minute,
so that the restart policy starts it again.

Under compose it is the `web` service, which a bare `up` starts, in
the image `Dockerfile.web` builds: Python, the package with this
extra, and the page, which Node builds in a stage of its own, with no
Node, `node_modules` or uv in the image. It runs as a uid of its own,
on a read-only root with no capabilities, and checks itself by asking
`/healthz` from inside.

    docker compose up -d

It serves on 8080, on the `edge` network, where the tunnel reaches it,
and publishes no port. Compose hands it the settings above from `.env`,
but for four it sets itself: `WEB_STATE_DIR`, the `web-sqlite` volume;
`WEB_SNAPSHOT`, `data/snapshots`, and `WEB_EXPORT_DIR`, `data/export`,
each mounted read-only; and `EDGE_SUBNET`,
the subnet compose gives `edge`, `172.16.128.0/24` unless `.env` says
otherwise. DEPLOY.md has how to route the site's hostname to it.

## The research tools: `chatsbom-research`

What the corpus is studied with, rather than how it is gathered: the
collector, the warehouse, the snapshot and the web service run none of
it. So since #167 the research tools are a command of their own,
`chatsbom-research`, installed with `chatsbom` wherever it is, and
their libraries are one extra, `research`. Without it, `classify`,
`openapi drift`, `list-paths` and `stats` stop and say to install it;
the rest need nothing more. No image has it: run them from a checkout,
where `uv sync` installs every extra, or from an install that has it.

```bash
pip install 'chatsbom[research]'
chatsbom-research openapi candidates
uvx --from 'chatsbom[research]' chatsbom-research classify --limit 10
```

They were `chatsbom openapi ...`, `chatsbom github classify` and
`chatsbom github readme`, and take the options they took and write
what they wrote, where they wrote it.

### `chatsbom-research classify` and `readme` — what each repository is

| Command | Purpose |
| --- | --- |
| `classify` | Classify repositories and extract metadata using an LLM (the `research` extra) |
| `readme` | Download README content |

`classify` asks an OpenAI-compatible API: OpenAI's,
`https://api.openai.com/v1`, for `gpt-4o-mini`, with `OPENAI_API_KEY`,
unless `OPENAI_BASE_URL` and `--model` name another endpoint and one of
its models. A server of your own, Ollama's for one, needs no key. It
classifies the repositories of the newest search snapshot,
`01-github-search/all-<date>.jsonl`, unless `--input` names a list,
and gives each the framework its current scan uses, read from the
warehouse (`data/warehouse.duckdb`, or `--warehouse`); without one, it
classifies them without.

`readme` downloads each listed repository's README into
`.cache/github-readme`, where `classify` looks for one before it asks
GitHub for it.

### `chatsbom-research openapi` — OpenAPI specification analysis

| Command | Purpose |
| --- | --- |
| `candidates` | Find repositories that ship an OpenAPI specification |
| `clone` | Clone candidate repositories for version-by-version analysis |
| `list-paths` | Export the API paths declared in each specification |
| `drift` | Measure how far each specification is from the endpoints its code implements |
| `stats` | Count each cloned repository's lines and tokens, and the LLM context windows it fits |

`list-paths`, `drift` and `stats` need the `research` extra;
`candidates` and `clone` need nothing more. `candidates` reads which
repositories use each framework, and at what version, from the
warehouse `warehouse build` makes (`data/warehouse.duckdb`, or
`--warehouse`): each one's current scan, of the corpus.

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
projects come back nearly empty. The collector keeps GitHub's own
dependency graph beside it, which parses manifests server-side:

| Repository | Syft | Dependency graph |
| --- | --- | --- |
| `spring-projects/spring-boot` | 0 | 303 |
| `elastic/elasticsearch` | 0 | 107 |
| `NationalSecurityAgency/ghidra` | 0 | 147 |

The two are complementary, not interchangeable, so every observation
records which produced it:

| Column | Meaning |
| --- | --- |
| `source` | `syft` (lockfile, resolved closure), `github-depgraph` (manifest, declared only) or `manifest` (Gradle build files, declared only; below) |
| `version_kind` | `resolved` (exact), `constraint` (`>= 0`, `^4.18`) or `unversioned` |

GitHub's graph is flat — the repository `DEPENDS_ON` each package with no
tree — so its rows are always `direct`. Its versions are the manifest's
constraints, which `version_kind` marks so a range is never charted as if
it were a resolution.

A scan's `manifest_sources` records which manifest files were read, so
a `transitive` verdict can be told apart from an unexamined one.

### The third source: Gradle build files

Syft reads no Gradle file — on 1.41.2, 40 of 40 sampled Gradle-only projects
had an empty SBOM, and 1.52.0 reads none either — and GitHub's graph is
partial for Gradle (halo-dev/halo: 105 packages, no Spring starter). So
the warehouse reads the build files itself (owner decision D1 on #55),
with the parsers `db index` used, and stores what they declare as rows with
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

Every current-state number — the rollups, the site, the snapshot and
the export — counts **the corpus**: the repositories of the current
search snapshot (owner decision D2 on #55). That is the newest complete
`all-*` snapshot in the store (`core/catalog.py`), which the warehouse
keeps as `corpus`. A repository the snapshot no longer lists (below the
star cut, deleted, private) keeps every row it has in `repositories`,
`scans` and `observations`, and is simply not counted. A store with no
complete snapshot counts every repository.

Numbers are keyed by **ecosystem** — npm, Maven, PyPI, Go, Composer,
Cargo, RubyGems and whatever else a collector reports, under the
canonical names of `core/ecosystems.py` — not by the repository's
language. A repository with an npm front end and a Maven back end is an
npm dependant of `react` and a Maven dependant of
`spring-boot-starter-web`. So a whole-corpus count is never the sum of
per-ecosystem counts: it is its own distinct count.

Coverage is measured against the whole corpus, collected or not: the
site gives the snapshot's size, and how many of its repositories have
dependency data from any source, from Syft, from the dependency graph
and from Gradle declarations. GitHub's language is kept as an attribute
and shown folded to the twelve most common and `other` (owner decision
D7), with `none` for a repository GitHub names no language for.

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

`observations` is **append-only**. Each row is an observation — this
package, at this version, in this repository, as seen in this scan — so
an update adds rows rather than replacing them.

That is a deliberate choice, and it decides what the project can answer.
Overwriting the current state destroys information on every refresh:
"how long did projects take to move off `mail` 2.7" is unanswerable once
the rows that knew are gone. Storage is no argument against it — 6.1M
rows compress to 17 MB, and a year of weekly deltas to roughly 220 MB.

| | |
| --- | --- |
| Engine | DuckDB, a file each pass rebuilds from the store (`warehouse build`) |
| Current state | each repository's newest scan of each source, of the corpus (`current_scans`); what they saw, deduplicated, is `facts`, which the rollups, the snapshot and the export read |
| History | every scan the store keeps: `mv_package_month_intervals`, and the exported `history` |

The history is what the store keeps, since the warehouse is rebuilt
from it: `data prune --keep N` leaves the N newest scans of each
repository. ClickHouse kept every scan it was given until it went
(#153); keeping every scan's documents, and pruning the trees alone, is
#128's Q10.

Counting is always `count(DISTINCT repository_id)`: a repository counts
once however many facts, and manifests reporting them, it has.

A package's adoption over time, monthly repository counts split by
direct and transitive, exists only because of this. `export parquet`
writes it to a separate `history.parquet` so the current-state tables
stay small.

### Resolving missing lockfiles

`sbom lock`, the resolver, closes the remaining gap: where a project
ships no lockfile, it runs the ecosystem's own resolver to produce one,
which the SBOM stage then folds into the scan, and the collector's due
set makes that scan due again once the lockfile is there. It resolves
the directories of each repository's current commit, and no older one,
as they become due; a failure is tried again after a backoff, kept in
`data/resolver.sqlite`, which holds nothing else: deleting it loses the
backoff alone.

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
- a network of its own, internal, made for it and removed after it,
  whose one other container, and one way out, is a proxy that lets
  through HTTPS to the recipe's registries alone: so resolutions running
  at once (`--workers`) cannot reach each other, and none reaches
  anything but Packagist or RubyGems
- images pinned by digest, so that every run resolves with the same
  composer and Ruby: a tag moves with each rebuild of its image; all
  pulled before a pass resolves anything, and never by a run

Network access is the one thing that cannot be removed — resolution *is*
fetching metadata from a registry — and it is to the registries alone.
What is left is what a registry serves: a project that needs a git
repository, a registry of its own or plain HTTP does not resolve. Requires
Docker.

Recipes exist for Composer and Bundler, and are chosen per *directory*
from the manifests there, not per repository from its language: a
directory with `composer.json` and no `composer.lock` is resolved
wherever it is in the repository (at most 10 directories a repository),
and the SBOM stage merges the result back at that directory. A
directory that already ships its lockfile is left alone: that lockfile
is what the project pins, so `sbom lock` does not resolve it again and
the SBOM stage never merges a resolved one over it. Go, Rust and npm are absent on purpose: those ecosystems commit
lockfiles as a matter of course, so Syft already reads them (Go coverage
is 90%, Rust 69%). Java and Python had recipes, withdrawn because Syft
reads neither file they wrote (`dependency-tree.txt`,
`requirements.lock`); Java also cannot resolve a multi-module POM from
the manifests the content stage stores (TODO.md, section E).

#### As a service

**The resolver, `sbom lock`, is a service of its own** (#168), with
none of the collector's tokens and a nested daemon of its own, so it
needs nothing on the host either:

```bash
docker compose --profile lock up -d        # the resolver, and its daemon
docker compose logs -f resolver
```

It resolves a lockfile for each directory the store makes due: at a
repository's current commit, one holding a manifest a recipe reads and
no lockfile, shipped or resolved, and no failure still backing off
(`data/resolver.sqlite`: 15 minutes, doubling to a week). The most
starred repositories go first. While nothing is due it sleeps
`CHATSBOM_RESOLVE_INTERVAL`, an hour; a stop cancels what is in flight,
with nothing half-written. The collector's SBOM stage folds each
lockfile into the next scan of that commit.

The question that shapes this is *where an escape lands*. `sbom lock`
runs an ecosystem's own resolver — a Gemfile is Ruby, a POM runs build
plugins — and mounting the host Docker socket into the collector would
put an escape on the host daemon, which is host root. Instead a
`docker:29-dind-rootless` sidecar, pinned by digest, provides the
daemon: its own root maps to an unprivileged host uid, it publishes no
port, and `compose down` destroys it.

Only `resolver` can reach it. The two share a network, `sandbox`, that
nothing else is on — not `web`, not the collector — and the API is TLS
on 2376, verified both ways. The image's entrypoint makes a CA and
certificates at every start; the client certificate reaches
`resolver` alone, read-only, through the `dind-certs` volume, and the
CA's key never leaves the daemon's container. It used to serve plain
TCP on 2375 on the default network, where every service, `web`
included, could start containers on it, and a resolver could reach
ClickHouse through it.

A resolution reaches its registries and nothing else. Each runs on a
network of the daemon's own, made for it and removed after it:
internal, and with no address on the daemon's side of its bridge, so
that nothing on it has a route out. The one other container on it is
its proxy, which is on the proxies' network too, the one with a route
out, and lets through CONNECT to port 443 of the recipe's registries,
and nothing else: repo.packagist.org and packagist.org for Composer,
rubygems.org and index.rubygems.org for Bundler. Another host, another
port, plain HTTP, an address in place of a name, a name that only ends
like a registry's, or TLS asking for another host than the tunnel's are
each refused, and logged with the directory that asked. The proxy is
our own, `chatsbom/core/egress.py`, in the standard library alone,
which its container runs on a pinned Python image, as nobody, read-only
and with no capability; the tests run the same source. `sandbox` stays
open: the daemon pulls every image a pass runs over it before it
resolves anything, and no resolution is on it.

Two things that took measuring rather than reasoning:

- Under a rootless daemon, `--user` is what *broke* the output write,
  when the lockfile was written to a mounted directory. A rootful
  daemon maps container uid 1000 to host uid 1000; a rootless one maps
  container *root* to the unprivileged host user, so an explicit uid
  lands on a subuid owning nothing and the resolver failed with
  `cp: /out/Gemfile.lock: Permission denied` after doing all the work.
  The sandbox probes `docker info` and drops only that flag.
- `./data` is mounted on the daemon as well as on `resolver`, at the
  same path. A container the daemon starts resolves a bind mount against
  *its own* filesystem, so a path only `resolver` could see would mount
  nothing, silently. The daemon's is read-only: a resolution only reads
  the project, and its lockfile comes back on stdout for `resolver` to
  write. For the same reason nothing of ours is mounted into a proxy:
  its source and its hosts reach it on its command line.

Verified end to end, before the lockfile came back on stdout: a hostile
Gemfile writing to `/project` and `/etc` was stopped at both, and
discourse's `Gemfile.lock` came out resolved and owned by the invoking
user.

The resolver stays out of the collector's process regardless — it
runs project-controlled code, so it runs apart, with no token, and only
behind the `lock` profile. `--workers N` resolves N directories at
once, each a container of up to `--memory` and `--cpus`, and each with
a network and a proxy of its own; the default is one at a time. `sbom
lock --once` is one pass, by hand: one process writes resolver.sqlite,
so stop the service first.

The Docker client lives only in the `lock` image, never the collector's.
An image with a Docker client and a reachable socket is one mistake away
from being an escape; splitting the images makes that a property of the
build rather than a rule someone has to remember. Both are stages of the
one `Dockerfile`, and the collector's never reaches the `lock` stage.

## Development

```bash
uv sync
uv run pytest                  # the suite: no server, no network
uv run pytest --cov            # with coverage, held to the floor in pyproject.toml
uv run pre-commit run -a       # lint, format, type-check
```

`uv sync` installs every extra, since the tests cover every command: the
`dev` group includes `chatsbom[all]`. `uv sync --no-dev` is chatsbom
without them; the collector's image adds the `export` extra alone, as
the systemd unit's install does.

Dependabot moves the packages pyproject.toml names, and nothing moves
what they pull in until a relock does. `python scripts/audit_lock.py`
asks OSV about every package uv.lock pins, whatever pulls it in, lists
each advisory with the versions that fix it, and exits 1 if there is
one; `uv lock --upgrade-package <name>` moves that package.

With `CI` set, as GitHub Actions sets it, a run in which any test skips
fails: CI provides everything the suite needs, syft among it. No test
needs a server since the ClickHouse one went (#153): what it answered
is kept in `tests/golden/`, and the warehouse, the snapshot and the
export are held to it.

A release is `uvx bump-my-version bump patch` (or `minor`, `major`) on a
clean tree. It rewrites the version wherever it is written, then runs
`uv lock` for the lockfile's copy; commit that, tag the commit
`v<version>` and push the tag, and the release workflow publishes it
once the tests pass. `[tool.bumpversion]` in pyproject.toml has the
rest, including why README's images stay on `main`.

## Use Case: Analyzing Framework Adoption

Find the most popular projects depending on a specific library (e.g.,
`gin`): ask the site's chat in natural language, or the warehouse in
SQL.

```sql
SELECT r.owner || '/' || r.repo AS repository, r.stars, f.version
FROM facts AS f JOIN repositories AS r ON r.id = f.repository_id
WHERE f.name = 'github.com/gin-gonic/gin'
ORDER BY r.stars DESC
LIMIT 10;
```
