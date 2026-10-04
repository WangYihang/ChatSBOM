# Deployment

Everything runs on one machine, under compose. The collector fills the
store; a pass builds the warehouse, the index, from the store and
publishes a snapshot of it; and one web service, `web`, serves the
page, its reads of that snapshot, and the chat. A Cloudflare tunnel
carries visitors to it, and nothing else reaches it from off the
machine. There is no database server (#153): the warehouse and the
snapshots are files in `data/`.

## The site

```
one machine                                            the internet
┌────────────────────────────────────────────────┐
│ collector      github · syft                   │
│        ↓                                       │
│ data/          the store                       │
│        ↓ warehouse build · snapshot build      │
│ data/snapshots CURRENT, <id>.sqlite            │
│ data/export    manifest.json, <table>-<hash>   │
│        ↓ read-only                             │
│ web            chatsbom web serve :8080        │──► DeepSeek, the chat
│        ↑ edge: internal, no host port          │
│ cloudflared    dials out                       │──► Cloudflare ──► visitors
└────────────────────────────────────────────────┘
```

`web` is `chatsbom web serve` (README, "`chatsbom web`"), in the image
`Dockerfile.web` builds: the page, its reads of the dataset as GETs
under a snapshot's id, the chat, on DeepSeek, the weekly Parquet export
under `/export/`, and `/healthz`, from one process on port 8080.

- **The image** is Python, the package with its `web` extra, and the
  page, which Node builds in a stage of its own: no Node,
  `node_modules` or uv. It runs as uid 10003, and checks itself from
  inside, asking `/healthz` with Python's urllib.
- **The container** has a read-only root, a 64 MB tmpfs on `/tmp`, no
  capabilities and `no-new-privileges`, 1 GB of memory and one CPU. It
  is on `edge`, where `cloudflared` reaches it, and on `default`, its
  way out to DeepSeek's API. It publishes no port.
- **Its state**, `web.sqlite`, the day's spend and the challenges used,
  is in the `web-sqlite` volume, which a recreate keeps. It reads the
  snapshots in `data/snapshots`, and the export in `data/export`, both
  mounted read-only.
- **Its settings** come from `.env`, as `.env.example` describes them,
  empty when unset. It does not start without `ALTCHA_HMAC_KEY`, and
  without `DEEPSEEK_API_KEY` the chat is off. Compose sets four
  itself: `WEB_STATE_DIR`, `WEB_SNAPSHOT` and `WEB_EXPORT_DIR`, its
  three mounts, and `EDGE_SUBNET`, the edge's subnet.

The web service reads no database server, only the snapshot's file.

### Serving it

1. Publish a snapshot into `data/snapshots`. From a checkout, after an
   index pass:

   ```bash
   uv run chatsbom warehouse build
   uv run chatsbom snapshot build
   ```

   The service reads the snapshot `data/snapshots/CURRENT` names as
   each question starts, so one published later is served without a
   restart. Without `data/snapshots`, compose refuses to start the
   service, saying `bind source path does not exist`, rather than make
   the directory owned by root. With no snapshot published in it, the
   service says so in its log, and is restarted until there is one.
   So too `data/export`, where the collector exports, and which it
   makes as it starts: before that, `mkdir -p data/export`. Until an
   export is written in it, `/export/` answers 404, and the rest is
   served all the same.
2. Put the service's settings in the `.env` beside
   `docker-compose.yaml`:

   ```bash
   ALTCHA_HMAC_KEY=<the output of: openssl rand -hex 32>
   DEEPSEEK_API_KEY=<DeepSeek's key>
   DAILY_SPEND_CAP_USD=1
   ```

3. From that directory, `docker compose up -d`, and `docker compose
   ps web` until it is `healthy`: the image's own check, `/healthz`
   asked from inside.

**The export** (#154) is served as the collector writes it (below, "The
warehouse, the snapshots and the export"):

- `/export/manifest.json`, the manifest, kept five minutes
  (`max-age=300`): its name never changes, and it names each table's
  file with its size and SHA-256.
- `/export/<file>`, each file the manifest names now, kept for good
  (`public, max-age=31536000, immutable`), since a file's name is its
  content's. Its ETag is its SHA-256, and it answers `Range` and
  `If-Range`, so DuckDB reads a table without fetching all of it:

  ```sql
  SELECT * FROM 'https://<the site>/export/<file>';
  ```

- Anything else under `/export/` is a 404: nothing is listed, and no
  file the manifest does not name is served, nor one reached through a
  link. Every request for the export counts against
  `EXPORT_RATE_LIMIT`, `600/60` unless set, a limit of its own: DuckDB
  asks for a table a row group a range, some 90 requests for the
  largest read whole, where a page view asks some 25 questions.

Checked with `chatsbom web serve` on a host, not yet under compose,
over an export of the contract corpus written under umask 077: the
directory came out `0755`, each file `0444` and the manifest `0644`,
and DuckDB 1.5.6 read each table over HTTP as the file holds it. A
table of 4M rows in 20 row groups, 9.9 MB, served the same way, was
read in ranges alone: its count fetched 0.4% of the file, one
package's rows 0.8%, and one column of every row 0.5%, in 22 ranges.

**Stopping it** sends SIGTERM: uvicorn takes no new request, answers
the ones in flight, and exits, with 143. Compose waits 30 s for that,
where Docker's default is 10, before a SIGKILL, which ends a question
in flight: its turn stays held against the day's cap.

Checked on Docker Engine 29.3.1 and Compose 5.1.1 (#145), with a
snapshot of the contract corpus and #143's stand-in for DeepSeek on
`default`. The image was 382 MB, 89 MB compressed, and had no Node,
npm, uv or curl. The service ran as uid 10003 with no capability and
`NoNewPrivs`, could write `/tmp` and its volume and nothing else, had
its watchdog's thread running, and was healthy 6 s after it started.
From a container on `edge`, the page and an asset answered 200 and
`/healthz` 404, and a challenge was signed for the address
`CF-Connecting-IP` named; from one on `default`, `/healthz` answered
200, and a challenge was signed for the container's own address,
whatever the header said. A question through the edge was answered, a
tool call, the text and `done`, and its two turns were settled in
`web.sqlite`, which a recreate kept. A stop while an answer streamed
waited 13 s for it to end, and exited 143; an idle one took 0.5 s.
Without `data/snapshots`, `up` failed on the mount and made nothing;
with it empty, or without `ALTCHA_HMAC_KEY`, the service said which
setting in its log, and was restarted.

### The tunnel

`cloudflared` runs as the `cloudflared` service, on two networks and
no others:

- **`edge`**, an internal network, holds `cloudflared` and `web`, and
  nothing else. It is `isolated` as well, so its bridge has no address
  on the host. Without that the host is on every internal network,
  reaches `web` from an address inside this one, and the containers
  reach whatever the host serves on all its interfaces. `isolated`
  needs Docker Engine 28.0 or newer.
- **`cloudflared-egress`** is the tunnel's way out, to Cloudflare, and
  nobody else's. `web` goes out to DeepSeek's API over `default`.
- **The tunnel's metrics** are served on its own loopback, where its
  healthcheck asks `/ready`: 200 while a connection to Cloudflare's
  edge is up, 503 while none is. On every interface, as the image
  serves them, they would be open to `web` and to the way out, the
  tunnel's routes at `/config` among them.

Nothing off this machine reaches `web` but through the tunnel, so the
`CF-Connecting-IP` the tunnel hands on, which the rate limits key on,
is the one Cloudflare's edge wrote. The service believes it from a peer
in the edge's subnet alone (`EDGE_SUBNET`), where only `cloudflared`
can be.

It is a remotely managed tunnel: its routes live in the Cloudflare
dashboard, and the container needs only its token.

1. In the Cloudflare dashboard, open **Networking → Tunnels**, select
   **Create a tunnel**, and name it. For Docker, the install command it
   shows ends in `--token` and a long value: that value is the token.
   Anyone who has it can serve your hostname, so keep it as you would a
   password.
2. On the tunnel's **Routes** tab, add a **published application**: the
   site's hostname, and the service URL `http://web:8080`. That is the
   compose service's name, which Docker's DNS answers on `edge`, and
   the port the service listens on. A hostname no rule names gets the
   catch-all at the end of the rules, `http_status:404`, which the
   dashboard keeps there.
3. In the `.env` beside `docker-compose.yaml`:

   ```bash
   COMPOSE_FILE=docker-compose.yaml:docker-compose.tunnel.yaml
   TUNNEL_TOKEN=eyJhIjoi...
   ```

   With a `docker-compose.override.yaml` of your own, add it to the
   list, last: with `COMPOSE_FILE` set, compose reads only the files it
   names.
4. From that directory, `docker compose up -d`.

Then check it, from this machine and from outside:

```bash
# Both containers' own checks: the service's /healthz, asked from
# inside, and the tunnel's /ready. Neither lists PORTS.
docker compose ps

# The routes the dashboard gave the tunnel: the hostname to
# http://web:8080, then http_status:404.
docker compose logs cloudflared | grep 'Updated to new configuration'

# The page, its snapshot and a number only the dataset has.
./scripts/health.sh https://sbom.example.com

# /healthz, through the tunnel: 404. It answers a peer outside the
# edge's subnet alone, so a 404 says the service knows the tunnel for
# the edge. A 200 would mean EDGE_SUBNET is not the edge's subnet, and
# every visitor would share one rate limit, cloudflared's.
curl -sS -o /dev/null -w '%{http_code}\n' https://sbom.example.com/healthz
```

Without a token, `cloudflared` stops at once, saying `"cloudflared
tunnel run" requires the ID or name of the tunnel to run`, and the
restart policy tries again; with one that is not a token, it says
`Provided Tunnel token is not valid.` One that cannot reach Cloudflare
prints its connectivity pre-checks: the tunnel needs outbound UDP to
port 7844, for QUIC, or TCP to 7844, for HTTP/2.

**The tunnel mode** is `docker-compose.tunnel.yaml` on top of
`docker-compose.yaml`, which switches `cloudflared` on for every `up`,
as `--profile tunnel` does for one command. Compose takes
`COMPOSE_FILE` from the `.env` in the directory it runs in. Run
anywhere else, it finds `docker-compose.yaml` alone, and an `up` there
leaves the tunnel off.

What is left is this machine itself. `web` is on `default`, for
DeepSeek's API, and a process here reaches it at its address there, as
it reaches any container's; the service keys it by that address,
whatever it says in `CF-Connecting-IP`. Another machine cannot: there
is no host port, and Docker 28.0 and newer drop direct routing to a
container's unpublished ports.

Checked on Docker Engine 29.3.1 with the tunnel mode up (#130), a
stand-in in the web service's place and a well-formed token for no
tunnel: the stand-in published no port, and the host's own addresses
refused; the host had no route to `edge`; a container on `edge` got the
page from the stand-in and was refused by the tunnel's metrics port; a
container on the tunnel's way out reached neither of the stand-in's
addresses; and `/ready` answered 503, with `"readyConnections":0`, until
Docker marked the tunnel unhealthy.

**The edge's subnet** is `172.16.128.0/24`, and `web` is told so. It
is private, and in none of the pools Docker gives a network its subnet
from when it is given none: `172.17.0.0` to `172.31.255.255` and
`192.168.0.0/16` on one host, and `10.0.0.0/8` for swarm's overlay
networks. In one of those, a network made first, for another project,
could hold it, and `up` would fail. The host has no route to an
isolated network, so a LAN on the same range stays reachable from this
machine. To move it, set one IPv4 subnet in the `.env`:

```bash
EDGE_SUBNET=172.16.129.0/24
```

The network and `web` both take it, so the two cannot differ, and the
next `up` makes the network again. If `up` says `Pool overlaps with
other one on this address space`, another project's network holds the
range; `docker network inspect <network>` says a network's subnet, and
another /24 of `172.16.0.0/16` will do.

## Continuous collection

The collector is one process, `chatsbom collect` (#171), the `collector`
service: containerised, so it leaves nothing on the host. Set
`GITHUB_TOKEN`, `UID` and `GID` in the `.env` beside
`docker-compose.yaml` (copy `.env.example` if you have none yet), then:

```bash
mkdir -p data/snapshots data/export .cache   # once, before the first `up`
docker compose --profile collect up -d --build
docker compose logs -f collector
docker compose --profile collect stop collector   # stop; `up -d` goes on where it was
docker compose down                 # gone — no units, no host installs
```

Coming from the old pipeline, the collector's loop and its `depgraph`
worker, follow "The cutover from the old pipeline", below, once.

What it does, each part a task of the one process, on one budget
(`chatsbom/collector/process.py`):

- **The universe**, every repository with at least 1,000 stars: loaded
  at the start from the newest complete search snapshot,
  `data/01-github-search/all-<date>.jsonl`, and searched again once that
  is `CHATSBOM_UNIVERSE_INTERVAL` old, a week, or at once when there is
  none: about 700 search requests, 26 minutes of the search bucket's 30
  a minute. A search that fails, or finds fewer than three quarters of
  the last universe, leaves the last one standing, and the next waits
  an hour.
- **The sweep** asks after every repository of the universe by node id,
  100 a GraphQL call, every `CHATSBOM_SWEEP_INTERVAL`, an hour: about
  650 of a token's 5,000 points for 65,000 repositories. It finds what
  was pushed, what moved its HEAD or its latest release, and what is
  gone.
- **The collections**, `CHATSBOM_REPOSITORIES_AT_ONCE` of them at once,
  four, one task a repository, its stages one after another: first what
  changed since it was collected, the longest changed first; then what
  was never collected, the most stars first; then what a new Syft or a
  new content stage makes due again, and a stage whose backoff has
  passed, which a walk of the universe in the store finds, a page at a
  time, beside the collections: they take what it has found so far, and
  never wait for it, so a store on a disk that seeks slows the walk, not
  the never collected.
- **The dependency graph**, a step when one is due and after every
  sweep, on its own bucket, `dependency_sbom`.
- **The index pass**, once something was collected since the last, at
  most every `CHATSBOM_INDEX_INTERVAL`, a day: `warehouse build`,
  `snapshot build`, `export parquet --output data/export` when the last
  export is a week old by its manifest, and `data prune --keep 2
  --apply`, each a child process, so that DuckDB's memory goes back when
  it exits ("The warehouse, the snapshots and the export", below).

**The budget.** Every request draws from its token's bucket, `core`,
`graphql`, `search` or `dependency_sbom`, the token with the most left,
four in flight a token at most. Where requests wait for the same room,
detection's go first, the universe's and the sweep's; then the
collections', a change's before a new repository's before a rescan's;
the dependency graph's last. So a backlog of collections never holds up
the next sweep. A bucket GitHub refuses, or that has only its reserve
(`CHATSBOM_GITHUB_RESERVE`) left, pauses what needs that bucket alone,
until its window resets: the rest goes on.

**Stopping.** `docker compose stop collector` sends TERM. It takes no
more work: the universe, the sweep, the graph and the index pass stop
at once, a sweep to go on where it was, a step of the index pass
interrupted as Ctrl-C would, and killed ten seconds later if it has not
exited. A collection in flight has ten seconds to end, and is then given
up on, its git and its Syft killed; each stage writes whole or not at
all, and what was given up is due again at the next start. It exits
within the 30 s grace compose gives it, with `The collector stopped` as
its last line. A second TERM gives up everything at once.

**Health.** Every 30 s the collector writes `data/collector.heartbeat`:
what each part is doing, idle until when, or busy until a deadline far
past what its work takes (a sweep two hours, a collection three).
Compose's healthcheck reads it, `python -m chatsbom.collector.health`:
unhealthy once the heartbeat is five minutes old, or a part is past its
deadline or ten minutes past the time it was to wake, and it says
which. `docker compose ps` shows it; so does the same command run in the
container:

```bash
docker compose exec collector python -m chatsbom.collector.health
```

**Logs.** JSON, one object per line on stderr, for `jq` or a log
collector: `docker-compose.yaml` sets `CHATSBOM_LOG_FORMAT=json` for it,
whatever `.env` says, and `docker compose run --rm cli ...` logs for a
person, as the CLI on the host does. One line per search of the
universe (`The universe was searched again`), per sweep (`The universe
was swept`, with what it found and cost), per index pass (`Index pass`,
each step and how it ended) and per repository collected (`Repository
collected`, its stages and how each ended); and each thing that went
wrong. A token appears as its label, `token 1` for `GITHUB_TOKEN` and on
through `CHATSBOM_GITHUB_TOKENS`, never as its value.

**More tokens** go in `CHATSBOM_GITHUB_TOKENS`, comma-separated, after
`GITHUB_TOKEN`: each serves every bucket. GitHub meters an account, not
a token, so a token adds to the budget only when it is another
account's; `scripts/probe_github.py` says whether yours share. A token
GitHub refuses is left out until the collector restarts; with none left
it stops, and says so.

`UID`/`GID` are not optional. `data/` and `.cache/` are bind mounts owned
by whoever cloned the repo, so a container running as its own baked-in
uid cannot write them — the first symptom is
`sqlite3.OperationalError: attempt to write a readonly database` from
collector.sqlite. `id -u` and `id -g` print them. They go in `.env`
rather than an `export`: bash holds `UID` read-only, so `export
UID=$(id -u)` fails, and stops a `set -e` script there.

The `mkdir` is for the same reason. None of these directories is in a
fresh clone, and Docker creates a missing bind-mount source owned by
root, which the containers, running as you, cannot write. Make them
before the first `up` or `run` of the `collect`, `lock` or `tools`
profile, all of which mount them. The collector checks as it starts,
and refuses to start on one it cannot write, with the `mkdir` and the
`sudo chown` that fix it in its log. Without a token it refuses to
start too, and says so in `docker compose logs collector`; compose
itself does not ask for one, so `ps`, `down` and the other services
work without it.

The image — the collector's and `cli`'s — installs chatsbom with one
extra, `export`, for the weekly Parquet export, and without development
tools, byte-compiled: all the collector runs needs. Its virtualenv is
263 MB, 161 MB of it pyarrow, which only the export loads. The research
tools, `chatsbom-research` (#167), are not the `cli` service's command,
and those that need the `research` extra would say they lack it there;
run them from a checkout.

Tunable in `.env` without rebuilding; `.env.example` says more of each:

| Variable | Default | Meaning |
| --- | --- | --- |
| `CHATSBOM_GITHUB_RESERVE` | `core=500,graphql=500,search=5` | What the collector leaves of each token's buckets for work run by hand, as `bucket=count` |
| `CHATSBOM_UNIVERSE_INTERVAL` | `7d` | How often the universe is searched again: a whole number and a unit, `s`, `m`, `h`, `d` or `w` |
| `CHATSBOM_SWEEP_INTERVAL` | `1h` | How often the sweep asks after the universe, in the same form |
| `CHATSBOM_REPOSITORIES_AT_ONCE` | `4` | Repositories collected at once, one task each |
| `CHATSBOM_INDEX_INTERVAL` | `1d` | How often at most the index pass runs, once something was collected since the last |
| `CHATSBOM_RECOLLECT_INTERVAL` | `7d` | How long at least between two collections of a repository for a change: one collected since waits, its change kept (#188) |
| `CHATSBOM_SYFT_SLOTS` | `1` | Syft scans at once: one of the container's two CPUs |
| `CHATSBOM_SYFT_TIMEOUT` | `10m` | How long a scan may run before it is killed and failed |
| `CHATSBOM_SYFT_MEMORY` | `2GiB` | How much a scan may hold; `0` is no limit |
| `CHATSBOM_DEPGRAPH_MAX_AGE` | `180d` | How long a dependency graph stands, unpushed, before it is fetched again anyway |
| `CHATSBOM_DEPGRAPH_MIN_INTERVAL` | `21d` | The least time between two fetches of a repository's graph |
| `CHATSBOM_DEPGRAPH_SETTLE` | `1h` | How old a push is before it makes a graph due |
| `CHATSBOM_DEPGRAPH_NO_GRAPH` | `30d` | How long a repository GitHub has no graph of is left before it is asked again |
| `CHATSBOM_DUCKDB_MEMORY_LIMIT` | `2GiB` | What DuckDB may hold in the warehouse, the snapshot and the export; it spills the rest to `data/` |
| `CHATSBOM_DUCKDB_THREADS` | `2` | The threads DuckDB runs: the container's two CPUs |

The container has 4 GiB and two CPUs. Syft held under 200 MB scanning
a content root of one manifest, and a root holds 64 MiB at most; the
index pass's snapshot build peaked at 2.9 GB at the documented shape,
with DuckDB's 2 GiB. The two run side by side, the collector's own few
hundred MB with them: raise `mem_limit` in `docker-compose.yaml` before
you raise `CHATSBOM_SYFT_SLOTS` or DuckDB's limit.

### The cutover from the old pipeline (once)

The old pipeline was the collector's loop (`deploy/collector-loop.sh`:
`queue sync`, `run`, an index pass and a retention pass on slices), the
`depgraph` worker beside it, and the ledger, `data/ledger.sqlite3`, that
both scheduled from. `chatsbom collect` replaces all three, and reads
none of what the ledger kept: what is done, the store says, and the
rest collector.sqlite starts again. On a host that ran the old pipeline,
once, from the checkout:

1. **Stop the old workers**, both, before anything else: they write the
   store, as the collector does, and must never run beside it. From the
   checkout as it is, whose compose file still names them:

   ```bash
   docker compose --profile collect stop collector depgraph
   docker compose ps -a      # neither running
   ```

   On a host that ran the systemd units instead, stop and remove them:

   ```bash
   systemctl --user disable --now \
       "$(systemd-escape --template=chatsbom-sync@.timer --path "$PWD")" \
       "$(systemd-escape --template=chatsbom-prune@.timer --path "$PWD")"
   rm ~/.config/systemd/user/chatsbom-sync@.* ~/.config/systemd/user/chatsbom-prune@.*
   systemctl --user daemon-reload
   ```

2. **Pull, and fold the old settings** into the collector's:

   ```bash
   git pull
   ```

   In `.env`, the tokens in `CHATSBOM_DEPGRAPH_TOKENS` go into
   `CHATSBOM_GITHUB_TOKENS`, after `GITHUB_TOKEN`, each once: every
   token of the collector serves every bucket, the dependency graph's
   too. Delete `CHATSBOM_DEPGRAPH_TOKENS`, `CHATSBOM_DEPGRAPH_API` and
   the loop's settings, which nothing reads now: `SYNC_INTERVAL_SECONDS`,
   `SYNC_SLICE`, `SYNC_QUOTA`, `RUN_LIMIT`, `RUN_QUOTA`,
   `DEPGRAPH_LIMIT`, `DEPGRAPH_RATE`, `DEPGRAPH_INTERVAL_SECONDS`,
   `INDEX_EVERY_SLICES`, `GENERATE_LIMIT`, `WAREHOUSE`,
   `EXPORT_INTERVAL_SECONDS`, `PRUNE_EVERY_SLICES` and `PRUNE_KEEP`.
   `.env.example` has the collector's own, each at its default. A
   `WAREHOUSE` of `off`, for a host that only collected, has no
   counterpart: the index pass builds the warehouse and a snapshot, and
   exports weekly, about 6 GB beside the store ("The warehouse, the
   snapshots and the export", below).
3. **Probe the tokens** the collector will use, from `.env` (a few
   dozen requests; it says how many first, writes nothing and prints no
   token):

   ```bash
   uv run python scripts/probe_github.py
   ```

   It measures what the collector counts on and no live token had: what
   a GraphQL `nodes(ids:)` call of 100 ids costs (the sweep's budget is
   one point a call), which bucket the dependency graph's endpoints
   answer from (`dependency_sbom`, as the budget draws them), whether a
   window's `X-RateLimit-Reset` holds, and whether your tokens share one
   account's limits. Where it finds otherwise, stop there: the sweep's
   interval, or the tokens, may need to change first, and the old
   workers can run again meanwhile ("Back to the old pipeline", below).
4. **Archive the ledger**, read-only. Nothing reads it any more: fold
   its WAL in, so that the file stands alone, then move it aside.

   ```bash
   mkdir -p data/archive
   uv run python -c 'import sqlite3; db = sqlite3.connect("data/ledger.sqlite3"); db.execute("PRAGMA wal_checkpoint(TRUNCATE)"); db.execute("PRAGMA journal_mode=DELETE"); db.close()'
   mv data/ledger.sqlite3 data/archive/
   rm -f data/ledger.sqlite3-wal data/ledger.sqlite3-shm   # empty now
   chmod a-w data/archive/ledger.sqlite3
   ```

   The rest of what the old pipeline left:

   | What | Now |
   | --- | --- |
   | `data/02-github-repo/*.jsonl`, `data/07-sbom/*.jsonl` | Kept: the warehouse reads each repository's description, licence and topics from them, which the collector does not fetch |
   | `data/01-github-search/<language>.jsonl`, `all.jsonl`, and the other stages' `<language>.jsonl` lists | Read by nothing: keep them, or move them to `data/archive/` |
   | `data/.sync.lock`, `data/.prune.lock` | The systemd units' locks: delete them |
   | `.requests-cache/` | The old pipeline's HTTP cache, which the research tools' README fetch shares: delete it, and they fetch what they need again |
   | `.cache/api.github.com/`, `.cache/git-tree/` | Its caches of GitHub's answers and of trees: delete them |
   | `data/_migration/` | The layout migration's journal (#55), which only `data migrate-layout --rollback` read, and that went with the old pipeline: delete it, unless you would roll the layout back with a checkout from before the cutover |
   | the store, `data/0[3-7]-*/<id>/`, `data/09-github-depgraph/`, `.cache/syft/` | The collector's, as they are |

5. **Start the collector** on the new image. `--remove-orphans`
   removes the old `depgraph` container, which the file no longer
   names:

   ```bash
   docker compose --profile collect up -d --build --remove-orphans
   docker compose logs -f collector
   ```

6. **The first hours.** `The collector starts`, with the tokens by
   label, first. collector.sqlite is new, so it has no universe: it
   loads the newest complete search snapshot, if one is less than a
   week old, and otherwise searches (about 700 search requests, 26
   minutes) before anything else, then says `The universe was searched
   again`. The first sweep follows at once: every repository asked
   after, some 650 GraphQL calls for 65,000, which takes a few minutes,
   then `The universe was swept`. Then the collections: every
   repository is one collector.sqlite has never collected, so each is
   collected once, the most starred first, a `Repository collected`
   line each. Most of its stages find their output in the store and
   cost nothing, but each content root the old pipeline fetched lacks
   the content stage's stamp, so it is fetched again, from
   raw.githubusercontent.com with no token, and scanned again: about
   1.6 CPU seconds of Syft a root, some twelve hours for 28,000 roots
   on the one slot. The first index pass comes once something was
   collected, and then daily. The dependency graph goes on from the
   graphs the `depgraph` worker kept.
7. **Healthy** is `docker compose ps` saying so; a `The universe was
   swept` line every hour; `Repository collected` lines while anything
   is due; an `Index pass` line a day with each step `:ok`; and
   `data/snapshots/CURRENT` moving when what the site serves changed.
   `bucket has no room` pauses are the budget working: they end at the
   window's reset. A line that repeats, `A part of the collector
   failed: it tries again`, or an unhealthy status, is worth reading.
8. **To stop it:** `docker compose --profile collect stop collector`;
   `docker compose --profile collect up -d` goes on where it was.

**Back to the old pipeline**, if it must be, from wherever the cutover
stopped: stop the collector if it started, check out the commit before
the cutover, put back what step 2 took out of `.env`, move
`data/archive/ledger.sqlite3` back to `data/` and make it writable if
step 4 moved it, and `docker compose --profile collect up -d --build`.
The old pipeline never reads collector.sqlite, and what the collector
wrote to the store is what the old stages wrote.

### The resolver

**The resolver, `sbom lock`, runs as a service of its own** (#168), with
a nested daemon of its own and none of the collector's tokens, so it
needs nothing on the host either:

```bash
docker compose --profile lock up -d            # the resolver, and its daemon
docker compose logs -f resolver                # each directory, each pass's counts
docker compose --profile lock stop resolver
```

It resolves what the store makes due: each directory of a repository's
current commit that holds a manifest a recipe reads and no lockfile,
shipped or resolved, and no failure still backing off; the universe's
repositories, the most starred first. The current commit is the one
that stands for the newest push the store has decided, once that
commit's content is in, stamped by the content stage in force: so the
resolver resolves what the collector has collected, and a content root
the old pipeline fetched waits until the collector fetches it again.
Until the collector runs over the universe, little is due. A failure is
kept in `data/resolver.sqlite`, tried again after 15 minutes, doubling
to a week, and nothing else is kept there: deleting it, with the service
stopped, loses the backoff and nothing more. While nothing is due it
sleeps `CHATSBOM_RESOLVE_INTERVAL` (`1h`, in `.env`). A stop cancels the
resolutions in flight, each container removed with its proxy and its
network, with nothing half-written, in a few seconds of its 30 s grace.
The collector's due set makes a commit's SBOM due again once a lockfile
is written for it, and its SBOM stage merges the lockfile in.

One pass by hand, of these repositories alone, found by name in the
universe: with the service stopped, since one process writes
resolver.sqlite.

```bash
docker compose --profile lock stop resolver
printf 'guzzle/guzzle\nrack/rack\n' > data/names.txt
docker compose --profile lock run --rm -e CHATSBOM_LOG_FORMAT=console \
    resolver sbom lock --once --repos-file data/names.txt
docker compose --profile lock start resolver
```

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
certificates at every start; the client certificate reaches `resolver`
alone, read-only, through the `dind-certs` volume, and the CA's key
never leaves the daemon's container. It used to serve plain TCP on 2375
on the default network, where every service, `web` included, could
start containers on it, and a resolver could reach ClickHouse through
it.

A resolution reaches its registries and nothing else:

- It runs on a network of the daemon's own, made for it and removed
  after it: internal, so that nothing on it has a route out, and with
  its bridge's gateway isolated, so that nothing on it reaches the
  daemon's side either, where the daemon's API listens. Two containers
  are on it: the resolver, and its proxy. Resolutions never share one,
  so none reaches another.
- The proxy is on the proxies' network too, `chatsbom-egress`, which has
  a route out, and on which traffic between containers is off. It lets
  through `CONNECT` to port 443 of the recipe's registries, and nothing
  else: repo.packagist.org and packagist.org for Composer, rubygems.org
  and index.rubygems.org for Bundler. It refuses another host, a name
  that only ends or begins like a registry's, another port, plain HTTP,
  an address in place of a name, a registry name that resolves to an
  address inside, and a tunnel whose TLS asks for another host than the
  one it was opened to. Each refusal is logged, as `Egress refused`,
  with the repository and the directory that asked.
- The resolver's environment names the proxy for each tool, curl's,
  Composer's, Ruby's and Bundler's: a tool that ignored it would find no
  route out. A project that needs a git repository, a registry of its
  own or plain HTTP does not resolve: its failure is kept, and tried
  again weekly.
- The proxy is our own, `chatsbom/core/egress.py`, the standard library
  alone, which its container runs on a pinned `python:3.14-slim`, as
  nobody, read-only, with no capability, no mount and bounded; its
  source and its hosts reach it on its command line, since the daemon
  sees its own filesystem and not the resolver's. One per resolution:
  its log is that resolution's, and a project that holds every
  connection of its proxy holds up its own resolution and no other.
- The daemon pulls every image a pass will run, the proxy's and the
  recipes', before the pass makes any network, over `sandbox`, which no
  resolution is on; each run is `--pull never`. So `sandbox` stays open
  to the internet, for the daemon, and no resolution uses it.

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
  write.

Verified end to end, before the lockfile came back on stdout: a hostile
Gemfile writing to `/project` and `/etc` was stopped at both, and
discourse's `Gemfile.lock` came out resolved and owned by the invoking
user. The egress limit is not verified in containers yet: this session
had no daemon. Its proxy was run against real resolutions on a
workstation, composer 2.8.12 and Bundler 4.0.9, which went through it
to repo.packagist.org and index.rubygems.org alone; the checks below
are the rest.

`--workers N` resolves N directories at once, each a container of up to
`--memory` and `--cpus`, with a network and a proxy of its own; the
default is one at a time.

The Docker client lives only in the `lock` image, never the collector's.
An image with a Docker client and a reachable socket is one mistake away
from being an escape; splitting the images makes that a property of the
build rather than a rule someone has to remember. Both are stages of the
one `Dockerfile`, and the collector's never reaches the `lock` stage.

To check the sandbox on a real daemon, with `dind` up (`docker compose
--profile lock up -d dind`, healthy in `docker compose ps`) and the
resolver service stopped:

```bash
# `d` runs the Docker CLI in `resolver`, against the nested daemon, over
# TLS; `py` runs Python there, with chatsbom, on the script on stdin.
d() { docker compose --profile lock run --rm -T --entrypoint docker resolver "$@"; }
py() { docker compose --profile lock run --rm -T --entrypoint python resolver -; }

# Three projects, and each resolved as the resolver resolves one: its
# network, its proxy, its recipe. Composer resolves through Packagist
# and Bundler through RubyGems, each tunnel logged; the third's Gemfile
# asks example.com through the proxy, refused as `host`, and dials an
# address past it, which has no route: `ENETUNREACH` or `EHOSTUNREACH`.
mkdir -p data/proof/composer data/proof/bundler data/proof/hostile
printf '{"require": {"monolog/monolog": "^3.0"}}\n' \
    > data/proof/composer/composer.json
printf "source 'https://rubygems.org'\ngem 'rack'\n" > data/proof/bundler/Gemfile
cat > data/proof/hostile/Gemfile <<'EOF'
require 'net/http'
require 'socket'
begin
  Net::HTTP.get(URI('https://example.com/'))
  warn 'through the proxy: REACHED example.com'
rescue StandardError => e
  warn "through the proxy: refused, #{e.class}"
end
begin
  Socket.tcp('151.101.1.227', 443, connect_timeout: 5).close
  warn 'around the proxy: REACHED 151.101.1.227'
rescue StandardError => e
  warn "around the proxy: #{e.class}"
end
source 'https://rubygems.org'
gem 'rack'
EOF
py <<'EOF'
from pathlib import Path
from chatsbom.core import sandbox
sandbox.sweep()
sandbox.prepare(sandbox.LOCK_RECIPES.values())
for ecosystem, name in (
    ('composer', 'composer'), ('gem', 'bundler'), ('gem', 'hostile'),
):
    project = Path('data/proof', name).absolute()
    result = sandbox.generate_lockfile(ecosystem, project, project / 'out')
    print(name, 'resolved' if result.ok else f'failed ({result.returncode})')
    for event in result.egress:
        print('   ', event['event'], event.get('host') or event.get('request'),
              event.get('reason', ''))
    for line in result.stderr.splitlines():
        if 'the proxy' in line:
            print('   ', line)
EOF

# A resolver's view of a network made as the sandbox makes one: no
# route out (the registry unreachable without its proxy), `web` does not
# resolve, and 2375 is closed everywhere.
d network create --internal \
    --opt com.docker.network.bridge.gateway_mode_ipv4=isolated \
    --opt com.docker.network.bridge.gateway_mode_ipv6=isolated proof-net
d run --rm --network proof-net --entrypoint sh \
  composer:2.10@sha256:9715c7f69044da2a212a5fbde29ee7da24e364d426560ae6367b060236f847d7 -c '
  ip route
  wget -q -T 5 -O- https://repo.packagist.org/packages.json >/dev/null \
    && echo "repo.packagist.org: REACHED" || echo "repo.packagist.org: no route"
  wget -q -T 5 -O- http://web:8080/healthz || echo "web: unreachable"
  for host in 172.17.0.1 dind; do
    wget -q -T 5 -O- "http://$host:2375/version" || echo "$host:2375: closed"
  done'
d network rm proof-net

# The API from `resolver`, with TLS but without its certificate: the
# daemon ends the handshake (certificate required), and 2375 is closed.
docker compose --profile lock run --rm -T --entrypoint docker \
  -e DOCKER_TLS_VERIFY= -e DOCKER_CERT_PATH=/nowhere resolver \
  --tls -H tcp://dind:2376 version
docker compose --profile lock run --rm -T --entrypoint docker \
  -e DOCKER_TLS_VERIFY= -e DOCKER_CERT_PATH=/nowhere resolver \
  -H tcp://dind:2375 version

# Nothing else reaches the daemon: `dind` does not resolve from `web`.
docker compose exec web python -c "import socket
try: socket.getaddrinfo('dind', None); print('dind: REACHABLE')
except OSError: print('dind: unreachable')"

# A hung resolution is removed at its deadline, with its proxy and its
# network: a Gemfile is Ruby, and `sleep 3600` hangs `bundle lock`.
mkdir -p data/proof/hung && echo 'sleep 3600' > data/proof/hung/Gemfile
py <<'EOF'
from pathlib import Path
from chatsbom.core import sandbox
project = Path('data/proof/hung').absolute()
result = sandbox.generate_lockfile(
    'gem', project, project / 'out', sandbox.SandboxLimits(timeout=20),
)
print(result.returncode, result.stderr.splitlines()[-1])
EOF
d ps -a --filter label=chatsbom.sandbox
d network ls --filter label=chatsbom.sandbox=resolution
rm -rf data/proof
```

The first `py` prints `composer resolved` with a tunnel to
repo.packagist.org, `bundler resolved` with tunnels to
index.rubygems.org, and `hostile resolved`, its rack resolved, with
`refused CONNECT example.com:443 HTTP/1.1 host` among its events, and
its own two lines: refused through the proxy, and unreachable around
it. The view prints no default route in `ip route`, and `no route`,
`unreachable` and `closed` for each; the certificate check ends the
handshake. The hung one prints `124` and `timed out after 20s` about 20
s in, and both listings are empty.

And the service's stop, while it resolves: `docker compose --profile
lock up -d`, then `docker compose stop resolver` while `logs -f` shows a
resolution under way. Its log ends with `Stopped, with nothing
half-written` within a few seconds, and `d ps -a --filter
label=chatsbom.sandbox` lists nothing.

### The warehouse, the snapshots and the export

The collector's index pass makes what the web service, `web`, serves
(#128 §2.3 and §2.4): `warehouse build` makes `data/warehouse.duckdb`
from the store alone, and `snapshot build` publishes a snapshot of it
in `data/snapshots`, but only when what it serves has changed; on most
days neither `CURRENT` nor a snapshot is touched. Then the public
Parquet export, `export parquet --output data/export`, which `web`
serves (#154, "The site" above): whenever the last is a week old, by
the age of `data/export/manifest.json`, which a restart does not
change, and first in the pass that builds the first warehouse. Then
`data prune --keep 2 --apply`. Each is a step, a child process: one
that fails is said in the log and stepped over, and the next pass tries
it again. The warehouse is the only index since the ClickHouse server
went (#153): the pass runs no `db raw` and no `db index` before it.

**What they take**, at the documented shape (19.4M observations, 16.1M
facts, 60,000 repositories), each step bounded as the collector's
container bounds it, 4 GiB and two CPUs, with DuckDB's defaults above:

| Step | When | Time | Peak memory | Disk |
| --- | --- | ---: | ---: | --- |
| `warehouse build` | each index pass | 13 min: 11 reading the store, 2 deriving; on an HDD, see below | 2.4 GB | 0.63 GB; twice that while the next is written, and up to 1.9 GB spilled |
| `snapshot build` | each index pass | 3.2 min, whether it publishes or not | 2.9 GB | 1.75 GB a snapshot: 5.3 GB for the three kept, 7 GB while the next is written, and 0.5 GB spilled |
| export | each week | 41 s | 2.4 GB | 0.1 GB, and 0.5 GB spilled |

Those times are of a store on an SSD. **On a disk that turns** the
store's directories and files are a seek apiece, and reading all of it
is hours: on the HDD this collector runs on, beside the collections,
two and a half hours for 65,294 repositories and 56.6 million
observations (8,992 s, 547 of them deriving), with sixteen of them
read at once (`warehouse/prefetch.py`); read one at a time, the first
5,000 took three times as long. So a pass reads only what changed
(#187): each repository whose directories in the store, or record,
changed since the last warehouse was built is read again, and every
other one is carried over from it, unread (README, "A pass reads what
changed"). A day changes about a tenth of the repositories; after one
to three hours of collection a pass took 22 to 25 minutes there: 960
and 3,099 repositories read, 64,423 and 62,284 carried over, 5 to 7
minutes asking 1.3 million directories whether they changed, and 11 to
14 deriving. And a pass reads for at most
`STEP_TIMEOUT` × `READING` of its step, 96 minutes of the two hours
(`chatsbom/collector/index.py`), then stops reading and keeps what it
read in `data/warehouse.duckdb.partial`, publishing nothing: the next
pass carries on from it. So the first pass after a deploy whose code
reads the store otherwise, which reads every repository, takes a pass a
part: two passes of the HDD's store (51,180 repositories read in
the first, the other 14,773 in the second), a day apart, while the last
snapshot is served. To have it at once, run it by hand, without a
limit (`docker compose --profile tools run --rm cli warehouse build`),
which takes the lock, so that a pass of the collector's meanwhile is
refused and goes on.

So an index pass on an SSD takes about 16 minutes, and the weekly one
17, while the collections go on beside it. Nothing was killed for memory, the
cgroup giving back page cache instead. On disk the three take about
6 GB between passes, and up to 8.5 GB during one: leave 10 GB free for
them, beside the store. None of it is backed up, since the store makes
all of it again. The read was measured at 1/20 of the corpus, 34 s, and
it grows with it; the rest at full scale.

**One export is kept.** `data/export` is exported into again: its files
are named by their content, a table that has not changed keeps its
file, and once the new manifest names the new ones, the last export's
go. So it is one export, the one `web` serves. The warehouse can make
it again at any time, and only a copy kept elsewhere, as a release's
assets, keeps the weeks before.

**DuckDB spills on the data volume,** into a directory of the process's
own beside the warehouse, in `data/`, and removes it when the process
closes the file. A step stopped mid-spill, by the collector's stop or
by the container's memory limit, leaves its directory; the next `warehouse
build` removes it, once no process has the warehouse open. Nothing is
fetched from the network at run time: what DuckDB needs is in its
wheel.

**Asking the warehouse by hand.** The DuckDB CLI is the query shell,
where `db query` and `db status` asked the ClickHouse server and went
with it (#153). Install it on the host (duckdb.org/docs/installation),
of the release `uv.lock` pins for `duckdb` or a later one, which reads
what an earlier one wrote, and open the warehouse read-only: a pass
renames the next one over it, and a reader keeps the one it opened.

```bash
duckdb -readonly data/warehouse.duckdb
```

```sql
-- What the last pass built, and from what: `db status`'s question.
SELECT * FROM build;
-- Who depends on a package, most starred first: `db query mail`'s.
SELECT r.owner || '/' || r.repo AS repository, r.stars, f.version,
       f.relationship, f.source
FROM facts AS f JOIN repositories AS r ON r.id = f.repository_id
WHERE f.name = 'mail'
ORDER BY r.stars DESC
LIMIT 10;
```

`facts` holds the current scans' dependencies, of the corpus;
`observations` every scan's, `scans` what each scan read, and the
`mv_*` tables what the site's charts count.

**`web` reads them as uid 10003**, through a read-only mount of
`data/snapshots`: neither the collector's `UID` nor in its group. So
`data/snapshots` is anyone's to list and enter, `CURRENT` anyone's to
read, and each snapshot anyone's to read and no one's to write,
whatever umask made them. The export is given the same (#154):
`data/export` anyone's to list and enter, its manifest anyone's to
read, and each of its files anyone's to read and no one's to write.
The warehouse follows the umask, as the rest of `data/` does: it is
not `web`'s.

Before the first pass:

1. **The disk.** `df -h data` should leave 10 GB beyond what the store
   grows into: the collector runs the pass whatever the disk.
2. **`data/snapshots`**, for `web` to start before anything is
   published: it does not start without the directory. Make it as you
   made `data/`, `mkdir -p data/snapshots`, whatever your umask: the
   first pass opens it to all. `web` does not start without
   `data/export` either, which the collector makes as it starts, and
   the first export opens to all; to start `web` first, make it too.
3. **The image.** The collector's carries pyarrow now, for the export:
   rebuild it, `docker compose --profile collect up -d --build`.
4. **When.** The first index pass comes once the collector has collected
   something, and each next once it has collected more, a day after the
   last at the soonest (`CHATSBOM_INDEX_INTERVAL`), counted from when
   the last pass started, one whose steps failed included, or the
   warehouse was built, whichever is later. A restart changes neither:
   when a pass that ran to its end started is kept in
   `data/index-pass.json`. A pass the collector stopping gave up on is
   due again at the next start. The first export is in the
   pass that builds the first warehouse, there being none yet, and the
   next when that one is a week old, by its manifest's age. To have
   them sooner, or at any time, run them by hand in the same image and
   mounts (`cli` takes the defaults for DuckDB's limits, not `.env`'s);
   a pass of the collector's that meets one is refused the lock, says
   so, and goes on:

   ```bash
   docker compose --profile tools run --rm cli warehouse build
   docker compose --profile tools run --rm cli snapshot build
   docker compose --profile tools run --rm cli \
       export parquet --output data/export
   ```

   and then `docker compose up -d web`.

Checked on Docker Engine 29.3.1 and Compose 5.1.1, on a synthetic store
of 3,000 repositories. The collector's service, as compose runs it
(1000:1000, 4 GiB, two CPUs) and here under umask 077, ran one slice of
the loop that went before the collector, with its index pass and the
export, the steps before them stood in for: the warehouse in 37 s, a
snapshot published, and the
export, with nothing spilled left. `data/snapshots` came out `0755`,
`CURRENT` `0644` and the snapshot `0444`, and `web`, uid 10003, was
healthy and read it through its read-only mount, which refused a
write. Before this change, the same steps stopped at their first
connection to DuckDB as a uid with no home to write, as the
collector's is (its `HOME` is `/`): DuckDB looked there for what the
time zone needs.

### With systemd instead

For a dedicated server rather than a dev machine,
`deploy/systemd/chatsbom-collect@.service` runs the collector as a user
service, with `ProtectSystem=strict`, `ProtectHome=read-only`, and
`ReadWritePaths` limited to `data/` and `.cache/`. It is a template:
the instance is the checkout's path, so nothing in it names a
directory and it runs wherever the checkout is. It starts the
checkout's own `.venv/bin/chatsbom` — `uv run` cannot start with a
read-only home — so `uv sync` has to have made it first, and the host
needs a Syft of the Dockerfile's version on PATH:

```bash
cd ~/ChatSBOM                    # the checkout, wherever it is
uv sync --frozen --no-dev --extra export   # .venv, and pyarrow for the export
[ -e .env ] || cp .env.example .env    # then set GITHUB_TOKEN in it
mkdir -p ~/.config/systemd/user
cp deploy/systemd/chatsbom-collect@.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now \
    "$(systemd-escape --template=chatsbom-collect@.service --path "$PWD")"
loginctl enable-linger "$USER"   # so it runs with nobody logged in
```

For a checkout in `/home/alice/ChatSBOM` that is
`chatsbom-collect@home-alice-ChatSBOM.service`; `journalctl --user -u
'chatsbom-*'` has what it did, and `.venv/bin/python -m
chatsbom.collector.health` whether it is making progress. `systemctl
--user stop` it as compose stops the container: TERM, within its 30 s.
It comes back by itself after a failure, five minutes later. Copy the
unit again after a pull that changes it, then `daemon-reload`. It
replaces `chatsbom-sync@` and `chatsbom-prune@`, whose timers ran the
old pipeline: the cutover above says how to remove them. On the host
the collector runs as many Syft scans as it has cores but one: set
`CHATSBOM_SYFT_SLOTS` in `.env` to fewer, where memory is short.

## Removing the ClickHouse server (once)

The warehouse is the index since #153, and nothing reads ClickHouse:
compose has no `clickhouse` service, and the CLI no `db` commands. What
the server held is not migrated (the owner's decision on #153). Its
rows are the store's, which the next index pass reads again, but for
the finished records `chatsbom run` kept in `raw_documents` and nowhere
else: of a repository only `run` collected, the warehouse has what the
search snapshots say, its name, stars, language and default branch, and
not its description, licence or topics, which the collector does not
fetch.

On a host that ran it, from the checkout, after pulling:

```bash
docker compose up -d --remove-orphans   # the old `clickhouse` container goes
sudo rm -rf database/                   # its data, owned by the image's uid
```

Then delete the `CLICKHOUSE_*` lines from `.env`: nothing reads them. A
copy of `database/data` taken first is the only way back to what the
server held.

## Upgrading Syft

`ARG SYFT_VERSION` in the Dockerfile moves it, with the two digests
beside it: the image installs the release's archive for the
architecture it is built for, and the build stops when the archive is
not the one `SYFT_SHA256_AMD64` or `SYFT_SHA256_ARM64` names. Both are
the release's own, from its checksums file:

```bash
v=1.53.0   # the new version
curl -sSfL "https://github.com/anchore/syft/releases/download/v$v/syft_${v}_checksums.txt" \
  | grep -E "  syft_${v}_linux_(amd64|arm64)\.tar\.gz\$"
```

CI's `SYFT_VERSION` and `SYFT_SHA256` (the amd64 digest) in
`.github/workflows/test.yml` move with them, and so do `SYFT_VERSION`
and `SYFT_SHA256` in `chatsbom/core/syft.py`, the release the CLI
suggests when it finds no Syft: `workflows_test` and `syft_hint_test`
hold each to the Dockerfile's. 1.41.2 to 1.52.0 was the last move.

The version keys the Syft cache (`.cache/syft/<version>/`),
and a stored SBOM that another version wrote is not current, however
new it is: its own `descriptor` says which Syft wrote it
(`staleness`). So the new Syft regenerates every stored SBOM,
once, and none of the old cache is used for it.

- **The collector** scans them again as it walks the universe in the
  store (`docker compose --profile collect up -d --build`): each SBOM
  another Syft wrote is due again, after what changed and what is new,
  in the order of the walk. Each is a `Repository collected` line in
  `docker compose logs collector`, with `priority` `rescan` and
  `sbom:done`.
- **How long.** The new version's cache starts empty, so nearly every
  root is a fresh scan, and a scan is about 1.6 CPU seconds whatever
  the root holds. On the collector's one Syft slot that is about twelve
  hours for 28,000 roots, while the sweep and the rest go on beside it.
  Two slots halve it, given a CPU and the memory for the second: raise
  the container's `cpus` and `mem_limit` first ("Continuous
  collection").
- **By hand**, one repository: `docker compose --profile tools run --rm
  cli collect repo <owner/name>`, with the collector stopped, since one
  process holds collector.sqlite.

When `syft version` fails, or says nothing that reads as a version, no
SBOM is regenerated for the Syft that wrote it: the times alone decide,
as before, and a warning says so. Once nothing will go back to the old
Syft, its cache can go: `rm -rf .cache/syft/1.41.2`.

## Why there is no message broker

The store is the queue's state, and a better fit than a broker. What is
done is what the store holds, derived per repository along its chain
(#100): a stage is due when its output for its key is missing, so a
killed process loses nothing a restart does not find due again. What
the store cannot say, collector.sqlite keeps: each repository as last
observed, which stage found nothing or failed and when it is due again,
and the dependency graph's reports pending. A broker gives
at-least-once delivery of ephemeral tasks; lose the message and you
lose that unit of work.

A broker earns its place with many independent producers and tasks cheap
to retry from scratch. Here there is one producer, the collector's own
clock and its sweep, and work that is expensive and idempotent per
repository, which is exactly the shape a store derived from serves and
a queue does not.
