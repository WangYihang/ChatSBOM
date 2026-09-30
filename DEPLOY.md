# Deployment

Everything runs on one machine, under compose. The collector fills the
store and ClickHouse; a pass builds the warehouse from the store and
publishes a snapshot of it; and one web service, `web`, serves the
page, its reads of that snapshot, and the chat. A Cloudflare tunnel
carries visitors to it, and nothing else reaches it from off the
machine.

## The site

```
one machine                                            the internet
┌────────────────────────────────────────────────┐
│ collector      github · syft · db index        │
│        ↓                                       │
│ data/          the store                       │
│        ↓ warehouse build · snapshot build      │
│ data/snapshots CURRENT, <id>.sqlite            │
│        ↓ read-only                             │
│ web            chatsbom web serve :8080        │──► DeepSeek, the chat
│        ↑ edge: internal, no host port          │
│ cloudflared    dials out                       │──► Cloudflare ──► visitors
└────────────────────────────────────────────────┘
```

`web` is `chatsbom web serve` (README, "`chatsbom web`"), in the image
`Dockerfile.web` builds: the page, its reads of the dataset as GETs
under a snapshot's id, the chat, on DeepSeek, and `/healthz`, from one
process on port 8080.

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
  snapshots in `data/snapshots`, mounted read-only.
- **Its settings** come from `.env`, as `.env.example` describes them,
  empty when unset. It does not start without `ALTCHA_HMAC_KEY`, and
  without `DEEPSEEK_API_KEY` the chat is off. Compose sets three
  itself: `WEB_STATE_DIR` and `WEB_SNAPSHOT`, its two mounts, and
  `EDGE_SUBNET`, the edge's subnet.

ClickHouse is bound to the loopback interface and is on no network the
tunnel reaches; the web service reads no database server at all, only
the snapshot's file.

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

Containerised, so it leaves nothing on the host. Set `GITHUB_TOKEN`,
`UID` and `GID` in the `.env` beside `docker-compose.yaml` (copy
`.env.example` if you have none yet), then:

```bash
mkdir -p data/snapshots .cache .requests-cache   # once, before the first `up`
docker compose --profile collect up -d --build
docker compose logs -f collector
docker compose down                 # gone — no units, no host installs
```

The `collect` profile starts two services from one image: `collector`,
the sync-and-run loop, and `depgraph`, the dependency-graph worker
(`collector-loop.sh depgraph`, `docker compose logs -f depgraph`). The
dependency graph is metered per token, apart from the core API, and its
synchronous endpoint closes after 2026-11-13, so it runs at its own pace
all the time. Seed the queue with every repository the search found,
not only the language lists, once:

```bash
docker compose --profile tools run --rm cli queue track \
    --snapshot data/01-github-search/all.jsonl
```

A second GitHub token doubles its throughput. Put it in `.env` as
`CHATSBOM_DEPGRAPH_TOKENS=<token>` (several: comma-separated) and
recreate the service (`docker compose --profile collect up -d
depgraph`); the log names each token by position and login, never by
value, and one GitHub rejects is skipped. `DEPGRAPH_RATE` (requests an
hour per token, default 90), `DEPGRAPH_LIMIT` and
`DEPGRAPH_INTERVAL_SECONDS` tune it; `queue status` shows what is due.

Both services log JSON, one object per line on stderr, for `jq` or a
log collector: `docker-compose.yaml` sets `CHATSBOM_LOG_FORMAT=json` for
them, whatever `.env` says. The loop's own lines and each command's
summary stay plain text, and `docker compose run --rm cli ...` logs for
a person, as the CLI on the host does.

The image — the collector's, `depgraph`'s and `cli`'s — installs
chatsbom with one extra, `export`, for the weekly Parquet export, and
without development tools, byte-compiled: all the loop runs needs. Its
virtualenv is 273 MB, about 150 MB of it pyarrow, which only the export
loads. `github classify` and the `openapi` analyses stop in
`cli` and say which extra they need; run those from a checkout. An
image built before this change lacks pyarrow, so rebuild it: `docker
compose --profile collect up -d --build`.

`UID`/`GID` are not optional. `data/` and `.cache/` are bind mounts owned
by whoever cloned the repo, so a container running as its own baked-in
uid cannot write them — the first symptom is
`sqlite3.OperationalError: attempt to write a readonly database` from the
ledger. `id -u` and `id -g` print them. They go in `.env` rather than an
`export`: bash holds `UID` read-only, so `export UID=$(id -u)` fails, and
stops a `set -e` script there.

The `mkdir` is for the same reason. None of the three directories is in
a fresh clone, and Docker creates a missing bind-mount source owned by
root, which the containers, running as you, cannot write. Make them
before the first `up` or `run` of the `collect`, `lock` or `tools`
profile, all of which mount them. The collector checks, and refuses to
start on one it cannot write, with the `sudo chown` that fixes it in
its log.

Without a token the collector refuses to start, and says so in
`docker compose logs collector`; compose itself no longer asks for one,
so `ps`, `down` and the other services work without it. A stop takes a
moment rather than the ten-second grace period: the loop passes TERM on
to the step in flight, a slice or a `run` pass, and waits for it, and a
step cut short loses at most the repository it was on.

One slice every 15 minutes by default, each followed by a `chatsbom run`
pass that collects what the slice made due; an index pass (`sbom
generate` for the SBOMs no longer current, `db raw --apply` and `db
index`, then `warehouse build` and `snapshot build`) and a retention
pass roughly daily; the Parquet export weekly; and, beside them,
`depgraph` passes five minutes apart. Tunable in `.env` without
rebuilding:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SYNC_INTERVAL_SECONDS` | `900` | Wait between slices |
| `SYNC_SLICE` | `500` | Repositories re-checked per slice |
| `SYNC_QUOTA` | `250` | Rate-limited requests per slice (304s are free) |
| `RUN_LIMIT` | `50` | Repositories a `run` pass advances |
| `RUN_QUOTA` | `400` | API requests a `run` pass may spend |
| `INDEX_EVERY_SLICES` | `96` | Slices between index passes |
| `GENERATE_LIMIT` | `all` | Content roots an index pass rescans at most; a number of 1 or more spreads the rescan after a Syft upgrade over days |
| `WAREHOUSE` | `on` | Whether an index pass builds the warehouse and publishes a snapshot, and the export runs; `off` for a host without the disk (below) |
| `EXPORT_EVERY_SLICES` | `672` | Slices between Parquet exports into `data/export`: a week, and every seventh index pass |
| `CHATSBOM_DUCKDB_MEMORY_LIMIT` | `2GiB` | What DuckDB may hold in the warehouse, the snapshot and the export; it spills the rest to `data/` |
| `CHATSBOM_DUCKDB_THREADS` | `2` | The threads DuckDB runs: the container's two CPUs |
| `PRUNE_EVERY_SLICES` | `96` | Slices between retention passes |
| `PRUNE_KEEP` | `2` | Scans retained per repository |
| `DEPGRAPH_LIMIT` | `200` | Repositories a `depgraph` pass fetches graphs for |
| `DEPGRAPH_RATE` | `90` | Dependency-graph requests an hour, per token |
| `DEPGRAPH_INTERVAL_SECONDS` | `300` | Wait between `depgraph` passes |
| `CHATSBOM_DEPGRAPH_API` | `auto` | How the graph is fetched; `.env.example` says what each choice does |

Watch these two:

```bash
docker compose run --rm cli queue status
docker compose run --rm cli queue status --metrics
```

`chatsbom_queue_due` climbing steadily means the slice size or interval is
too low. `chatsbom_queue_failing` climbing means something is wrong that
backoff is hiding.

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

To check the sandbox on a real daemon, with `dind` up (`docker compose
--profile lock up -d dind`, healthy in `docker compose ps`) and one
`sbom lock` run done, which makes the `chatsbom-lock` network:

```bash
# `d` runs the Docker CLI in `lock`, against the nested daemon, over TLS.
d() { docker compose --profile lock run --rm -T --entrypoint docker lock "$@"; }

# A resolver's view: the network `sbom lock` runs it on, and the image.
# `clickhouse` must not resolve, 2375 must be closed everywhere, and
# 2376 must not answer without a client certificate.
d run --rm --network chatsbom-lock --entrypoint sh \
  composer:2.10@sha256:9715c7f69044da2a212a5fbde29ee7da24e364d426560ae6367b060236f847d7 -c '
  wget -q -T 5 -O- http://clickhouse:8123/ping || echo "clickhouse: unreachable"
  gw=$(ip route | awk "/default/ {print \$3}")
  for host in 172.17.0.1 "$gw" dind; do
    wget -q -T 5 -O- "http://$host:2375/version" || echo "$host:2375: closed"
    wget -q -T 5 --no-check-certificate -O- "https://$host:2376/version" \
      || echo "$host:2376: no answer without a client certificate"
  done'

# The same from `lock`, with TLS but without its certificate: the
# daemon ends the handshake (certificate required), and 2375 is closed.
docker compose --profile lock run --rm -T --entrypoint docker \
  -e DOCKER_TLS_VERIFY= -e DOCKER_CERT_PATH=/nowhere lock \
  --tls -H tcp://dind:2376 version
docker compose --profile lock run --rm -T --entrypoint docker \
  -e DOCKER_TLS_VERIFY= -e DOCKER_CERT_PATH=/nowhere lock \
  -H tcp://dind:2375 version

# Nothing else reaches the daemon: `dind` does not resolve from `web`.
docker compose exec web python -c "import socket
try: socket.getaddrinfo('dind', None); print('dind: REACHABLE')
except OSError: print('dind: unreachable')"

# After any run, interrupted or not, no resolver container is left.
d ps -a --filter name=chatsbom-lock-
```

A hung resolver is removed at the deadline: a Gemfile is Ruby, so
`sleep 3600` in one hangs `bundle lock`. Try it in a scratch checkout
(`git worktree add ../lockcheck`), as a compose project of its own, so
that nothing else is resolved and nothing is left behind:

```bash
cd ../lockcheck
printf 'UID=%s\nGID=%s\n' "$(id -u)" "$(id -g)" > .env
root=data/06-github-content/9/0000000000000000000000000000000000000000
mkdir -p .cache "$root" && echo 'sleep 3600' > "$root/Gemfile"
docker compose -p lockcheck --profile lock run --rm lock sbom lock --timeout 20
docker compose -p lockcheck --profile lock run --rm -T --entrypoint docker lock \
  ps -a --filter name=chatsbom-lock-
docker compose -p lockcheck --profile lock down -v
```

The log says `timed out after 20s` about 20 s in, and `ps -a` lists
nothing. Ctrl-C during the same run ends it at once, with the same
empty list.

### The warehouse, the snapshots and the export

Each index pass ends with what the web service, `web`, serves
(#128 §2.3 and §2.4): `warehouse build` makes `data/warehouse.duckdb`
from the store alone, and `snapshot build` publishes a snapshot of it
in `data/snapshots`, but only when what it serves has changed; on most
days neither `CURRENT` nor a snapshot is touched. Every seventh index
pass, a week at the defaults, is followed by the public Parquet export,
`export parquet --output data/export`. Each is a step
as the others are: one that fails is said in the log and stepped over,
and the next pass tries again. `WAREHOUSE=off` in `.env` turns all
three off.

**What they take**, at the documented shape (19.4M observations, 16.1M
facts, 60,000 repositories), each step bounded as the collector's
container bounds it, 4 GiB and two CPUs, with DuckDB's defaults above:

| Step | When | Time | Peak memory | Disk |
| --- | --- | ---: | ---: | --- |
| `warehouse build` | each index pass | 13 min: 11 reading the store, 2 deriving | 2.4 GB | 0.63 GB; twice that while the next is written, and up to 1.9 GB spilled |
| `snapshot build` | each index pass | 3.2 min, whether it publishes or not | 2.9 GB | 1.75 GB a snapshot: 5.3 GB for the three kept, 7 GB while the next is written, and 0.5 GB spilled |
| export | each week | 41 s | 2.4 GB | 0.1 GB, and 0.5 GB spilled |

So an index pass grows by about 16 minutes, one interval's worth of
slices, and the weekly one by 17. Nothing was killed for memory, the
cgroup giving back page cache instead. On disk the three take about
6 GB between passes, and up to 8.5 GB during one: leave 10 GB free for
them, beside the store. None of it is backed up, since the store makes
all of it again. The read was measured at 1/20 of the corpus, 34 s, and
it grows with it; the rest at full scale.

**One export is kept.** `data/export` is exported into again: its files
are named by their content, a table that has not changed keeps its
file, and once the new manifest names the new ones, the last export's
go. So it is one export, the one to publish whole, as a release's
assets or as files the site serves, which is the owner's to decide.
The warehouse can make it again at any time, and a published copy is
the archive of past weeks.

**DuckDB spills on the data volume,** into a directory of the process's
own beside the warehouse, in `data/`, and removes it when the process
closes the file. A step stopped mid-spill, by the loop's stop or by the
container's memory limit, leaves its directory; the next `warehouse
build` removes it, once no process has the warehouse open. Nothing is
fetched from the network at run time: what DuckDB needs is in its
wheel.

**`web` reads them as uid 10003**, through a read-only mount of
`data/snapshots`: neither the collector's `UID` nor in its group. So
`data/snapshots` is anyone's to list and enter, `CURRENT` anyone's to
read, and each snapshot anyone's to read and no one's to write,
whatever umask made them. The warehouse and the export follow the
umask, as the rest of `data/` does: they are not `web`'s.

Before the first pass:

1. **The disk.** `df -h data` should leave 10 GB beyond what the store
   grows into. Otherwise set `WAREHOUSE=off`.
2. **`data/snapshots`**, for `web` to start before anything is
   published: it does not start without the directory. Make it as you
   made `data/`, `mkdir -p data/snapshots`, whatever your umask: the
   first pass opens it to all.
3. **The image.** The collector's carries pyarrow now, for the export:
   rebuild it, `docker compose --profile collect up -d --build`.
4. **When.** The first index pass comes `INDEX_EVERY_SLICES` slices
   after the collector starts, a day at the defaults, and the first
   export `EXPORT_EVERY_SLICES` after it. Both count from the
   container's start, so a restart starts them again: a collector
   restarted more often than weekly never exports. To have them sooner,
   or at any time, run them by hand in the same image and mounts (`cli`
   takes the defaults for DuckDB's limits, not `.env`'s):

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
the loop with its index pass and the export, the steps before them
stood in for: the warehouse in 37 s, a snapshot published, and the
export, with nothing spilled left. `data/snapshots` came out `0755`,
`CURRENT` `0644` and the snapshot `0444`, and `web`, uid 10003, was
healthy and read it through its read-only mount, which refused a
write. Before this change, the same steps stopped at their first
connection to DuckDB as a uid with no home to write, as the
collector's is (its `HOME` is `/`): DuckDB looked there for what the
time zone needs.

### With systemd instead

For a dedicated server rather than a dev machine, `deploy/systemd/` has
units for the same two schedules, with `ProtectSystem=strict`,
`ProtectHome=read-only`, and `ReadWritePaths` limited to `data/`,
`.cache/` and `.requests-cache/`. They are user units, and templates:
the instance is the checkout's path, so nothing in them names a
directory and they run wherever the checkout is. They start the
checkout's own `.venv/bin/chatsbom` — `uv run` cannot start with a
read-only home — so `uv sync` has to have made it first:

```bash
cd ~/ChatSBOM                    # the checkout, wherever it is
uv sync --frozen --no-dev        # makes .venv, without the extras
[ -e .env ] || cp .env.example .env    # then set GITHUB_TOKEN in it
mkdir -p ~/.config/systemd/user
cp deploy/systemd/* ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now \
    "$(systemd-escape --template=chatsbom-sync@.timer --path "$PWD")" \
    "$(systemd-escape --template=chatsbom-prune@.timer --path "$PWD")"
loginctl enable-linger "$USER"   # so they run with nobody logged in
```

For a checkout in `/home/alice/ChatSBOM` those are
`chatsbom-sync@home-alice-ChatSBOM.timer` and its `chatsbom-prune@`
twin; `journalctl --user -u 'chatsbom-*'` has what they did. Copy the
units again after a pull that changes them, then `daemon-reload`.

The sync unit holds `data/.sync.lock` while a slice runs, so a slice run
by hand takes the same lock and cannot overlap one the timer started:

```bash
flock --nonblock data/.sync.lock .venv/bin/chatsbom queue sync --slice 500 --quota 250
```

### Comparing the derived due set with the ledger

`chatsbom queue due --compare` (#100) says what is due when it is
derived from the store rather than read from the ledger, and why the two
differ (README, "`queue due`"). It reads and never writes: the ledger is
opened read-only and left byte for byte as it was, WAL included, and
nothing is made beside it. So it runs beside the collector, and across a
redeploy of it, without stopping anything.

It still reads the store, a few files for every repository, so on the
host give it the lowest priority there is, and take the corpus a shard
at a time (`--shard K/N`, the repositories whose id is K modulo N; the
same sample run after run). From the checkout's own environment (`uv
sync --frozen --no-dev` makes `.venv`, as for systemd above):

```bash
cd ~/ChatSBOM
# One sixteenth of the corpus: a few seconds.
ionice -c3 nice -n19 .venv/bin/chatsbom queue due --compare \
    --shard 0/16 --syft-version 1.52.0 --json ~/due-0-of-16.json

# The whole corpus, a sixteenth at a time.
for k in $(seq 0 15); do
    ionice -c3 nice -n19 .venv/bin/chatsbom queue due --compare \
        --shard "$k/16" --syft-version 1.52.0 --json ~/due-"$k"-of-16.json
done
```

`--syft-version` names the Syft the collector's image runs (the
`SYFT_VERSION` of `Dockerfile`): an SBOM is current only if that Syft
wrote it, and the host's own Syft, if it has one, may be another. `-c3`
is the idle I/O class, served only when no one else wants the disk; the
report says how long each part took, 35 s for all 65,000 repositories of
a synthetic corpus with the cache warm, so a shard at a time keeps each
run short. Add `--rediscover` to see how many content roots a stage
version bump would re-fetch for nothing (`selection-unchanged`); it
reads each such tree whole, so keep it to a shard. `--inventory` counts
the scans nothing points to any more.

The same runs in the collector's image, whose Syft needs no naming,
where the host has no checkout environment; there `nice` and `ionice`
are the image's, in front of the command:

```bash
docker compose --profile tools run --rm --entrypoint nice cli \
    -n 19 ionice -c 3 chatsbom queue due --compare --shard 0/16
```

---

## Moving `data/` to the repository-keyed layout (once)

`data migrate-layout` (#55 §7) moves every stage artefact from
`<stage>/<lang>/<owner>/<repo>/<ref>/<sha>` to `<stage>/<id>/<sha>` with
`rename(2)`: nothing is copied, fetched or deleted, and `data/` and
`.cache/` must be on one filesystem (they are: `/mnt/hdd-tank`). The code
and the layout change together, so collection stops for the window.
Measured on the corpus of 2026-09-28: about 179,000 renames (38.5 GiB,
not copied), 14 identical duplicates set aside, 24,946 `meta.json`
written for the legacy graphs, 0 conflicts. Plan an hour; two with the
equivalence check.

Run everything from the checkout, on the host, with the new code
(`uv sync` after checking it out). `W=data/_migration` below; every
command takes `--workdir` if it should be elsewhere.

1. **Freeze writers**, in compose and on the host (a host-side
   `chatsbom run --stage depgraph` loop counts too), and any timer that
   starts one (`systemctl list-timers 'chatsbom*'`).
   ```bash
   docker compose --profile collect --profile lock stop
   pkill -f 'chatsbom run'                   # a host-side worker, if any
   pgrep -af 'bin/chatsbom'                  # must print nothing
   ```
   The web keeps serving its snapshot.
2. **Snapshot.** `--apply` backs the ledger up itself, into
   `$W/ledger.pre.sqlite3`; the lists and ClickHouse are yours:
   ```bash
   mkdir -p data/_migration
   (cd data && tar czf _migration/lists.pre.tar.gz */*.jsonl)
   for t in raw_documents artifacts repositories; do
     docker compose exec clickhouse clickhouse-client -u admin --password admin \
       -q "ALTER TABLE chatsbom.$t FREEZE WITH NAME 'pre_layout'"
   done
   ```
3. **Inventory** (about 3 minutes): every file's size and mtime, and a
   sha256 for a 1% sample.
   ```bash
   uv run chatsbom data migrate-layout --inventory
   ```
4. **Dry run** (about 2–3 minutes; writes only `$W/plan.tsv` and
   `$W/dry-run.json`). It must say `Conflicts: none`, and every
   `raw_documents` row must be `found`. A name two ids have worn is
   settled by the one more lists recorded, and printed; check it.
   ```bash
   uv run chatsbom data migrate-layout
   ```
5. **Apply** (estimate 10–25 minutes on the HDD): the renames, journaled
   (`$W/journal.tsv`, fsynced before each batch); then the
   `raw_documents` path rewrite (old paths kept in
   `raw_documents_layout_backup`); then the ledger (`stage_state` from the
   watermarks; `github_language` from the newest metadata where empty).
   Interrupted, it resumes: run the same command again.
   ```bash
   uv run chatsbom data migrate-layout --apply
   ```
6. **Verify** — files and bytes per root against `pre.tsv`, the sample's
   hashes where they went, every destination there and no source,
   `raw_documents` rows per kind unchanged and every path a file, every
   watermark adopted — and the transform equivalence check: the new code
   indexes the rewritten landing zone into a scratch database, and every
   repository's current artifacts per source must match production.
   ```bash
   uv run chatsbom data migrate-layout --prepare-scratch chatsbom_migration_check
   CLICKHOUSE_DB=chatsbom_migration_check uv run chatsbom db index --rebuild
   uv run chatsbom data migrate-layout --verify --scratch-db chatsbom_migration_check
   ```
7. **Swap in**, and drop the scratch database:
   ```bash
   uv run chatsbom db index --rebuild
   uv run python scripts/verify_rollups.py
   docker compose exec clickhouse clickhouse-client -u admin --password admin \
     -q 'DROP DATABASE chatsbom_migration_check'
   ```
8. **Restart collection** on the new image:
   ```bash
   docker compose --profile collect up -d --build
   ```

**Rollback**, at any point before collection restarts:

```bash
uv run chatsbom data migrate-layout --rollback   # files, raw_documents paths, ledger
git checkout <the commit before this change> && uv sync
uv run chatsbom data migrate-layout --inventory --workdir data/_migration/after-rollback
```

It replays the journal backwards (every rename undone, every directory
it removed made again, every `meta.json` it wrote deleted), restores the
`raw_documents` paths from `raw_documents_layout_backup`, and puts
`ledger.pre.sqlite3` back; it is safe to run twice. The last command's
per-root totals must equal `pre.tsv`'s. If the paths cannot be restored,
the `pre_layout` FREEZE is the last resort: copy its parts from
`database/data/shadow/pre_layout/` into the table's `detached/` and
`ALTER TABLE … ATTACH PART` each.

**Later.** `--archive-lists` (planned with the dry run) also moves the
per-language `<lang>.jsonl` lists to `<stage>/_legacy-lists/` and
`all.jsonl` to `all-2026-03-09.jsonl`. Leave it until nothing reads them
(the stage-major `github`/`sbom` commands, `db index --from-files` and
`db raw`'s metadata overlay still do). `.cache/syft/_unversioned/`
(10.6 GiB, never read) can be deleted once the rollback window has
closed (owner decision D6).

## Deploying manifest discovery and the ledger-mastered index (PRs C and D of #55)

PR C (#62, merged) discovers manifests from the tree and bumps
`STAGE_VERSION` for content, lock and SBOM to 2. PR D makes `db index`
master on the ledger, adds the `manifest` source (Gradle build files and
version catalogs), judges direct/transitive per ecosystem, and adds four
columns to `repositories`. They are deployed together. No file moves, and
no migration beyond four additive `ALTER TABLE … ADD COLUMN`s that
`ensure_schema` makes.

**What becomes due.**

- *D, at once and with no API:* the next `db index` writes a row for
  every repository the ledger tracks: 60,080 today, against 28,078 rows
  now. The 32,008 new ones are the repositories seeded from the search
  snapshot, which have no record; each gets its dependency graph as the
  depgraph worker lands them. Existing Syft rows are rewritten with
  per-ecosystem verdicts, and `manifest` rows are added for whatever
  Gradle files the content roots already hold (few until C's content
  pass: the stored roots are root-only).
- *C, through `chatsbom run`:* every tracked repository is due for the
  content stage (version 2), and the 28,122 seeded with no language for
  the whole chain. Expect about 200 k `raw.githubusercontent.com` GETs
  for the stored trees (242,684 files after the cap, 46,425 stored) and
  about as many again for the seeded repositories once they have trees;
  about +9 GB in `06-github-content`; Syft re-run over every content root
  that changes.
- **The release stage.** No repository has a `release` row in
  `stage_state` (only `sbom`/`content` watermarks were ever backfilled),
  so `run` walks RELEASE for all 60 k. Before PR F each tag without a
  release cost one `/commits/{sha}` call (a mean of 47.4 a repository,
  about 2.8 M core calls) and `run --quota` did not count them. Deploy
  C and D with PR F, which dates tags with `git` and counts what the
  stage sends (see the next section).

**Runbook.** From the checkout on the host, `uv sync` after pulling.

1. **Stop the writers** that run old code: the host depgraph worker
   (`pkill -TERM -f 'chatsbom run --stage depgraph'`; it finishes the
   fetch in flight) and the compose services if they run
   (`docker compose --profile collect stop`).
2. **Snapshot** what D rewrites, as hard links:
   ```bash
   for t in artifacts repositories; do
     docker compose exec clickhouse clickhouse-client -u admin --password admin \
       -q "ALTER TABLE chatsbom.$t FREEZE WITH NAME 'pre_prd'"
   done
   sqlite3 data/ledger.sqlite3 ".backup data/_migration/ledger.pre-prd.sqlite3"
   ```
3. **Update:** `git pull && uv sync` (and `docker compose build` for the
   containers).
4. **Land and index** (no API). `db raw` lands the graphs the depgraph
   worker kept since the last pass; `db index` adds the columns and
   indexes every tracked repository. Measured read-only against
   production (below): about 30 minutes for the ingest.
   ```bash
   uv run chatsbom db raw --apply
   uv run chatsbom db index
   uv run python scripts/verify_rollups.py       # all checks agree
   ```
5. **Verify.**
   ```sql
   -- one row per tracked repository (the ledger's count)
   SELECT count() FROM chatsbom.repositories FINAL;
   -- the three sources
   SELECT source, count(), uniqExact(repository_id)
   FROM chatsbom.current_artifacts GROUP BY source;
   -- manifest rows are declared versions only
   SELECT version_kind, count() FROM chatsbom.artifacts
   WHERE source = 'manifest' GROUP BY version_kind;   -- constraint | unversioned
   ```
6. **Pilot C on the named repositories** (costs API, see above), then
   index them:
   ```bash
   mkdir -p data/_pilot
   printf '%s\n' jeecgboot/JeecgBoot halo-dev/halo \
     Stirling-Tools/Stirling-PDF appsmithorg/appsmith > data/_pilot/named.txt
   uv run chatsbom run --repos-file data/_pilot/named.txt --limit 4
   uv run chatsbom run --stage depgraph --repos-file data/_pilot/named.txt
   uv run chatsbom db raw --apply --repos-file data/_pilot/named.txt
   uv run chatsbom db index --repos-file data/_pilot/named.txt
   ```
   Each must then have a Spring Boot web starter in `current_artifacts`:
   ```sql
   SELECT r.owner, r.repo, a.source, a.name
   FROM chatsbom.current_artifacts AS a
   JOIN (SELECT id, owner, repo FROM chatsbom.repositories FINAL) AS r
     ON r.id = a.repository_id
   WHERE match(a.name, '(^|:)spring-boot-starter-(web|webflux|webmvc)$')
   ORDER BY r.repo, a.source;
   ```
   Expected (the scratch run in PR D): JeecgBoot from `syft` and
   `github-depgraph` (`-web`); appsmith from `syft` and `github-depgraph`
   (`-webflux`); halo from `manifest` only (`-webflux`, `api/build.gradle`);
   Stirling-PDF from `manifest` only (`-web`, `app/common/build.gradle`).
7. **Restart** the depgraph worker as before, and the collector. With
   PR F its `--quota` bounds the release stage too.

**What changes on the dashboard.** The live dashboard reads ClickHouse,
so it changes at step 4: the corpus is every tracked repository, so
coverage ratios fall (the denominator grows from 28 k to 60 k, which is
the honest one), languages outside the old eight appear, and totals
include `manifest` rows, which the source chart (Syft vs dependency
graph) does not show until PR E (below).

**Rollback.** Before collection restarts, or after:

```bash
git checkout <the commit before this change> && uv sync
```
```sql
-- rows only D writes
ALTER TABLE chatsbom.artifacts DELETE WHERE source = 'manifest';
DELETE FROM chatsbom.repositories
WHERE id NOT IN (SELECT DISTINCT repository_id FROM chatsbom.raw_documents
                 WHERE kind = 'repo');
-- optional: the columns are additive and the old code ignores them
ALTER TABLE chatsbom.repositories DROP COLUMN ecosystems,
  DROP COLUMN github_language, DROP COLUMN depgraph_ref,
  DROP COLUMN depgraph_commit_sha;
```
then `uv run chatsbom db index` with the old code, which rewrites the
Syft rows with its language-keyed verdicts. The `manifest` rows must go
first: the old `current_artifacts` would count them as current, since
they carry the scan's commit. The `pre_prd` FREEZE is the last resort
(its parts into `detached/`, then `ALTER TABLE … ATTACH PART`). C's
ledger rows need nothing: a `stage_state` row at version 2 is not due
for code at version 1. Restore `ledger.pre-prd.sqlite3` only to forget
what C's walk recorded.

## Deploying rollups by ecosystem (PR E of #55)

Deploy with C and D, or after them: it reads the `repositories`
columns D adds (`github_language`, `ecosystems`) and adds one of its own,
`snapshot`. No API calls.

**What changes.**

- *The corpus is the current search snapshot* (owner decision D2): the
  newest `all-*` snapshot the ledger records. Every current-state reader
  — rollups, dashboard, D1 and Parquet exports, `db query`, `db status`
  — counts only its repositories. Today that is 60,017 of the 60,080
  `repositories` rows; the other 63 (tracked, but in no snapshot, or
  indexed from a record the ledger does not track) keep their rows and
  are not counted.
- *Rollups are keyed by ecosystem.* `mv_package_language` and
  `mv_language_totals` are dropped by `ensure_schema`;
  `mv_package_ecosystem`, `mv_ecosystem_totals` and
  `mv_ecosystem_coverage` replace them. `mv_top_packages` is keyed
  `(ecosystem, direct_only, rank)`; `mv_totals` gains `tracked`.
- *GitHub's language is folded* to the top twelve, `other` and `none`
  (D7), by the `language_buckets` view.
- *D1 export schema 8.* `agg_*` keyed by ecosystem, a new
  `agg_ecosystem_coverage`, `repositories.github_language`,
  `language_bucket` and `ecosystems`.

**Runbook.**

1. `git pull && uv sync`.
2. `uv run chatsbom db index` (not `--rebuild`): it stamps `snapshot`
   on every row, and `ensure_schema` declares the views, the
   dictionary and the new rollups and drops the two language rollups.
   Measured on a scratch copy of production: 26 minutes for 60,080
   repositories.
3. `uv run python scripts/verify_rollups.py` — 23 checks, all agree on
   the scratch copy — and `uv run chatsbom db status`.

**Rollback.** `git checkout <previous> && uv sync`, `db index`, and the
previous web build. `ensure_schema` recreates the language rollups; the
`snapshot` column is additive and ignored by the old code.

## Search refresh and git-dated tags (PR F of #55)

PR F makes an unfiltered search a dated snapshot, dates release tags
with `git`, and bumps `STAGE_VERSION[release]` to 2. Nothing moves and no
table changes.

**What becomes due.** Every repository's release stage (version 2).
Commit follows only where the tag chosen changes, and tree, content and
SBOM only where the commit does.

**Cost.** Measured on 20 real repositories (16 sampled from the corpus
plus gradle, linux, laravel, WebGoat): the release stage sent 1.55 REST
requests a repository where the old code would have sent 110.5 (1.25
against 39.2 on the 16 sampled), and git and the API agreed on all 20
tag dates checked. Over 60,080 repositories that is about 70–75 k core
requests (release pages, 1.16 a repository in the corpus) instead of
about 2.9 M: about 18 hours at 4,000 an hour. A repository takes about
3–4 s (API pages, `ls-remote`, a 1.5–2.5 s tag fetch), so one `run`
process needs about 55 hours; three or four in parallel are paced by
the token instead.

**Runbook.**

1. **Refresh the search** (search API, 30 a minute; about 700 requests
   for 65 k repositories, about 25 minutes). It writes
   `data/01-github-search/all-<today, UTC>.jsonl`; re-running it the
   same day resumes it.
   ```bash
   GITHUB_TOKEN=$(gh auth token) uv run chatsbom github search --min-stars 1000
   ```
2. **Seed the queue from it.** Stars, GitHub language and default branch
   come from the new snapshot; a new repository also gets its push.
   Repositories only older unfiltered snapshots list get `snapshot = ''`
   (kept, not deleted: owner decision D2). A snapshot that would unlist
   more than a quarter of what it lists is taken for a search cut short,
   and unlists nothing.
   ```bash
   uv run chatsbom queue track --snapshot data/01-github-search/all-<date>.jsonl
   ```
3. **Collect.** `run --quota` now bounds the release stage:
   ```bash
   GITHUB_TOKEN=$(gh auth token) uv run chatsbom run --stage release --limit 5000 --quota 4000
   ```

## When the daemon cannot grant ClickHouse 262,144 open files

`docker-compose.yaml` asks for 262,144 open files for `clickhouse`,
soft and hard, as ClickHouse's own `docker run` examples do. The server
raises its soft limit to the hard one as it starts, and a merge or a
query over many parts opens many files at once; running out fails it
with `Too many open files`. A daemon that may not raise a container's
hard limit above its own cannot start the container at all: `up` stops
with `error setting rlimit type 7: operation not permitted`. A rootless
daemon, bounded by its user's limit, is one; so was the daemon #118 ran
on, whose hard limit was 20,000.

There, give ClickHouse what the daemon has, in an override file, which
compose reads beside `docker-compose.yaml` and git ignores:

```yaml
# docker-compose.override.yaml
services:
  clickhouse:
    ulimits:
      nofile:
        soft: 20000
        hard: 20000
```

Compose reads it by itself only while `COMPOSE_FILE` is unset. In the
tunnel mode, which sets it, name the override last:
`COMPOSE_FILE=docker-compose.yaml:docker-compose.tunnel.yaml:docker-compose.override.yaml`.

The daemon's limit is what a container that asks for none gets:

```bash
docker run --rm --entrypoint sh "$(docker compose config --images clickhouse)" -c 'ulimit -Hn'
```

README's `docker run`, and the CLI's own when it finds no server, take
`--ulimit nofile=20000:20000` there instead.

The default stays 262,144 rather than the lowest a daemon has been met
with: lowered for everyone, every deployment would be held to what one
kind of daemon grants, and whether 20,000 is enough for this corpus's
merges has not been measured. The override is one machine's.

## ClickHouse 25.12 to 26.8 (once)

compose runs ClickHouse 26.8, the long-term support release, where it
ran 25.12, which is out of security support. The upgrade is `up` on the
new image, on the same `database/data`. Four things first:

- **It is one-way.** 25.12 does not start on a data directory 26.8 has
  run on, and, with the renamed logs below dropped, detaches every part
  26.8 wrote as `broken-on-start`. The way back is a copy, taken first.
- **The host needs AVX2.** From 26.6 the amd64 build targets x86-64-v3:
  Intel Haswell, AMD Excavator or later. `grep -c avx2 /proc/cpuinfo`
  prints 0 on a host without it.
- **The first start renames the server's own logs.** Each `system.*_log`
  table becomes `*_log_0` (the next free number, if that is taken)
  beside a new one. They hold 25.12's logs only; drop them when nothing
  in them is wanted.
- **`async_insert` stays off.** 26.3 made it the default: each INSERT
  waits in a buffer for the server to flush it, which on 26.8 took a
  small insert from 5 ms to 61 ms, and `db index` and the collector send
  many. `database/config/users.d/admin.xml` turns it off for admin, the
  account that writes, and comes with the pull.

```bash
docker compose --profile '*' stop                     # everything
sudo cp -a database/data ../clickhouse-data-25.12     # the way back
git pull
docker compose up -d --wait clickhouse                # 26.8, on the same data
docker compose exec clickhouse clickhouse-client -u admin --password admin \
  -q "SELECT version(), getSetting('async_insert')"   # 26.8.…, false
docker compose up -d                                  # the dashboard
docker compose --profile collect up -d                # if it ran
```

Then, once nothing in them is wanted, the old logs:

```bash
docker compose exec clickhouse clickhouse-client -u admin --password admin \
  -q "SELECT name FROM system.tables
      WHERE database = 'system' AND match(name, '_log_[0-9]+$')"
docker compose exec clickhouse clickhouse-client -u admin --password admin \
  -q 'DROP TABLE system.query_log_0'                  # and so on, each listed
```

**Rollback:** `docker compose --profile '*' stop`, put
`../clickhouse-data-25.12` back as `database/data`, then `git checkout
<the commit before this change>` and `up`. What was written under 26.8
is lost with it.

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
(`is_current_sbom`). So the new Syft regenerates every stored SBOM,
once, and none of the old cache is used for it.

- **The collector loop** regenerates them in its first index pass with
  the new image (`docker compose --profile collect up -d --build`),
  within a day of deploying it: `INDEX_EVERY_SLICES` counts from the
  container's start. `sbom generate` runs first, and says why in
  `docker compose logs collector` before it starts, `N SBOM(s) were not
  written by Syft 1.52.0 and will be regenerated`. The same pass lands
  and indexes what it wrote (`db raw --apply`, `db index`): each SBOM is
  a new file, newer than what `db raw` stored of it.
- **How long.** The new version's cache starts empty, so nearly every
  root is a fresh scan, and a scan is about 1.6 CPU seconds whatever
  the root holds. On the collector's two CPUs, with `sbom generate`'s
  5 workers, that measured 1.1 roots a second: about seven hours for
  28,000 roots, with no slice meanwhile. `GENERATE_LIMIT` spreads it
  over days instead (4,000 is about an hour a pass); each pass takes up
  where the last stopped.
- **Sooner, or without the loop** (the systemd units run no index
  pass), run it by hand once after deploying. The `cli` service has no
  CPU limit: on 4 CPUs it measured 1.6 roots a second, about five hours
  for 28,000. Beside the loop's own pass it only duplicates work: each
  SBOM is written whole.
  ```bash
  uv run chatsbom sbom generate                             # a checkout
  docker compose --profile tools run --rm cli sbom generate # compose
  ```
- **`chatsbom run`** regenerates the SBOM of each repository it walks,
  but walks only those due for another reason, such as a push or a
  stage version that moved: the ledger does not know which Syft wrote
  an SBOM. On its own it would leave the 41% of repositories not pushed
  in a year on the old Syft for as long, which is why the index pass
  runs `sbom generate`.

When `syft version` fails, or says nothing that reads as a version, no
SBOM is regenerated for the Syft that wrote it: the times alone decide,
as before, and a warning says so. Once nothing will go back to the old
Syft, its cache can go: `rm -rf .cache/syft/1.41.2`.

## Why there is no message broker

The work ledger is already the queue, and it is a better fit than a
broker would be. Its items are **durable per-repository state** — the
ETag we hold, how far each stage has got, how many times it has failed —
not messages. A broker gives at-least-once delivery of ephemeral tasks;
lose the message and you lose that unit of work. The ledger loses nothing
to a killed process, because progress is a watermark rather than an
in-flight message, and claims are leased so they expire rather than
stranding.

A broker earns its place when there are many independent producers and
tasks that are cheap to retry from scratch. Here there is one producer
(the clock) and the work is expensive and idempotent per repository,
which is exactly the shape a ledger serves and a queue does not.
