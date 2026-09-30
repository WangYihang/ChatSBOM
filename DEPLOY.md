# Deployment

Two serving models. Both keep collection where Syft and Docker are,
because Cloudflare Workers has neither.

## Local ClickHouse behind a tunnel — the current one

Everything runs on one machine and `cloudflared` carries requests to the
Worker. The dashboard reads the live database, so `db index` takes
effect immediately and there is no snapshot to keep in step.

The Worker serves the page its image was built with. The page this
tree builds asks the Python service instead, and no longer the Worker
(#144), so a `web` built from it fails every panel: until the cutover,
a deployed `web` keeps the image it has ("The Python service, on a
second hostname", below).

```
one machine                                      the internet
┌───────────────────────────────────────┐
│ collector      github · syft · index  │
│        ↓                              │
│ ClickHouse     19,361,638 rows        │
│        ↑ 127.0.0.1:8123 only          │
│ Worker         wrangler dev :8787     │
│                static assets          │
│        ↑ edge, internal; no host port │
│ cloudflared    dials out              │──► Cloudflare ──► visitors
└───────────────────────────────────────┘
```

ClickHouse is bound to the loopback interface and is **not** on the
tunnel; only the Worker is. The account the Worker connects as is
read-only with server-enforced ceilings — 30 s, 4 GB, 2e9 rows read, 16
concurrent — so a query that gets through and is expensive fails as a
query rather than as a server.

Under compose the spend counter behind `DAILY_SPEND_CAP_USD`, a Durable
Object that `wrangler dev` runs locally, is kept in the `web-state`
volume, so recreating the container does not reset the day's cap, and
secrets reach the Worker through a mode-0600 `.dev.vars` written at
start rather than on its command line.

The tunnel runs one of two ways: as a service of this compose project,
with the Worker's port published nowhere, as the picture shows; or
outside the project, reaching a port the Worker publishes.

### The tunnel as a compose service

`cloudflared` runs beside the Worker as the `cloudflared` service, and
the Worker's port is published nowhere. Nothing off this machine
reaches the Worker except through the tunnel, so the `CF-Connecting-IP`
it hands on, which both rate limiters key on, is the one Cloudflare's
edge wrote.

- **`edge`**, an internal network, holds `cloudflared` and `web`, and
  `site` when its profile is on (below), and nothing else. It is
  `isolated` as well, so its bridge has no address on the host. Without
  that the host is on every internal network, reaches `web` from an
  address inside this one, and the containers reach whatever the host
  serves on all its interfaces. `isolated` needs Docker Engine 28.0 or
  newer. Its subnet is its own, `172.16.128.0/24`, which `site` is told.
- **`cloudflared-egress`** is the tunnel's way out, to Cloudflare, and
  nobody else's. `web` keeps `default`, for ClickHouse and for the
  model API, and `site` is on it for DeepSeek's.
- **The tunnel's metrics** are served on its own loopback, where its
  healthcheck asks `/ready`: 200 while a connection to Cloudflare's
  edge is up, 503 while none is. On every interface, as the image
  serves them, they would be open to `web` and to the way out, the
  tunnel's routes at `/config` among them.

It is a remotely managed tunnel: its routes live in the Cloudflare
dashboard, and the container needs only its token.

1. In the Cloudflare dashboard, open **Networking → Tunnels**, select
   **Create a tunnel**, and name it. For Docker, the install command it
   shows ends in `--token` and a long value: that value is the token.
   Anyone who has it can serve your hostname, so keep it as you would a
   password.
2. On the tunnel's **Routes** tab, add a **published application**: the
   site's hostname, and the service URL `http://web:8787`. That is the
   compose service's name, which Docker's DNS answers on `edge`, and
   the port `wrangler dev` listens on. The Python service that is to
   replace the Worker is tried on a second hostname first (below). A
   hostname no rule names gets the catch-all at the end of the rules,
   `http_status:404`, which the dashboard keeps there.
3. In the `.env` beside `docker-compose.yaml`:

   ```bash
   COMPOSE_FILE=docker-compose.yaml:docker-compose.tunnel.yaml
   TUNNEL_TOKEN=eyJhIjoi...
   ```

   With a `docker-compose.override.yaml` of your own, add it to the
   list, last: with `COMPOSE_FILE` set, compose reads only the files it
   names.
4. From that directory, `docker compose up -d`.

Then check that the port is closed and that the tunnel is the way in:

```bash
# Both containers' own checks: the Worker's, and the tunnel's /ready.
# web lists no PORTS.
docker compose ps

# No host port. Compose says so, nothing answers on this machine's own
# addresses, and from another machine http://<this machine>:8787 is
# refused as well.
docker compose port web 8787       # no port 8787/tcp for container ...
curl -m 5 http://127.0.0.1:8787/   # Couldn't connect to server

# The routes the dashboard gave the tunnel: the hostname to
# http://web:8787, then http_status:404.
docker compose logs cloudflared | grep 'Updated to new configuration'

# The site, from outside.
./scripts/health.sh --no-local https://sbom.example.com
```

Without a token, `cloudflared` stops at once, saying `"cloudflared
tunnel run" requires the ID or name of the tunnel to run`, and the
restart policy tries again; with one that is not a token, it says
`Provided Tunnel token is not valid.` One that cannot reach Cloudflare
prints its connectivity pre-checks: the tunnel needs outbound UDP to
port 7844, for QUIC, or TCP to 7844, for HTTP/2.

**Why a file, and not the profile.** The tunnel mode is
`docker-compose.tunnel.yaml` on top of `docker-compose.yaml`, because a
compose profile can add a service but cannot take a port away from
another. The file makes two changes, both with compose's `!reset`:
`cloudflared` leaves its `tunnel` profile, so `up` starts it, and `web`
publishes no port, so `WEB_BIND` has nothing to bind. The default is
what it was: without the file, `web` publishes 8787 on every interface,
and a tunnel outside the project, below, reaches it as before. A
loopback-only default would have cut off every such tunnel that runs in
a container. `--profile tunnel` alone runs the tunnel beside that port,
where the address is only as good as `EDGE_SECRET` makes it.

Compose takes `COMPOSE_FILE` from the `.env` in the directory it runs
in. Run anywhere else, it finds `docker-compose.yaml` alone, and an
`up` there publishes the port again.

`EDGE_SECRET` and its Transform Rule are not needed here: only the
tunnel reaches the Worker, and every address it hands on is
Cloudflare's. Remove both, or keep both, but not one: the secret
without the rule puts every visitor in one rate-limit bucket.

What is left is this machine itself. `web` is on `default`, for
ClickHouse and the model API, and a process here reaches it at its
address there, as it reaches any container's, and says what it likes
in `CF-Connecting-IP`. Another machine cannot: there is no host port,
and Docker 28.0 and newer drop direct routing to a container's
unpublished ports.

Checked on Docker Engine 29.3.1 with the tunnel mode up, `web` played by
a stand-in and a well-formed token for no tunnel: `docker compose port
web 8787` answered `no port 8787/tcp`; 127.0.0.1:8787 and the host's
own address refused; the host had no route to `edge`; a container on
`edge` got the page from `http://web:8787` and was refused by the
tunnel's metrics port; a container on the tunnel's way out reached
neither of `web`'s addresses; and `/ready` answered 503, with
`"readyConnections":0`, until Docker marked the tunnel unhealthy.

### The Python service, on a second hostname

`site` is the Python web service that is to replace the Worker (#128):
`chatsbom web serve`, in the image `Dockerfile.site` builds, serving
the page, its reads of the dataset, the chat and `/healthz` on port
8080. Until the cutover it runs beside the Worker, and the tunnel
reaches it on a second hostname. The site's own hostname stays the
Worker's.

- **The image** is Python, the package with its `web` extra, and the
  page, which Node builds in a stage of its own: no Node,
  `node_modules` or uv. It runs as uid 10003, and checks itself from
  inside, asking `/healthz` with Python's urllib.
- **The container** has a read-only root, a 64 MB tmpfs on `/tmp`, no
  capabilities and `no-new-privileges`, 1 GB of memory and one CPU. It
  is on `edge`, where `cloudflared` reaches it, and on `default`, its
  way out to DeepSeek's API. It publishes no port, in either mode.
- **Its state**, `web.sqlite`, is in the `site-state` volume, and it
  reads the snapshots in `data/snapshots`, mounted read-only.
- **Its settings** come from `.env`, as `.env.example` describes them,
  empty when unset. It does not start without `ALTCHA_HMAC_KEY`, and
  without `DEEPSEEK_API_KEY` the chat is off. Compose sets three
  itself: `WEB_STATE_DIR` and `WEB_SNAPSHOT`, its two mounts, and
  `EDGE_SUBNET`, the edge's subnet.

To try it, in the tunnel mode:

1. Publish a snapshot into `data/snapshots` (README, "`chatsbom web`"),
   and put the service's settings in the `.env`:

   ```bash
   ALTCHA_HMAC_KEY=<the output of: openssl rand -hex 32>
   DEEPSEEK_API_KEY=<DeepSeek's key>
   DAILY_SPEND_CAP_USD=1
   ```

   Without `data/snapshots`, compose refuses to start the service,
   saying `bind source path does not exist`, rather than make the
   directory owned by root. With no snapshot published in it, the
   service says so in its log, and is restarted until there is one.
2. On the tunnel's **Routes** tab, add a second published application
   beside the site's: a hostname of its own, `next.sbom.example.com`
   say, and the service URL `http://site:8080`. The site's route stays
   `http://web:8787`, and the catch-all stays last.
3. From the directory the `.env` is in, `docker compose --profile site
   up -d`. The first `up` after this change makes `edge` again, with
   its subnet, and stops `web` and `cloudflared` for a moment to do it.

Then check it, from this machine and from outside:

```bash
# healthy: the image's own check, /healthz asked from inside.
docker compose ps site

# The routes the dashboard gave the tunnel: both hostnames, then
# http_status:404.
docker compose logs cloudflared | grep 'Updated to new configuration'

# The page, through the tunnel: 200.
curl -sS -o /dev/null -w '%{http_code}\n' https://next.sbom.example.com/

# /healthz, through the tunnel: 404. It answers a peer outside the
# edge's subnet alone, so a 404 says the service knows the tunnel for
# the edge. A 200 would mean EDGE_SUBNET is not the edge's subnet, and
# every visitor would share one rate limit, cloudflared's.
curl -sS -o /dev/null -w '%{http_code}\n' https://next.sbom.example.com/healthz
```

The cutover (#128, phase 4) is then a change of the site's route, to
`http://site:8080`, in the dashboard.

**Until then, `web` keeps the image it has.** The page this tree
builds asks the Python service (#144), and no longer the Worker, which
answers each of its reads with `index.html`: a `web` built from this
tree serves a page whose every panel fails. `up --build` builds each
service it starts, `web` among them, whatever profile it is given, so
name the ones it is to build: `docker compose --profile collect up -d
--build collector depgraph` builds the collector's image and nothing
else.

**The edge's subnet** is `172.16.128.0/24`, and `site` is told so. It
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

The network and `site` both take it, so the two cannot differ, and the
next `up` makes the network again. If `up` says `Pool overlaps with
other one on this address space`, another project's network holds the
range; `docker network inspect <network>` says a network's subnet, and
another /24 of `172.16.0.0/16` will do.

**Two caps.** The site keeps the day's spend in `web.sqlite`, and the
Worker in its own counter, each against `DAILY_SPEND_CAP_USD`. While
both answer questions, a day can cost up to twice the cap, across
DeepSeek's key and Anthropic's.

**Stopping it** sends SIGTERM: uvicorn takes no new request, answers
the ones in flight, and exits, with 143. Compose waits 30 s for that,
where Docker's default is 10, before a SIGKILL, which ends a question
in flight: its turn stays held against the day's cap.

Checked on Docker Engine 29.3.1 and Compose 5.1.1, with a snapshot of
the contract corpus and #143's stand-in for DeepSeek on `default`. The
image was 382 MB, 89 MB compressed, where the Worker's built on the
same machine was 1.85 GB, 449 MB compressed, and had no Node, npm, uv
or curl. The service ran as uid 10003 with no capability and
`NoNewPrivs`, could write `/tmp` and its volume and nothing else, had
its watchdog's thread running, and was healthy 6 s after it started.
`edge` was `172.16.128.0/24`, and the host had no route to it; an
`edge` made without a subnet was made again by the next `up`. From a
container on `edge`, the page and an asset answered 200 and `/healthz`
404, and a challenge was signed for the address `CF-Connecting-IP`
named; from one on `default`, `/healthz` answered 200, and a challenge
was signed for the container's own address, whatever the header said.
A question through the edge was answered, a tool call, the text and
`done`, and its two turns were settled in `web.sqlite`, which a
recreate kept. A stop while an answer streamed waited 13 s for it to
end, and exited 143; an idle one took 0.5 s. Without `data/snapshots`,
`up` failed on the mount and made nothing; with it empty, or without
`ALTCHA_HMAC_KEY`, the service said which setting in its log, and was
restarted.

### A tunnel outside the project

Without the tunnel mode the Worker's port is published, and more widely
than a tunnel needs. Under compose it is on every interface by default,
because a tunnel container reaches it through the host gateway rather
than loopback (README, "Putting it on the internet") — and every
interface includes the LAN. `WEB_BIND=172.17.0.1` publishes it on the
docker bridge alone. The image already switches off the worst of what a
direct client could reach there: wrangler's local explorer, which reads
and writes every binding, the spend counter included. Such a client
also sets its own
`CF-Connecting-IP`, which both rate limiters key on, and could claim a
new address — a new budget — on every request. Nothing in a request
tells the tunnel's from a direct client's, so let the edge vouch for its
own: set `EDGE_SECRET` to a random value (`openssl rand -hex 32`), and
give the site's hostname a request-header Transform Rule in the
Cloudflare dashboard that sets `X-Edge-Secret` to the same value. The
Worker then believes the address only on a request carrying it, and
every other request shares one bucket, whatever address it claims
(`web/src/ratelimit.ts`). The container's healthcheck and its watchdog
send the secret too, from their environment, never their command line,
so a client emptying that bucket cannot fail them and restart a working
Worker (#115). A quick tunnel's hostname is Cloudflare's, not yours, and
cannot have the rule; there, `WEB_BIND` is the protection.

`scripts/serve.sh` does the three steps: build, start the Worker, open
a quick tunnel. `wrangler dev` previews the **build**, not the sources,
so the build is not optional.

A quick tunnel is fine for looking at the thing and wrong for anything
you want to link to: the hostname is random per restart, and new ones
have taken minutes to become resolvable — the tunnel registers, the
name does not answer, and nothing in the log says which. For a stable
address, `scripts/tunnel-named.sh <hostname>` writes a named-tunnel
config whose ingress is the Worker's port and a `404` terminator, so an
unmatched hostname is refused rather than forwarded somewhere by
accident. It needs `cloudflared tunnel login` once, which is
interactive. That tunnel is locally managed, its routes in the file the
script writes, for a `cloudflared` on the host; the compose service
above runs a remotely managed one, whose routes are the dashboard's.

### Why the queries are fast enough to serve live

The dashboard's questions split in two, and only one half needed help.

**Point lookups** take an arbitrary package name, so nothing about them
can be precomputed. `artifacts` is sorted by `name` and stored at
`index_granularity = 1024`, so `WHERE name = ?` reads 9,216 rows of
19,361,638 — 1 to 3 ms. Repository metadata comes from a dictionary
rather than a join: 28,075 rows hashed into 9 MiB turned the dependants
query from 13.6 ms to 4.3 ms on `ms`.

**The overview** asks a fixed set of questions whose answers together
are under a million rows, and every one of them was a full scan. Twelve
refreshable materialized views do the distinct-counting once, at refresh
time, and the panels read plain integers:

| | before | after |
|---|---:|---:|
| dependencyDistribution | 251.9 ms | 0.5 ms |
| topPackages | 180.6 ms | 1.4 ms |
| licenseShares | 87.3 ms | 1.3 ms |
| sourceComparison | 82.4 ms | 0.8 ms |
| totals | 77.8 ms | 0.6 ms |
| languageCoverage | 20.6 ms | 0.5 ms |
| **21 queries, summed** | **836.0 ms** | **24.2 ms** |

A `PROJECTION` was tried first and is the wrong tool: rows read fell
from 19,361,638 to 624,543 and the time did not move, because the cost
is merging 624,543 `uniqExact` states rather than I/O. `uniq` would buy
the time back for half a percent of error, which is not a trade to make
on a page that prints "198 dependants".

The rollups are exact rather than approximate because no whole-corpus
distinct count is a sum of group counts. They used to be keyed by the
repository's language and summed across languages, which was exact only
while every repository had exactly one. They are keyed by ecosystem now
(#55 §4.12), and a repository has as many ecosystems as it has
manifests for, so each whole-corpus repository or package count is its
own `uniqExact` over the facts; only record counts, which do partition
by ecosystem, are summed. `scripts/verify_rollups.py` checks each one
against an independent computation.

Refreshing all twelve plus the dictionary takes 2.9 seconds, and
`db index` does it at the end of every run. `REFRESH EVERY 1 DAY` is a
backstop, not the mechanism.

## D1 — a snapshot at the edge

Still supported, and the right choice if you want no machine of your own
in the request path. `chatsbom export d1` writes the SQL; the current
corpus is 831 MB applied, which is over D1's 500 MB free tier and inside
the 10 GB paid one. The Worker picks whichever store is configured, so
the two differ by bindings alone.

```
your machine / a server              Cloudflare
┌──────────────────────────┐         ┌────────────────────────────┐
│ collector (container)    │         │ Worker                     │
│   github · syft · index  │         │   static assets  (the SPA) │
│           ↓              │  import │   /api/q    → D1           │
│ ClickHouse               │ ──────► │   /api/chat → Anthropic    │
│           ↓              │         │                            │
│ export d1   454 MB SQL   │         │ D1: the dataset, queried   │
└──────────────────────────┘         └────────────────────────────┘
```

A visitor downloads about 250 KB, fonts included, and every answer is
one Worker request. The overview's panels are precomputed at export
time into `agg_*` tables, which is D1's version of the rollups above —
SQLite cannot afford them live.

Nothing is served from R2. An earlier design shipped the dataset as
Parquet for a query engine in the browser — 28 MB on a first load — and
`chatsbom export parquet` still produces those files for anyone
consuming the dataset directly, but the Worker does not serve them and
the dashboard does not read them.

---

## 0. Before you start

| You need | Why |
| --- | --- |
| A Cloudflare account | Workers + D1 |
| `wrangler` logged in | `npx wrangler login` |
| A populated ClickHouse | `chatsbom db status` should report rows |
| An `ANTHROPIC_API_KEY` | **Only** for `/api/chat`; the dashboard works without it |

Costs to know about up front: the database is 831 MB at 16.8 million
artifact rows, over D1's 500 MB free tier and 8% of the paid plan's
10 GB, and Workers' free tier covers the dashboard. D1 bills for rows read, which is why the
overview's panels are precomputed — they would otherwise read 6,062,896
rows per visitor. The AI chat needs **paid
Workers** (CPU time) and bills per token to Anthropic, up to the daily
cap in `wrangler.jsonc`, which it does not pass (section 3).

---

## Running it locally first

```bash
cd web
npm install
npx wrangler d1 create chatsbom          # once
# put the returned database_id into wrangler.jsonc
for f in ../dist/d1/[0-9][0-9]-*.sql; do
  npx wrangler d1 execute chatsbom --local --file "$f" || break
done
npm run dev
```

`--local` keeps everything in `.wrangler/state`; nothing is uploaded.
The files are applied in the order of their names, which is the order
they must go in (section 2). Importing the data locally takes a while:
at 16.8 million artifact rows it is about 450 MB of SQL.

`npm run preview` serves the built output instead, which is what the
deploy runs.

If `/api/q` answers 503, the `DB` binding is missing from
`wrangler.jsonc`. If it answers but every number is zero, the data
script has not been applied.

---

## 1. Export the dataset

```bash
uv run chatsbom db edges                 # if it has not run since the last index
uv run chatsbom export d1 --output dist/d1
```

The package-to-package edges come from the `edges` table `db edges`
fills, the one the ClickHouse dashboard reads. An export with that
table empty stops and says so, before it writes anything: it would
ship a dashboard whose edge panels are empty.

`chatsbom export parquet` also exists. It is not part of deploying —
nothing serves it — but it produces a 20.6 MB self-describing copy of
the dataset that DuckDB or pandas can read directly, which is worth
attaching to a release. It needs pyarrow, the `export` extra, which a
checkout's `uv sync` installs and the collector's image does not; so
from a checkout, or `pip install 'chatsbom[export]'`, and not `run --rm
cli`.

```bash
uv run chatsbom export parquet --output dist/data
```

Expect roughly this, each file named after its table and the first
eight hex digits of its SHA-256. If the artifacts file is much smaller,
**stop** — that is the truncation bug. An export's queries go out with
every overflow mode set to `throw`, so a result cap on the connecting
account fails the export rather than cutting a table short, and the
export names the table it stopped in:

```
artifacts-<hash>.parquet      6,062,896 rows   16.7 MB
history-<hash>.parquet          141,938 rows    1.1 MB
licenses-<hash>.parquet             500 rows    6.8 kB
repositories-<hash>.parquet      28,075 rows    2.8 MB
                                             ─────────
total                                          20.6 MB
```

`manifest.json` is the one file whose name is fixed, and it names the
others: each one's size, SHA-256 and table, and the row counts. Keep it
with them. A table that changed is a new name, so an export into the
same directory removes the files the previous one wrote and the new
manifest does not name.

---

## 2. Create the D1 database and import

```bash
cd web
npx wrangler d1 create chatsbom
# put the returned database_id into wrangler.jsonc under d1_databases
```

Then apply every file, one `wrangler d1 execute` each, **in the order
of their names**. The order is not stylistic, and the names give it:

```bash
D=../dist/d1   # wherever `chatsbom export d1 --output` wrote them

for f in "$D"/[0-9][0-9]-*.sql; do
  npx wrangler d1 execute chatsbom --remote --file "$f" || break
done
```

The export prints this loop for its own directory.

- **Schema first**, and it drops before it creates: D1 keeps whatever a
  previous import left, so applying the data twice against existing
  tables doubles every row rather than replacing it.
- **The data next**, in parts of at most 50 MB: `02-<table>-0001.sql`
  onwards, a table at a time. One file of all of it came to about
  450 MB, and a failure anywhere in it meant the whole import again.
- **Aggregates after the data**, because they are computed *from* it.
  They are derived inside SQLite rather than by a second trip to
  ClickHouse, so they cannot disagree with the rows they describe.
- **Indexes last.** Inserting into an indexed table updates every index
  per row; building them once over finished data is markedly faster.

**Every file can be applied again.** When one fails, or times out
without saying whether it went through, run it again and carry on with
the files after it:

```bash
# resume from the file that failed, here 02-artifacts-0007.sql
ls "$D"/[0-9][0-9]-*.sql | sed -n '/02-artifacts-0007.sql/,$p' |
  while read -r f; do
    npx wrangler d1 execute chatsbom --remote --file "$f" || break
  done
```

A data part first removes the rows its table has from the part's own
first row on — what it, and any later part of that table, wrote — so
its rows go in once however often it runs, and the parts after it put
theirs back. `03-aggregates.sql` empties every table it fills before
filling it, and `04-indexes.sql` creates each index only if it is not
there.

### What you are importing

    01-schema.sql                 drops, then creates, every table
    02-agg_edges-0001.sql         the rows, a table at a time, in
    02-artifacts-0001.sql         parts of at most 50 MB: a table
    02-artifacts-0002.sql         larger than that, the artifacts
    …                             above all, takes several
    02-history-0001.sql
    02-kinds-0001.sql
    02-licenses-0001.sql
    02-meta-0001.sql
    02-observations-0001.sql
    02-packages-0001.sql
    02-repositories-0001.sql
    02-versions-0001.sql
    03-aggregates.sql             the overview's aggregates
    04-indexes.sql                the indexes

At 6,062,896 artifact rows the applied database was 294.7 MB, inside
D1's free tier (500 MB); at 16.8 million it is 831 MB, inside the paid
plan's 10 GB. It is that small because the artifact rows are
normalised: a direct translation of the Parquet schema measured
**762.6 MB** at six million rows with the same indexes. Most of the
saving is one table — the five low-cardinality columns take only 45
distinct combinations across six million rows, and were stored as five
strings on every one of them.

The data parts use batched multi-row INSERTs. `sqlite3 .dump` would
write one statement per row — 6,062,896 of them, against D1's 100,000
byte statement cap and over a network. The batches are measured in
bytes, not characters: a Chinese description is three bytes a
character, and batches of them had come out at 112–144 KB.

### Re-importing

The schema script drops and recreates, so a re-import replaces rather
than appends. There is no partial-update path: this is a snapshot of a
collection run, and a half-updated snapshot is worse than an old one.

A re-export into the same directory first removes the files the last
one wrote there, and so does an export that fails: the files are
applied by name, all of them, and a part left behind would be applied
with the new ones.

---

## 3. Optional — the AI chat

Skip this and the dashboard still works; `/api/chat` answers 503 and says
so.

```bash
npx wrangler secret put ANTHROPIC_API_KEY
```

The spend cap needs nothing created by hand: its counter is a Durable
Object that the deploy creates (below).

Two more, both worth doing before the URL is public:

```bash
# Turnstile: add a widget for your hostname in the Cloudflare dashboard
# (Turnstile → Add widget, Managed). It gives a site key and a secret.
npx wrangler secret put TURNSTILE_SECRET
# and the site key, which is public, into wrangler.jsonc under vars:
#   "TURNSTILE_SITE_KEY": "0x4AAAAAAA..."
```

Without `TURNSTILE_SECRET` the chat endpoint accepts unverified requests
— fine for a private URL, not for a public one. Under compose, set both
in `.env` instead: `TURNSTILE_SECRET` reaches the Worker through
`.dev.vars`, and `TURNSTILE_SITE_KEY` on its command line, as does
`TURNSTILE_HOSTNAMES` (below).

With Turnstile on, a question goes like this (#32):

1. Before each question the page asks `GET /api/chat` what it needs,
   and is told the site key and the action, `ask`, to render the
   widget with. It loads Cloudflare's script then, and only then: a
   deployment without Turnstile loads nothing from Cloudflare.
2. The widget is drawn in the Ask panel, out of sight unless Cloudflare
   wants a click, and the token it gives is sent with the question's
   first turn. The Worker checks it with Cloudflare's `siteverify`,
   and then what `siteverify` says of it (#115): that it was solved on
   this site's page, one of `TURNSTILE_HOSTNAMES` or, unset, the host
   the request was sent to, and for the action `ask`. A widget's site
   key can serve several hostnames, so a token from another of them is
   refused, and the log says where it was solved. `siteverify` is told
   the visitor's address only when the edge vouched for it
   (`EDGE_SECRET`, above); otherwise the client chose it, and it is
   told none.
3. The answer carries a session: an HMAC under `TURNSTILE_SECRET`,
   bound to the question — the conversation up to and including it —
   and to the client the rate limiter sees, good for ten minutes. The
   question's later turns present it instead of a token, since
   Cloudflare accepts a token once. A turn whose session is refused
   says so with the site key, and the page passes a fresh challenge and
   posts that turn again.

Set both keys or neither: the secret alone would refuse every question
for want of a token the page cannot get, so the Worker answers 503 and
logs which setting is missing. The page's policy (`public/_headers`)
allows `https://challenges.cloudflare.com` for the script and the
widget's frame, and nothing else from elsewhere. To try it without a
real widget, Cloudflare's test keys always pass: site key
`1x00000000000000000000AA`, secret `1x0000000000000000000000000000000AA`.
Their `siteverify` names `example.com` and no action wherever the page
is, so under a test secret the Worker checks neither.

The host a request was sent to is the `Host` header, which the tunnel
passes on as the site's hostname and `wrangler dev` keeps. A client
that reaches 8787 directly chooses it, though, and a proxy in front
may rewrite it: set `TURNSTILE_HOSTNAMES` to the site's hostnames,
comma-separated, in `.env` under compose or under `vars` for a deploy.

Only the dashboard's own page gets answers. A request must be
`application/json` and same-origin — by `Sec-Fetch-Site` or `Origin`,
which every browser sends — so a `curl` against `/api/chat` gets 403
unless it names the origin (`-H 'Origin: https://your.host'`), and its
conversation must be one the page's agent loop could have produced.
That stops other sites spending the budget through their visitors'
browsers; it does not stop a script, which is what Turnstile, the rate
limiter and the spend cap are for.

The rate limiters, one for the chat and one for `/api/q`, are set under
`vars` in `wrangler.jsonc`: `CHAT_RATE_LIMIT`, 20 questions a minute,
and `QUERY_RATE_LIMIT`, 100 calls in ten seconds, each a `limit` of
requests from one client in a `period` of seconds. Both key on the
visitor's address as `EDGE_SECRET`, above, decides it. They are counted
by a Durable Object, `RateLimiter` in `src/ratelimit.ts`, bound as
`RATE_LIMITER`: an object per limiter and Cloudflare location, created
by the deploy like the spend counter, and run by `wrangler dev`, so a
429 under compose is the real limiter.

The window slides (#115). They were `ratelimits` bindings, which
`wrangler dev` counts in windows aligned to the wall clock: a client's
budget came back whole at every multiple of the period, and a burst just
before one and another just after got twice it in moments. Now the
calls a client made in the last period are counted — the current window,
and the share of the one before that the period still covers — and a
burst across a boundary gets the limit once, the rest coming back as the
window slides on. A request refused is not counted. A setting that is
not a limit, or one with no `RATE_LIMITER` bound, refuses every request
and logs why, and a request the counter cannot be reached to count is
refused too, rather than let through uncounted.

Upgrading from the `ratelimits` bindings needs nothing by hand: take
the new `wrangler.jsonc`, whose `v2` migration creates `RateLimiter` on
the next deploy, or rebuild the image under compose. The two
`namespace_id`s it held are unused from then on.

`DAILY_SPEND_CAP_USD` in `wrangler.jsonc` defaults to `5`, dollars a
UTC day; empty or `0` is no cap, and anything else that is not a number
of dollars refuses every question rather than lifting the cap. It is a
bound, not an estimate (#33). Its counter is a Durable Object per UTC
day, `SpendCounter` in `src/spend.ts`, bound as `SPEND_COUNTER`. Before
a turn is sent to the model, the counter holds the most that turn could
cost — a token for every byte sent, priced as a cache write, and
`max_tokens` of output: about 26 cents for a question's first turn, up
to about $1.90 for the largest conversation the Worker takes — or
refuses with a 429 if that would take the day past the cap, however
many turns arrive at once. Once the answer is back, the hold becomes
what the turn cost, which is usually a few cents. A turn the API
refused holds nothing; one lost on the way, which may still have been
billed, keeps its hold for the day. So a cap of `5` admits turns while
there is room for their worst case, and they cost at most $5. A turn is
one call, sent once (#115): the SDK would send it again after a dropped
connection, when the first attempt may already have been billed, so
the Worker turns its retries off. A turn that fails fails the question,
with a 502, and asking again reserves again.

It replaced a running total in KV, checked before a call and added to
after it, which admitted all of 20 questions sent at once against a $5
cap — about $44 of calls — and recorded $2.20 of them.

`wrangler dev` runs the counter locally and keeps it in
`.wrangler/state/v3/do/chatsbom-SpendCounter/`, one SQLite file a day:
under compose, in the `web-state` volume, so a restart or a rebuild
does not reset the day's spend. With a cap set and no `SPEND_COUNTER`
bound — an older `wrangler.jsonc` — chat answers 503 and logs why.

A day's counter clears itself an hour after the day ends (#115): its
first write sets an alarm, by when nothing can reserve against the day
and the last turn reserved before midnight has settled, and the alarm
deletes what it stored. Deployed, that frees the object; `wrangler dev`
leaves its file behind, emptied to 4 KB, where it used to keep about
86 KB a day. A day counted before the alarm existed was never given
one, and keeps its file: nothing reads it again.

### Upgrading a deployment that had the KV counter

The counter moved from a KV namespace to a Durable Object (#33):

1. Take the new `wrangler.jsonc`. It declares `durable_objects` and a
   `migrations` entry (`v1`, `new_sqlite_classes: ["SpendCounter"]`)
   and no longer declares `kv_namespaces`. If yours carried a real KV
   id there, drop that block rather than merging it back.
2. `npm ci && npm run build && npm run deploy`. The first deploy
   applies the migration, which creates the class; Durable Objects of
   this kind are on every Workers plan.
3. The KV namespace is unused from then on. Delete it when you like:
   `npx wrangler kv namespace delete --namespace-id <id>`.
4. The day's total starts from nothing at the switch; the KV total is
   not carried over. On the day of the upgrade up to one more day's
   cap can be spent, unless you deploy just after 00:00 UTC or lower
   `DAILY_SPEND_CAP_USD` for that day.

Under compose, `docker compose up -d --build web` is the whole upgrade:
the counter starts in the same `web-state` volume, beside the old KV
data in `.wrangler/state/v3/kv/`, which nothing reads any more. Point 4
applies there too.

---

## 4. Build and deploy

**Until the cutover (#128):** the page this tree builds asks the Python
service, `chatsbom web serve` (`/api/meta`, `/api/v/...` and
`/api/ask`, #144), and no longer the Worker, which answers none of
those paths: a Worker deployed from this tree serves a page whose every
question fails. Keep a deployed Worker on the build it has until the
cutover.

```bash
cd web
npm ci
npm run schema        # regenerate types from the Python schema
npm run typecheck
npm run validate:palette
npm test
npm run build
npm run deploy
```

`npm run schema` before `typecheck` is not ceremony: `src/schema.ts` is
generated from `chatsbom/export/schema.py`, and a stale copy means the
dashboard is typed against a contract that no longer exists. CI runs the
same sequence and fails on a diff.

---

## 5. Verify

Check the path the dashboard uses, not just that the page loads.

```bash
# The query endpoint answers, and the numbers are the ones you exported.
curl -s https://your.workers.dev/api/q \
  -H 'content-type: application/json' \
  -d '{"method":"totals"}'

# Provenance: which build, which contract, how fresh.
curl -s https://your.workers.dev/api/q \
  -H 'content-type: application/json' \
  -d '{"method":"meta"}'

# A real lookup. `mail` is the useful probe: it is a Ruby gem with 118
# dependants *and* a Maven artifactId with 6, so a correct answer is 124
# with two ecosystems, not one number.
curl -s https://your.workers.dev/api/q \
  -H 'content-type: application/json' \
  -d '{"method":"ecosystemsFor","params":{"name":"mail"}}'
```

Expect from `meta` a generator naming the release that exported the
data, `chatsbom/` and its version, a schema version, and an observation
span — two dates, because on this corpus the ends are seven months apart
and a single date would imply otherwise.

A 503 from `/api/q` means no `DB` binding is configured. A 400 means the
method name is wrong; the endpoint accepts an allow-list and never SQL.

## 6. Refreshing the data

Re-export and re-import. No redeploy: the Worker holds no data.

```bash
uv run chatsbom db edges
uv run chatsbom export d1 --output dist/d1
cd web
for f in ../dist/d1/[0-9][0-9]-*.sql; do
  npx wrangler d1 execute chatsbom --remote --file "$f" || break
done
```

The schema script drops and recreates, so this replaces rather than
appends. That also means **there is a window** — roughly the length of
the data import — where the dashboard queries tables that are empty or
half-filled. On a snapshot of a collection run that is the honest
trade: a half-updated dataset is worse than a briefly unavailable one,
and the numbers on the page are cross-referenced, so serving old
repositories against new artifacts would produce figures that are wrong
rather than stale.

If that window matters, import into a second database and switch the
binding, which is a redeploy but an atomic one.

## Continuous collection

Containerised, so it leaves nothing on the host. Set `GITHUB_TOKEN`,
`UID` and `GID` in the `.env` beside `docker-compose.yaml` (copy
`.env.example` if you have none yet), then:

```bash
mkdir -p data .cache .requests-cache   # once, before the first `up`
docker compose --profile collect up -d --build
docker compose logs -f collector
docker compose down                 # gone — no units, no host installs
```

Until the cutover (#128), name what it is to build, `docker compose
--profile collect up -d --build collector depgraph`: a bare `--build`
builds the Worker's image again as well, from a page that no longer
asks the Worker ("The Python service, on a second hostname", above).

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
docker compose exec web node -e "require('dns').lookup('dind', \
  e => console.log(e ? 'dind: unreachable' : 'dind: REACHABLE'))"

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

Each index pass ends with what the Python web service, `site`, serves
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

**`site` reads them as uid 10003**, through a read-only mount of
`data/snapshots`: neither the collector's `UID` nor in its group. So
`data/snapshots` is anyone's to list and enter, `CURRENT` anyone's to
read, and each snapshot anyone's to read and no one's to write,
whatever umask made them. The warehouse and the export follow the
umask, as the rest of `data/` does: they are not `site`'s.

Before the first pass:

1. **The disk.** `df -h data` should leave 10 GB beyond what the store
   grows into. Otherwise set `WAREHOUSE=off`.
2. **`data/snapshots`**, for `site` to start before anything is
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

   and then `docker compose --profile site up -d`.

Checked on Docker Engine 29.3.1 and Compose 5.1.1, on a synthetic store
of 3,000 repositories. The collector's service, as compose runs it
(1000:1000, 4 GiB, two CPUs) and here under umask 077, ran one slice of
the loop with its index pass and the export, the steps before them
stood in for: the warehouse in 37 s, a snapshot published, and the
export, with nothing spilled left. `data/snapshots` came out `0755`,
`CURRENT` `0644` and the snapshot `0444`, and `site`, uid 10003, was
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
   The web keeps serving from ClickHouse and D1.
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

## The release and commit decisions from `raw_documents` (once, before phase 5)

From this change on, `chatsbom run`'s release and commit stages keep
what they decide in the store: a release decision per push, the release
list it names, and a commit decision per tag or head
(`03-github-release/<id>/…`, `04-github-commit/<id>/…`; README, "The
repository-keyed layout"), #147. What they decided before is in
ClickHouse's `raw_documents` alone, inside the records `RecordStore`
landed, and phase 5 of #128 removes that table. `data backfill-decisions`
writes it into the store, from each repository's newest complete record
(one with a push and its releases), keyed by the record's own push and
chosen tag.

**When:** once, after this change is collecting, so that every decision
made from then on is the stage's own, and before phase 5. `RecordStore`
lands each record as `run` finishes it, so the records are all there
already; the check in step 3 wants an index pass after the last of them.
Collection need not stop: the command reads the database and writes only
those two directories, never over a file that is there, so a decision
the collector made meanwhile is left as it is.

**As whom:** as the collector runs, the `cli` service, which is the
`UID` and `GID` in `.env` (below, "Continuous collection"). What it
writes are directories, `03-github-release/<id>/…` and
`04-github-commit/<id>/…`, that the collector writes into after it:
made by another user, root under `sudo` say, the collector cannot
write them, and its release stage fails for their repositories on
every walk. From a checkout on the host, run it as that same user,
never with `sudo`.

1. **Report** (reads only): how many decisions and lists it would write,
   how many the store has already, and the repositories with no complete
   record, by why (`no push`, `no releases`), which have nothing to write.
   ```bash
   docker compose --profile tools run --rm cli data backfill-decisions
   ```
2. **Write**, and run it again to see it write nothing:
   ```bash
   docker compose --profile tools run --rm cli data backfill-decisions --apply
   # Again: "Nothing to write."
   docker compose --profile tools run --rm cli data backfill-decisions --apply
   ```
   "Kept differently" counts decisions the collector has made for the
   same push since, with another result; the store keeps the one it had
   first. A tag the collector has resolved since to another commit is
   not one of them: the record's commit is kept beside it, as a later
   resolution for the record's push (README, "The repository-keyed
   layout").
3. **Check** it against ClickHouse: build the warehouse from the store,
   and compare. The parity check's `releases`, `repository_releases` and
   `refs` compare the releases and the current scans' refs with what `db
   index` read from `raw_documents`:
   ```bash
   uv run chatsbom warehouse build
   uv run python scripts/warehouse_parity.py
   ```
   The differences it is meant to have are listed in
   `chatsbom/warehouse/parity.py`: a release withdrawn since, a
   repository whose newest record's releases could not be fetched (the
   store has the last decision that had them), and a push decided twice.
   Anything else is to be explained before phase 5.

`data prune` keeps what the current scan descends from and the newest
`PRUNE_KEEP` release decisions of each repository (README, `chatsbom
data`), so the backfilled decisions of a repository pushed since are
pruned like any other: nothing needs to be done about them.

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
graph) does not show until PR E (below). D1 is unchanged until it is
exported again; leave that to PR E.

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

1. `git pull && uv sync`, and `docker compose build web`.
2. `uv run chatsbom db index` (not `--rebuild`): it stamps `snapshot`
   on every row, and `ensure_schema` declares the views, the
   dictionary and the new rollups and drops the two language rollups.
   Measured on a scratch copy of production: 26 minutes for 60,080
   repositories.
3. **Restart the web container right after step 2** (`docker compose
   up -d web`). The dashboard built from the previous commit reads the
   dropped rollups and `mv_top_packages.language`, so its overview
   panels fail between the two steps. Point lookups keep working.
4. `uv run python scripts/verify_rollups.py` — 23 checks, all agree on
   the scratch copy — and `uv run chatsbom db status`.
5. D1, if it is used: `uv run chatsbom export d1` and apply its files
   as in section 2. The Worker must be deployed with the same
   commit, since the `agg_*` tables changed shape.

**Compatibility.** For one release the Worker still accepts `language`
where the ranking and the relationship split now take `ecosystem`, and
reads it as the ecosystem that language's list stood for (`php` as
Composer, `java` as Maven); a language with none is the whole corpus.
`relationshipByLanguage` answers with the per-ecosystem rows, each with
`language` set to its ecosystem. The dependants' `language` filter
matches the folded bucket.

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
