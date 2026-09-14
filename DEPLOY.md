# Deployment

Two things get deployed, and they run in different places for a reason
that is not negotiable: **collection needs Syft and Docker, which
Cloudflare Workers does not have.** So the collector runs wherever you
keep it, and only the published artefacts reach the edge.

```
your machine / a server              Cloudflare
┌──────────────────────────┐         ┌────────────────────────────┐
│ collector (container)    │         │ Worker                     │
│   github · syft · index  │         │   static assets  (the SPA) │
│           ↓              │  import │   /api/q    → D1           │
│ ClickHouse               │ ──────► │   /api/chat → Anthropic    │
│           ↓              │         │                            │
│ export d1   165 MB SQL   │         │ D1: the dataset, queried   │
└──────────────────────────┘         └────────────────────────────┘
```

A visitor downloads about 110 KB, and every answer is one Worker
request. The overview's panels are precomputed at export time, so they
are single-row reads rather than aggregations over 6,062,896 rows.

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

Costs to know about up front: the database is 294.7 MB, inside D1's
500 MB free tier and 2.9% of the 10 GB paid limit, and Workers' free
tier covers the dashboard. D1 bills for rows read, which is why the
overview's panels are precomputed — they would otherwise read 6,062,896
rows per visitor. The AI chat needs **paid
Workers** (CPU time) and bills per token to Anthropic — the daily cap in
`wrangler.jsonc` is a backstop, not an accountant.

---

## Running it locally first

```bash
cd web
npm install
npx wrangler d1 create chatsbom          # once
# put the returned database_id into wrangler.jsonc
npx wrangler d1 execute chatsbom --local --file ../dist/d1/01-schema.sql
npx wrangler d1 execute chatsbom --local --file ../dist/d1/02-data.sql
npx wrangler d1 execute chatsbom --local --file ../dist/d1/03-aggregates.sql
npx wrangler d1 execute chatsbom --local --file ../dist/d1/04-indexes.sql
npm run dev
```

`--local` keeps everything in `.wrangler/state`; nothing is uploaded.
Importing 165 MB of SQL locally takes a couple of minutes.

`npm run preview` serves the built output instead, which is what the
deploy runs.

If `/api/q` answers 503, the `DB` binding is missing from
`wrangler.jsonc`. If it answers but every number is zero, the data
script has not been applied.

---

## 1. Export the dataset

```bash
uv run chatsbom export d1 --output dist/d1
```

`chatsbom export parquet` also exists. It is not part of deploying —
nothing serves it — but it produces a 20.6 MB self-describing copy of
the dataset that DuckDB or pandas can read directly, which is worth
attaching to a release.

```bash
uv run chatsbom export parquet --output web/dist/data
```

Expect roughly this. If `artifacts.parquet` is much smaller, **stop** —
that is the truncation bug, and the export now raises rather than
printing a cheerful total:

```
artifacts.parquet     6,062,896 rows   16.7 MB
history.parquet         141,938 rows    1.1 MB
licenses.parquet            500 rows    6.8 kB
repositories.parquet     28,075 rows    2.8 MB
                                     ─────────
total                                  20.6 MB
```

`manifest.json` carries a SHA-256 per file. Keep it: step 5 verifies
against it, and it is the only file whose name is fixed — the Parquet
files are named `repositories-659592a2.parquet`, after their own
content.

That is what makes their `immutable, max-age=31536000` header truthful.
With fixed names it was not: a browser that had `repositories.parquet`
kept it for a year while revalidating a manifest describing a different
file, and the symptom was a schema error rather than a cache one —
`Binder Error: Table "r" does not have a column named "observed_at"`.

**Upload the Parquet before the manifest.** A manifest naming files that
are not there yet is a broken deployment; the reverse is merely a stale
one. Old generations can be deleted once no manifest names them.

---

## 2. Create the D1 database and import

```bash
cd web
npx wrangler d1 create chatsbom
# put the returned database_id into wrangler.jsonc under d1_databases
```

Then apply the four scripts **in order**. The order is not stylistic:

```bash
D=../dist/d1   # wherever `chatsbom export d1 --output` wrote them

npx wrangler d1 execute chatsbom --remote --file "$D/01-schema.sql"
npx wrangler d1 execute chatsbom --remote --file "$D/02-data.sql"
npx wrangler d1 execute chatsbom --remote --file "$D/03-aggregates.sql"
npx wrangler d1 execute chatsbom --remote --file "$D/04-indexes.sql"
```

- **Schema first**, and it drops before it creates: D1 keeps whatever a
  previous import left, so applying the data twice against existing
  tables doubles every row rather than replacing it.
- **Aggregates after the data**, because they are computed *from* it.
  They are derived inside SQLite rather than by a second trip to
  ClickHouse, so they cannot disagree with the rows they describe.
- **Indexes last.** Inserting into an indexed table updates every index
  per row; building them once over finished data is markedly faster.

### What you are importing

    01-schema.sql        3.7 kB
    02-data.sql        165.4 MB   6,062,896 artifact rows, batched
    03-aggregates.sql    3.6 kB
    04-indexes.sql       611 B
                      ─────────
    applied            294.7 MB in D1

294.7 MB fits D1's free tier (500 MB) and is 2.9% of the paid limit
(10 GB). It is that small because the artifact rows are normalised: a
direct translation of the Parquet schema measures **762.6 MB** with the
same indexes, which does not fit. Most of the saving is one table — the
five low-cardinality columns take only 45 distinct combinations across
six million rows, and were stored as five strings on every one of them.

`02-data.sql` uses batched multi-row INSERTs. `sqlite3 .dump` would
write one statement per row — 6,062,896 of them, against D1's 100,000
byte statement cap and over a network.

### Re-importing

The schema script drops and recreates, so a re-import replaces rather
than appends. There is no partial-update path: this is a snapshot of a
collection run, and a half-updated snapshot is worse than an old one.

---

## 3. Optional — the AI chat

Skip this and the dashboard still works; `/api/chat` answers 503 and says
so.

```bash
npx wrangler kv namespace create SPEND
# put the returned id into wrangler.jsonc under kv_namespaces

npx wrangler secret put ANTHROPIC_API_KEY
```

Two more, both worth doing before the URL is public:

```bash
# Turnstile: create a widget in the dashboard, then
npx wrangler secret put TURNSTILE_SECRET
```

Without `TURNSTILE_SECRET` the chat endpoint accepts unverified requests
— fine for a private URL, not for a public one.

The rate limiter needs a namespace id in `wrangler.jsonc` under
`unsafe.bindings`. `1001` is a placeholder; any unused integer works, and
the binding is per-Worker.

`DAILY_SPEND_CAP_USD` in `wrangler.jsonc` defaults to `5`. The KV
read-modify-write behind it is not atomic, so concurrent requests can
overshoot slightly — it exists to stop a runaway becoming a large bill,
not to be exact.

---

## 4. Build and deploy

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

Expect from `meta` a generator like `chatsbom/0.5.4`, a schema version,
and an observation span — two dates, because on this corpus the ends are
seven months apart and a single date would imply otherwise.

A 503 from `/api/q` means no `DB` binding is configured. A 400 means the
method name is wrong; the endpoint accepts an allow-list and never SQL.

## 6. Refreshing the data

Re-export and re-import. No redeploy: the Worker holds no data.

```bash
uv run chatsbom export d1 --output dist/d1
cd web
for f in 01-schema 02-data 03-aggregates 04-indexes; do
  npx wrangler d1 execute chatsbom --remote --file "../dist/d1/$f.sql"
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

Containerised, so it leaves nothing on the host:

```bash
export GITHUB_TOKEN=ghp_...
export UID=$(id -u) GID=$(id -g)   # see below
docker compose --profile collect up -d --build
docker compose logs -f collector
docker compose down                 # gone — no units, no host installs
```

`UID`/`GID` are not optional. `data/` and `.cache/` are bind mounts owned
by whoever cloned the repo, so a container running as its own baked-in
uid cannot write them — the first symptom is
`sqlite3.OperationalError: attempt to write a readonly database` from the
ledger. Putting them in a `.env` beside the compose file works too.

One slice every 15 minutes by default, a retention pass roughly daily.
Tunable without rebuilding:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SYNC_INTERVAL_SECONDS` | `900` | Wait between slices |
| `SYNC_SLICE` | `500` | Repositories re-checked per slice |
| `SYNC_QUOTA` | `250` | Rate-limited requests per slice (304s are free) |
| `PRUNE_KEEP` | `2` | Scans retained per repository |

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
units for the same two schedules, with `ProtectSystem=strict` and
`ReadWritePaths` limited to `data/` and `.cache/`.

---

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
