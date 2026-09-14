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
│           ↓              │  upload │   /data/*  → R2 ranges     │
│ ClickHouse               │ ──────► │   /api/chat → Anthropic    │
│           ↓              │         │                            │
│ export parquet  20.6 MB  │         │ R2 bucket: the Parquet     │
└──────────────────────────┘         └────────────────────────────┘
```

Nothing on the Cloudflare side ever queries a database. The browser
downloads the Parquet and queries it with DuckDB-WASM, which is why the
serving side has no running cost beyond bandwidth.

---

## 0. Before you start

| You need | Why |
| --- | --- |
| A Cloudflare account | R2 + Workers |
| `wrangler` logged in | `npx wrangler login` |
| A populated ClickHouse | `chatsbom db status` should report rows |
| An `ANTHROPIC_API_KEY` | **Only** for `/api/chat`; the dashboard works without it |

Costs to know about up front: R2 storage for ~21 MB is negligible, and
Workers' free tier covers the dashboard. The AI chat needs **paid
Workers** (CPU time) and bills per token to Anthropic — the daily cap in
`wrangler.jsonc` is a backstop, not an accountant.

---

## Running it locally first

The dashboard reads its data through the Worker's R2 binding, and a local
`wrangler dev` binds the **preview** bucket. A fresh clone's preview
bucket is empty, so without seeding it the page loads and then reports
that it could not fetch the dataset manifest — nothing is broken, there
is simply nothing there.

```bash
uv run chatsbom export parquet --output web/dist/data   # once
cd web
npm install
npm run seed        # dist/data + the engine -> the local preview bucket
npm run dev         # http://localhost:5173
```

`npm run seed` touches only `.wrangler/state`. Nothing is uploaded.

`npm run preview` serves the built output instead, which is what the
deploy runs; use it to check anything that behaves differently between
the dev server and the real Worker.

Two things worth knowing when the page seems stuck on *Loading dataset*:

- The engine is ~33 MB on a cold load and cached immutably afterwards, so
  the first visit is slow and later ones are not.
- Seeding writes to the bucket named by `preview_bucket_name`, not
  `bucket_name`. Seeding the production name locally puts objects
  somewhere nothing reads.

---

## 1. Export the dataset

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
against it.

---

## 2. Create the R2 bucket and upload

```bash
cd web
npx wrangler r2 bucket create chatsbom-data
npx wrangler r2 bucket create chatsbom-data-preview   # for `wrangler dev`

for f in repositories artifacts licenses history; do
  npx wrangler r2 object put "chatsbom-data/$f.parquet" \
    --file "dist/data/$f.parquet" --content-type application/vnd.apache.parquet
done
npx wrangler r2 object put chatsbom-data/manifest.json \
  --file dist/data/manifest.json --content-type application/json

# The query engine itself. Without it the dashboard boots and then
# reports that it could not fetch the engine.
npx wrangler r2 object put chatsbom-data/duckdb-eh.wasm \
  --file node_modules/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm \
  --content-type application/wasm
```

The Parquet lives in R2 rather than in static assets for two reasons:
assets cap at **25 MiB per file**, and R2 serves the ranged reads DuckDB
issues — including the suffix range a Parquet reader uses to find the
footer before it knows the file length.

The **engine** is in R2 for the same size reason — `duckdb-eh.wasm` is
~33 MB — and it is served from this origin at all because of a hard
browser rule: `new Worker(url)` refuses a cross-origin script. DuckDB's
own `getJsDelivrBundles()` hands back CDN URLs, and passing one to
`new Worker` fails outright:

```
Failed to construct 'Worker': Script at
'https://cdn.jsdelivr.net/.../duckdb-browser-eh.worker.js'
cannot be accessed from origin 'https://your.host'
```

So the ~0.7 MB worker script ships as a static asset (Vite emits it from
a `?url` import) and the module comes from R2 under `/wasm/`, cached
immutably. A side benefit: no third-party CDN is on the critical path,
which matters in networks where jsDelivr is unreachable.

`/wasm/*` must be listed in `run_worker_first` alongside `/data/*`. A
prefix left out of it is answered by the SPA fallback, so the fetch for
WebAssembly silently returns an HTML page.

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

```bash
BASE=https://chatsbom.<your-subdomain>.workers.dev

# the manifest, and that it is revalidated rather than cached forever
curl -si "$BASE/data/manifest.json" | grep -i cache-control
# expect: public, max-age=60, must-revalidate

# ranged reads, which is what makes the whole thing cheap
curl -si "$BASE/data/artifacts.parquet" -H 'Range: bytes=0-99' | head -3
# expect: HTTP/2 206, content-range: bytes 0-99/…

# the suffix range a Parquet reader uses for the footer
curl -si "$BASE/data/artifacts.parquet" -H 'Range: bytes=-8' | head -2
# expect: HTTP/2 206

# an immutable Parquet, and a path that is not servable
curl -si "$BASE/data/artifacts.parquet" | grep -i cache-control
curl -so /dev/null -w '%{http_code}\n' "$BASE/data/../wrangler.jsonc"
# expect: immutable; then 404
```

Then open the page. Checks that actually catch a broken deploy:

- The stat row shows real numbers, not zeros — zeros mean the Parquet did
  not load.
- The footer reports the row counts and payload size from `manifest.json`.
- Switch to **Query**, type `mail`. It should report 118 dependants of
  which 17 declare it, and offer an **Ecosystem** selector, because `mail`
  is also a Maven artifactId with 6 more.
- Click a bar in *Most declared packages*: the URL should become
  `#/query/<name>` and the query view should already be filled in.
- Toggle your OS to dark mode. The charts must re-render in the dark
  palette — they read the theme at draw time.

---

## 6. Refreshing the data

The dashboard reads whatever is in R2. To publish a newer dataset, repeat
steps 1 and 2 — no redeploy needed, because the Worker only serves bytes.

```bash
uv run chatsbom export parquet --output web/dist/data
cd web && for f in repositories artifacts licenses history manifest; do
  ext=$([ "$f" = manifest ] && echo json || echo parquet)
  npx wrangler r2 object put "chatsbom-data/$f.$ext" --file "dist/data/$f.$ext"
done
```

Upload the Parquet **before** the manifest. Readers follow the manifest,
so that order means they see either the old dataset or the new one, never
half of each.

---

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
