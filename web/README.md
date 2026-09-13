# ChatSBOM Dashboard

A static dashboard over the SBOM dataset. The Parquet files are queried
**in the browser** with DuckDB-WASM, so there is no query backend: page
loads are Cloudflare static assets, and a query costs a couple of ranged
`GET`s against R2.

## Why this shape

The entire dependency graph — 6.1M rows across 28k repositories —
compresses to about 19 MB of Parquet. That is small enough to hand to a
browser, which removes the database from the serving path entirely.

| | |
| --- | --- |
| `repositories.parquet` | one row per repository, sorted by stars |
| `artifacts.parquet` | one row per (repo, package, version), sorted by package name |
| `manifest.json` | row counts, sizes and SHA-256 of each file |

`artifacts.parquet` is sorted by package name so DuckDB can prune row
groups using Parquet statistics: answering "who depends on `mail`" reads
a few hundred kilobytes, not the file.

## Types are generated, not written

`src/schema.ts` is generated from `chatsbom/export/schema.py`, the single
source of truth for the export contract:

```bash
npm run schema     # regenerates src/schema.ts and src/schema.json
```

A renamed column becomes a TypeScript compile error rather than an
`undefined` three layers into a chart. The Python test suite fails if the
checked-in copy is stale, so the two cannot drift.

The `Relationship` union (`'direct' | 'transitive' | 'unknown'`) comes
across from the Python `Literal` of the same name — the type the type
system is actually good at, expressed once.

## Development

```bash
npm install
npm run schema      # generate types from the Python schema
npm run typecheck   # worker and browser are separate tsconfig projects
npm test
npm run dev         # vite dev, with the Worker running via the CF plugin
npm run build       # -> dist/client (assets) + dist/chatsbom (worker)
```

The Worker and the browser get **separate tsconfig projects**: both
runtimes define `Response`, and checking them together makes DOM calls
resolve against Workers types.

## Deploying

```bash
# 1. Export the dataset (needs ClickHouse populated)
cd .. && uv run chatsbom export parquet --output web/dist/data

# 2. Upload it to R2
npx wrangler r2 bucket create chatsbom-data
npx wrangler r2 object put chatsbom-data/repositories.parquet --file dist/data/repositories.parquet
npx wrangler r2 object put chatsbom-data/artifacts.parquet --file dist/data/artifacts.parquet
npx wrangler r2 object put chatsbom-data/manifest.json --file dist/data/manifest.json

# 3. Deploy the Worker and assets
npm run deploy
```

## What the Worker does

Almost nothing on the data path, by design. `/data/*` streams byte ranges
out of R2 with immutable cache headers; everything else is a static asset.
The Parquet files live in R2 rather than in static assets because assets
cap at 25 MiB per file and R2 serves the ranged reads DuckDB issues.

## Ask a question

`/api/chat` answers natural-language questions, and the split of
responsibility is the interesting part: **the agent loop runs in the
page**, because that is where the data is.

```
browser                             worker                    anthropic
  |-- messages ------------------------>|
  |                                     |-- one model turn ------>|
  |<-- tool_use blocks -----------------|<------------------------|
  |-- run against DuckDB                |
  |-- messages + tool_result ---------->|
  |                                     |-- next turn ----------->|
  |<-- final text ----------------------|<------------------------|
```

Consequences worth stating:

- The Worker holds the API key; the browser holds the data. Neither holds
  both, and **query results never reach the server**.
- The model's tools are the typed functions in `src/queries.ts` — it
  cannot pass SQL, so a prompt cannot become a query plan. This is what
  the local `chatsbom chat` TUI could not offer: there, the model writes
  SQL directly.
- The loop is bounded (8 turns) because every turn is a paid call.
- Failed tools are returned as `is_error` results rather than dropped; a
  missing `tool_result` is a malformed conversation.

The Worker owns what a client cannot be trusted with: the key, Turnstile
verification, per-IP rate limiting, request bounds, and a daily spend cap.

### Configuring chat

```bash
wrangler secret put ANTHROPIC_API_KEY     # required; without it /api/chat 503s
wrangler secret put TURNSTILE_SECRET      # optional; when set, a token is required
wrangler kv namespace create SPEND        # put the id in wrangler.jsonc
```

`DAILY_SPEND_CAP_USD` in `wrangler.jsonc` is a backstop, not an
accountant: the KV read-modify-write is not atomic, so concurrent
requests can overshoot slightly. It exists to stop a runaway becoming a
large bill. The dashboard keeps working when the cap is hit.
