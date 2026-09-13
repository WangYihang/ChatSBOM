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
npm run typecheck
npm test
npm run dev         # wrangler dev
```

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

Almost nothing, by design. `/data/*` streams byte ranges out of R2 with
immutable cache headers; everything else is a static asset. The Parquet
files live in R2 rather than in static assets because assets cap at
25 MiB per file and R2 serves the ranged reads DuckDB issues.

The query surface in `src/queries.ts` is a set of typed functions, never
raw SQL from the caller. When the AI chat is added it will be given those
functions as tools, so a prompt cannot turn into a query plan.
