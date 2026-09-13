# ChatSBOM Dashboard

A static dashboard over the SBOM dataset. The Parquet files are queried
**in the browser** with DuckDB-WASM, so there is no query backend: page
loads are Cloudflare static assets, and a query costs a couple of ranged
`GET`s against R2.

## Two views

The **overview** answers standing questions; the **query** view answers
one about a package. They are peers, not a page and a sub-page: every bar
in the overview's rankings hands its package to the query view, and the
segmented control or the back button returns. State lives in the hash
(`#/query/mail`), so a view is linkable and history works — no router
library, and no server that needs to know about routes.

`Router.go()` uses `pushState` and applies synchronously. Assigning
`location.hash` fires `hashchange` *asynchronously*, so a caller that
navigated and then focused the new view's input would be focusing a still
hidden element.

## Charts

Eight forms, all hand-authored inline SVG. No chart library: every one is
bars, stacked bars, an area or a histogram, and a library would cost more
bundle than the dashboard's own JavaScript. Adding all eight grew the
client bundle by 16 kB.

| Chart | Form, and why |
| --- | --- |
| How dependencies arrived | one stacked bar — this is a whole in parts, not three quantities |
| SBOM coverage by language | ranked bars, one hue, with an inset for "with an SBOM" |
| Most declared packages | ranked bars; every bar links into the query view |
| Dependencies per repository | histogram, bucketed — the spread covers three orders of magnitude |
| Where the data came from | grouped bars, Syft against the dependency graph |
| Licences | ranked bars, unknown included rather than dropped |
| Adoption over time | area plus line, **one** axis — both series are repository counts |
| Versions in use | ranked bars, per package |

### The palette was computed, not chosen

Every chart hue came out of the palette validator. The first attempt — the
project's own accent `#0F6B57` with a violet-blue — failed two checks:
the accent sits at chroma 0.086 and reads as grey once it is a fill, and
the green/blue pair separates by only ΔE 5.1 under tritanopia. Neither is
visible to a normal-vision reader looking at the chart, which is the whole
reason for running the check.

The shipped values, with their results, are recorded in `src/palette.ts`.
Dark is a **separate selection**, not an inversion: its lightness band is
L 0.48–0.67 against light's 0.43–0.77, so the light steps fall outside it
and fail outright.

Two rules the charts follow that are easy to get wrong:

- **One axis, always.** Adoption-over-time plots two series, but both are
  repository counts, so a second scale would be the dual-axis mistake.
- **Colour follows the entity, not its rank.** Filtering the language does
  not repaint the surviving series.

State is also encoded in **form**: the relationship pills use a solid,
dashed or dotted border as well as a colour, so the distinction survives
colour-vision deficiency and greyscale printing.

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
