# ChatSBOM Dashboard

A Cloudflare Worker (`src/worker.ts`) and the page it serves. The page
holds none of the data: it asks the Worker one question per request, by
name, and the Worker answers from a database, either ClickHouse read
live or D1 holding a snapshot the export wrote.

```
browser                        Worker                          store
  |-- the page, /assets/* ---> static assets; the Worker does not run
  |-- POST /api/q ------------> src/d1/api.ts ------------> ClickHouse, over HTTP
  |                                                     or D1, the DB binding
  |-- GET, POST /api/chat ----> src/chat.ts --------------> the Messages API
```

| Route | What answers it |
| --- | --- |
| `/api/q` | `POST {"method", "params"}`: one method of `DatasetQueries` (`src/backend.ts`), from the allow-list in `src/d1/api.ts`, run against the store that is configured |
| `/api/chat` | `GET`: what a question needs first, a Turnstile site key or nothing. `POST`: one model turn, relayed to the Messages API |
| anything else | the page, from static assets. `run_worker_first` sends only `/api/*` to the Worker, so a page load costs no invocation |

A third route, `/data/*`, used to stream Parquet out of R2 to a query
engine in the browser. It is gone, and so is the R2 binding;
`src/worker.ts` records what went with it.

## Two stores, one set of questions

`/api/q` answers from ClickHouse when `CLICKHOUSE_URL` is set, and
otherwise from the D1 binding `DB`; with neither, it answers 503.
ClickHouse wins when both are there, as under compose, where
`wrangler.jsonc` declares the D1 binding too: it holds the live data
(`selectDataset` in `src/d1/api.ts`).

The seam is `DatasetQueries` in `src/backend.ts`: a method per
question, never `query(sql)`. The two stores want different strategies
rather than one SQL in two dialects. SQLite cannot afford the
overview's aggregates live, so the D1 export precomputes them into
`agg_*` tables, while ClickHouse keeps them as refreshable materialized
views (`mv_*`); a point lookup on D1 joins through integer references,
where ClickHouse reads the fact table by its sort key. So each store
implements the questions its own way, in `src/clickhouse/queries.ts`
and `src/d1/queries.ts`.

What they share is written once, in `src/dataset/`: the questions
answered by reading one stored table (`reads.ts`), how much a question
may ask for and how a row becomes a result (`shape.ts`), and the result
types (`types.ts`). `test/contract.test.ts` asks both stores the same
questions about one corpus and expects one answer.

## The query endpoint

The page names a method (`src/d1/client.ts`). It cannot send SQL, and
nothing in the endpoint would take any: each method owns its statement
and takes only the parameters it declares. The allow-list has no
prototype, so `constructor` or `__proto__` is an unknown method, a 400.

The endpoint checks a call before a store sees it: whole numbers where
a method counts, strings no longer than what they name, a body of at
most 4 KiB. A per-client rate limit, `QUERY_RATE_LIMITER`, keyed as
`src/ratelimit.ts` says, runs before the body is read. A database error
is logged and answered with a plain 500, since its text carries table
names and SQL.

## Ask a question

`/api/chat` answers questions in natural language, one model turn per
request. The loop runs in the page (`src/agent.ts`): post a turn, run
the tools the model asked for, post their results, and again, at most
eight turns, since each is a paid call. A tool that fails goes back as
an `is_error` result rather than being dropped: a missing `tool_result`
is a malformed conversation.

The model's tools (`src/tools.ts`) are typed functions over the same
`/api/q` methods the dashboard's controls call. It cannot pass SQL, so
a question cannot reach data the page could not. What the model is told
about them, and the system prompt, are the Worker's (`src/prompt.ts`):
the page runs the tools but never loads their descriptions, and it
loads the agent loop only when it first draws the Ask panel. The
queries run in the Worker, so the Worker sees both the question and
what the data says; `src/chat.ts` says what that changed from the
Parquet design, where it saw only the question.

The Worker holds what a client cannot be trusted with. Before a turn
reaches the model it checks that a key is configured, that the request
is same-origin JSON of at most 256 KiB, the per-client rate limit
(`CHAT_RATE_LIMITER`), that the conversation is one the page's own loop
could have produced (`parseChatRequest`), Turnstile, and, last, the
daily spend cap.

- **Turnstile**, when `TURNSTILE_SECRET` is set (#32). Before each
  question the page asks `GET /api/chat` for the site key and the
  action to render the widget with, passes the challenge, and sends the
  token with the question's first turn. The Worker takes it only if
  `siteverify` says it was solved on this site, `TURNSTILE_HOSTNAMES`
  or the host the request was sent to, for that action (#115). The
  answer carries a session, an HMAC bound to the question and the
  client for ten minutes (`src/session.ts`), which the question's later
  turns present instead: Cloudflare accepts a token once.
- **The spend cap**, `DAILY_SPEND_CAP_USD` (#33). A Durable Object per
  UTC day (`src/spend.ts`) holds each turn's worst case before the model
  is asked, refuses a turn that would take the day past the cap, and
  settles the rest at what they cost. The dashboard keeps working when
  the cap is reached.

```bash
npx wrangler secret put ANTHROPIC_API_KEY   # required; without it /api/chat answers 503
npx wrangler secret put TURNSTILE_SECRET    # optional; with TURNSTILE_SITE_KEY under vars,
                                            # every question passes Turnstile first
```

`wrangler.jsonc` has `DAILY_SPEND_CAP_USD` and `TURNSTILE_SITE_KEY`
under `vars`, and names the rest the Worker reads: `EDGE_SECRET`,
`TURNSTILE_HOSTNAMES`, `ANTHROPIC_BASE_URL`, the ClickHouse settings
and `GENERATOR`. The
spend counter needs nothing created by hand; the deploy creates its
class. DEPLOY.md, section 3, has the details.

## The page

Two views, the overview and the query view for one package, are peers:
a bar in the overview hands its package to the query view, and the
segmented control or Back returns. The view is in the hash
(`#/query/mail`), so it can be linked, with no router library and no
server that knows about routes (`src/router.ts`, `src/hooks.ts`).

The charts are inline SVG, drawn by hand on visx's scales
(`src/charts/`). Their hues came out of a validator rather than taste,
`npm run validate:palette`, and `src/palette.ts` records the values and
what they passed. Dark is a separate selection, not an inversion.

A name is not a package. `mail` is a Ruby gem with 118 dependants and a
Maven artifactId with 6; counted together, 124 dependants of something
that does not exist. So the query view offers an ecosystem when a name
is in more than one, with each one's count, and the model has an
`ecosystems_for` tool.

## Types are generated, not written

`src/schema.ts` is generated from `chatsbom/export/schema.py`, the
single source of the export contract:

```bash
npm run schema     # regenerates src/schema.ts and src/schema.json
```

A renamed column is a TypeScript compile error rather than an
`undefined` three layers into a chart, and the Python suite fails when
the checked-in copy is stale.

## Development

```bash
npm ci
npm run schema            # the types, from the Python schema
npm run typecheck         # the Worker, the page and the tests, each its own tsconfig
npm run validate:palette
npm test
npm run dev               # vite, with the Worker run by the Cloudflare plugin
npm run build             # dist/client, the assets, and dist/chatsbom, the Worker
npm run preview           # wrangler dev, serving the build
```

The Worker and the page are separate tsconfig projects because both
runtimes define `Response`, and checked together, DOM calls resolve
against the Workers types. Under compose, the `web` service builds this
and serves it with `wrangler dev` against ClickHouse (`Dockerfile.web`,
`deploy/web-entrypoint.sh`).

## Deploying on D1

DEPLOY.md has the whole of it. In short, from the repository's root:

```bash
uv run chatsbom db edges
uv run chatsbom export d1 --output dist/d1
cd web
npx wrangler d1 create chatsbom   # once: its id replaces the placeholder in wrangler.jsonc
for f in ../dist/d1/[0-9][0-9]-*.sql; do
  npx wrangler d1 execute chatsbom --remote --file "$f" || break
done
npm ci && npm run build && npm run deploy
```

The files are applied in the order of their names: `01-schema.sql`,
then the rows, a table at a time in parts of at most 50 MB
(`02-<table>-0001.sql` onwards; `observations`, each source's date for
each repository, is one of the tables since #41), `03-aggregates.sql`,
and `04-indexes.sql`. Any file can be applied again, so an import that
fails goes on from the file that failed.

`chatsbom export parquet` still writes the dataset as Parquet, one
content-addressed file a table, `<table>-<8 hex>.parquet`, and a
`manifest.json` that names them, for DuckDB, pandas or a release.
Nothing here serves or reads them.
