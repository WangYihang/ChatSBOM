# ChatSBOM Dashboard

The dashboard's page, and the Cloudflare Worker (`src/worker.ts`) that
served it until the Python service replaces it (#128). The page holds
none of the data. It asks the service, `chatsbom web serve`, which
snapshot of the dataset is current, and then each question as a GET
under that snapshot, one request a panel; the Ask panel posts a
question, and reads the answer as it streams (#144).

```
browser                                   chatsbom web serve (chatsbom/server/)
  |-- the page, /assets/* ---------------> web/dist/client, the build
  |-- GET /api/meta ---------------------> which snapshot is current
  |-- GET /api/v/<snapshot>/<method> ----> the dataset API, over that snapshot
  |-- GET /api/ask/challenge ------------> an ALTCHA challenge
  |-- POST /api/ask ---------------------> the model's loop, on DeepSeek,
                                           answered as server-sent events
```

| Route | What answers it |
| --- | --- |
| `/api/meta` | the current snapshot's id and provenance, kept a minute |
| `/api/v/<snapshot>/<method>?...` | one method of `DatasetQueries` (`src/backend.ts`), by its name, with its parameters by theirs, asked of that snapshot: kept for good. A snapshot no longer served answers 410 |
| `/api/ask/challenge` | a proof of work for the next question |
| `/api/ask` | `POST {"question", "prior", "altcha"}`: a question, answered as events |
| anything else | the page, `index.html` |

The Worker keeps its code and its tests until the cutover, and answers
none of these: it still has `/api/q` and `/api/chat`, below, and sends
any other path to the page. So the page this tree builds does not work
against a Worker, and a Worker deployed from it serves a page whose
every question fails. The Worker's deployment stays on the build it has
until the cutover.

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

The Python port of these questions, `chatsbom/dataset/` (#138), is held
to D1's answers: every call the suite makes of D1 is kept with its
answer in `test/fixtures/contract/calls.json`, which
`tests/dataset_contract_test.py` asks the Python again. A call of the
suite that the file does not hold, answered as D1 answers it now, fails
and names the command that records the file again, which
`test/contractcalls.ts` explains; and `test/contractcalls.test.ts` asks
D1 every call in the file, so the file cannot fall behind a change to a
D1 statement or to the export.

## The page's reads

The page names a method (`src/d1/client.ts`), and the service asks the
Python dataset API (`chatsbom/dataset/`) the method of that name. It
cannot send SQL, and nothing in the service would take any: each method
owns its statement and checks the parameters it declares.

It asks `/api/meta` once, and every question after under the snapshot
it names: `/api/v/<snapshot>/dependentsOf?directOnly=true&name=mail`,
each parameter written as text, in the order of the names. One question
is one URL, and a snapshot never changes, so its answer is kept for
good, by the browser and by the edge. A question two panels ask at once
is one request. When a pass has published two more snapshots since the
page asked, the service no longer serves the one it asks under, and
answers 410: the page asks `/api/meta` again, past the minute the
browser may keep it, once for all the questions told so at once, and
each of them once more. `test/fixtures/contract/urls.json` records the URL
the page asks for each call the contract suite makes, and the service's
tests hold the service to D1's answer for each
(`test/contracturls.test.ts`).

## The Worker's query endpoint, until the cutover

The Worker's `/api/q` names a method the same way. The allow-list has
no prototype, so `constructor` or `__proto__` is an unknown method, a
400.

The endpoint checks a call before a store sees it: whole numbers where
a method counts, strings no longer than what they name, a body of at
most 4 KiB. A per-client rate limit, `QUERY_RATE_LIMIT`, keyed as
`src/ratelimit.ts` says, runs before the body is read. It is counted by
the `RateLimiter` Durable Object over a window that slides, so a burst
across a boundary of the period gets the limit once, not twice (#115).
A database error is logged and answered with a plain 500, since its
text carries table names and SQL.

## Ask a question

The Ask panel posts a question to the service's `/api/ask`, with the
last three questions and answers of its conversation as text, and the
solution to a proof of work. The service runs the model's loop, and its
tools, itself (#140), and answers with server-sent events, which the
page reads with `fetch` and the body's reader, since an EventSource
cannot post (`src/ask/stream.ts`): `tool`, each tool as it runs, which
the panel lists; `text`, the answer as it is written; `done`; and
`error`. Every code the service fails a question with, before the
answer or during it, is said in the page's words in both languages
(`test/askcodes.test.ts` reads them from the service's source).

The proof of work is ALTCHA's (`src/ask/altcha.ts`), solved before each
question in Web Workers, out of sight: the widget fetches a challenge
from `/api/ask/challenge`, and its solution goes with the question. The
page's policy allows this origin alone and nothing inline, so the page
takes the widget's `altcha/external` entry, with `altcha/altcha.css`
and its PBKDF2 worker imported with `?worker`: each a file of the build,
where the default entry writes a `<style>` and starts its workers from
`blob:` URLs. It is loaded when the first question is asked, not with
the panel.

### The Worker's chat, until the cutover

`/api/chat` answers questions in natural language, one model turn per
request. The loop ran in the page (`src/agent.ts`), which the Worker's
tests still drive it with: post a turn, run the tools the model asked
for, post their results, and again, at most eight turns, since each is
a paid call. A tool that fails goes back as an `is_error` result rather
than being dropped: a missing `tool_result` is a malformed
conversation.

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
(`CHAT_RATE_LIMIT`, counted as the query endpoint's is), that the
conversation is one the page's own loop could have produced
(`parseChatRequest`), Turnstile, and, last, the daily spend cap.

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
  settles the rest at what they cost. A turn is sent once: the SDK's
  retries are off, since a call sent again may be billed again under
  the one hold (#115). Each day's object clears itself an hour after
  the day ends, with an alarm. The dashboard keeps working when the cap
  is reached.

```bash
npx wrangler secret put ANTHROPIC_API_KEY   # required; without it /api/chat answers 503
npx wrangler secret put TURNSTILE_SECRET    # optional; with TURNSTILE_SITE_KEY under vars,
                                            # every question passes Turnstile first
```

`wrangler.jsonc` has the two rate limits, `DAILY_SPEND_CAP_USD` and
`TURNSTILE_SITE_KEY` under `vars`, and names the rest the Worker reads:
`EDGE_SECRET`, `TURNSTILE_HOSTNAMES`, `ANTHROPIC_BASE_URL`, the
ClickHouse settings and `GENERATOR`. The spend counter and the rate
limiter need nothing created by hand; the deploy creates their classes.
DEPLOY.md, section 3, has the details.

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
npm run build             # dist/client, the assets, and dist/chatsbom, the Worker
```

The page asks the Python service, which serves the build: from the
repository's root, `uv run chatsbom web serve` serves `web/dist/client`
on 127.0.0.1:8080 (README, `chatsbom web`). `npm run dev` and `npm run
preview` still run the Worker, which the page no longer asks.

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
