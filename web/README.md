# ChatSBOM Dashboard

The dashboard's page: React and TypeScript, which Vite builds into
`dist/client`. The page holds none of the data. The web service,
`chatsbom web serve` (`chatsbom/server/`), serves the build and answers
everything the page asks: which snapshot of the dataset is current, and
then each question as a GET under that snapshot, one request a panel;
and the Ask panel's questions, whose answers it reads as they stream
(#144).

```
browser                                   chatsbom web serve (chatsbom/server/)
  |-- the page, /assets/* ---------------> web/dist/client, the build
  |-- GET /api/meta ---------------------> which snapshot is current
  |-- GET /api/v/<snapshot>/<method> ----> the dataset API, over that snapshot
  |-- GET /api/ask/challenge ------------> an ALTCHA challenge
  |-- POST /api/ask ---------------------> the model's loop, on DeepSeek,
                                           answered as server-sent events
  |-- GET /export/manifest.json, <file> -> the weekly Parquet export, as
                                           the collector wrote it
```

| Route | What answers it |
| --- | --- |
| `/api/meta` | the current snapshot's id and provenance, kept a minute |
| `/api/v/<snapshot>/<method>?...` | one method of the page's client (`src/dataset/client.ts`), by its name, with its parameters by theirs, asked of that snapshot: kept for good. A snapshot no longer served answers 410 |
| `/api/ask/challenge` | a proof of work for the next question |
| `/api/ask` | `POST {"question", "prior", "altcha"}`: a question, answered as events |
| `/export/manifest.json` | the weekly Parquet export's manifest (#154), which the footer links to: kept five minutes |
| `/export/<file>` | each file the manifest names, kept for good, and read in ranges: DuckDB's `SELECT * FROM 'https://<the site>/export/<file>'` |
| anything else | the page, `index.html` |

Until #151 a Cloudflare Worker served the page, answered its questions
from D1 or ClickHouse, and relayed the model's turns; it is gone, and
so are its tests and its toolchain.

## The page's reads

The page names a method (`src/dataset/client.ts`), and the service asks
the Python dataset API (`chatsbom/dataset/`) the method of that name. It
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
each of them once more.

What an answer looks like is declared once, in `src/dataset/types.ts`,
and the Python's answer types are held to it field for field.

## The contract

Before the Worker was deleted, its two stores were asked the same
questions about one small corpus and held to one answer, and every call
that suite made of D1 was recorded with D1's answer:
`test/fixtures/contract/calls.json`, over the corpus as D1 held it,
`test/fixtures/contract/d1.sql`, made from the seed in
`test/fixtures/contract/build.py`. They are kept, and nothing records
them again.

- `tests/dataset_contract_test.py` asks the Python dataset API every
  recorded call over the same corpus, and expects D1's answer. It reads
  the page's client too, and fails if the page asks a method the Python
  lacks, or the Python offers one the page does not ask.
- `test/contracturls.test.ts` records the URL the page's client asks for
  each call, in `test/fixtures/contract/urls.json`, and
  `tests/server_queries_test.py` asks the service each URL and expects
  D1's answer. A parameter the page names otherwise than the service
  reads it fails there.

A method the page comes to ask needs a call written into `calls.json`,
with its answer, by hand, and its URL recorded again, as
`test/contracturls.test.ts` says how.

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

The page loads neither the model's instructions nor its tools'
descriptions, and runs no loop of its own (`test/split.test.ts`): the
service holds them, and sends them to the model.

## The page

Two views, the overview and the query view for one package, are peers:
a bar in the overview hands its package to the query view, and the
segmented control or Back returns. The view is in the hash
(`#/query/mail`), so it can be linked, with no router library and no
server that knows about routes (`src/router.ts`, `src/hooks.ts`).

The footer says what the page is showing, and links to the dataset
itself: the export's manifest, and the DuckDB query that reads one of
its files where the service serves it, at the page's own address, in
the reader's language (`src/app.tsx`, `test/download.test.tsx`).

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
npm run typecheck         # the page, and its tests
npm run lint
npm run validate:palette
npm test
npm run build             # dist/client: index.html and its assets
```

The service serves the build: from the repository's root, with
`ALTCHA_HMAC_KEY` and `WEB_SNAPSHOT` set, `uv run chatsbom web serve`
serves `web/dist/client` on 127.0.0.1:8080 (README, `chatsbom web`).
`npm run dev` serves the page from its sources instead, reloading as
they change, and hands `/api/` to that service.

The web service's image builds the page in a stage of its own, and
carries `dist/client` and nothing of Node (DEPLOY.md).
