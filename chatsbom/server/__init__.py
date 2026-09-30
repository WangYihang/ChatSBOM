"""The web service (#128, section 2.5): one FastAPI process, on
uvicorn, `chatsbom web serve`. It replaced the Worker (#151).

It comes in parts. The first needs no dataset (#134):

  app        the routes, the headers, and uvicorn as `web serve` runs it
  settings   what it is configured with, read before it starts
  clients    who a request is from: the trust rule for CF-Connecting-IP
  ratelimit  per-client limits, over a window that slides
  challenge  ALTCHA challenges, issued and verified
  spend      the daily spend cap's ledger
  state      web.sqlite, which the ledger and the used challenges are in
  watchdog   the watch on its own event loop

The chat is the second (#140), on DeepSeek:

  ask        POST /api/ask: the checks, the question loop, its events
  model      one streamed turn of the model, and how it failed
  tools      the tools the model may call: the dataset API, bounded
  prompt     what the model is told: the system prompt, and the tools
  pricing    what a turn costs, and the most it can, by the hour

And the page's reads, versioned by snapshot (#144):

  queries    GET /api/meta, and GET /api/v/{snapshot}/{method}: the
             dataset's questions, each kept for good under its snapshot

`app`, `ask`, `challenge`, `model` and `queries` import the `web`
extra's libraries; `web serve` imports them once it has checked for the
extra.
"""
