"""The Python web service, which is to replace the Worker (#128,
section 2.5): one FastAPI process, on uvicorn, `chatsbom web serve`.

It comes in parts. The first needs no dataset (#134):

  app        the routes, the headers, and uvicorn as `web serve` runs it
  settings   what it is configured with, read before it starts
  clients    who a request is from: the trust rule for CF-Connecting-IP
  ratelimit  per-client limits, over a window that slides
  challenge  ALTCHA challenges, issued and verified
  spend      the daily spend cap's ledger
  state      web.sqlite, which the ledger and the used challenges are in
  watchdog   the watch on its own event loop

The dataset's routes and the chat come after it. `app` and `challenge`
import the `web` extra's libraries; `web serve` imports them once it has
checked for the extra.
"""
