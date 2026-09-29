"""The Python web service, which is to replace the Worker (#128,
section 2.5): one FastAPI process, on uvicorn.

It comes in parts. The first needs no dataset (#134): who a request is
from, the rate limits, the daily spend ledger, the proof of work a
question will carry, the watch on the service's own event loop, and the
page. The dataset's routes and the chat come after it.
"""
