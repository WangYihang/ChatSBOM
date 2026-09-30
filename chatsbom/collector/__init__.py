"""The collector: one process that owns every GitHub token's budget and
schedules every stage (#128 section 2.1, #155).

Its foundations (#156), which nothing uses yet:

- `state`: collector.sqlite, what the process keeps between runs:
  repositories as last observed, REST validators, `nothing` and failure
  outcomes with their backoff, and the dependency graph's pending
  reports. Never what is done: the store says that.
- `client`: an async GitHub client on httpx2, for REST with conditional
  requests, GraphQL and search, whose errors are typed (`errors`).
- `budget`: every token's buckets, as GitHub's answers say they stand,
  which choose the token for each request.
- `settings`: the tokens, and the reserve each bucket keeps for manual
  work, from the environment.
"""
