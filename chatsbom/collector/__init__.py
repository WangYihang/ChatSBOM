"""The collector: one process that owns every GitHub token's budget and
schedules every stage (#128 section 2.1, #155).

Its foundations (#156):

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

What it detects with them (#160), which nothing runs yet:

- `universe`: the repositories to collect, the newest complete search
  snapshot of those with 1,000 stars or more, searched again weekly,
  and each one's node id in collector.sqlite.
- `sweep`: every repository of the universe asked after hourly by its
  node id, 100 a GraphQL call; what changed, which the stages read, and
  what it cost.
- `retry`: what GitHub failed to answer, asked again after a pause.

Its stages (#161), which `chatsbom collect repo` runs for one repository
by hand; the process that runs them all is 6e's (#155):

- `due`: what is due for a repository, derived from the store, #147's
  decisions and collector.sqlite, along the chain from the push to the
  SBOM; and which repositories to collect next, highest priority first.
- `stages`: the release and commit decisions, the tree, the content and
  the SBOM, written as today's stages write them, by today's rules
  (`releases`, `content`); on the API client, git (`gitremote`), raw
  content with no token (`raw`) and Syft in a pool of `cores - 1` slots
  (`syftpool`).
- `runner`: a repository's due stages run one after another, what each
  did kept, and the repository marked collected.

The dependency graph (#162), which nothing runs yet:

- `depgraph`: each repository's graph through GitHub's report flow, on
  the client and from a bucket of its own; fetched again once a push has
  settled, never within 21 days of the last fetch, and at a 180-day
  backstop; kept in the store's layout, and not again when it is as it
  was.
"""
