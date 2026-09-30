"""The resolver: `chatsbom sbom lock` as a service of its own (#128
section 2.1, #168), beside the collector and with none of its tokens.

It resolves lockfiles for the directories of the store's content roots
that ship none, each in a container of project-controlled code whose
one way out is a proxy to the package registries (`core/sandbox.py`,
`core/egress.py`):

- `due`: which directories are due, from the store: those of each
  repository's current commit whose manifest has no lockfile, shipped
  or resolved, and no failure still backing off; the most-starred
  repositories first.
- `state`: resolver.sqlite, the failures and when each is tried again.
- `service`: a pass over what is due, and the loop that runs one after
  another, and sleeps while nothing is due.
"""
