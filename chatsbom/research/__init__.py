"""The research tools, `chatsbom-research` (#167).

What the corpus is studied with, not how it is gathered: the collector,
the warehouse, the snapshot and the web service run none of it. So it
is out of the core (the owner's decision of 2026-09-30, #155), as a
command of its own in the same distribution, and one extra, `research`,
for the libraries only it uses: instructor, openai, pandas and tiktoken.

- `openapi`: which projects of each web framework ship an OpenAPI
  specification, read from the warehouse (`candidates`); a snapshot of
  each (`clone`); the paths each specification declares
  (`list-paths`); how far each is from the endpoints its code
  implements (`drift`); and how much of a model's context each
  repository fills (`stats`).
- `classify`: what each repository is, asked of an LLM, with the
  framework its current scan uses; and `readme`, the READMEs it reads.

They read what the core makes, the warehouse, the search snapshots and
the store's trees, and the core never imports them
(tests/research/boundary_test.py).
"""
