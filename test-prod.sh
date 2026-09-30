#!/bin/bash -x

# 1. The collector, until Ctrl-C: the universe, every repository with
#    1,000 stars or more, swept hourly for what changed; every
#    repository's stages, its dependency graph, and the warehouse and a
#    snapshot of it, daily. Stop it once it has collected enough.
uv run python -m chatsbom collect

# 2. Index: the warehouse, rebuilt from the store, now
uv run python -m chatsbom warehouse build

# 3. Generate framework usage, from the warehouse: the research tools,
#    chatsbom-research (the `research` extra)
uv run python -m chatsbom.research openapi candidates

# 4. Clone repositories
uv run python -m chatsbom.research openapi clone

# 5. Detect framework drift
uv run python -m chatsbom.research openapi drift

# 6. Publish a snapshot of the warehouse, for the web service
uv run python -m chatsbom snapshot build

# 7. Query dependencies: who uses gin, with the DuckDB CLI
duckdb -readonly data/warehouse.duckdb \
    "SELECT r.owner || '/' || r.repo AS repository, r.stars, f.version
     FROM facts AS f JOIN repositories AS r ON r.id = f.repository_id
     WHERE f.name = 'github.com/gin-gonic/gin'
     ORDER BY r.stars DESC LIMIT 10"

# 8. Serve the page, its reads and the chat (ALTCHA_HMAC_KEY, and
#    DEEPSEEK_API_KEY for the chat, in .env)
WEB_SNAPSHOT=data/snapshots uv run python -m chatsbom web serve
