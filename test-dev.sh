#!/bin/bash -x

# 1. Collect a few repositories by hand, every stage now: the release and
#    commit decisions, the tree, every manifest the tree lists, and an
#    SBOM of them
uv run python -m chatsbom collect repo gin-gonic/gin
uv run python -m chatsbom collect repo labstack/echo

# 2. Index: the warehouse, rebuilt from the store
uv run python -m chatsbom warehouse build

# 3. Publish a snapshot of it, for the web service
uv run python -m chatsbom snapshot build

# 4. Query dependencies: who uses gin, with the DuckDB CLI
duckdb -readonly data/warehouse.duckdb \
    "SELECT r.owner || '/' || r.repo AS repository, r.stars, f.version
     FROM facts AS f JOIN repositories AS r ON r.id = f.repository_id
     WHERE f.name = 'github.com/gin-gonic/gin'
     ORDER BY r.stars DESC LIMIT 10"

# 5. Serve the page, its reads and the chat (ALTCHA_HMAC_KEY, and
#    DEEPSEEK_API_KEY for the chat, in .env)
WEB_SNAPSHOT=data/snapshots uv run python -m chatsbom web serve
