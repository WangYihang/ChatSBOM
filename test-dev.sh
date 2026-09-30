#!/bin/bash -x

# 1. Search for repositories (e.g., top Go repos)
uv run python -m chatsbom github search --language go --min-stars 40000

# 2. Enrich Repository metadata
uv run python -m chatsbom github repo --language go

# 3. Enrich Release information
uv run python -m chatsbom github release --language go

# 4. Resolve Commits
uv run python -m chatsbom github commit --language go

# 5. Download dependency files
uv run python -m chatsbom github content --language go

# 6. Generate SBOMs
uv run python -m chatsbom sbom generate

# 7. Index: the warehouse, rebuilt from the store
uv run python -m chatsbom warehouse build

# 8. Publish a snapshot of it, for the web service
uv run python -m chatsbom snapshot build

# 9. Query dependencies: who uses gin, with the DuckDB CLI
duckdb -readonly data/warehouse.duckdb \
    "SELECT r.owner || '/' || r.repo AS repository, r.stars, f.version
     FROM facts AS f JOIN repositories AS r ON r.id = f.repository_id
     WHERE f.name = 'github.com/gin-gonic/gin'
     ORDER BY r.stars DESC LIMIT 10"

# 10. Serve the page, its reads and the chat (ALTCHA_HMAC_KEY, and
#     DEEPSEEK_API_KEY for the chat, in .env)
WEB_SNAPSHOT=data/snapshots uv run python -m chatsbom web serve
