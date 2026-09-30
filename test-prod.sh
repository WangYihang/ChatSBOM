#!/bin/bash -x

# 1. Search for repositories (e.g., top Go repos)
uv run python -m chatsbom github search --min-stars 1000

# 2. Enrich Repository metadata
uv run python -m chatsbom github repo

# 3. Enrich Release information
uv run python -m chatsbom github release

# 4. Resolve Commits
uv run python -m chatsbom github commit

# 5. Download dependency files
uv run python -m chatsbom github content

# 6. Fetch file trees
uv run python -m chatsbom github tree

# 7. Generate SBOMs
uv run python -m chatsbom sbom generate

# 8. Index: the warehouse, rebuilt from the store
uv run python -m chatsbom warehouse build

# 9. Generate framework usage, from the warehouse: the research tools,
#    chatsbom-research (the `research` extra)
uv run python -m chatsbom.research openapi candidates

# 10. Clone repositories
uv run python -m chatsbom.research openapi clone

# 11. Detect framework drift
uv run python -m chatsbom.research openapi drift

# 12. Publish a snapshot of the warehouse, for the web service
uv run python -m chatsbom snapshot build

# 13. Query dependencies: who uses gin, with the DuckDB CLI
duckdb -readonly data/warehouse.duckdb \
    "SELECT r.owner || '/' || r.repo AS repository, r.stars, f.version
     FROM facts AS f JOIN repositories AS r ON r.id = f.repository_id
     WHERE f.name = 'github.com/gin-gonic/gin'
     ORDER BY r.stars DESC LIMIT 10"

# 14. Serve the page, its reads and the chat (ALTCHA_HMAC_KEY, and
#     DEEPSEEK_API_KEY for the chat, in .env)
WEB_SNAPSHOT=data/snapshots uv run python -m chatsbom web serve
