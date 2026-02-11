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

# 6. Generate SBOMs
uv run python -m chatsbom sbom generate

# 6.5 Generate framework usage
uv run python -m chatsbom openapi candidates

# 6.6 Clone repositories
uv run python -m chatsbom openapi clone

# 6.7 Search for OpenAPI specs
uv run python -m chatsbom openapi search

# 7. Index into database
uv run python -m chatsbom db index

# 8. Show database statistics
uv run python -m chatsbom db status

# 9. Query dependencies
uv run python -m chatsbom db query gin

# 10. Chat with the database
uv run python -m chatsbom chat
