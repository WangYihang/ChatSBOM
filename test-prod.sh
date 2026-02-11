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

# 8. Generate framework usage
uv run python -m chatsbom openapi candidates

# 9. Clone repositories
uv run python -m chatsbom openapi clone

# 10. Detect framework drift
uv run python -m chatsbom openapi drift

# 11. Plot framework drift
uv run python -m chatsbom openapi plot-drift

# 12. Index into database
uv run python -m chatsbom db index

# 13. Show database statistics
uv run python -m chatsbom db status

# 14. Query dependencies
uv run python -m chatsbom db query gin

# 15. Chat with the database
uv run python -m chatsbom chat
