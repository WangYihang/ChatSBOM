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

# 7. Index into database
uv run python -m chatsbom db index

# 8. Show database statistics
uv run python -m chatsbom db status

# 9. Query dependencies
uv run python -m chatsbom db query gin

# 10. Chat with the database
uv run python -m chatsbom chat
