#!/bin/bash -x

# 1. Collect repository links from GitHub (e.g., top Go repos)
uv run python -m chatsbom collect --min-stars 20000

# 2. Enrich
uv run python -m chatsbom enrich

# 3. Download dependency files
uv run python -m chatsbom download

# 4. Convert to standard SBOM format
uv run python -m chatsbom convert

# 5. Index SBOM data into database
uv run python -m chatsbom index

# 6. Show database statistics
uv run python -m chatsbom status

# 7. Query dependencies
uv run python -m chatsbom query gin
