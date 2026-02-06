# 1. Collect repository links from GitHub (e.g., top Go repos)
uv run python -m chatsbom collect --language go --min-stars 10000

# 2. Download dependency files
uv run python -m chatsbom download --language go

# 3. Convert to standard SBOM format
uv run python -m chatsbom convert --language go

# 4. Index SBOM data into database
uv run python -m chatsbom index --language go

# 5. Show database statistics
uv run python -m chatsbom status

# 6. Query dependencies
uv run python -m chatsbom query gin --language go

# 7. Launch AI chat interface
uv run python -m chatsbom chat
