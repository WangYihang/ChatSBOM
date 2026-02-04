# SBOM Insight

CLI tool to search GitHub repositories and download SBOM-related files.

## Installation

```bash
pip install sbom-insight
```

## Usage

### 1. Search Repositories
Search for repositories by language and star count.

```bash
# Search Go repos with >1000 stars (default)
sbom-insight search-github --language go --min-stars 1000

# Output will be saved to {language}.jsonl (e.g., go.jsonl)
```

### 2. Download SBOM Files
Download SBOM files (e.g., `go.mod`, `pom.xml`, `package.json`) from the search results.

```bash
# Download for Go
sbom-insight download-sbom --language go --input-file go.jsonl --output-dir sbom_data

# Data structure: sbom_data/go/{owner}/{repo}/{branch}/{filename}
```
