# ChatSBOM

**Talk to your Supply Chain. Chat with SBOMs.**

ChatSBOM is a CLI tool for deep insights into Software Bill of Materials (SBOM) data.

![Demo](figures/demo.gif)

## Motivation

GitHub's Dependency Graph shows which repositories depend on your project, but there's no way to sort dependents by stars ([isaacs/github#1537](https://github.com/isaacs/github/issues/1537)). This makes it difficult for maintainers of popular packages to identify their most important downstream users. **ChatSBOM** solves this by collecting and indexing SBOM data, enabling queries like "which popular projects use my library?"

## Key Features

- **Search**: Find high-quality repos on GitHub (stars/language).
- **Download**: Fetch SBOM files (`go.mod`, `pom.xml`, etc.).
- **Import**: Load metadata into ClickHouse.
- **Query**: Analyze library usage and framework stats.
- **Agent**: AI-powered TUI for natural language queries.

## Quick Start

### Prerequisites

- [uv](https://github.com/astral-sh/uv) - Python package manager for fast installation and execution of the CLI tool
- [syft](https://github.com/anchore/syft) - SBOM generation tool for extracting dependency data from project files
- [docker](https://github.com/docker/docker) - Container runtime for running infrastructure services
- [docker-compose](https://github.com/docker/compose) - Container orchestration tool for managing multi-container deployments
- [clickhouse](https://github.com/ClickHouse/ClickHouse) - Columnar database for storing and querying SBOM metadata efficiently

### Usage

Install `uv`

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Run commands directly with `uvx`:

```bash
# 1. Collect repository links from GitHub (e.g., top Go repos)
uvx chatsbom collect --language go --min-stars 1000

# 2. Download SBOM files
uvx chatsbom download --language go

# 3. Index SBOM data into database
uvx chatsbom index --language go

# 4. Show database statistics
uvx chatsbom status

# 5. Query dependencies
uvx chatsbom query requests

# 6. Launch AI chat interface
uvx chatsbom chat
```

## Use Cases

### Asking AI Agent to retrieve the top 10 projects using gin framework.

![01](figures/use-cases/gin/01.png)
![02](figures/use-cases/gin/02.png)
