# ChatSBOM

**Talk to your Supply Chain. Chat with SBOMs.**

ChatSBOM is a CLI tool for indexing and querying Software Bill of Materials (SBOM) data, providing deep insights into project dependencies.

![Demo](figures/demo.gif)

## Why ChatSBOM?

Standard tools like GitHub Dependency Graph don't allow sorting dependents by popularity (stars) ([isaacs/github#1537](https://github.com/isaacs/github/issues/1537)). ChatSBOM solves this by collecting, indexing, and enabling natural language queries over SBOM data.

## Features

- **Discover**: Find high-quality repositories on GitHub by stars and language.
- **Collect**: Enrich metadata and fetch dependency files (`go.mod`, `package.json`, etc.).
- **Generate**: Transform files into standard SBOM format using [Syft](https://github.com/anchore/syft).
- **Index**: Load SBOM data into [ClickHouse](https://clickhouse.com/) for high-performance queries.
- **Query**: Use the CLI for stats/searches or AI-powered natural language chat.

## Getting Started

### 1. Prerequisites

- [Docker](https://www.docker.com/) (for ClickHouse)
- [Syft](https://github.com/anchore/syft) (for SBOM generation)

### 2. Installation

```bash
# Via pip
pip install chatsbom

# Via pipx
pipx install chatsbom

# Or run directly via uvx
uvx chatsbom
```

### 3. Setup

1. **Start Database**: `docker-compose up -d`
2. **Configure Environment**: Copy `.env.example` to `.env` and add your keys:
   - `GITHUB_TOKEN`: For GitHub API access.
   - `ANTHROPIC_AUTH_TOKEN`: For the `chat` feature.

### 4. Basic Workflow

```bash
# 1. Search and collect data
chatsbom github search --language go --min-stars 10000
chatsbom github repo --language go
chatsbom github release --language go
chatsbom github commit --language go
chatsbom github content --language go

# 2. Generate and index SBOMs
chatsbom sbom generate --language go
chatsbom db index --language go

# 3. Query insights
chatsbom db status
chatsbom db query gin
chatsbom chat
```

## Use Case: Analyzing Framework Adoption

Find the most popular projects depending on a specific library (e.g., `gin`) using natural language.

![Query](figures/use-cases/gin/01.png)
![Result](figures/use-cases/gin/02.png)
