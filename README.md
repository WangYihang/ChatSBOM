# SBOM Insight

SBOM Insight is a powerful CLI tool designed to provide insights into Software Bill of Materials (SBOM) data. It enables researchers and developers to:

1.  **Search** GitHub for high-quality repositories by star count and language.
2.  **Download** SBOM-related files (e.g., `go.mod`, `pom.xml`, `package.json`, `requirements.txt`) efficiently.
3.  **Import** metadata and dependency information into a ClickHouse database for analysis.
4.  **Query** the database to find library usage, framework statistics, and more.
5.  **Visualize** and interact with the data through a terminal-based agent.

## Installation

### Prerequisites

- Python 3.12+
- Docker & Docker Compose (for the database)

### Install via Pip

You can install the package directly in editable mode:

```bash
pip install -e .
```

## Infrastructure Setup

SBOM Insight uses ClickHouse as its backend database. Start the required services using Docker Compose:

```bash
docker compose up -d
```

This will start:
- **ClickHouse Server**: Stores repository and artifact data (Ports: 8123, 9000).

## Configuration

Environmental configuration is handled via a `.env` file. A sample is provided in `.env.example`.

## Usage Guide

### 1. Search Repositories

Search for GitHub repositories based on language and star count.

```bash
# Search for Go repositories with >1000 stars
sbom-insight search-github --language go --min-stars 1000 --output data/go.jsonl
```

**Options:**
- `--language`: Target programming language (e.g., `go`, `python`, `java`).
- `--min-stars`: Minimum number of stars (default: 1000).
- `--output`: Output JSONL file path (default: `{language}.jsonl` in current dir, recommend using `data/`).

### 2. Download SBOM Files

Download dependency files from the repositories found in the search step.

```bash
# Download SBOM files for Go repositories found in data/go.jsonl
sbom-insight download-sbom --language go --input-file data/go.jsonl --output-dir data
```

**Options:**
- `--input-file`: Path to the JSONL file containing repo metadata.
- `--output-dir`: Directory to save downloaded files.
- `--concurrency`: Number of concurrent download threads (default: 32).

### 3. Import Data

Import the downloaded repository metadata and scanned SBOM artifacts into ClickHouse.

```bash
# Import Go data
sbom-insight import --language go
# Or specify a file directly
sbom-insight import --input-file data/go.jsonl
```

**Options:**
- `--clean`: Drop existing tables before importing.
- `--language`: Import default files for specific languages.
- `--input-file`: Import a specific JSONL file.

### 4. Query Data

Quickly find which repositories depend on a specific library.

```bash
# Find projects using 'requests' library
sbom-insight query requests

# Find projects using 'gin' in Go
sbom-insight query gin --language go
```

### 5. Summarize Statistics

Get a high-level overview of the database, including total repositories, top languages, and framework usage.

```bash
sbom-insight summarize
```

### 6. Interactive Agent (Experimental)

Launch an AI-powered TUI agent to query the database using natural language.

```bash
sbom-insight agent
```

*Note: Requires `ANTHROPIC_API_KEY` to be set in `.env` if using the default Claude backend.*

## Development

To run tests or contribute:

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest
```
