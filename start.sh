uv run python -m sbom_insight crawl-repos --language go --min-stars 10000
uv run python -m sbom_insight download-sbom --language go
uv run python -m sbom_insight convert-sbom --language go
uv run python -m sbom_insight import-sbom --language go
uv run python -m sbom_insight query-deps gin --language go
uv run python -m sbom_insight show-stats
uv run python -m sbom_insight run-agent
