uvx sbom-insight crawl-repos --language go --min-stars 1000
uvx sbom-insight download-sbom --language go
uvx sbom-insight convert-sbom --language go
uvx sbom-insight import-sbom --language go
uvx sbom-insight query-deps gin --language go
uvx sbom-insight show-stats
uvx sbom-insight run-agent
