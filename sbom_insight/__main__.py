import typer

from sbom_insight import agent
from sbom_insight import converter
from sbom_insight import crawler
from sbom_insight import downloader
from sbom_insight import importer
from sbom_insight import query
from sbom_insight import summarize

app = typer.Typer(
    help='SBOM Insight CLI: Search GitHub repositories and download SBOMs.',
    no_args_is_help=True,
    add_completion=False,
)

app.command(name='crawl-repos')(crawler.main)
app.command(name='download-sbom')(downloader.main)
app.command(name='convert-sbom')(converter.main)
app.command(name='import-sbom')(importer.main)
app.command(name='query-deps')(query.main)
app.command(name='run-agent')(agent.main)
app.command(name='show-stats')(summarize.main)

if __name__ == '__main__':
    app()
