import typer

from sbom_insight import converter
from sbom_insight import downloader
from sbom_insight import importer
from sbom_insight import searcher

app = typer.Typer(
    help='SBOM Insight CLI: Search GitHub repositories and download SBOMs.',
    no_args_is_help=True,
    add_completion=False,
)

app.command(name='search-github')(searcher.main)
app.command(name='download-sbom')(downloader.main)
app.command(name='convert-sbom')(converter.main)
app.command(name='importer')(importer.main)

if __name__ == '__main__':
    app()
