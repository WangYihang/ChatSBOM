import typer

from sbom_insight.commands import convert_sbom
from sbom_insight.commands import crawl_repos
from sbom_insight.commands import download_sbom
from sbom_insight.commands import import_sbom
from sbom_insight.commands import query_deps
from sbom_insight.commands import run_agent
from sbom_insight.commands import show_stats

app = typer.Typer(
    help='SBOM Insight CLI: Search GitHub repositories and download SBOMs.',
    no_args_is_help=True,
    add_completion=False,
)

app.command(name='crawl-repos')(crawl_repos.main)
app.command(name='download-sbom')(download_sbom.main)
app.command(name='convert-sbom')(convert_sbom.main)
app.command(name='import-sbom')(import_sbom.main)
app.command(name='query-deps')(query_deps.main)
app.command(name='run-agent')(run_agent.main)
app.command(name='show-stats')(show_stats.main)

if __name__ == '__main__':
    app()
