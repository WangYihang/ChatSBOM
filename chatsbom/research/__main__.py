"""`chatsbom-research`: the research tools' command (#167).

The distribution's second command, beside `chatsbom`, and started as it
is (chatsbom/__main__.py): the `.env` nearest the working directory
first, then logging.
"""
import typer

from chatsbom.core import config
from chatsbom.core.logging import setup_logging
from chatsbom.research.commands import classify
from chatsbom.research.commands import openapi
from chatsbom.research.commands import readme

app = typer.Typer(
    help=(
        'ChatSBOM research tools: OpenAPI analyses and LLM classification '
        'of the corpus the collector gathers.'
    ),
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)

app.add_typer(openapi.app, name='openapi')
app.add_typer(classify.app, name='classify')
app.add_typer(readme.app, name='readme')


@app.callback()
def main(
    debug: bool = typer.Option(False, '--debug', help='Enable debug logging'),
):
    """
    ChatSBOM research tools: OpenAPI analyses and LLM classification.
    """
    # As `chatsbom` starts: the `.env` before anything reads the
    # environment, logging from CHATSBOM_LOG_FORMAT, and each command's
    # options after, `--api-key` from OPENAI_API_KEY among them.
    config.load_env_file()
    setup_logging(level='DEBUG' if debug else 'INFO')


if __name__ == '__main__':
    app()
