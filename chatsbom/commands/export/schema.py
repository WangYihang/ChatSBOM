from pathlib import Path

import structlog
import typer

from chatsbom.core.logging import console
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.typescript import render_typescript

logger = structlog.get_logger('export_schema')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    json_path: Path | None = typer.Option(
        None, '--json', help='Write the schema as JSON to this path',
    ),
    typescript_path: Path | None = typer.Option(
        None, '--typescript', help='Write generated TypeScript types to this path',
    ),
) -> None:
    """Emit the export contract as JSON and/or TypeScript.

    The dashboard's types are generated from this, so a renamed column
    breaks the TypeScript build instead of returning undefined at runtime.
    """
    if json_path is None and typescript_path is None:
        console.print(EXPORT_SCHEMA.to_json(), end='')
        return

    for path, render in (
        (json_path, EXPORT_SCHEMA.to_json),
        (typescript_path, lambda: render_typescript(EXPORT_SCHEMA)),
    ):
        if path is None:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render(), encoding='utf-8')
        console.print(f'[green]Wrote[/green] {path}')
