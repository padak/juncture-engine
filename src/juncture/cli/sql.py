"""CLI sub-app: ``juncture sql translate | sanitize | split``."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

sql_app = typer.Typer(
    name="sql",
    help="SQL translation and manipulation utilities.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
_console = Console()


@sql_app.command("translate")
def translate(
    sql: str = typer.Argument(..., help="SQL snippet or path to .sql file."),
    read: str = typer.Option("snowflake", "--from", "-f", help="Source dialect."),
    write: str = typer.Option("duckdb", "--to", "-t", help="Target dialect."),
) -> None:
    """Translate SQL between dialects via SQLGlot."""
    from juncture.parsers.sqlglot_parser import translate_sql

    sql_text = Path(sql).read_text() if Path(sql).exists() else sql
    translated = translate_sql(sql_text, read=read, write=write)
    _console.print(Panel(translated, title=f"{read} -> {write}", border_style="cyan"))


@sql_app.command("sanitize")
def sanitize(
    project: Path = typer.Option(
        Path("."),
        "--project",
        "-p",
        help="Juncture project directory whose models/*.sql files will be re-translated in place.",
    ),
    source_dialect: str = typer.Option("snowflake", "--from-dialect"),
    target_dialect: str = typer.Option("duckdb", "--to-dialect"),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report which files would change without writing them.",
    ),
    schema_aware: bool = typer.Option(
        True,
        "--schema-aware/--no-schema-aware",
        help="Load seed schemas from the project and feed them into translate_sql "
        "so VARCHAR/numeric/timestamp mismatches get TRY_CAST wrappers automatically.",
    ),
) -> None:
    """Re-translate all ``models/*.sql`` files through ``translate_sql``.

    Intended for projects produced by an older migration pass where Snowflake
    SQL was copied verbatim and DuckDB chokes on dialect-specific constructs
    (implicit VARCHAR coercion in CASE, etc.). With ``--schema-aware`` (default)
    the seed schemas are read from the project; ``translate_sql`` then uses them
    to resolve VARCHAR-on-typed comparisons and ``timestamp ± integer`` arithmetic.
    """
    from rich.table import Table

    from juncture.core.project import Project
    from juncture.parsers.sqlglot_parser import translate_sql

    models_dir = project / "models"
    if not models_dir.is_dir():
        _console.print(f"[red]No models/ directory at {models_dir}[/]")
        raise typer.Exit(code=1)

    schema: dict[str, dict[str, str]] | None = None
    if schema_aware:
        try:
            project_obj = Project.load(project)
            schema = project_obj.seed_schemas()
        except Exception as exc:
            _console.print(f"[yellow]Schema-aware mode unavailable: {exc}[/]")
            _console.print("[dim]Falling back to syntax-only translation.[/]")
            schema = None

    table = Table(title=f"Sanitize {source_dialect} -> {target_dialect}")
    table.add_column("Model")
    table.add_column("Before", justify="right")
    table.add_column("After", justify="right")
    table.add_column("Changed", justify="center")

    changed_count = 0
    total = 0
    for sql_file in sorted(models_dir.glob("*.sql")):
        original = sql_file.read_text()
        translated = translate_sql(
            original,
            read=source_dialect,
            write=target_dialect,
            schema=schema,
        )
        changed = translated != original
        total += 1
        if changed:
            changed_count += 1
        mark = "[yellow]yes[/]" if changed else "no"
        table.add_row(sql_file.name, f"{len(original):,}", f"{len(translated):,}", mark)
        if changed and not dry_run:
            sql_file.write_text(translated)

    _console.print(table)
    summary = f"{changed_count}/{total} model(s) updated"
    if schema:
        summary += f" (schema-aware, {len(schema)} seed(s) annotated)"
    if dry_run:
        summary += " (dry-run, nothing written)"
    _console.print(Panel(summary, title="sql sanitize", border_style="green"))


@sql_app.command("split")
def split(
    sql_path: Path = typer.Argument(
        ...,
        help="Multi-statement EXECUTE SQL file to split (e.g. models/main_task.sql).",
    ),
    out_dir: Path = typer.Option(
        ...,
        "--out",
        "-o",
        help="Target directory for generated models. Created if missing.",
    ),
    source_dialect: str = typer.Option(
        "duckdb",
        "--source-dialect",
        help="Dialect for SQLGlot parse (normally duckdb after sync-pull translation).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="List what would be created without writing files.",
    ),
) -> None:
    """Split a multi-statement EXECUTE script into CTAS mini-models + residual.

    Each ``CREATE [OR REPLACE] TABLE|VIEW X AS SELECT ...`` becomes its own
    ``<X>.sql`` model with intra-script table references rewritten to
    ``{{ ref('X') }}``. Non-CTAS statements (INSERT, UPDATE, DELETE, ALTER,
    SET, USE) land in ``_residual.sql`` with ``materialization: execute``.
    """
    from rich.table import Table

    from juncture.migration.split_execute import SplitExecuteError, split_execute_script

    sql = sql_path.read_text()
    try:
        result = split_execute_script(sql, dialect=source_dialect)
    except SplitExecuteError as exc:
        _console.print(f"[red]split failed:[/] {exc}")
        raise typer.Exit(code=1) from exc

    summary = Table(title=f"sql split — {sql_path.name}")
    summary.add_column("Model")
    summary.add_column("Materialization")
    summary.add_column("Source idx", justify="right")
    for m in result.models:
        summary.add_row(m.name, m.materialization, str(m.source_index))
    if result.residual:
        summary.add_row("_residual", "execute", "—")
    _console.print(summary)

    if dry_run:
        _console.print(
            Panel(
                f"Dry-run: would create {len(result.models)} model(s)"
                + (" + _residual" if result.residual else "")
                + f" in {out_dir}",
                title="sql split",
                border_style="cyan",
            )
        )
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    for m in result.models:
        body = m.body.rstrip(";").strip()
        (out_dir / f"{m.name}.sql").write_text(
            f"-- Auto-generated by `juncture sql split` from {sql_path.name}.\n"
            f"-- Source statement index: {m.source_index}\n"
            f"{body}\n"
        )

    schema_entries: list[str] = []
    for m in result.models:
        schema_entries.append(
            f"  - name: {m.name}\n"
            f"    materialization: {m.materialization}\n"
            f"    config:\n"
            f"      generated_by: sql-split\n"
            f"      source_index: {m.source_index}\n"
        )

    if result.residual:
        ref_loads = "".join(
            f"SELECT 1 FROM {{{{ ref('{name}') }}}} LIMIT 0;\n" for name in result.residual_depends_on
        )
        (out_dir / "_residual.sql").write_text(
            f"-- Auto-generated by `juncture sql split` from {sql_path.name}.\n"
            f"-- Non-CTAS statements (INSERT/UPDATE/DELETE/DDL/DML).\n"
            f"-- LIMIT 0 ref() hints seed Juncture's DAG depends_on.\n"
            f"{ref_loads}"
            f"\n"
            f"{result.residual}\n"
        )
        schema_entries.append(
            "  - name: _residual\n"
            "    materialization: execute\n"
            "    config:\n"
            "      generated_by: sql-split\n"
        )

    (out_dir / "schema.yml").write_text("models:\n" + "\n".join(schema_entries))

    _console.print(
        Panel(
            f"[green]Split complete:[/]\n"
            f"  {len(result.models)} CTAS model(s) written to {out_dir}\n"
            f"  {'1 residual (depends on ' + str(len(result.residual_depends_on)) + ' models)' if result.residual else 'no residual'}\n\n"
            f"Next: [bold]juncture compile --project <project>[/] to see the new DAG.",
            title="sql split",
            border_style="green",
        )
    )
