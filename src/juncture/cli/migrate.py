"""CLI sub-app: ``juncture migrate keboola | sync-pull``."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

migrate_app = typer.Typer(
    name="migrate",
    help="Migrate Keboola transformations to a Juncture project.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
_console = Console()


@migrate_app.command("keboola")
def keboola(
    config: Path = typer.Argument(..., help="Path to a Keboola SQL transformation config JSON."),
    output: Path = typer.Option(Path("migrated"), "--output", "-o", help="Target project directory."),
    backend: str = typer.Option("duckdb", "--backend", "-b", help="Target adapter (duckdb, snowflake)."),
) -> None:
    """Convert a Keboola SQL transformation config JSON into a Juncture project."""
    from juncture.migration import migrate_keboola_sql_transformation

    result = migrate_keboola_sql_transformation(
        config_json_path=config,
        target_dir=output,
        backend=backend,
    )
    table = Table(title=f"Migrated {len(result.models)} model(s) -> {result.project_path}")
    table.add_column("#", justify="right")
    table.add_column("Model")
    for i, name in enumerate(result.models, start=1):
        table.add_row(str(i), name)
    _console.print(table)
    _console.print(
        Panel(
            f"[green]Project at[/] {result.project_path}\n\n"
            f"Next: [bold]juncture compile --project {result.project_path}[/]",
            title="migrate keboola",
            border_style="green",
        )
    )


@migrate_app.command("sync-pull")
def sync_pull(
    source: Path = typer.Argument(
        ...,
        help="Directory produced by kbagent sync pull, e.g. "
        "main/transformation/keboola.snowflake-transformation/<name>/",
    ),
    output: Path = typer.Option(Path("migrated-sync-pull"), "--output", "-o"),
    seeds: Path = typer.Option(
        ...,
        "--seeds",
        help="Directory with parquet seed data, as produced by "
        "`kbagent storage unload-table --file-type parquet --download`.",
    ),
    duckdb_path: str = typer.Option("data/juncture.duckdb", "--duckdb-path"),
    source_dialect: str = typer.Option(
        "snowflake",
        "--source-dialect",
        help="Source SQL dialect (snowflake, bigquery, redshift...); use 'duckdb' to skip translation.",
    ),
    target_dialect: str = typer.Option("duckdb", "--target-dialect"),
    validate: bool = typer.Option(
        False,
        "--validate",
        help="Pre-flight check only: parse every statement and resolve seed paths, "
        "but don't write any project files. Exit code 1 if parse errors or missing seeds.",
    ),
) -> None:
    """Convert a kbagent sync-pull transformation directory into a Juncture project."""
    from juncture.migration import migrate_keboola_sync_pull, validate_sync_pull_migration

    if validate:
        report = validate_sync_pull_migration(
            transformation_dir=source,
            seeds_source=seeds,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
        )
        table = Table(title=f"Validation — {report.transformation_name}")
        table.add_column("Metric")
        table.add_column("Value", justify="right")
        table.add_row("SQL lines", f"{report.sql_line_count:,}")
        table.add_row("Statements", str(report.statement_count))
        parse_style = "red" if report.parse_errors else "green"
        table.add_row("Parse errors", f"[{parse_style}]{len(report.parse_errors)}[/]")
        table.add_row("Input seeds expected", str(len(report.input_seeds_expected)))
        missing_style = "red" if report.seeds_missing else "green"
        table.add_row("Missing seeds", f"[{missing_style}]{len(report.seeds_missing)}[/]")
        table.add_row("Output tables", str(len(report.output_tables)))
        _console.print(table)

        if report.parse_errors:
            _console.print("\n[bold red]First 5 parse errors:[/]")
            for idx, msg in report.parse_errors[:5]:
                _console.print(f"  [dim]#{idx}[/] {msg.splitlines()[0][:150]}")
            if len(report.parse_errors) > 5:
                _console.print(f"  [dim]... +{len(report.parse_errors) - 5} more[/]")

        if report.seeds_missing:
            _console.print("\n[bold red]Missing seeds:[/]")
            for dest in report.seeds_missing[:10]:
                expected = report.input_seeds_expected.get(dest, "?")
                _console.print(f"  [dim]{dest}[/] (source: {expected})")
            if len(report.seeds_missing) > 10:
                _console.print(f"  [dim]... +{len(report.seeds_missing) - 10} more[/]")

        ok = not report.parse_errors and not report.seeds_missing
        _console.print(
            Panel(
                "[green]Validation passed — ready to migrate.[/]"
                if ok
                else "[yellow]Validation found issues. Fix them or accept partial migration "
                "(see `--continue-on-error` once migrated).[/]",
                title="migrate sync-pull --validate",
                border_style="green" if ok else "yellow",
            )
        )
        if not ok:
            raise typer.Exit(code=1)
        return

    result = migrate_keboola_sync_pull(
        transformation_dir=source,
        output_dir=output,
        seeds_source=seeds,
        duckdb_path=duckdb_path,
        source_dialect=source_dialect,
        target_dialect=target_dialect,
    )
    table = Table(title=f"Migrated transformation {result.transformation_name!r}")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("SQL lines", f"{result.sql_line_count:,}")
    table.add_row(
        "SQL translated", f"{source_dialect} -> {target_dialect}" if result.sql_translated else "no"
    )
    table.add_row("Input seeds linked", str(result.seeds_linked))
    table.add_row("Output tables", str(len(result.output_tables)))
    table.add_row("Missing seeds", str(len(result.seeds_missing)))
    _console.print(table)
    _console.print(
        Panel(
            f"[green]Project at[/] {result.project_path}\n\n"
            f"Next: [bold]juncture compile --project {result.project_path}[/]",
            title="migrate sync-pull",
            border_style="green",
        )
    )
