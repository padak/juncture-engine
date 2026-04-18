"""CLI sub-app: ``juncture debug diagnostics``."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

debug_app = typer.Typer(
    name="debug",
    help="Diagnostics and repair tools for migration triage.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
_console = Console()


@debug_app.command("diagnostics")
def diagnostics(
    project: Path = typer.Option(Path("."), "--project", "-p"),
    select: list[str] = typer.Option([], "--select", "-s"),
    connection: str | None = typer.Option(None, "--connection", "-c"),
    threads: int = typer.Option(4, "--threads", "-t"),
    show_fixes: bool = typer.Option(
        True,
        "--show-fixes/--no-show-fixes",
        help="Print the fix_hint template next to each error bucket.",
    ),
) -> None:
    """Run the project with ``--continue-on-error`` and classify every error.

    Migration triage shortcut: spawns the runner in continue-on-error
    mode, collects per-statement errors from every EXECUTE model, feeds
    them through :func:`juncture.diagnostics.classify_error`, and
    prints a bucketised summary ("15 type_mismatch, 4 conversion, 2
    missing_object"). Intended to be the first command run against a
    freshly migrated body; once every bucket is down to a handful of
    entries you switch back to ``juncture run``.
    """
    from collections import Counter

    from rich.table import Table

    from juncture.core.runner import Runner, RunRequest
    from juncture.diagnostics import classify_statement_errors

    request = RunRequest(
        project_path=project.resolve(),
        select=select,
        connection=connection,
        threads=threads,
        continue_on_error=True,
        run_tests=False,
    )
    report = Runner().run(request)

    all_errors: list[object] = []
    for model_run in report.models.runs:
        if model_run.result is None:
            continue
        all_errors.extend(model_run.result.statement_errors)

    if not all_errors:
        _console.print(
            Panel(
                "[green]Clean run — no statement errors to classify.[/]",
                title="juncture debug diagnostics",
                border_style="green",
            )
        )
        return

    classifications = classify_statement_errors(all_errors)
    by_bucket: Counter[str] = Counter(c.bucket.value for c in classifications)

    summary = Table(title="Error buckets")
    summary.add_column("Bucket")
    summary.add_column("Count", justify="right")
    for bucket, count in by_bucket.most_common():
        summary.add_row(bucket, str(count))
    _console.print(summary)

    if show_fixes:
        _console.print("\n[bold]Representative error per subcategory:[/]\n")
        seen_subs: set[str] = set()
        for c in classifications:
            if c.subcategory in seen_subs:
                continue
            seen_subs.add(c.subcategory)
            _console.print(
                f"[bold]{c.bucket.value}/{c.subcategory}[/]\n"
                f"  error: {c.error_message.splitlines()[0][:150]}\n"
                f"  fix:   [green]{c.fix_hint}[/]\n"
            )

    raise typer.Exit(code=0)
