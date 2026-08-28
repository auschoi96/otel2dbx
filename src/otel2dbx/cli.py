from __future__ import annotations

import os
import shutil
from pathlib import Path

import typer
from dotenv import load_dotenv, set_key
from rich.console import Console
from rich.table import Table

from otel2dbx.config import (
    DEFAULT_EXPERIMENT_ID,
    DEFAULT_WAREHOUSE_ID,
    DEFAULT_ZEROBUS_REGION,
    PROJECT_ROOT,
    DatabricksTarget,
    UnityCatalogTarget,
    effective_profile,
)
from otel2dbx.databricks import ZerobusOTLPSink
from otel2dbx.errors import OTel2DbxError
from otel2dbx.manifest import RunManifest
from otel2dbx.migration import MigrationRunner, MigrationSummary
from otel2dbx.setup import ensure_experiment, ensure_secret_scope, read_secret_scope
from otel2dbx.sources import OtlpJsonSource
from otel2dbx.zerobus import bootstrap_zerobus

app = typer.Typer(
    no_args_is_help=True,
    help="Migrate OpenTelemetry traces through Zerobus into Databricks managed MLflow.",
)
migrate_app = typer.Typer(no_args_is_help=True, help="Migrate traces from a supported source.")
zerobus_app = typer.Typer(no_args_is_help=True, help="Configure Zerobus OTLP ingestion.")
app.add_typer(migrate_app, name="migrate")
app.add_typer(zerobus_app, name="zerobus")
console = Console()

load_dotenv(PROJECT_ROOT / ".env")


def _uc_target(
    catalog: str | None, schema: str | None, prefix: str | None
) -> UnityCatalogTarget | None:
    provided = [catalog, schema, prefix]
    if any(provided) and not all(provided):
        raise typer.BadParameter(
            "--uc-catalog, --uc-schema, and --table-prefix must be provided together"
        )
    if all(provided):
        return UnityCatalogTarget(str(catalog), str(schema), str(prefix))
    return None


def _sink(
    profile: str | None,
    experiment_id: str,
    warehouse_id: str,
    catalog: str | None,
    schema: str | None,
    prefix: str | None,
    secret_scope: str | None = None,
) -> ZerobusOTLPSink:
    resolved_profile = effective_profile(profile)
    scope = secret_scope or os.getenv("OTEL2DBX_SECRET_SCOPE")
    if scope:
        # Seed the environment from the scope so jobs need no local .env file.
        # Explicitly set environment variables always win.
        for key, value in read_secret_scope(scope, resolved_profile).items():
            os.environ.setdefault(key, value)
    return ZerobusOTLPSink(
        DatabricksTarget(
            profile=resolved_profile,
            experiment_id=experiment_id,
            warehouse_id=warehouse_id,
            uc_target=_uc_target(catalog, schema, prefix),
            zerobus_workspace_id=os.getenv("ZEROBUS_WORKSPACE_ID") or None,
            zerobus_region=os.getenv("ZEROBUS_REGION", DEFAULT_ZEROBUS_REGION),
            zerobus_client_id=os.getenv("ZEROBUS_CLIENT_ID") or None,
            zerobus_client_secret=os.getenv("ZEROBUS_CLIENT_SECRET") or None,
        )
    )


def _print_summary(summary: MigrationSummary, manifest: RunManifest) -> None:
    table = Table(title=f"Migration run {summary.run_id}")
    table.add_column("Metric")
    table.add_column("Count", justify="right")
    for name in (
        "discovered",
        "spans",
        "exported",
        "existing",
        "skipped",
        "verified",
        "failed",
        "warnings",
    ):
        table.add_row(name.replace("_", " ").title(), str(getattr(summary, name)))
    console.print(table)
    console.print(f"Manifest: [cyan]{manifest.path}[/cyan]")


def _run_migration(
    traces: object,
    source_name: str,
    sink: ZerobusOTLPSink,
    *,
    dry_run: bool,
    verify: bool,
    force: bool,
    resume: str | None,
) -> None:
    runner = MigrationRunner(sink, progress=lambda message: console.print(f"  {message}"))
    summary, manifest = runner.run(
        traces,  # type: ignore[arg-type]
        source_name=source_name,
        dry_run=dry_run,
        verify=verify,
        force=force,
        resume_run_id=resume,
    )
    _print_summary(summary, manifest)
    if summary.failed:
        raise typer.Exit(1)


@app.command()
def doctor(
    profile: str | None = None,
    experiment_id: str = DEFAULT_EXPERIMENT_ID,
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
    uc_catalog: str | None = None,
    uc_schema: str | None = None,
    table_prefix: str | None = None,
) -> None:
    """Validate the Databricks destination: workspace auth, experiment, UC spans table, Zerobus."""
    checks: list[tuple[str, bool, str]] = []
    cli_path = shutil.which("databricks")
    checks.append(("databricks CLI", cli_path is not None, cli_path or "not installed"))

    sink = _sink(profile, experiment_id, warehouse_id, uc_catalog, uc_schema, table_prefix)
    destination = None
    try:
        destination = sink.resolve()
        zerobus = sink.validate_zerobus_auth()
        warehouse = sink.validate_warehouse()
        checks.append(("Workspace OAuth", True, sink.target.profile or "ambient (job)"))
        checks.append(("MLflow experiment", True, destination.experiment_id))
        checks.append(("UC spans table", True, destination.uc_target.spans_table))
        checks.append(("Zerobus OAuth", True, zerobus["client_id"]))
        checks.append(("Zerobus OTLP", True, zerobus["endpoint"]))
        checks.append(("SQL warehouse", True, f"{warehouse['name']} ({warehouse['state']})"))
    except Exception as exc:
        checks.append(("Databricks destination", False, str(exc)))
    finally:
        sink.close()

    table = Table(title="otel2dbx doctor")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Details")
    for name, ok, details in checks:
        table.add_row(name, "[green]PASS[/green]" if ok else "[red]FAIL[/red]", details)
    console.print(table)
    if destination is not None:
        console.print(f"Managed MLflow UC table: [cyan]{destination.uc_target.spans_table}[/cyan]")
    if not all(ok for _, ok, _ in checks):
        raise typer.Exit(1)


@zerobus_app.command("bootstrap")
def zerobus_bootstrap(
    profile: str | None = None,
    experiment_id: str = DEFAULT_EXPERIMENT_ID,
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
    service_principal_name: str = "otel2dbx-zerobus-migrator",
    rotate_secret: bool = False,
    uc_catalog: str | None = None,
    uc_schema: str | None = None,
    table_prefix: str | None = None,
) -> None:
    """Provision table-scoped Zerobus OAuth for an experiment already bound to Unity Catalog."""
    sink = _sink(profile, experiment_id, warehouse_id, uc_catalog, uc_schema, table_prefix)
    try:
        destination = sink.resolve()
        result = bootstrap_zerobus(
            profile=sink.target.profile,
            warehouse_id=warehouse_id,
            destination=destination,
            env_path=PROJECT_ROOT / ".env",
            display_name=service_principal_name,
            rotate_secret=rotate_secret,
        )
    finally:
        sink.close()
    action = "created" if result.created else "reused"
    secret_action = "created" if result.secret_created else "reused"
    console.print(
        f"Zerobus service principal {action}: [cyan]{result.display_name}[/cyan] "
        f"({result.application_id})"
    )
    console.print(f"OAuth secret {secret_action} and stored in the ignored .env file.")
    console.print(f"Granted USE CATALOG, USE SCHEMA, SELECT, and MODIFY on {result.table_name}.")


@app.command("setup")
def setup_destination(
    experiment_id: str | None = typer.Option(None, help="Existing MLflow experiment ID."),
    experiment_name: str | None = typer.Option(
        None,
        help="Create or reuse an experiment with this name when --experiment-id is omitted.",
    ),
    profile: str | None = None,
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
    uc_catalog: str = typer.Option(..., help="Unity Catalog catalog for trace storage."),
    uc_schema: str = typer.Option(..., help="Unity Catalog schema for trace storage."),
    table_prefix: str = typer.Option(..., help="Table prefix for the UC trace location."),
    service_principal_name: str = "otel2dbx-zerobus-migrator",
    secret_scope: str | None = typer.Option(
        None,
        help="Also store the Zerobus credentials in this Databricks secret scope.",
    ),
    rotate_secret: bool = False,
    zerobus_workspace_id: str | None = typer.Option(None, envvar="ZEROBUS_WORKSPACE_ID"),
    zerobus_region: str = typer.Option(DEFAULT_ZEROBUS_REGION, envvar="ZEROBUS_REGION"),
    local_env: bool = typer.Option(
        True,
        "--local-env/--no-local-env",
        help="Also write credentials to the local .env file.",
    ),
) -> None:
    """Set up a migration destination: experiment, UC binding, service principal, grants."""
    resolved_profile = effective_profile(profile)
    if zerobus_workspace_id:
        os.environ["ZEROBUS_WORKSPACE_ID"] = zerobus_workspace_id
    os.environ["ZEROBUS_REGION"] = zerobus_region
    if not experiment_id:
        if not experiment_name:
            raise typer.BadParameter("Provide --experiment-id or --experiment-name")
        experiment_id = ensure_experiment(resolved_profile, experiment_name)
        console.print(f"Experiment ready: [cyan]{experiment_id}[/cyan] ({experiment_name})")
    sink = _sink(
        resolved_profile, experiment_id, warehouse_id, uc_catalog, uc_schema, table_prefix
    )
    try:
        destination = sink.resolve()
        result = bootstrap_zerobus(
            profile=resolved_profile,
            warehouse_id=warehouse_id,
            destination=destination,
            env_path=(PROJECT_ROOT / ".env") if local_env else None,
            display_name=service_principal_name,
            rotate_secret=rotate_secret,
        )
    finally:
        sink.close()
    action = "created" if result.created else "reused"
    console.print(
        f"Zerobus service principal {action}: [cyan]{result.display_name}[/cyan] "
        f"({result.application_id})"
    )
    console.print(
        f"Granted USE CATALOG, USE SCHEMA, SELECT, and MODIFY on {result.table_name}."
    )
    if secret_scope:
        ensure_secret_scope(
            secret_scope,
            {
                "zerobus_client_id": result.application_id,
                "zerobus_client_secret": result.client_secret,
                "zerobus_workspace_id": destination.workspace_id,
                "zerobus_region": destination.region,
            },
            resolved_profile,
        )
        console.print(f"Stored Zerobus credentials in secret scope [cyan]{secret_scope}[/cyan].")
    if local_env:
        env_file = PROJECT_ROOT / ".env"
        env_file.touch(mode=0o600, exist_ok=True)
        persisted = {
            "OTEL2DBX_EXPERIMENT_ID": experiment_id,
            "OTEL2DBX_WAREHOUSE_ID": warehouse_id,
        }
        if resolved_profile:
            persisted["OTEL2DBX_DATABRICKS_PROFILE"] = resolved_profile
        for key, value in persisted.items():
            set_key(env_file, key, value, quote_mode="always")
        env_file.chmod(0o600)
        console.print(
            "Saved OTEL2DBX_EXPERIMENT_ID, OTEL2DBX_WAREHOUSE_ID"
            + (", OTEL2DBX_DATABRICKS_PROFILE" if resolved_profile else "")
            + " to .env; later commands run zero-config."
        )
    console.print("\nDestination ready. Migrate with:")
    console.print(
        f"  uv run otel2dbx migrate otlp-json <traces.jsonl> --experiment-id {experiment_id}"
    )


@migrate_app.command("otlp-json")
def migrate_otlp_json(
    path: Path,
    profile: str | None = None,
    experiment_id: str = DEFAULT_EXPERIMENT_ID,
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
    uc_catalog: str | None = None,
    uc_schema: str | None = None,
    table_prefix: str | None = None,
    secret_scope: str | None = typer.Option(
        None,
        "--secret-scope",
        envvar="OTEL2DBX_SECRET_SCOPE",
        help="Databricks secret scope holding Zerobus credentials.",
    ),
    dry_run: bool = False,
    no_verify: bool = False,
    force: bool = False,
    resume: str | None = None,
) -> None:
    """Migrate an OTLP JSON export (OpenTelemetry Collector file-exporter format)."""
    source = OtlpJsonSource(path)
    sink = _sink(
        profile, experiment_id, warehouse_id, uc_catalog, uc_schema, table_prefix, secret_scope
    )
    _run_migration(
        source.iter_traces(),
        f"otlp-json:{path}",
        sink,
        dry_run=dry_run,
        verify=not no_verify,
        force=force,
        resume=resume,
    )


@app.command("verify")
def verify_run(run_id: str) -> None:
    """Display persisted per-trace verification results for a migration run."""
    manifest = RunManifest.load(run_id)
    table = Table(title=f"Verification run {run_id}")
    table.add_column("Source trace")
    table.add_column("Destination trace")
    table.add_column("Spans", justify="right")
    table.add_column("Status")
    for source_id, record in manifest.data.get("traces", {}).items():
        table.add_row(
            source_id,
            str(record.get("destination_trace_id", "")),
            str(record.get("span_count", "")),
            str(record.get("status", "")),
        )
    console.print(table)
    if any(record.get("status") == "failed" for record in manifest.data.get("traces", {}).values()):
        raise typer.Exit(1)


def main() -> None:
    try:
        app()
    except OTel2DbxError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc


if __name__ == "__main__":
    main()
