from __future__ import annotations

import json
import os
import re
import shutil
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import httpx
import typer
from dotenv import load_dotenv, set_key
from rich.console import Console
from rich.table import Table

from otel2dbx.config import (
    DEFAULT_EXPERIMENT_ID,
    DEFAULT_LANGFUSE_URL,
    DEFAULT_WAREHOUSE_ID,
    DEFAULT_ZEROBUS_REGION,
    PROJECT_ROOT,
    STATE_DIR,
    DatabricksTarget,
    UnityCatalogTarget,
    effective_profile,
)
from otel2dbx.databricks import ZerobusOTLPSink
from otel2dbx.demo import (
    DEFAULT_DEMO_PROMPT,
    initialize_env,
    load_task_bank,
    reset_fixture,
    run_claude,
    sample_tasks,
)
from otel2dbx.demo import (
    down as demo_down,
)
from otel2dbx.demo import (
    status as demo_status,
)
from otel2dbx.demo import (
    up as demo_up,
)
from otel2dbx.errors import OTel2DbxError
from otel2dbx.langgraph_agent import DEFAULT_MODEL_ENDPOINT, run_langgraph_task
from otel2dbx.manifest import RunManifest
from otel2dbx.migration import MigrationRunner, MigrationSummary
from otel2dbx.setup import ensure_experiment, ensure_secret_scope, read_secret_scope
from otel2dbx.sources import LangfuseSource, OtlpJsonSource
from otel2dbx.zerobus import bootstrap_zerobus

app = typer.Typer(
    no_args_is_help=True,
    help="Migrate OTLP traces through Zerobus into Databricks managed MLflow.",
)
demo_app = typer.Typer(no_args_is_help=True, help="Manage the local Langfuse demonstration.")
migrate_app = typer.Typer(no_args_is_help=True, help="Migrate traces from a supported source.")
zerobus_app = typer.Typer(no_args_is_help=True, help="Configure Zerobus OTLP ingestion.")
app.add_typer(demo_app, name="demo")
app.add_typer(migrate_app, name="migrate")
app.add_typer(zerobus_app, name="zerobus")
console = Console()

load_dotenv(PROJECT_ROOT / ".env")


def _hook_trace_ids() -> set[str]:
    path = STATE_DIR / "claude-hook-state.json"
    try:
        return set(json.loads(path.read_text(encoding="utf-8")).get("sent", []))
    except (OSError, ValueError):
        return set()


def _duration(value: str) -> timedelta:
    match = re.fullmatch(r"(\d+)([smhd])", value.strip().lower())
    if not match:
        raise typer.BadParameter("Use a duration such as 30m, 2h, or 1d")
    count = int(match.group(1))
    unit = match.group(2)
    field = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}[unit]
    return timedelta(**{field: count})


def _window(since: str, from_time: str | None, to_time: str | None) -> tuple[datetime, datetime]:
    if bool(from_time) != bool(to_time):
        raise typer.BadParameter("--from and --to must be provided together")
    if from_time and to_time:
        return (
            datetime.fromisoformat(from_time.replace("Z", "+00:00")),
            datetime.fromisoformat(to_time.replace("Z", "+00:00")),
        )
    end = datetime.now(UTC)
    return end - _duration(since), end


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
    """Validate local tools, Zerobus OTLP, and the managed MLflow destination."""
    checks: list[tuple[str, bool, str]] = []
    for binary in ("docker", "claude", "databricks"):
        path = shutil.which(binary)
        checks.append((binary, path is not None, path or "not installed"))
    try:
        response = httpx.get(
            f"{os.getenv('LANGFUSE_BASE_URL', DEFAULT_LANGFUSE_URL)}/api/public/health", timeout=3
        )
        checks.append(("Langfuse", response.is_success, f"HTTP {response.status_code}"))
    except httpx.HTTPError as exc:
        checks.append(("Langfuse", False, str(exc)))
    try:
        response = httpx.get("http://localhost:13133/", timeout=3)
        checks.append(("OTEL Collector", response.is_success, f"HTTP {response.status_code}"))
    except httpx.HTTPError as exc:
        checks.append(("OTEL Collector", False, str(exc)))

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
        checks.append(("Databricks MLflow", False, str(exc)))
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


@demo_app.command("init")
def demo_init(
    force: bool = typer.Option(False, help="Replace the current local demo secrets."),
) -> None:
    path, password = initialize_env(force=force)
    console.print(f"Created [cyan]{path}[/cyan]")
    console.print("Langfuse URL: [link=http://localhost:3000]http://localhost:3000[/link]")
    console.print("Local user: demo@example.com")
    console.print(f"Local password: {password}")


@demo_app.command("up")
def demo_up_command() -> None:
    demo_up()
    console.print("Langfuse is ready at [link=http://localhost:3000]http://localhost:3000[/link]")


@demo_app.command("down")
def demo_down_command() -> None:
    demo_down()


@demo_app.command("status")
def demo_status_command() -> None:
    demo_status()


@zerobus_app.command("bootstrap")
def zerobus_bootstrap(
    profile: str | None = None,
    experiment_id: str = DEFAULT_EXPERIMENT_ID,
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
    service_principal_name: str = "otel2dbx-zerobus-demo",
    rotate_secret: bool = False,
    uc_catalog: str | None = None,
    uc_schema: str | None = None,
    table_prefix: str | None = None,
) -> None:
    """Provision table-scoped Zerobus OAuth for the selected MLflow experiment."""
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
    service_principal_name: str = "otel2dbx-zerobus-demo",
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


@demo_app.command("reset")
def demo_reset_command(clear_archive: bool = True) -> None:
    reset_fixture(clear_archive=clear_archive)
    console.print("Reset the safe demo fixture" + (" and OTLP archive" if clear_archive else ""))


@demo_app.command("capture")
def demo_capture(
    prompt: str = typer.Option(DEFAULT_DEMO_PROMPT, help="Sanitized task sent to Claude Code."),
    count: int = typer.Option(
        1,
        "--count",
        "-n",
        help="Run this many tasks per agent, sampled randomly from its task bank.",
    ),
    seed: int | None = typer.Option(None, help="Seed for reproducible task sampling."),
    agent: str = typer.Option(
        "claude",
        "--agent",
        help="Which agent to trace: claude, langgraph, or both.",
    ),
    model_endpoint: str = typer.Option(
        DEFAULT_MODEL_ENDPOINT,
        envvar="OTEL2DBX_MODEL_ENDPOINT",
        help="Databricks model serving endpoint (AI Gateway) used by the LangGraph agent.",
    ),
    warehouse_id: str = typer.Option(
        DEFAULT_WAREHOUSE_ID,
        envvar="OTEL2DBX_WAREHOUSE_ID",
        help="SQL warehouse the LangGraph agent queries with its read-only SQL tool.",
    ),
    interactive: bool = typer.Option(False, help="Open an interactive Claude session."),
    no_reset: bool = typer.Option(False, help="Keep the current demo fixture state."),
) -> None:
    if agent not in {"claude", "langgraph", "both"}:
        raise typer.BadParameter("--agent must be claude, langgraph, or both")
    if agent != "claude":
        if prompt != DEFAULT_DEMO_PROMPT:
            raise typer.BadParameter("--prompt is only supported with --agent claude --count 1")
        if interactive:
            raise typer.BadParameter("--interactive is only supported with --agent claude")
    if count > 1:
        if prompt != DEFAULT_DEMO_PROMPT:
            raise typer.BadParameter("--prompt is only supported with --count 1")
        if interactive:
            raise typer.BadParameter("--interactive is only supported with --count 1")
    agents = ["claude", "langgraph"] if agent == "both" else [agent]
    before = _hook_trace_ids()
    if not no_reset:
        reset_fixture(clear_archive=False)
    langgraph_traces: list[str] = []
    for name in agents:
        if count == 1:
            prompts = [prompt] if name == "claude" else [load_task_bank("langgraph")[0]]
        else:
            prompts = sample_tasks(count, seed=seed, agent=name)
        for index, task_prompt in enumerate(prompts, start=1):
            console.print(
                f"[bold]{name} task {index}/{len(prompts)}[/bold]: {task_prompt[:100]}"
            )
            if name == "claude":
                code = run_claude(target="langfuse", prompt=task_prompt, interactive=interactive)
                if code:
                    raise typer.Exit(code)
            else:
                result = run_langgraph_task(
                    task_prompt,
                    model_endpoint=model_endpoint,
                    profile=effective_profile(None),
                    warehouse_id=warehouse_id,
                )
                langgraph_traces.append(result.trace_id)
                console.print(f"  answer: {result.answer[:200]}")
    created = sorted(_hook_trace_ids() - before)
    total = len(created) + len(langgraph_traces)
    noun = "trace" if total == 1 else "traces"
    console.print(f"{total} new {noun} sent through the local OTEL Collector to Langfuse.")
    for trace_id in created:
        console.print(f"Claude trace: [cyan]{trace_id}[/cyan]")
    for trace_id in langgraph_traces:
        console.print(f"LangGraph trace: [cyan]{trace_id}[/cyan]")
    console.print("Source UI: [link=http://localhost:3000]http://localhost:3000[/link]")


@migrate_app.command("langfuse")
def migrate_langfuse(
    since: str = typer.Option("15m", help="Discovery window when --from/--to are omitted."),
    from_time: str | None = typer.Option(None, "--from", help="Inclusive ISO-8601 start time."),
    to_time: str | None = typer.Option(None, "--to", help="Exclusive ISO-8601 end time."),
    langfuse_url: str = typer.Option(DEFAULT_LANGFUSE_URL, envvar="LANGFUSE_BASE_URL"),
    public_key: str | None = typer.Option(None, envvar="LANGFUSE_PUBLIC_KEY", hidden=True),
    secret_key: str | None = typer.Option(None, envvar="LANGFUSE_SECRET_KEY", hidden=True),
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
    start, end = _window(since, from_time, to_time)
    source = LangfuseSource(
        langfuse_url,
        public_key or os.getenv("LANGFUSE_PUBLIC_KEY", ""),
        secret_key or os.getenv("LANGFUSE_SECRET_KEY", ""),
    )
    sink = _sink(
        profile, experiment_id, warehouse_id, uc_catalog, uc_schema, table_prefix, secret_scope
    )
    _run_migration(
        source.iter_traces(start, end),
        f"langfuse:{langfuse_url}:{start.isoformat()}:{end.isoformat()}",
        sink,
        dry_run=dry_run,
        verify=not no_verify,
        force=force,
        resume=resume,
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


@app.command("claude")
def claude_cutover(
    target: Annotated[str, typer.Option(help="langfuse or zerobus")] = "zerobus",
    prompt: str = typer.Option(DEFAULT_DEMO_PROMPT),
    interactive: bool = False,
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
) -> None:
    """Run Claude with process-scoped OTLP routing; does not change global settings."""
    if target not in {"langfuse", "zerobus"}:
        raise typer.BadParameter("--target must be langfuse or zerobus")
    resolved_profile = effective_profile(profile)
    destination = None
    sink = None
    if target == "zerobus":
        sink = _sink(
            profile, experiment_id, warehouse_id, uc_catalog, uc_schema, table_prefix, secret_scope
        )
        try:
            destination = sink.resolve()
        finally:
            sink.close()
    before = _hook_trace_ids()
    code = run_claude(
        target=target,  # type: ignore[arg-type]
        prompt=prompt,
        destination=destination,
        profile=resolved_profile,
        warehouse_id=warehouse_id,
        interactive=interactive,
    )
    if code:
        raise typer.Exit(code)
    created = sorted(_hook_trace_ids() - before)
    if not created:
        console.print("[yellow]Claude completed, but the hook did not record a new trace.[/yellow]")
        raise typer.Exit(1)
    trace_id = created[-1]
    if target == "langfuse":
        console.print(f"Langfuse trace: [cyan]{trace_id}[/cyan]")
        return
    verification_sink = _sink(
        profile, experiment_id, warehouse_id, uc_catalog, uc_schema, table_prefix, secret_scope
    )
    try:
        deadline = time.monotonic() + 120
        trace = None
        while time.monotonic() < deadline:
            trace = verification_sink.get_trace(trace_id)
            if trace is not None:
                break
            time.sleep(2)
        if trace is None:
            console.print(
                "[yellow]Databricks accepted trace "
                f"{trace_id}, but it is not queryable yet.[/yellow]"
            )
            raise typer.Exit(1)
        console.print(
            f"Zerobus cutover verified: [cyan]{trace_id}[/cyan] ({len(trace.data.spans)} spans)"
        )
    finally:
        verification_sink.close()


def main() -> None:
    try:
        app()
    except OTel2DbxError as exc:
        console.print(f"[red]Error:[/red] {exc}")
        raise typer.Exit(1) from exc


if __name__ == "__main__":
    main()
