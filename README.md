# OTEL to Databricks MLflow

This project demonstrates a complete OpenTelemetry trace migration:

```text
Claude Code transcript -> Stop hook -> OTLP Collector -> local Langfuse
                                                |
                                                +-> OTLP archive
Langfuse -> otel2dbx -> Zerobus Ingest OTLP -> Unity Catalog -> MLflow UI
Claude Code transcript -> Stop hook ------------------------------> Zerobus (cutover)
```

It never starts or targets an open-source MLflow tracking server. The destination is always Databricks managed MLflow, with traces stored in Unity Catalog.

Claude Code and Langfuse are only the example workload. Any platform that can produce OTLP — the OpenTelemetry Collector file exporter, Langfuse, LangSmith, an in-house agent framework — migrates through the same destination and verification path.

## Migrate any OTEL traces (self-serve)

The included [Databricks Asset Bundle](databricks.yml) parameterizes the entire destination, so any SA can stand it up in their own workspace:

```bash
# 1. Point the bundle at your workspace: copy the `prod` target in databricks.yml
#    and fill in profile, uc_catalog, warehouse_id, zerobus_workspace_id, zerobus_region.
databricks bundle validate -t prod
databricks bundle deploy -t prod

# 2. One job creates/binds the experiment, the service principal, grants, and a
#    secret scope holding the Zerobus credentials.
databricks bundle run setup_destination -t prod

# 3. Drop any OTLP JSON export into the volume and migrate it.
databricks fs cp traces.jsonl dbfs:/Volumes/<catalog>/<schema>/otel_trace_drops/
databricks bundle run migrate_traces -t prod
```

Prefer to run the migration from a laptop (e.g. the source platform is only reachable locally)? The same two steps work without the bundle:

```bash
uv run otel2dbx setup \
  --experiment-name "Customer X trace migration" \
  --uc-catalog <catalog> --uc-schema <schema> --table-prefix <prefix> \
  --warehouse-id <warehouse> --profile <profile>
uv run otel2dbx migrate otlp-json traces.jsonl --experiment-id <id-from-setup>
```

Notes:

- `zerobus_workspace_id` is the `o=` number in workspace URLs; `zerobus_region` is the workspace region. Both are bundle variables, CLI options, and env vars (`ZEROBUS_WORKSPACE_ID`, `ZEROBUS_REGION`).
- The bundle jobs authenticate with the deployer's ambient identity and read Zerobus credentials from the secret scope created by `setup`; nothing is hardcoded.
- The `otlp-json` adapter replays the original OTLP protobuf unchanged (IDs, kinds, scope, events, links), so it is the preferred adapter for an arbitrary OTEL estate.
- Binding an experiment to a UC trace location is permanent. Interactive runs ask for confirmation first; jobs proceed deliberately.

## Prerequisites

- Python 3.13 and `uv`
- Claude Code 2.1.218 or newer
- Docker Desktop, or Colima plus the standard Docker CLI/Compose, with at least 4 CPUs and 8–16 GB RAM
- A Databricks CLI profile authenticated to your workspace
- A SQL warehouse you have `CAN USE` on (`databricks warehouses list`)
- Permission to create a service principal and grant UC privileges for initial setup
- The Databricks OTel/Unity Catalog tracing previews and Zerobus availability in the region

```bash
uv sync
databricks auth login https://<your-workspace>.cloud.databricks.com --profile <your-profile>
uv run otel2dbx demo init
uv run otel2dbx demo up
uv run otel2dbx zerobus bootstrap
uv run otel2dbx doctor
```

`zerobus bootstrap` creates or reuses a dedicated service principal, grants explicit
`USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` privileges on the MLflow-bound spans
table, and stores its OAuth secret only in the ignored `.env` file. For customer
deployments where identity is centrally managed, set `ZEROBUS_CLIENT_ID` and
`ZEROBUS_CLIENT_SECRET` yourself instead.

For a fresh workspace, `uv run otel2dbx setup` wraps the whole destination setup: it
creates or reuses the MLflow experiment, binds the UC trace location, runs this
bootstrap, and can also store the credentials in a Databricks secret scope
(`--secret-scope`) so bundle jobs need no `.env` file.

If the experiment has no UC trace location, every Databricks command stops without changing it. Supply the intended namespace explicitly:

```bash
--uc-catalog <catalog> --uc-schema <schema> --table-prefix <prefix>
```

Binding is permanent for that experiment. The command requires `USE CATALOG`, `USE SCHEMA`, and `CREATE TABLE`; trace use also requires explicit `SELECT` and `MODIFY` on the generated tables.

## Demo walkthrough

Pre-pull and start the local source first:

```bash
uv run otel2dbx demo init
uv run otel2dbx demo up
```

Sign in to <http://localhost:3000> with `demo@example.com` and the generated password printed by `demo init`. Then use this flow:

```bash
# 1. Generate a safe Claude Code trace with LLM, Read, Bash, and Edit spans.
uv run otel2dbx demo capture
#    (or ten random task traces at once: uv run otel2dbx demo capture --count 10)
#    (or trace a custom LangGraph agent: --agent langgraph, or --agent both)

# 2. Show exactly what will migrate.
uv run otel2dbx migrate langfuse --since 15m --dry-run

# 3. Backfill and verify against managed MLflow.
uv run otel2dbx migrate langfuse --since 15m

# 4. Reopen the persisted parity report.
uv run otel2dbx verify <run-id>

# 5. Prove the cutover: the next trace bypasses Langfuse.
uv run otel2dbx demo reset --no-clear-archive
uv run otel2dbx claude --target zerobus
```

Open the Databricks experiment and select the SQL warehouse in the **Traces** tab:

`https://<your-workspace>.cloud.databricks.com/ml/experiments/<your-experiment-id>/overview/usage`

The Claude wrapper uses an additional settings file and `--setting-sources project`. It does not edit `~/.claude/settings.json`, and it disables the unrelated global MLflow Claude hook for the child process. The hook converts Claude's completed transcript into standard OTLP protobuf. This also works on company-managed machines where policy locks Claude's native OTLP exporter to a corporate endpoint.

## Reusable migration commands

Langfuse v4 API backfill:

```bash
uv run otel2dbx migrate langfuse \
  --from 2026-08-03T16:00:00Z \
  --to 2026-08-03T17:00:00Z
```

Portable OTLP JSON exported by the OpenTelemetry Collector file exporter:

```bash
uv run otel2dbx migrate otlp-json .otel2dbx/archive/claude-traces.jsonl
```

Useful controls:

- `--dry-run`: discover and normalize without exporting
- `--resume <run-id>`: continue from a saved checkpoint
- `--no-verify`: do not wait for Databricks query visibility
- `--force`: resend trace IDs that already exist
- `--secret-scope <scope>`: read Zerobus credentials from a Databricks secret scope
  instead of `.env` (this is how the bundle jobs authenticate)
- `--profile`, `--experiment-id`, `--warehouse-id`: override the demo defaults on any
  destination command
- Default behavior skips destination trace IDs that are already queryable

Run manifests are written to `.otel2dbx/runs/` and contain no credentials.

## Configuration

Nothing is hardcoded to a workspace. Set these once in your gitignored `.env` to run
zero-config, or pass each per command (flag) or per workspace (bundle target in
`databricks.yml`):

| Environment variable | Default | Purpose |
|---|---|---|
| `OTEL2DBX_DATABRICKS_PROFILE` | `DEFAULT` | Databricks CLI profile; unused on Databricks compute, where the ambient identity applies |
| `OTEL2DBX_EXPERIMENT_ID` | required | Destination MLflow experiment (`otel2dbx setup` creates one) |
| `OTEL2DBX_WAREHOUSE_ID` | required | SQL warehouse for grants and trace reads |
| `ZEROBUS_WORKSPACE_ID` | auto-derived | Zerobus OTLP endpoint host; derived from the authenticated profile when unset |
| `ZEROBUS_REGION` | `us-east-1` | Zerobus OTLP endpoint region |
| `OTEL2DBX_SECRET_SCOPE` | unset | Secret scope holding the four Zerobus values; replaces `.env` in jobs |
| `OTEL2DBX_MODEL_ENDPOINT` | `databricks-kimi-k3` | AI Gateway model serving endpoint used by the LangGraph demo agent |

Zerobus provides at-least-once delivery. Deterministic OTEL IDs, destination preflight,
and manifests prevent ordinary reruns; an ambiguous network failure can still produce a
duplicate row when a request is retried after it was accepted. Avoid `--force` unless a
deliberate resend is required.

## Fidelity contract

The OTLP JSON adapter preserves the original protobuf structure and IDs. The payload is
sent unchanged to `https://<workspace-id>.zerobus.<region>.cloud.databricks.com/v1/traces`.
Routing remains outside the payload through `x-databricks-zerobus-table-name`.

Langfuse's v2 Observations API exposes the tree, timestamps, I/O, metadata, resource attributes, model, and usage, but not the original span kind, instrumentation scope, links, or events. The Langfuse adapter reconstructs what the public API exposes and reports this limitation on every run. Non-OTel vendor IDs are mapped deterministically while the originals are retained as attributes.

For Claude traces, the adapter adds standard GenAI attributes so Databricks renders agent, chat, and tool span types and aggregates token usage on the root span.

## Security

- The demo captures full content only from `demo_workspace`, a purpose-built throwaway fixture.
- Do not point full-content Claude telemetry at a real customer repository.
- Langfuse and Zerobus service-principal secrets are excluded from Git.
- Table-scoped Zerobus access tokens are refreshed dynamically, kept in memory, and never
  written into generated settings or manifests.
- `docker compose down` preserves local data; delete volumes only when you intentionally want to destroy the demo database.

## Tests

```bash
uv run pytest
```
