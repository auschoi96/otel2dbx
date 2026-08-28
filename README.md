# otel2dbx

Migrate an existing OpenTelemetry trace estate into **Databricks managed MLflow** — backfill
history through Zerobus Ingest into Unity Catalog, verify parity trace-by-trace, then cut new
traffic straight over — without rewriting a line of application instrumentation.

> [!IMPORTANT]
> **Unofficial example — not a Databricks product.** This is a personal demonstration repo,
> provided as-is under the [MIT license](LICENSE) with no warranty or support. It exercises
> Databricks capabilities that are still rolling out, so it will not run in every workspace.
> Read [**Can you run this?**](#can-you-run-this) before investing time.

```text
Historical backfill
  Claude Code / LangGraph / any OTEL source
        └─ OTLP ─▶ OTEL Collector ─▶ Langfuse ──▶ otel2dbx ─▶ Zerobus Ingest OTLP
                        └─▶ OTLP JSON archive ──▶ otel2dbx ─┘          │
                                                                       ▼
                                            Unity Catalog trace tables ─▶ managed MLflow Traces UI

Direct cutover
  Claude Code ─▶ Stop hook ─▶ Zerobus Ingest OTLP ─▶ Unity Catalog ─▶ managed MLflow Traces UI
```

The destination is always Databricks managed MLflow with traces stored in Unity Catalog; this
project never starts or targets an open-source MLflow tracking server. Claude Code and Langfuse
are only the *example* workload — any platform that emits OTLP (the OpenTelemetry Collector file
exporter, Langfuse, LangSmith, an in-house framework) migrates through the same path.

## What it demonstrates

- **Backfill:** replay historical traces from a source (a live Langfuse API, or a portable OTLP
  JSON export) into managed MLflow, with a per-trace parity report (visibility, state, span
  count, span names) and idempotent reruns.
- **Cutover:** point new traffic directly at Zerobus and confirm it lands in MLflow without
  touching the old collector or Langfuse.
- **Framework-agnostic proof:** two unrelated workloads — Claude Code (a coding agent, traced via
  a Stop hook) and a LangGraph ReAct agent (traced with the vanilla OpenTelemetry SDK) — flow
  through one collector, one migration path, one destination.

## Can you run this?

There are two halves with very different requirements. **You can run the local half on any
laptop with no Databricks access at all** and watch real OTEL traces flow into a local Langfuse.
The migration and cutover halves need a capable Databricks workspace.

### Local half — any laptop

- **Docker** — Docker Desktop, or Colima + the Docker CLI/Compose. Give it ≥ 4 CPUs and 8–16 GB
  RAM (the local Langfuse stack is Postgres + ClickHouse + Redis + MinIO + the OTEL Collector).
- **Python 3.13** and [`uv`](https://docs.astral.sh/uv/).
- **Claude Code** 2.1.218+ — only for the Claude-capture and cutover steps; the LangGraph path
  does not need it.

### Databricks half — a capable workspace

- **Gating requirement (check this first):** your workspace must have **MLflow tracing on
  OpenTelemetry with Unity Catalog trace locations**, and **Zerobus Ingest**, available. These
  are newer Databricks capabilities and are **not enabled everywhere.**
  **How to check:** run the `otel2dbx setup …` command below. If it fails at the *“bind the
  experiment to Unity Catalog”* step, or if `otel2dbx doctor` shows the Zerobus checks failing,
  your workspace does not have these features yet — the local half still works, but the
  migration and cutover halves will not.
- **Unity Catalog privileges:** `USE CATALOG`, `USE SCHEMA`, and `CREATE TABLE` on a catalog you
  choose, plus permission to **create a service principal** for Zerobus ingestion.
- **A SQL warehouse** you have `CAN USE` on (`databricks warehouses list`).
- **For the LangGraph agent only:** a **model serving endpoint** reachable through AI Gateway
  (default `databricks-claude-sonnet-4-5`; override with `--model-endpoint`).

### Time and cost

- The first `demo up` **pulls several GB** of container images.
- A SQL warehouse and a model serving endpoint incur **normal Databricks costs while running** —
  stop the warehouse when you are done. Everything else this creates (one MLflow experiment, one
  small UC spans table, a secret scope, a service principal) is negligible.
- This is a demo, not a load test: it moves a handful of traces, not production volume.

## Quickstart (guided local demo)

```bash
uv sync
databricks auth login https://<your-workspace>.cloud.databricks.com --profile <your-profile>
uv run otel2dbx demo init          # writes local Langfuse secrets + a login password to .env
uv run otel2dbx demo up            # starts local Langfuse + OTEL Collector (first run pulls images)

# Create the destination in YOUR workspace. Fill in a catalog you can create tables in,
# a SQL warehouse you have CAN USE on, and your profile:
uv run otel2dbx setup \
  --experiment-name "otel2dbx demo" \
  --uc-catalog <catalog> --uc-schema mlflow_traces --table-prefix otel \
  --warehouse-id <warehouse-id> --profile <your-profile>

uv run otel2dbx doctor             # expect every check green before demoing
```

`setup` is the only command a fresh workspace needs. It creates (or reuses) the MLflow
experiment, **permanently** binds it to a Unity Catalog trace location
(`<catalog>.<schema>.<experiment-id>_otel_spans`), creates (or reuses) a dedicated Zerobus
service principal, grants it `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on the spans
table, and writes the Zerobus credentials **plus** `OTEL2DBX_EXPERIMENT_ID` and
`OTEL2DBX_WAREHOUSE_ID` to the gitignored `.env` — so every later command runs zero-config.
Interactive runs confirm before the permanent UC binding. Add `--secret-scope <scope>` to also
store the credentials in a Databricks secret scope (how the bundle jobs authenticate).

> If you already have an experiment bound to a UC trace location, `uv run otel2dbx zerobus
> bootstrap` provisions just the service principal and grants against it instead of running the
> full `setup`.

Only have the local half? You can still run `uv run otel2dbx demo up` and
`uv run otel2dbx demo capture` and watch traces appear in Langfuse — just skip `setup`, `doctor`,
and every `migrate`/`claude --target zerobus` step.

## Demo walkthrough

The full presenter script — with talk track, what to point at in each UI, and recovery steps —
is in **[DEMO.md](DEMO.md)**. The five beats:

```bash
# 1. Create traces in the old system (Langfuse). --agent both adds the LangGraph agent.
uv run otel2dbx demo capture                      # one Claude Code trace (local-only)

# 2. Preview exactly what will migrate — discovery, span counts, ID mapping, fidelity warnings.
uv run otel2dbx migrate langfuse --since 15m --dry-run

# 3. Backfill and prove parity against managed MLflow.
uv run otel2dbx migrate langfuse --since 15m
uv run otel2dbx verify <run-id>

# 4. Show the portable path: lossless OTLP JSON from any OpenTelemetry Collector.
uv run otel2dbx migrate otlp-json .otel2dbx/archive/claude-traces.jsonl

# 5. Cut over: the next trace goes straight to Zerobus, bypassing the collector and Langfuse.
uv run otel2dbx demo reset --no-clear-archive
uv run otel2dbx claude --target zerobus
```

Sign in to the local Langfuse at <http://localhost:3000> with `demo@example.com` and the password
printed by `demo init`. Watch the destination in your experiment's **Traces** tab at
`https://<your-workspace>.cloud.databricks.com/ml/experiments/<your-experiment-id>/overview/usage`
(select the SQL warehouse there once).

The Claude wrapper uses a generated settings file with `--setting-sources project`; it does not
edit `~/.claude/settings.json` and disables the unrelated global MLflow Claude hook for the child
process. The Stop hook converts Claude's completed transcript into standard OTLP protobuf, which
also works on company-managed machines where policy locks Claude's native OTLP exporter to a
corporate endpoint.

## Run it as a bundle (self-serve, no laptop stack)

The included [Databricks Asset Bundle](databricks.yml) parameterizes the whole destination, so you
can stand it up entirely in a workspace — no local Langfuse required. Migrate any OTLP JSON export
this way.

```bash
# 1. In databricks.yml set the target's `profile` and its REPLACE_ME variables (uc_catalog,
#    warehouse_id). zerobus_workspace_id and zerobus_region auto-derive; override only if needed.
databricks bundle validate -t dev
databricks bundle deploy -t dev

# 2. One job creates/binds the experiment + volume, the service principal, grants, and a
#    secret scope holding the Zerobus credentials.
databricks bundle run setup_destination -t dev

# 3. Drop any OTLP JSON export into the volume and migrate it.
databricks fs cp traces.jsonl dbfs:/Volumes/<catalog>/<schema>/otel_trace_drops/
databricks bundle run migrate_traces -t dev
```

The bundle jobs authenticate with the deployer's ambient identity and read Zerobus credentials from
the secret scope created by `setup_destination`; nothing is hardcoded. A `prod` target is included
alongside `dev` — fill in the same values.

## Reusable migration commands

Langfuse API backfill over an explicit window:

```bash
uv run otel2dbx migrate langfuse --from 2026-08-03T16:00:00Z --to 2026-08-03T17:00:00Z
```

Portable OTLP JSON (OpenTelemetry Collector file-exporter format) — the preferred adapter for an
arbitrary OTEL estate, since it replays the original protobuf unchanged (IDs, kinds, scope, events,
links):

```bash
uv run otel2dbx migrate otlp-json path/to/traces.jsonl --experiment-id <id>
```

Useful controls (any `migrate`/destination command):

- `--dry-run` — discover and normalize without exporting.
- `--resume <run-id>` — continue from a saved checkpoint.
- `--no-verify` — do not wait for Databricks query visibility.
- `--force` — resend trace IDs that already exist (see the delivery note under Configuration).
- `--secret-scope <scope>` — read Zerobus credentials from a Databricks secret scope instead of
  `.env` (how the bundle jobs authenticate).
- `--profile`, `--experiment-id`, `--warehouse-id` — override the configured values per command.
- By default, destination trace IDs that are already queryable are skipped.

Run manifests are written to `.otel2dbx/runs/` and contain no credentials.

## Configuration

Nothing is hardcoded to a workspace. `otel2dbx setup` writes the destination values into your
gitignored `.env` for you; you can also set any of these by hand, pass them per command (flag), or
set them per workspace (bundle target in `databricks.yml`). Precedence: **flag → environment →
Databricks ambient identity (on compute) → built-in default.**

| Environment variable | Default | Purpose |
|---|---|---|
| `OTEL2DBX_DATABRICKS_PROFILE` | `DEFAULT` | Databricks CLI profile; unused on Databricks compute, where the ambient identity applies |
| `OTEL2DBX_EXPERIMENT_ID` | required | Destination MLflow experiment (`otel2dbx setup` creates one) |
| `OTEL2DBX_WAREHOUSE_ID` | required | SQL warehouse for grants and trace reads |
| `ZEROBUS_WORKSPACE_ID` | auto-derived | Zerobus OTLP endpoint host (the `o=` number in workspace URLs); derived from the authenticated profile when unset |
| `ZEROBUS_REGION` | `us-east-1` | Zerobus OTLP endpoint region |
| `OTEL2DBX_SECRET_SCOPE` | unset | Secret scope holding the four `ZEROBUS_*` values; replaces `.env` in jobs |
| `OTEL2DBX_MODEL_ENDPOINT` | `databricks-claude-sonnet-4-5` | AI Gateway model serving endpoint used by the LangGraph demo agent |

`ZEROBUS_CLIENT_ID` / `ZEROBUS_CLIENT_SECRET` are the service-principal OAuth credentials; `setup`
and `zerobus bootstrap` write them to `.env`. See [`.env.example`](.env.example) for the full list.

Zerobus provides at-least-once delivery. Deterministic OTEL IDs, destination preflight, and
manifests prevent duplicates on ordinary reruns; an ambiguous network failure can still produce a
duplicate row if a request is retried after it was accepted. Avoid `--force` unless a deliberate
resend is required.

## How migration preserves fidelity

- **OTLP JSON adapter:** preserves the original protobuf structure and IDs exactly. The payload is
  sent unchanged to `https://<workspace-id>.zerobus.<region>.cloud.databricks.com/v1/traces`;
  routing to the target table stays outside the payload via the
  `x-databricks-zerobus-table-name` header.
- **Langfuse adapter:** Langfuse's public Observations API exposes the span tree, timestamps, I/O,
  metadata, resource attributes, model, and usage — but *not* the original span kind,
  instrumentation scope, links, or events. The adapter reconstructs what the API exposes,
  deterministically maps non-OTel vendor IDs (retaining the originals as attributes), and reports
  this limitation on every run.
- **Claude traces:** the adapter adds standard GenAI attributes so Databricks renders agent, chat,
  and tool span types and aggregates token usage on the root span.

## Security & data handling

- **Full trace content is captured only from `demo_workspace`,** a purpose-built throwaway fixture
  regenerated by `demo reset`. **Do not point full-content Claude telemetry at a real repository.**
- Langfuse and Zerobus service-principal secrets live only in the gitignored `.env` (or a secret
  scope) and are excluded from Git.
- Table-scoped Zerobus access tokens are minted on demand, kept in memory, and never written into
  generated settings or run manifests.
- `docker compose down` stops the local stack but **preserves its data**; only delete the volumes
  when you intentionally want to destroy the local demo database.

## Tests

```bash
uv run pytest            # unit tests (no Docker or Databricks required)
uv run ruff check src tests
```

## Disclaimer & license

This repository is an **unofficial, individual demonstration** — not an official Databricks
product, not a supported [Solution Accelerator](https://www.databricks.com/solutions/accelerators),
and not covered by any Databricks support agreement. It relies on preview/rolling-out capabilities
that may change or be unavailable in your workspace. Use it at your own risk.

Licensed under the [MIT License](LICENSE) — © 2026 Austin Choi.
