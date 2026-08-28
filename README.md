# otel2dbx

Migrate OpenTelemetry traces from **any source** into **Databricks managed MLflow** — replay
them through Zerobus Ingest into Unity Catalog and verify parity trace-by-trace, without
rewriting a line of application instrumentation.

> [!IMPORTANT]
> **Unofficial example — not a Databricks product.** This is a personal demonstration repo,
> provided as-is under the [MIT license](LICENSE) with no warranty or support. It exercises
> Databricks capabilities that are still rolling out, so it will not run in every workspace.
> Read [**Can you run this?**](#can-you-run-this) before investing time.

```text
Any OpenTelemetry source              otel2dbx
(Collector, SDK, agent framework, ─┐
 an in-house exporter, …)           │   OTLP JSON ──▶ Zerobus Ingest OTLP ──▶ Unity Catalog
                                    └────────────────────────────────────────────┐  trace tables
                                                                                  ▼        │
                                                                    verify parity ◀── managed MLflow
                                                                                       Traces UI
```

The input is **OTLP JSON** — one `ExportTraceServiceRequest` per line, exactly what the
OpenTelemetry Collector [file exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/fileexporter)
writes. Anything that can emit OTLP can produce it, so the source is irrelevant: a Collector,
a language SDK, or an in-house framework all migrate through the same path. The destination is
always Databricks managed MLflow with traces stored in Unity Catalog; this project never starts
or targets an open-source MLflow tracking server.

A small [`examples/sample-traces.jsonl`](examples/sample-traces.jsonl) is included so you can run
a real end-to-end migration immediately, before wiring up your own source.

## What it does

- **Lossless replay:** the original OTLP protobuf is sent to Zerobus **unchanged** — trace and
  span IDs, span kinds, instrumentation scope, events, and links are all preserved.
- **Parity verification:** after export it reads the trace back from managed MLflow and checks
  visibility, state, span count, and span names, writing a per-trace report.
- **Idempotent + resumable:** trace IDs already present in the destination are skipped; runs
  write a manifest to `.otel2dbx/runs/` and can be resumed.
- **Two ways to run:** a local CLI pointed at a file, or a Databricks Asset Bundle that ingests
  from a Unity Catalog volume entirely in-workspace.

## Can you run this?

You need a **Databricks workspace with the required capabilities** — this is the gating factor.
You do **not** need Docker, and thanks to the bundled sample you don't need your own traces to
start.

- **Gating requirement (check this first):** your workspace must have **MLflow tracing on
  OpenTelemetry with Unity Catalog trace locations**, and **Zerobus Ingest**, available. These
  are newer Databricks capabilities and are **not enabled everywhere.**
  **How to check:** run the `otel2dbx setup …` command below. If it fails at the *“bind the
  experiment to Unity Catalog”* step, or `otel2dbx doctor` shows the Zerobus checks failing, your
  workspace does not have these features yet.
- **Unity Catalog privileges:** `USE CATALOG`, `USE SCHEMA`, and `CREATE TABLE` on a catalog you
  choose, plus permission to **create a service principal** for Zerobus ingestion.
- **A SQL warehouse** you have `CAN USE` on (`databricks warehouses list`) — used for grants and
  reading traces back. It incurs normal DBU cost while running; stop it when you are done.
- **Local tooling:** Python 3.13, [`uv`](https://docs.astral.sh/uv/), and the Databricks CLI.
- **Traces to migrate:** the bundled sample, or your own OTLP JSON export (see
  [Bring your own traces](#bring-your-own-traces)).

Everything this creates (one MLflow experiment, one small UC spans table, a secret scope, a
service principal) is negligible in cost. This is a demo, not a load test — it moves a handful of
traces, not production volume.

## Quickstart

```bash
uv sync
databricks auth login https://<your-workspace>.cloud.databricks.com --profile <your-profile>

# Create the destination in YOUR workspace. Fill in a catalog you can create tables in,
# a SQL warehouse you have CAN USE on, and your profile:
uv run otel2dbx setup \
  --experiment-name "otel2dbx demo" \
  --uc-catalog <catalog> --uc-schema mlflow_traces --table-prefix otel \
  --warehouse-id <warehouse-id> --profile <your-profile>

uv run otel2dbx doctor                                   # expect every check green

# Migrate the bundled sample end-to-end, then reopen the parity report.
uv run otel2dbx migrate otlp-json examples/sample-traces.jsonl --dry-run   # preview first
uv run otel2dbx migrate otlp-json examples/sample-traces.jsonl
uv run otel2dbx verify <run-id>
```

Open the result in your experiment's **Traces** tab (select the SQL warehouse there once):
`https://<your-workspace>.cloud.databricks.com/ml/experiments/<your-experiment-id>/overview/usage`

`setup` is the only command a fresh workspace needs. It creates (or reuses) the MLflow experiment,
**permanently** binds it to a Unity Catalog trace location
(`<catalog>.<schema>.<experiment-id>_otel_spans`), creates (or reuses) a dedicated Zerobus service
principal, grants it `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on the spans table, and
writes the Zerobus credentials **plus** `OTEL2DBX_EXPERIMENT_ID` and `OTEL2DBX_WAREHOUSE_ID` to the
gitignored `.env` — so every later command runs zero-config. Interactive runs confirm before the
permanent UC binding. Add `--secret-scope <scope>` to also store the credentials in a Databricks
secret scope (how the bundle jobs authenticate).

> If you already have an experiment bound to a UC trace location, `uv run otel2dbx zerobus
> bootstrap` provisions just the service principal and grants against it instead of running `setup`.

## Bring your own traces

The migrator reads **OTLP JSON**: each line is one OTLP/JSON `ExportTraceServiceRequest`. Produce it
from whatever already emits OpenTelemetry — no re-instrumentation needed. The most portable route is
the OpenTelemetry Collector's file exporter:

```yaml
# collector-config.yaml (excerpt)
exporters:
  file:
    path: /var/lib/otel/traces.jsonl   # one ExportTraceServiceRequest per line
service:
  pipelines:
    traces:
      exporters: [file]
```

Then migrate the file (or a directory of `.json`/`.jsonl` files):

```bash
uv run otel2dbx migrate otlp-json /var/lib/otel/traces.jsonl
```

Any system that can write OTLP/JSON works — a Collector, a language SDK's OTLP exporter, or an
in-house tool. IDs and structure are replayed unchanged, so this is the preferred path for an
arbitrary OTEL estate.

## Run it as a bundle (in-workspace, no laptop)

The included [Databricks Asset Bundle](databricks.yml) parameterizes the whole destination and runs
migration as a serverless job that reads OTLP JSON from a Unity Catalog volume — no local machine
required.

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

## Command reference

`otel2dbx migrate otlp-json <path>` accepts a file or a directory of `.json`/`.jsonl` files, plus:

- `--dry-run` — discover and normalize without exporting.
- `--resume <run-id>` — continue from a saved checkpoint.
- `--no-verify` — do not wait for Databricks query visibility.
- `--force` — resend trace IDs that already exist (see the delivery note under Configuration).
- `--secret-scope <scope>` — read Zerobus credentials from a Databricks secret scope instead of
  `.env` (how the bundle jobs authenticate).
- `--profile`, `--experiment-id`, `--warehouse-id` — override the configured values per command.
- By default, destination trace IDs that are already queryable are skipped.

Other commands: `otel2dbx setup` (create the destination), `otel2dbx zerobus bootstrap` (just the
service principal + grants), `otel2dbx doctor` (validate the destination), `otel2dbx verify
<run-id>` (reopen a run's parity report). Run manifests are written to `.otel2dbx/runs/` and contain
no credentials.

## Configuration

Nothing is hardcoded to a workspace. `otel2dbx setup` writes the destination values into your
gitignored `.env`; you can also set any of these by hand, pass them per command (flag), or set them
per workspace (bundle target in `databricks.yml`). Precedence: **flag → environment → Databricks
ambient identity (on compute) → built-in default.**

| Environment variable | Default | Purpose |
|---|---|---|
| `OTEL2DBX_DATABRICKS_PROFILE` | `DEFAULT` | Databricks CLI profile; unused on Databricks compute, where the ambient identity applies |
| `OTEL2DBX_EXPERIMENT_ID` | required | Destination MLflow experiment (`otel2dbx setup` creates one) |
| `OTEL2DBX_WAREHOUSE_ID` | required | SQL warehouse for grants and trace reads |
| `ZEROBUS_WORKSPACE_ID` | auto-derived | Zerobus OTLP endpoint host (the `o=` number in workspace URLs); derived from the authenticated profile when unset |
| `ZEROBUS_REGION` | `us-east-1` | Zerobus OTLP endpoint region |
| `OTEL2DBX_SECRET_SCOPE` | unset | Secret scope holding the four `ZEROBUS_*` values; replaces `.env` in jobs |

`ZEROBUS_CLIENT_ID` / `ZEROBUS_CLIENT_SECRET` are the service-principal OAuth credentials, written by
`setup` / `zerobus bootstrap`. See [`.env.example`](.env.example) for the full list.

Zerobus provides at-least-once delivery. Deterministic OTEL IDs, destination preflight, and
manifests prevent duplicates on ordinary reruns; an ambiguous network failure can still produce a
duplicate row if a request is retried after it was accepted. Avoid `--force` unless a deliberate
resend is required.

## Fidelity

The OTLP JSON adapter preserves the original protobuf structure and IDs exactly. The payload is sent
unchanged to `https://<workspace-id>.zerobus.<region>.cloud.databricks.com/v1/traces`; routing to
the target table stays outside the payload via the `x-databricks-zerobus-table-name` header. A
multi-trace request is split into one migration unit per trace, but every span within a trace is
replayed byte-for-byte. Standard GenAI span attributes (e.g. `gen_ai.operation.name`) present in the
source render as agent/chat/tool span types in the Databricks Traces UI.

## Security & data handling

- Zerobus service-principal secrets live only in the gitignored `.env` (or a secret scope) and are
  excluded from Git.
- Table-scoped Zerobus access tokens are minted on demand, kept in memory, and never written into
  run manifests.
- Run manifests record trace IDs, span counts, and status — never trace content or credentials.
- `examples/sample-traces.jsonl` contains only synthetic spans. When migrating your own exports, be
  aware they may contain prompt/response content — treat the source files accordingly.

## Tests

```bash
uv run pytest                  # unit tests — no Databricks required
uv run ruff check src tests
```

## Disclaimer & license

This repository is an **unofficial, individual demonstration** — not an official Databricks product,
not a supported [Solution Accelerator](https://www.databricks.com/solutions/accelerators), and not
covered by any Databricks support agreement. It relies on preview/rolling-out capabilities that may
change or be unavailable in your workspace. Use it at your own risk.

Licensed under the [MIT License](LICENSE) — © 2026 Austin Choi.
