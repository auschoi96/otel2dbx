# OTEL to Databricks managed MLflow demo

> This is the presenter script. Before following it, read
> [README.md → Can you run this?](README.md#can-you-run-this) to confirm your workspace has
> the required capabilities (MLflow tracing on OpenTelemetry with Unity Catalog trace
> locations, and Zerobus Ingest) — they are not enabled in every workspace. This is an
> unofficial demo provided as-is; see the README's disclaimer.

## Outcome

Show that an existing OpenTelemetry trace estate can be backfilled into Databricks managed
MLflow and then cut over to direct ingestion without rewriting application instrumentation.

The demo traces two kinds of agents — Claude Code (a coding agent) and a LangGraph
ReAct agent (a custom data agent) — with local Langfuse as the source system. The
migration CLI is vendor-adapter based, so the same destination and verification path
also accepts lossless OTLP JSON from any OpenTelemetry Collector.

## Guardrails

- The destination is a Databricks managed MLflow experiment you own.
- Trace storage is a Unity Catalog table named `<catalog>.<schema>.<experiment-id>_otel_spans`.
- No open-source MLflow tracking server is started or supported by this demo.
- Full trace content is captured only from the throwaway `demo_workspace` fixture.
- OAuth tokens and Langfuse credentials stay in ignored local configuration.
- The local UI is Langfuse; the destination UI is the Databricks MLflow Traces tab. A
  custom UI would obscure the migration rather than make it easier to understand.
- Only the LangGraph agent's model calls go through Databricks (an AI Gateway endpoint
  with workspace OAuth). Claude Code talks to Anthropic directly with the presenter's
  own login; its traces come from the Stop-hook transcript, not from Databricks.

## Architecture

```text
Historical backfill

Claude Code ----> Stop hook -----> OTEL Collector -> Langfuse
LangGraph agent -> OTLP HTTP ----'       |                 |
                                         +-> OTLP JSON     +-> otel2dbx Langfuse adapter
                                                                   |
                                                                   v
                                                         Zerobus Ingest OTLP
                                                                   |
                                                                   v
                                                         Unity Catalog trace tables
                                                                   |
                                                                   v
                                                         managed MLflow Traces UI

Direct cutover

Claude Code -> Stop hook -> Zerobus Ingest OTLP -> Unity Catalog -> MLflow UI
```

The Stop hook is intentionally explicit. Company-managed Claude installations can lock
Claude's native telemetry environment variables to corporate endpoints; the hook reads
Claude's safe transcript after a turn and produces standard OTLP protobuf without
modifying the user's managed settings.

The LangGraph agent calls a Databricks model serving endpoint through AI Gateway
(default `databricks-claude-sonnet-4-5`) and answers questions about workspace data with a
read-only SQL tool on the demo SQL warehouse, instrumented with the vanilla
OpenTelemetry SDK. Same collector, same Langfuse, same migration path — that is the
framework-agnostic proof.

## Setup (run once)

```bash
# 0. Start the Docker daemon. This machine uses Colima; Docker Desktop: open the app.
#    Compose fails with a bare exit 1 when the daemon is down.
colima start

# 1. Install dependencies and authenticate to the demo workspace.
uv sync          # also picks up the LangGraph agent's packages
databricks auth login https://<your-workspace>.cloud.databricks.com --profile <your-profile>

# 2. Start the local source stack. First run pulls several GB of images.
uv run otel2dbx demo init   # prints the Langfuse password — keep it
uv run otel2dbx demo up     # waits for Langfuse to report healthy

# 3. Create the destination in your workspace: MLflow experiment + permanent UC trace
#    binding, Zerobus service principal, grants, and credentials written to .env.
uv run otel2dbx setup \
  --experiment-name "otel2dbx demo" \
  --uc-catalog <catalog> --uc-schema mlflow_traces --table-prefix otel \
  --warehouse-id <warehouse-id> --profile <your-profile>
uv run otel2dbx doctor      # expect every check green before demoing

# 4. Warm up: starts the SQL warehouse and proves both agents trace end to end.
#    The traces it creates become part of the estate you backfill on camera.
uv run otel2dbx demo capture --agent both
```

Open these two tabs:

1. Langfuse at <http://localhost:3000> — sign in with `demo@example.com` and the
   password printed by `demo init` (run `uv run otel2dbx demo init` again to reprint
   it; it does not rotate without `--force`).
2. The Databricks managed MLflow experiment:
   `https://<your-workspace>.cloud.databricks.com/ml/experiments/<your-experiment-id>/overview/usage`

Then walk through this five-step story:

1. **Create traces in the old system.**

   ```bash
   uv run otel2dbx demo capture --agent both --count 5
   ```

   Ten traces from two different frameworks in one command — five Claude Code coding
   tasks and five LangGraph data tasks. Open one trace of each service in Langfuse
   (`claude-code-demo` vs `langgraph-demo`): Claude spans show the agent, LLM, and
   Read/Bash/Edit tools; LangGraph spans show the ReAct loop and the actual SQL the
   agent wrote against the `samples` catalog. Explain that both applications already
   emit OTEL and neither was rewritten.

   For a single canonical trace, plain `uv run otel2dbx demo capture` still runs the
   price-parser fix. Other variations: `--agent langgraph` (custom agent only),
   `--count 10` (bulk tasks, sampled randomly from `demo_assets/tasks.json` or
   `demo_assets/langgraph_tasks.json`), `--seed` (reproducible draw),
   `--model-endpoint <endpoint>` (swap the LangGraph model for any from
   `databricks serving-endpoints list`).

2. **Preview the backfill.**

   ```bash
   uv run otel2dbx migrate langfuse --since 15m --dry-run
   ```

   Call out trace discovery, span count, deterministic ID mapping, and the fidelity
   warning for fields that Langfuse's public API does not expose. Widen the window
   (`--since 2h`) if the warm-up traces should be included.

3. **Migrate and prove parity.**

   ```bash
   uv run otel2dbx migrate langfuse --since 15m
   uv run otel2dbx verify <run-id>
   ```

   Show the manifest summary, then open the same trace ID in managed MLflow. The verifier
   compares trace visibility, state, span count, and span names. Rerunning the migration
   demonstrates idempotent destination detection. Point out the Zerobus `/v1/traces`
   endpoint and table-scoped OAuth checks printed by `doctor`.

4. **Explain the portable path.**

   ```bash
   uv run otel2dbx migrate otlp-json .otel2dbx/archive/claude-traces.jsonl
   ```

   Do not resend unless needed. Explain that this adapter preserves the original OTLP
   protobuf structure, including IDs, kinds, scope, events, and links, making it the
   preferred adapter for an arbitrary OTEL estate.

5. **Cut over new traffic.**

   ```bash
   uv run otel2dbx demo reset --no-clear-archive
   uv run otel2dbx claude --target zerobus
   ```

   Open the printed trace ID in managed MLflow. The local OTLP archive remains unchanged,
   demonstrating that this trace bypassed both the collector and Langfuse.

## Presentation style

Keep the terminal and the two real product UIs on screen; avoid a bespoke dashboard.
For any new visuals, use maroon `#98102A` for fragmented point-tool state, green
`#00A972` for governed Databricks state, amber `#FFAB00` for caveats, and lava `#FF5F46`
for one emphasis per slide.

## Recovery

- `uv run otel2dbx doctor` identifies local, OAuth, experiment, table, and warehouse issues.
- Compose fails with a bare exit 1 or "daemon unreachable": the Docker daemon is not
  running — `colima start` (or open Docker Desktop), then rerun `uv run otel2dbx demo up`.
- "The LangGraph demo dependencies are not installed": run `uv sync` (those packages
  were added after the original lock).
- First LangGraph task stalls on SQL: the warehouse was cold; it stays warm afterwards.
  If the default model endpoint is unavailable in your workspace, pass `--model-endpoint
  <endpoint>` with any endpoint from `databricks serving-endpoints list`.
- Lost the Langfuse password: `uv run otel2dbx demo init` reprints it.
- `uv run otel2dbx demo reset` restores the safe fixture if a prior run already fixed it.
- `uv run otel2dbx migrate langfuse --resume <run-id>` resumes a partial migration.
- `docker compose down` stops the source stack while preserving its data.

## Running it yourself

The flow above runs against whatever workspace your CLI profile points at. To stand the
destination up as code instead, use the self-serve bundle path in
[README.md](README.md#migrate-any-otel-traces-self-serve): deploy the bundle, run
`setup_destination`, then generate source traces locally with `uv run otel2dbx demo
capture --count 10` or drop any OTLP JSON export into the trace-drop volume and run
`migrate_traces`.
