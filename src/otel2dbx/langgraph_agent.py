"""LangGraph demo agent: a custom (non-coding) agent traced with vanilla OpenTelemetry.

The agent calls a Databricks model serving endpoint through AI Gateway using the
presenter's workspace identity (no extra secrets), and queries workspace data through
a read-only SQL tool backed by the demo SQL warehouse. OTLP spans export to the same
local collector as the Claude Code hook — proving the pipeline is framework-agnostic.
Heavy dependencies (langgraph, langchain, openinference) are imported lazily so the
rest of the CLI works without them installed.
"""

from __future__ import annotations

import ast
import json
import operator
import os
import uuid
from dataclasses import dataclass
from typing import Any

from otel2dbx.config import DEFAULT_USER_ID, DEFAULT_WAREHOUSE_ID
from otel2dbx.errors import ConfigurationError

# A Databricks-hosted foundation model endpoint available in most workspaces. Override
# with OTEL2DBX_MODEL_ENDPOINT (or --model-endpoint) to any endpoint from
# `databricks serving-endpoints list`.
DEFAULT_MODEL_ENDPOINT = os.getenv("OTEL2DBX_MODEL_ENDPOINT", "databricks-claude-sonnet-4-5")
DEFAULT_COLLECTOR_ENDPOINT = os.getenv(
    "OTEL2DBX_COLLECTOR_ENDPOINT", "http://localhost:4318/v1/traces"
)

# All tasks in one capture process share a session so the MLflow Sessions view
# groups them, matching how Claude Code's multi-task sessions render.
_DEMO_SESSION_ID = f"langgraph-demo-{uuid.uuid4().hex[:8]}"

_MAX_POWER_EXPONENT = 1000
_MAX_RESULT_ROWS = 50
_MAX_CELL_CHARS = 200
_READ_ONLY_PREFIXES = ("select", "with", "show", "describe", "desc", "explain")

_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
}


@dataclass(frozen=True)
class LangGraphTaskResult:
    trace_id: str
    answer: str


def _safe_eval(expression: str) -> float:
    """Evaluate a basic arithmetic expression; no names, calls, or attributes."""

    def evaluate(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return evaluate(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return float(node.value)
        if isinstance(node, ast.BinOp) and type(node.op) in _OPS:
            left = evaluate(node.left)
            right = evaluate(node.right)
            if isinstance(node.op, ast.Pow) and abs(right) > _MAX_POWER_EXPONENT:
                raise ValueError("exponent too large")
            return float(_OPS[type(node.op)](left, right))
        if isinstance(node, ast.UnaryOp) and type(node.op) in _OPS:
            return float(_OPS[type(node.op)](evaluate(node.operand)))
        raise ValueError(f"unsupported expression: {expression!r}")

    return evaluate(ast.parse(expression, mode="eval"))


def _is_read_only(statement: str) -> bool:
    return statement.strip().lower().startswith(_READ_ONLY_PREFIXES)


def _format_sql_result(columns: list[str], rows: list[list[Any]]) -> str:
    """Render query results as compact text for the agent's tool observation."""
    if not rows:
        return "(no rows)"
    lines = [", ".join(columns)]
    for row in rows[:_MAX_RESULT_ROWS]:
        lines.append(
            ", ".join("" if value is None else str(value)[:_MAX_CELL_CHARS] for value in row)
        )
    if len(rows) > _MAX_RESULT_ROWS:
        lines.append(f"... truncated to {_MAX_RESULT_ROWS} of {len(rows)} rows")
    return "\n".join(lines)


def _final_answer(result: dict[str, Any]) -> str:
    messages = result.get("messages") or []
    if not messages:
        return ""
    content = getattr(messages[-1], "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [
            str(block.get("text", ""))
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ]
        return "\n".join(part for part in parts if part)
    return str(content)


def run_langgraph_task(
    prompt: str,
    *,
    model_endpoint: str = DEFAULT_MODEL_ENDPOINT,
    profile: str | None = None,
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
    collector_endpoint: str = DEFAULT_COLLECTOR_ENDPOINT,
) -> LangGraphTaskResult:
    """Run one LangGraph ReAct task and export its OTLP trace to the collector."""
    try:
        from langchain_core.tools import tool
        from langchain_openai import ChatOpenAI
        from langgraph.prebuilt import create_react_agent
        from openinference.instrumentation.langchain import LangChainInstrumentor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:
        raise ConfigurationError(
            "The LangGraph demo dependencies are not installed. Run `uv sync` to pick "
            "up langgraph, langchain-openai, and the OpenTelemetry exporter."
        ) from exc

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    from databricks.sdk.service.sql import Disposition

    config = Config(profile=profile) if profile else Config()
    try:
        headers = config.authenticate()
        host = (config.host or "").rstrip("/")
    except Exception as exc:
        raise ConfigurationError(
            "Cannot authenticate to Databricks for the LangGraph demo. Run "
            "`databricks auth login` for your profile first."
        ) from exc
    if not host or "Authorization" not in headers:
        raise ConfigurationError(
            "Cannot authenticate to Databricks for the LangGraph demo. Run "
            "`databricks auth login` for your profile first."
        )
    workspace = WorkspaceClient(config=config)

    @tool
    def calculator(expression: str) -> str:
        """Evaluate a basic arithmetic expression such as '12.5 * (3 + 1)'."""
        try:
            return str(_safe_eval(expression))
        except Exception as exc:  # return the error so the agent can retry
            return f"error: {exc}"

    @tool
    def query_sql(statement: str) -> str:
        """Run a read-only SQL query against the Databricks SQL warehouse and return
        the result as text. Useful sample data: samples.tpch (nation, region, customer,
        orders, lineitem, part, supplier, partsupp) and samples.nyctaxi.trips. Always
        add a LIMIT when selecting from large tables."""
        if not _is_read_only(statement):
            return (
                "error: only read-only queries "
                "(SELECT, WITH, SHOW, DESCRIBE, EXPLAIN) are allowed"
            )
        try:
            response = workspace.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
                disposition=Disposition.INLINE,
                wait_timeout="30s",
            )
        except Exception as exc:
            return f"error: {exc}"
        state = str(getattr(response.status, "state", ""))
        if not state.endswith("SUCCEEDED"):
            error = getattr(response.status, "error", None)
            return f"error: {getattr(error, 'message', state)}"
        columns: list[str] = []
        if response.manifest and response.manifest.schema:
            columns = [str(column.name) for column in response.manifest.schema.columns]
        rows: list[list[Any]] = []
        if response.result and response.result.data_array:
            rows = [list(row) for row in response.result.data_array]
        return _format_sql_result(columns, rows)

    resource = Resource.create(
        {
            "service.name": "langgraph-demo",
            "deployment.environment.name": "local",
            "otel2dbx.capture.mode": "langgraph-demo",
        }
    )
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=collector_endpoint))
    )
    LangChainInstrumentor().instrument(tracer_provider=provider)

    # AI Gateway endpoints expose an OpenAI-compatible API; the CLI profile's
    # OAuth token is the API key. No extra secrets needed.
    llm = ChatOpenAI(
        model=model_endpoint,
        base_url=f"{host}/serving-endpoints",
        api_key=headers["Authorization"].removeprefix("Bearer "),
        temperature=0,
    )
    agent = create_react_agent(llm, tools=[calculator, query_sql])

    tracer = provider.get_tracer("otel2dbx.langgraph_demo", "0.1.0")
    with tracer.start_as_current_span("langgraph_demo.task") as span:
        span.set_attribute("gen_ai.operation.name", "invoke_agent")
        # session.id on the root span is what the MLflow Sessions view groups by.
        span.set_attribute("session.id", _DEMO_SESSION_ID)
        span.set_attribute("user.id", DEFAULT_USER_ID)
        span.set_attribute(
            "gen_ai.input.messages",
            json.dumps([{"role": "user", "content": prompt}]),
        )
        result = agent.invoke({"messages": [("user", prompt)]})
        answer = _final_answer(result)
        span.set_attribute(
            "gen_ai.output.messages",
            json.dumps([{"role": "assistant", "content": answer}]),
        )
        trace_id = format(span.get_span_context().trace_id, "032x")
    provider.force_flush(timeout_millis=10_000)
    return LangGraphTaskResult(trace_id=trace_id, answer=answer)
