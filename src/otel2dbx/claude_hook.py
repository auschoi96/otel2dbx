from __future__ import annotations

import hashlib
import json
import os
import sys
from collections import OrderedDict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import InstrumentationScope, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span, Status

from otel2dbx.config import (
    DEFAULT_DATABRICKS_PROFILE,
    DEFAULT_EXPERIMENT_ID,
    DEFAULT_USER_ID,
    DEFAULT_WAREHOUSE_ID,
    DatabricksTarget,
)
from otel2dbx.databricks import ZerobusOTLPSink
from otel2dbx.otel import iso_to_ns, set_attribute, to_any_value

MAX_CHARS = 20_000


def _log(message: str) -> None:
    path = Path(os.getenv("OTEL2DBX_HOOK_LOG", ".otel2dbx/claude-hook.log"))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(f"{datetime.now(UTC).isoformat()} {message}\n")
    except OSError:
        pass


def _hash_id(value: str, size: int) -> bytes:
    result = hashlib.sha256(value.encode()).digest()[:size]
    return result if any(result) else b"\x01" + result[1:]


def _text(content: Any) -> str:
    if isinstance(content, str):
        return content[:MAX_CHARS]
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") in {"text", "thinking"} and block.get("text"):
            parts.append(str(block["text"]))
        elif block.get("type") == "tool_result":
            value = block.get("content")
            parts.append(_text(value) if isinstance(value, list) else str(value or ""))
    return "\n".join(part for part in parts if part)[:MAX_CHARS]


def _content(row: dict[str, Any]) -> Any:
    message = row.get("message")
    return message.get("content") if isinstance(message, dict) else row.get("content")


def _timestamp(row: dict[str, Any], fallback: int) -> int:
    return iso_to_ns(row.get("timestamp"), fallback)


def _read_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                rows.append(value)
    return rows


def _is_prompt(row: dict[str, Any]) -> bool:
    if row.get("type") != "user":
        return False
    content = _content(row)
    if isinstance(content, list) and any(
        isinstance(block, dict) and block.get("type") == "tool_result" for block in content
    ):
        return False
    return bool(_text(content))


def _last_turn(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prompt_indexes = [index for index, row in enumerate(rows) if _is_prompt(row)]
    return rows[prompt_indexes[-1] :] if prompt_indexes else []


def _message_groups(turn: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    groups: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    for row in turn:
        if row.get("type") != "assistant" or not isinstance(row.get("message"), dict):
            continue
        message = row["message"]
        identifier = str(message.get("id") or row.get("uuid") or len(groups))
        groups.setdefault(identifier, []).append(row)
    return list(groups.values())


def _tool_results(turn: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for row in turn:
        if row.get("type") != "user" or not isinstance(_content(row), list):
            continue
        for block in _content(row):
            if isinstance(block, dict) and block.get("type") == "tool_result":
                results[str(block.get("tool_use_id", ""))] = {
                    "content": _text(block.get("content")),
                    "is_error": bool(block.get("is_error")),
                    "timestamp": row.get("timestamp"),
                }
    return results


def _tool_uses(turn: list[dict[str, Any]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    uses: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in turn:
        if row.get("type") != "assistant" or not isinstance(_content(row), list):
            continue
        for block in _content(row):
            if isinstance(block, dict) and block.get("type") == "tool_use":
                uses.append((row, block))
    return uses


def _usage(groups: list[list[dict[str, Any]]]) -> tuple[int, int]:
    input_tokens = 0
    output_tokens = 0
    for group in groups:
        group_input = 0
        group_output = 0
        for row in group:
            usage = row.get("message", {}).get("usage") or {}
            group_input = max(group_input, int(usage.get("input_tokens") or 0))
            group_output = max(group_output, int(usage.get("output_tokens") or 0))
        input_tokens += group_input
        output_tokens += group_output
    return input_tokens, output_tokens


def build_trace(
    session_id: str, turn: list[dict[str, Any]]
) -> tuple[str, ExportTraceServiceRequest]:
    prompt_row = turn[0]
    prompt_uuid = str(prompt_row.get("uuid") or prompt_row.get("timestamp") or "prompt")
    trace_id = _hash_id(f"trace:{session_id}:{prompt_uuid}", 16)
    root_id = _hash_id(f"root:{session_id}:{prompt_uuid}", 8)
    now = int(datetime.now(UTC).timestamp() * 1_000_000_000)
    start = _timestamp(prompt_row, now)
    end = max((_timestamp(row, start) for row in turn), default=start)
    groups = _message_groups(turn)
    input_tokens, output_tokens = _usage(groups)
    assistant_output = "\n".join(
        part
        for group in groups
        if (part := "\n".join(_text(_content(row)) for row in group).strip())
    )[:MAX_CHARS]

    root = Span(
        trace_id=trace_id,
        span_id=root_id,
        name="claude_code.interaction",
        kind=Span.SPAN_KIND_INTERNAL,
        start_time_unix_nano=start,
        end_time_unix_nano=max(end, start),
    )
    set_attribute(root, "gen_ai.operation.name", "invoke_agent")
    set_attribute(root, "session.id", session_id)
    set_attribute(root, "user.id", DEFAULT_USER_ID)
    set_attribute(
        root,
        "gen_ai.input.messages",
        json.dumps([{"role": "user", "content": _text(_content(prompt_row))}]),
    )
    if assistant_output:
        set_attribute(
            root,
            "gen_ai.output.messages",
            json.dumps([{"role": "assistant", "content": assistant_output}]),
        )
    set_attribute(root, "gen_ai.usage.input_tokens", input_tokens)
    set_attribute(root, "gen_ai.usage.output_tokens", output_tokens)
    set_attribute(root, "otel2dbx.capture.mode", "claude-stop-hook")

    spans = [root]
    for group_index, group in enumerate(groups):
        message = group[-1].get("message") or {}
        model = message.get("model")
        group_start = min((_timestamp(row, start) for row in group), default=start)
        group_end = max((_timestamp(row, group_start) for row in group), default=group_start)
        output = "\n".join(_text(_content(row)) for row in group).strip()[:MAX_CHARS]
        llm = Span(
            trace_id=trace_id,
            span_id=_hash_id(f"llm:{session_id}:{prompt_uuid}:{group_index}", 8),
            parent_span_id=root_id,
            name="claude_code.llm_request",
            kind=Span.SPAN_KIND_CLIENT,
            start_time_unix_nano=group_start,
            end_time_unix_nano=max(group_end, group_start),
        )
        set_attribute(llm, "gen_ai.operation.name", "chat")
        set_attribute(llm, "gen_ai.system", "anthropic")
        if model:
            set_attribute(llm, "gen_ai.request.model", str(model))
        if output:
            set_attribute(
                llm,
                "gen_ai.output.messages",
                json.dumps([{"role": "assistant", "content": output}]),
            )
        usage = message.get("usage") or {}
        set_attribute(llm, "gen_ai.usage.input_tokens", int(usage.get("input_tokens") or 0))
        set_attribute(llm, "gen_ai.usage.output_tokens", int(usage.get("output_tokens") or 0))
        spans.append(llm)

    results = _tool_results(turn)
    for index, (row, tool) in enumerate(_tool_uses(turn)):
        tool_id = str(tool.get("id") or index)
        result = results.get(tool_id, {})
        tool_start = _timestamp(row, start)
        tool_end = iso_to_ns(result.get("timestamp"), tool_start)
        span = Span(
            trace_id=trace_id,
            span_id=_hash_id(f"tool:{session_id}:{prompt_uuid}:{tool_id}", 8),
            parent_span_id=root_id,
            name=f"claude_code.tool.{tool.get('name') or 'unknown'}",
            kind=Span.SPAN_KIND_INTERNAL,
            start_time_unix_nano=tool_start,
            end_time_unix_nano=max(tool_end, tool_start),
        )
        set_attribute(span, "gen_ai.operation.name", "execute_tool")
        set_attribute(span, "gen_ai.tool.name", str(tool.get("name") or "unknown"))
        set_attribute(span, "gen_ai.tool.call.id", tool_id)
        set_attribute(
            span,
            "gen_ai.input.messages",
            json.dumps(tool.get("input") or {}, default=str)[:MAX_CHARS],
        )
        if result.get("content"):
            set_attribute(span, "gen_ai.output.messages", str(result["content"])[:MAX_CHARS])
        if result.get("is_error"):
            span.status.code = Status.STATUS_CODE_ERROR
            span.status.message = "Claude Code tool returned an error"
        spans.append(span)

    request = ExportTraceServiceRequest()
    resource_spans = ResourceSpans(
        resource=Resource(
            attributes=[
                KeyValue(key="service.name", value=to_any_value("claude-code-demo")),
                KeyValue(key="deployment.environment.name", value=to_any_value("local")),
            ]
        )
    )
    scope_spans = ScopeSpans(
        scope=InstrumentationScope(name="otel2dbx.claude_hook", version="0.1.0")
    )
    scope_spans.spans.extend(spans)
    resource_spans.scope_spans.append(scope_spans)
    request.resource_spans.append(resource_spans)
    return trace_id.hex(), request


def _state_path() -> Path:
    return Path(os.getenv("OTEL2DBX_HOOK_STATE", ".otel2dbx/claude-hook-state.json"))


def _already_sent(trace_id: str) -> bool:
    path = _state_path()
    try:
        return trace_id in json.loads(path.read_text(encoding="utf-8")).get("sent", [])
    except (OSError, ValueError):
        return False


def _mark_sent(trace_id: str) -> None:
    path = _state_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"sent": []}
        payload.setdefault("sent", []).append(trace_id)
        payload["sent"] = list(dict.fromkeys(payload["sent"][-1000:]))
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(temporary, path)
    except OSError as exc:
        _log(f"state write failed: {exc}")


def _send(request: ExportTraceServiceRequest) -> None:
    if os.getenv("OTEL2DBX_HOOK_TARGET", "langfuse") == "zerobus":
        target = DatabricksTarget(
            profile=os.getenv("OTEL2DBX_DATABRICKS_PROFILE", DEFAULT_DATABRICKS_PROFILE),
            experiment_id=os.getenv("OTEL2DBX_EXPERIMENT_ID", DEFAULT_EXPERIMENT_ID),
            warehouse_id=os.getenv("OTEL2DBX_WAREHOUSE_ID", DEFAULT_WAREHOUSE_ID),
            zerobus_workspace_id=os.getenv("ZEROBUS_WORKSPACE_ID") or None,
            zerobus_region=os.getenv("ZEROBUS_REGION") or None,
            zerobus_client_id=os.getenv("ZEROBUS_CLIENT_ID") or None,
            zerobus_client_secret=os.getenv("ZEROBUS_CLIENT_SECRET") or None,
        )
        sink = ZerobusOTLPSink(target)
        try:
            sink.export(request)
        finally:
            sink.close()
        return
    endpoint = os.getenv("OTEL2DBX_COLLECTOR_ENDPOINT", "http://localhost:4318/v1/traces")
    response = httpx.post(
        endpoint,
        content=request.SerializeToString(),
        headers={"content-type": "application/x-protobuf"},
        timeout=30,
    )
    response.raise_for_status()


def main() -> int:
    try:
        payload = json.load(sys.stdin)
        session_id = str(payload.get("session_id") or payload.get("sessionId") or "")
        transcript = payload.get("transcript_path") or payload.get("transcriptPath")
        if not session_id or not transcript:
            return 0
        turn = _last_turn(_read_rows(Path(str(transcript))))
        if not turn:
            return 0
        trace_id, request = build_trace(session_id, turn)
        if _already_sent(trace_id):
            return 0
        _send(request)
        _mark_sent(trace_id)
        span_count = sum(
            len(scope.spans)
            for resource in request.resource_spans
            for scope in resource.scope_spans
        )
        _log(f"sent trace={trace_id} spans={span_count}")
    except Exception as exc:
        _log(f"failed: {type(exc).__name__}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
