"""A small, realistic OTLP trace used by tests and to generate examples/sample-traces.jsonl.

Vendor-neutral GenAI trace: an agent root span with an LLM call and a tool call beneath it,
so it renders as agent / chat / tool span types in the Databricks MLflow Traces UI.
"""

from __future__ import annotations

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import InstrumentationScope, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span, Status

from otel2dbx.otel import set_attribute, to_any_value

TRACE_ID = bytes.fromhex("4bf92f3577b34da6a3ce929d0e0e4736")
ROOT_ID = bytes.fromhex("00f067aa0ba902b7")
LLM_ID = bytes.fromhex("1a2b3c4d5e6f7081")
TOOL_ID = bytes.fromhex("2b3c4d5e6f708192")

# Fixed base timestamp so the generated sample file is byte-stable across regenerations.
_BASE_NS = 1_722_686_400_000_000_000  # 2024-08-03T12:00:00Z


def _span(
    span_id: bytes,
    name: str,
    kind: int,
    start_offset_ns: int,
    duration_ns: int,
    parent_id: bytes | None,
    attributes: dict[str, object],
) -> Span:
    span = Span(
        trace_id=TRACE_ID,
        span_id=span_id,
        name=name,
        kind=kind,
        start_time_unix_nano=_BASE_NS + start_offset_ns,
        end_time_unix_nano=_BASE_NS + start_offset_ns + duration_ns,
    )
    if parent_id is not None:
        span.parent_span_id = parent_id
    span.status.code = Status.STATUS_CODE_OK
    for key, value in attributes.items():
        set_attribute(span, key, value)
    return span


def sample_request() -> ExportTraceServiceRequest:
    root = _span(
        ROOT_ID,
        "invoke_agent example-agent",
        Span.SPAN_KIND_SERVER,
        0,
        2_000_000_000,
        None,
        {
            "gen_ai.operation.name": "invoke_agent",
            "session.id": "session-abc123",
            "gen_ai.usage.input_tokens": 128,
            "gen_ai.usage.output_tokens": 57,
        },
    )
    llm = _span(
        LLM_ID,
        "chat example-model-v1",
        Span.SPAN_KIND_CLIENT,
        100_000_000,
        900_000_000,
        ROOT_ID,
        {
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "example-model-v1",
            "gen_ai.usage.input_tokens": 128,
            "gen_ai.usage.output_tokens": 57,
        },
    )
    tool = _span(
        TOOL_ID,
        "execute_tool search_docs",
        Span.SPAN_KIND_INTERNAL,
        1_100_000_000,
        300_000_000,
        ROOT_ID,
        {
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "search_docs",
        },
    )

    resource = Resource(
        attributes=[
            KeyValue(key=key, value=to_any_value(value))
            for key, value in {
                "service.name": "example-agent",
                "service.version": "1.0.0",
                "deployment.environment.name": "demo",
            }.items()
        ]
    )
    scope_spans = ScopeSpans(
        scope=InstrumentationScope(name="otel2dbx.example", version="1.0.0")
    )
    scope_spans.spans.extend([root, llm, tool])
    resource_spans = ResourceSpans(resource=resource)
    resource_spans.scope_spans.append(scope_spans)
    request = ExportTraceServiceRequest()
    request.resource_spans.append(resource_spans)
    return request
