from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from google.protobuf.json_format import Parse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import ScopeSpans, Span

from otel2dbx.models import TraceEnvelope


def to_any_value(value: Any) -> AnyValue:
    result = AnyValue()
    if value is None:
        result.string_value = "null"
    elif isinstance(value, bool):
        result.bool_value = value
    elif isinstance(value, int):
        result.int_value = value
    elif isinstance(value, float):
        result.double_value = value
    elif isinstance(value, str):
        result.string_value = value
    elif isinstance(value, (bytes, bytearray)):
        result.bytes_value = bytes(value)
    elif isinstance(value, (list, tuple)):
        result.array_value.values.extend(to_any_value(item) for item in value)
    elif isinstance(value, dict):
        for key, item in value.items():
            result.kvlist_value.values.append(KeyValue(key=str(key), value=to_any_value(item)))
    else:
        result.string_value = json.dumps(value, default=str, sort_keys=True)
    return result


def set_attribute(span: Span, key: str, value: Any) -> None:
    for attribute in span.attributes:
        if attribute.key == key:
            attribute.value.CopyFrom(to_any_value(value))
            return
    span.attributes.append(KeyValue(key=key, value=to_any_value(value)))


def attributes_to_dict(attributes: Iterable[KeyValue]) -> dict[str, Any]:
    def decode(value: AnyValue) -> Any:
        selected = value.WhichOneof("value")
        if selected == "array_value":
            return [decode(item) for item in value.array_value.values]
        if selected == "kvlist_value":
            return {item.key: decode(item.value) for item in value.kvlist_value.values}
        return getattr(value, selected) if selected else None

    return {item.key: decode(item.value) for item in attributes}


def iter_spans(request: ExportTraceServiceRequest) -> Iterator[Span]:
    for resource in request.resource_spans:
        for scope in resource.scope_spans:
            yield from scope.spans


def trace_ids(request: ExportTraceServiceRequest) -> list[str]:
    return list(dict.fromkeys(span.trace_id.hex() for span in iter_spans(request)))


def split_request_by_trace(
    request: ExportTraceServiceRequest,
    *,
    source: str,
    lossless: bool,
) -> Iterator[TraceEnvelope]:
    """Split a multi-trace OTLP request into one TraceEnvelope per trace, preserving the
    original resource/scope grouping and every span exactly as received."""
    for trace_id in trace_ids(request):
        output = ExportTraceServiceRequest()
        raw_trace_id = bytes.fromhex(trace_id)
        for resource_spans in request.resource_spans:
            matching_scopes: list[tuple[ScopeSpans, list[Span]]] = []
            for scope_spans in resource_spans.scope_spans:
                spans = [span for span in scope_spans.spans if span.trace_id == raw_trace_id]
                if spans:
                    matching_scopes.append((scope_spans, spans))
            if not matching_scopes:
                continue
            new_resource = output.resource_spans.add()
            new_resource.resource.CopyFrom(resource_spans.resource)
            new_resource.schema_url = resource_spans.schema_url
            for scope_spans, spans in matching_scopes:
                new_scope = new_resource.scope_spans.add()
                new_scope.scope.CopyFrom(scope_spans.scope)
                new_scope.schema_url = scope_spans.schema_url
                for span in spans:
                    new_scope.spans.add().CopyFrom(span)
        yield TraceEnvelope(
            source_trace_id=trace_id,
            destination_trace_id=trace_id,
            request=output,
            source=source,
            lossless=lossless,
        )


def load_otlp_json_lines(path: Path) -> Iterator[TraceEnvelope]:
    """Yield one TraceEnvelope per trace from an OTLP JSON export.

    Each non-empty line is one OTLP/JSON ``ExportTraceServiceRequest`` — the format the
    OpenTelemetry Collector file exporter writes. The protobuf is replayed unchanged
    (IDs, kinds, scope, events, links all preserved), so this adapter is lossless.
    """
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            request = ExportTraceServiceRequest()
            try:
                Parse(line, request, ignore_unknown_fields=False)
            except Exception as exc:  # protobuf exposes several parse exception types
                raise ValueError(f"Invalid OTLP JSON on {path}:{line_number}: {exc}") from exc
            yield from split_request_by_trace(
                request,
                source=f"otlp-json:{path}",
                lossless=True,
            )
