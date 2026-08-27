from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from google.protobuf.json_format import Parse
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, InstrumentationScope, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span, Status

from otel2dbx.models import TraceEnvelope


def iso_to_ns(value: str | None, fallback: int = 0) -> int:
    if not value:
        return fallback
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1_000_000_000)


def normalize_otel_id(value: str, size: int, namespace: str) -> tuple[bytes, bool]:
    expected = size * 2
    try:
        raw = bytes.fromhex(value)
        if len(value) == expected and len(raw) == size and any(raw):
            return raw, True
    except (TypeError, ValueError):
        pass
    digest = hashlib.sha256(f"{namespace}:{value}".encode()).digest()[:size]
    if not any(digest):
        digest = b"\x01" + digest[1:]
    return digest, False


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


def _operation_name(name: str, observation_type: str) -> str | None:
    lowered = name.lower()
    if "claude_code.interaction" in lowered:
        return "invoke_agent"
    if "claude_code.llm_request" in lowered or observation_type == "GENERATION":
        return "chat"
    if "claude_code.tool" in lowered or observation_type == "TOOL":
        return "execute_tool"
    return None


def _json_message(role: str, content: str) -> str:
    return json.dumps([{"role": role, "content": content}], ensure_ascii=False)


def _observation_span(
    observation: dict[str, Any],
    trace_id: bytes,
    span_ids: dict[str, bytes],
    warnings: list[str],
) -> tuple[Span, dict[str, Any]]:
    source_span_id = str(observation["id"])
    span = Span(
        trace_id=trace_id,
        span_id=span_ids[source_span_id],
        name=observation.get("name") or observation.get("type") or "langfuse.observation",
        kind=Span.SPAN_KIND_INTERNAL,
        start_time_unix_nano=iso_to_ns(observation.get("startTime")),
        end_time_unix_nano=iso_to_ns(observation.get("endTime")),
    )
    parent = observation.get("parentObservationId")
    if parent:
        if str(parent) in span_ids:
            span.parent_span_id = span_ids[str(parent)]
        else:
            warnings.append(f"Span {source_span_id} references missing parent {parent}")

    if span.end_time_unix_nano == 0:
        span.end_time_unix_nano = span.start_time_unix_nano
        warnings.append(f"Span {source_span_id} had no end timestamp")
    elif span.end_time_unix_nano < span.start_time_unix_nano:
        span.end_time_unix_nano = span.start_time_unix_nano
        warnings.append(f"Span {source_span_id} had a negative duration")

    metadata = dict(observation.get("metadata") or {})
    source_attributes = metadata.pop("attributes", {}) or {}
    resource_attributes = metadata.pop("resourceAttributes", {}) or {}
    if not isinstance(source_attributes, dict):
        source_attributes = {"langfuse.raw_attributes": source_attributes}
    if not isinstance(resource_attributes, dict):
        resource_attributes = {"langfuse.raw_resource_attributes": resource_attributes}

    for key, value in source_attributes.items():
        set_attribute(span, str(key), value)

    source_id_raw, source_id_valid = normalize_otel_id(source_span_id, 8, "span")
    if not source_id_valid or source_id_raw != span.span_id:
        set_attribute(span, "otel2dbx.source.span_id", source_span_id)
    set_attribute(span, "otel2dbx.source.system", "langfuse")
    set_attribute(span, "langfuse.observation.id", source_span_id)
    set_attribute(span, "langfuse.trace.id", str(observation.get("traceId", "")))
    set_attribute(span, "langfuse.observation.type", observation.get("type", "SPAN"))
    if metadata:
        set_attribute(span, "langfuse.observation.metadata", metadata)

    for key, source_key in (
        ("session.id", "sessionId"),
        ("user.id", "userId"),
        ("deployment.environment.name", "environment"),
        ("langfuse.version", "version"),
    ):
        if observation.get(source_key) not in (None, ""):
            set_attribute(span, key, observation[source_key])

    if observation.get("tags"):
        set_attribute(span, "langfuse.trace.tags", observation["tags"])
    if observation.get("release"):
        set_attribute(span, "langfuse.release", observation["release"])
    if observation.get("traceName"):
        set_attribute(span, "langfuse.trace.name", observation["traceName"])

    if observation.get("input") not in (None, ""):
        set_attribute(span, "gen_ai.input.messages", observation["input"])
    if observation.get("output") not in (None, ""):
        set_attribute(span, "gen_ai.output.messages", observation["output"])

    current_attributes = attributes_to_dict(span.attributes)
    prompt = current_attributes.get("user_prompt")
    if prompt and prompt != "<REDACTED>" and "gen_ai.input.messages" not in current_attributes:
        set_attribute(span, "gen_ai.input.messages", _json_message("user", str(prompt)))

    model = observation.get("providedModelName") or current_attributes.get("model")
    if model:
        set_attribute(span, "gen_ai.request.model", model)

    usage = observation.get("usageDetails") or {}
    input_tokens = usage.get("input", observation.get("inputUsage"))
    output_tokens = usage.get("output", observation.get("outputUsage"))
    input_tokens = current_attributes.get("input_tokens", input_tokens)
    output_tokens = current_attributes.get("output_tokens", output_tokens)
    if input_tokens is not None:
        set_attribute(span, "gen_ai.usage.input_tokens", int(input_tokens))
    if output_tokens is not None:
        set_attribute(span, "gen_ai.usage.output_tokens", int(output_tokens))

    operation = _operation_name(span.name, str(observation.get("type", "SPAN")))
    if operation:
        set_attribute(span, "gen_ai.operation.name", operation)

    level = str(observation.get("level", "DEFAULT")).upper()
    message = observation.get("statusMessage") or ""
    if level == "ERROR":
        span.status.code = Status.STATUS_CODE_ERROR
        span.status.message = str(message)
    else:
        span.status.code = Status.STATUS_CODE_UNSET
        if message:
            span.status.message = str(message)
    return span, resource_attributes


def build_langfuse_trace(observations: list[dict[str, Any]]) -> TraceEnvelope:
    if not observations:
        raise ValueError("Cannot build an OTLP trace from zero observations")
    source_trace_id = str(observations[0]["traceId"])
    raw_trace_id, trace_id_valid = normalize_otel_id(source_trace_id, 16, "trace")
    destination_trace_id = raw_trace_id.hex()
    warnings: list[str] = [
        "Langfuse's public API does not expose original span kind, "
        "instrumentation scope, links, or events"
    ]
    if not trace_id_valid:
        warnings.append(f"Mapped non-OTel trace ID {source_trace_id!r} to {destination_trace_id}")

    span_ids: dict[str, bytes] = {}
    for observation in observations:
        source_span_id = str(observation["id"])
        span_id, valid = normalize_otel_id(source_span_id, 8, "span")
        span_ids[source_span_id] = span_id
        if not valid:
            warnings.append(f"Mapped non-OTel span ID {source_span_id!r} to {span_id.hex()}")
    if len(set(span_ids.values())) != len(span_ids):
        raise ValueError(f"Span ID collision while mapping Langfuse trace {source_trace_id}")

    grouped: dict[str, tuple[dict[str, Any], list[Span]]] = {}
    root_spans: list[Span] = []
    for observation in sorted(observations, key=lambda item: item.get("startTime") or ""):
        span, resource_attributes = _observation_span(observation, raw_trace_id, span_ids, warnings)
        if not span.parent_span_id:
            root_spans.append(span)
        group_key = json.dumps(resource_attributes, sort_keys=True, default=str)
        grouped.setdefault(group_key, (resource_attributes, []))[1].append(span)

    all_spans = [span for _, spans in grouped.values() for span in spans]
    if not root_spans:
        synthetic = Span(
            trace_id=raw_trace_id,
            span_id=normalize_otel_id(f"synthetic:{source_trace_id}", 8, "span")[0],
            name="otel2dbx.synthetic_root",
            kind=Span.SPAN_KIND_INTERNAL,
            start_time_unix_nano=min(span.start_time_unix_nano for span in all_spans),
            end_time_unix_nano=max(span.end_time_unix_nano for span in all_spans),
        )
        set_attribute(synthetic, "otel2dbx.synthetic_root", True)
        set_attribute(synthetic, "otel2dbx.source.trace_id", source_trace_id)
        for span in all_spans:
            if not span.parent_span_id:
                span.parent_span_id = synthetic.span_id
        grouped.setdefault("{}", ({}, []))[1].append(synthetic)
        root_spans = [synthetic]
        warnings.append("Synthesized a root span because the source trace had none")

    if len(root_spans) == 1:
        root = root_spans[0]
        input_total = 0
        output_total = 0
        for span in all_spans:
            values = attributes_to_dict(span.attributes)
            input_total += int(values.get("gen_ai.usage.input_tokens") or 0)
            output_total += int(values.get("gen_ai.usage.output_tokens") or 0)
        root_values = attributes_to_dict(root.attributes)
        if input_total and "gen_ai.usage.input_tokens" not in root_values:
            set_attribute(root, "gen_ai.usage.input_tokens", input_total)
        if output_total and "gen_ai.usage.output_tokens" not in root_values:
            set_attribute(root, "gen_ai.usage.output_tokens", output_total)
        if not trace_id_valid:
            set_attribute(root, "otel2dbx.source.trace_id", source_trace_id)

    request = ExportTraceServiceRequest()
    for resource_attributes, spans in grouped.values():
        resource = Resource(
            attributes=[
                KeyValue(key=str(key), value=to_any_value(value))
                for key, value in resource_attributes.items()
            ]
        )
        resource_spans = ResourceSpans(resource=resource)
        scope_spans = ScopeSpans(
            scope=InstrumentationScope(name="otel2dbx.langfuse", version="0.1.0")
        )
        scope_spans.spans.extend(spans)
        resource_spans.scope_spans.append(scope_spans)
        request.resource_spans.append(resource_spans)

    return TraceEnvelope(
        source_trace_id=source_trace_id,
        destination_trace_id=destination_trace_id,
        request=request,
        source="langfuse",
        warnings=list(dict.fromkeys(warnings)),
        lossless=False,
    )
