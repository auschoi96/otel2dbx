from __future__ import annotations

import json
from pathlib import Path

from google.protobuf.json_format import MessageToJson

from otel2dbx.otel import (
    attributes_to_dict,
    build_langfuse_trace,
    iter_spans,
    load_otlp_json_lines,
    normalize_otel_id,
)


def observations(*, valid_ids: bool = True) -> list[dict[str, object]]:
    trace_id = "1" * 32 if valid_ids else "langfuse-trace"
    root_id = "2" * 16 if valid_ids else "root-observation"
    child_id = "3" * 16 if valid_ids else "child-observation"
    return [
        {
            "id": root_id,
            "traceId": trace_id,
            "parentObservationId": None,
            "name": "claude_code.interaction",
            "type": "SPAN",
            "startTime": "2026-08-03T12:00:00Z",
            "endTime": "2026-08-03T12:00:02Z",
            "level": "DEFAULT",
            "metadata": {
                "attributes": {"user_prompt": "Fix the safe demo parser"},
                "resourceAttributes": {"service.name": "claude-code-demo"},
            },
            "sessionId": "session-1",
            "userId": "demo-user",
            "traceName": "Claude demo",
        },
        {
            "id": child_id,
            "traceId": trace_id,
            "parentObservationId": root_id,
            "name": "claude_code.llm_request",
            "type": "GENERATION",
            "startTime": "2026-08-03T12:00:00.1Z",
            "endTime": "2026-08-03T12:00:01Z",
            "level": "DEFAULT",
            "providedModelName": "claude-sonnet",
            "input": json.dumps([{"role": "user", "content": "Fix it"}]),
            "output": json.dumps([{"role": "assistant", "content": "Done"}]),
            "usageDetails": {"input": 12, "output": 4},
            "metadata": {
                "attributes": {"model": "claude-sonnet", "input_tokens": 12, "output_tokens": 4},
                "resourceAttributes": {"service.name": "claude-code-demo"},
            },
        },
    ]


def test_normalize_preserves_valid_otel_id() -> None:
    value, valid = normalize_otel_id("ab" * 16, 16, "trace")
    assert valid is True
    assert value.hex() == "ab" * 16


def test_normalize_maps_vendor_id_deterministically() -> None:
    first, first_valid = normalize_otel_id("vendor-id", 16, "trace")
    second, second_valid = normalize_otel_id("vendor-id", 16, "trace")
    assert not first_valid and not second_valid
    assert first == second
    assert len(first) == 16


def test_build_langfuse_trace_preserves_tree_and_enriches_claude() -> None:
    envelope = build_langfuse_trace(observations())
    spans = list(iter_spans(envelope.request))
    root = next(span for span in spans if not span.parent_span_id)
    child = next(span for span in spans if span.parent_span_id)
    root_attributes = attributes_to_dict(root.attributes)
    child_attributes = attributes_to_dict(child.attributes)

    assert envelope.destination_trace_id == "1" * 32
    assert envelope.span_count == 2
    assert child.parent_span_id == root.span_id
    assert root_attributes["gen_ai.operation.name"] == "invoke_agent"
    assert root_attributes["gen_ai.usage.input_tokens"] == 12
    assert root_attributes["gen_ai.usage.output_tokens"] == 4
    assert child_attributes["gen_ai.operation.name"] == "chat"
    assert child_attributes["gen_ai.request.model"] == "claude-sonnet"
    assert "public API does not expose" in envelope.warnings[0]


def test_build_langfuse_trace_retains_invalid_source_ids() -> None:
    envelope = build_langfuse_trace(observations(valid_ids=False))
    root = next(span for span in iter_spans(envelope.request) if not span.parent_span_id)
    attributes = attributes_to_dict(root.attributes)
    assert envelope.destination_trace_id != "langfuse-trace"
    assert attributes["otel2dbx.source.trace_id"] == "langfuse-trace"
    assert attributes["otel2dbx.source.span_id"] == "root-observation"


def test_otlp_json_round_trip_is_lossless(tmp_path: Path) -> None:
    original = build_langfuse_trace(observations()).request
    path = tmp_path / "traces.jsonl"
    path.write_text(MessageToJson(original, indent=None) + "\n", encoding="utf-8")
    envelopes = list(load_otlp_json_lines(path))
    assert len(envelopes) == 1
    assert envelopes[0].lossless is True
    assert envelopes[0].request.SerializeToString() == original.SerializeToString()
