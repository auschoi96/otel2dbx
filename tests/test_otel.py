from __future__ import annotations

from pathlib import Path

from google.protobuf.json_format import MessageToJson
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from otel2dbx.otel import iter_spans, load_otlp_json_lines, split_request_by_trace, trace_ids
from tests.otlp_fixtures import ROOT_ID, TRACE_ID, sample_request


def test_iter_spans_and_trace_ids() -> None:
    request = sample_request()
    spans = list(iter_spans(request))
    assert len(spans) == 3
    assert trace_ids(request) == [TRACE_ID.hex()]
    root = next(span for span in spans if not span.parent_span_id)
    assert root.span_id == ROOT_ID


def test_otlp_json_round_trip_is_lossless(tmp_path: Path) -> None:
    original = sample_request()
    path = tmp_path / "traces.jsonl"
    path.write_text(MessageToJson(original, indent=None) + "\n", encoding="utf-8")

    envelopes = list(load_otlp_json_lines(path))

    assert len(envelopes) == 1
    envelope = envelopes[0]
    assert envelope.lossless is True
    assert envelope.source_trace_id == TRACE_ID.hex()
    assert envelope.destination_trace_id == TRACE_ID.hex()
    assert envelope.span_count == 3
    # The protobuf is replayed byte-for-byte — the core fidelity guarantee.
    assert envelope.request.SerializeToString() == original.SerializeToString()


def test_split_request_by_trace_separates_traces() -> None:
    # Two independent single-span traces in one request split into two envelopes.
    merged = ExportTraceServiceRequest()
    for request in (sample_request(), _second_trace()):
        merged.resource_spans.extend(request.resource_spans)

    envelopes = list(split_request_by_trace(merged, source="test", lossless=True))

    assert {env.source_trace_id for env in envelopes} == {TRACE_ID.hex(), "b" * 32}
    assert all(len(trace_ids(env.request)) == 1 for env in envelopes)


def _second_trace() -> ExportTraceServiceRequest:
    request = sample_request()
    other_trace = bytes.fromhex("b" * 32)
    for resource in request.resource_spans:
        for scope in resource.scope_spans:
            del scope.spans[1:]  # keep a single span
            for span in scope.spans:
                span.trace_id = other_trace
    return request
