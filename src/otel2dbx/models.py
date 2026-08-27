from __future__ import annotations

from dataclasses import dataclass, field

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest


@dataclass
class TraceEnvelope:
    source_trace_id: str
    destination_trace_id: str
    request: ExportTraceServiceRequest
    source: str
    warnings: list[str] = field(default_factory=list)
    lossless: bool = False

    @property
    def span_count(self) -> int:
        return sum(
            len(scope.spans)
            for resource in self.request.resource_spans
            for scope in resource.scope_spans
        )
