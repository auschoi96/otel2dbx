from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from otel2dbx.databricks import ZerobusOTLPSink
from otel2dbx.manifest import RunManifest
from otel2dbx.models import TraceEnvelope


@dataclass
class MigrationSummary:
    run_id: str
    discovered: int = 0
    exported: int = 0
    existing: int = 0
    skipped: int = 0
    verified: int = 0
    failed: int = 0
    spans: int = 0
    warnings: int = 0


class MigrationRunner:
    def __init__(
        self,
        sink: ZerobusOTLPSink,
        *,
        progress: Callable[[str], None] | None = None,
    ) -> None:
        self.sink = sink
        self.progress = progress or (lambda _: None)

    def run(
        self,
        traces: Iterable[TraceEnvelope],
        *,
        source_name: str,
        dry_run: bool = False,
        verify: bool = True,
        force: bool = False,
        resume_run_id: str | None = None,
    ) -> tuple[MigrationSummary, RunManifest]:
        destination = self.sink.resolve()
        manifest = (
            RunManifest.load(resume_run_id)
            if resume_run_id
            else RunManifest.create(source_name, destination.uc_target.spans_table)
        )
        summary = MigrationSummary(run_id=manifest.run_id)
        try:
            for envelope in traces:
                summary.discovered += 1
                summary.spans += envelope.span_count
                summary.warnings += len(envelope.warnings)
                if manifest.completed(envelope.source_trace_id):
                    summary.skipped += 1
                    self.progress(f"skip {envelope.source_trace_id} (checkpoint)")
                    continue

                base_record: dict[str, Any] = {
                    "destination_trace_id": envelope.destination_trace_id,
                    "span_count": envelope.span_count,
                    "lossless": envelope.lossless,
                    "warnings": envelope.warnings,
                }
                if dry_run:
                    base_record["status"] = "dry-run"
                    manifest.record_trace(envelope.source_trace_id, base_record)
                    self.progress(f"plan {envelope.source_trace_id} ({envelope.span_count} spans)")
                    continue

                if not force and self.sink.trace_exists(envelope.destination_trace_id):
                    summary.existing += 1
                    if verify:
                        result = self.sink.verify(
                            envelope.destination_trace_id,
                            envelope.request,
                        )
                        base_record["status"] = "verified"
                        base_record["verification"] = result
                        summary.verified += 1
                        self.progress(f"verify {envelope.source_trace_id} (already in Databricks)")
                    else:
                        base_record["status"] = "existing"
                        self.progress(f"skip {envelope.source_trace_id} (already in Databricks)")
                    manifest.record_trace(envelope.source_trace_id, base_record)
                    continue

                try:
                    self.sink.export(envelope.request)
                    base_record["status"] = "exported"
                    manifest.record_trace(envelope.source_trace_id, base_record)
                    summary.exported += 1
                    self.progress(
                        f"export {envelope.source_trace_id} ({envelope.span_count} spans)"
                    )
                    if verify:
                        result = self.sink.verify(
                            envelope.destination_trace_id,
                            envelope.request,
                        )
                        base_record["status"] = "verified"
                        base_record["verification"] = result
                        manifest.record_trace(envelope.source_trace_id, base_record)
                        summary.verified += 1
                except Exception as exc:
                    summary.failed += 1
                    base_record["status"] = "failed"
                    base_record["error"] = str(exc)
                    manifest.record_trace(envelope.source_trace_id, base_record)
                    manifest.data.setdefault("errors", []).append(
                        {"trace_id": envelope.source_trace_id, "error": str(exc)}
                    )
                    manifest.save()
                    self.progress(f"fail {envelope.source_trace_id}: {exc}")
            manifest.finish("failed" if summary.failed else "dry-run" if dry_run else "complete")
            return summary, manifest
        finally:
            self.sink.close()
