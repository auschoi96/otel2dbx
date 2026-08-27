from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from otel2dbx.errors import SourceError
from otel2dbx.models import TraceEnvelope
from otel2dbx.otel import load_otlp_json_lines


class OtlpJsonSource:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _files(self) -> list[Path]:
        if self.path.is_file():
            return [self.path]
        if self.path.is_dir():
            return sorted(
                candidate
                for candidate in self.path.iterdir()
                if candidate.is_file() and candidate.suffix in {".json", ".jsonl"}
            )
        raise SourceError(f"OTLP JSON path does not exist: {self.path}")

    def iter_traces(self) -> Iterator[TraceEnvelope]:
        files = self._files()
        if not files:
            raise SourceError(f"No .json or .jsonl files found in {self.path}")
        for path in files:
            try:
                yield from load_otlp_json_lines(path)
            except (OSError, ValueError) as exc:
                raise SourceError(str(exc)) from exc
