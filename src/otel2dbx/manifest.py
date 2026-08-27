from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from otel2dbx.config import RUNS_DIR


@dataclass
class RunManifest:
    path: Path
    data: dict[str, Any]

    @classmethod
    def create(cls, source: str, destination: str, *, run_id: str | None = None) -> RunManifest:
        identifier = run_id or uuid4().hex[:12]
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        manifest = cls(
            path=RUNS_DIR / f"{identifier}.json",
            data={
                "run_id": identifier,
                "source": source,
                "destination": destination,
                "status": "running",
                "started_at": datetime.now(UTC).isoformat(),
                "traces": {},
                "warnings": [],
                "errors": [],
            },
        )
        manifest.save()
        return manifest

    @classmethod
    def load(cls, run_id: str) -> RunManifest:
        path = RUNS_DIR / f"{run_id}.json"
        return cls(path=path, data=json.loads(path.read_text(encoding="utf-8")))

    @property
    def run_id(self) -> str:
        return str(self.data["run_id"])

    def completed(self, source_trace_id: str) -> bool:
        return (self.data.get("traces", {}).get(source_trace_id) or {}).get("status") in {
            "exported",
            "verified",
            "existing",
        }

    def record_trace(self, source_trace_id: str, record: dict[str, Any]) -> None:
        self.data.setdefault("traces", {})[source_trace_id] = record
        self.save()

    def finish(self, status: str) -> None:
        self.data["status"] = status
        self.data["finished_at"] = datetime.now(UTC).isoformat()
        self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.data, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, self.path)
