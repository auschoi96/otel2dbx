from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import httpx

from otel2dbx.errors import ConfigurationError, SourceError
from otel2dbx.models import TraceEnvelope
from otel2dbx.otel import build_langfuse_trace

ALL_FIELDS = "core,basic,time,io,metadata,model,usage,prompt,metrics,trace_context"


class LangfuseSource:
    def __init__(
        self,
        base_url: str,
        public_key: str,
        secret_key: str,
        *,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        if not public_key or not secret_key:
            raise ConfigurationError("LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are required")
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self.client = client or httpx.Client(
            base_url=self.base_url,
            auth=httpx.BasicAuth(public_key, secret_key),
            timeout=timeout,
            headers={"accept": "application/json"},
        )

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def _pages(self, params: dict[str, Any]) -> Iterator[list[dict[str, Any]]]:
        cursor: str | None = None
        while True:
            page_params = dict(params)
            if cursor:
                page_params["cursor"] = cursor
            try:
                response = self.client.get("/api/public/v2/observations", params=page_params)
                response.raise_for_status()
                payload = response.json()
            except (httpx.HTTPError, ValueError) as exc:
                raise SourceError(f"Langfuse observations request failed: {exc}") from exc
            data = payload.get("data", [])
            if not isinstance(data, list):
                raise SourceError("Langfuse returned a non-list observations payload")
            yield data
            cursor = (payload.get("meta") or {}).get("cursor")
            if not cursor:
                return

    def discover_trace_ids(self, start: datetime, end: datetime) -> list[str]:
        params = {
            "fields": "core",
            "limit": 1000,
            "fromStartTime": start.isoformat(),
            "toStartTime": end.isoformat(),
        }
        trace_ids: dict[str, None] = {}
        for observations in self._pages(params):
            for observation in observations:
                trace_id = observation.get("traceId")
                if trace_id:
                    trace_ids[str(trace_id)] = None
        return list(trace_ids)

    def fetch_trace(self, trace_id: str) -> list[dict[str, Any]]:
        params = {
            "fields": ALL_FIELDS,
            "limit": 1000,
            "traceId": trace_id,
        }
        observations: list[dict[str, Any]] = []
        for page in self._pages(params):
            observations.extend(page)
        if not observations:
            raise SourceError(f"Langfuse trace {trace_id} disappeared during migration")
        return observations

    def iter_traces(self, start: datetime, end: datetime) -> Iterator[TraceEnvelope]:
        try:
            for trace_id in self.discover_trace_ids(start, end):
                yield build_langfuse_trace(self.fetch_trace(trace_id))
        finally:
            self.close()
