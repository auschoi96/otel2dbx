from __future__ import annotations

from datetime import UTC, datetime

import httpx

from otel2dbx.sources.langfuse import LangfuseSource
from tests.test_otel import observations


def test_langfuse_source_paginates_discovery_and_refetches_trace() -> None:
    calls: list[dict[str, str]] = []
    rows = observations()

    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        calls.append(params)
        if params.get("fields") == "core" and not params.get("cursor"):
            return httpx.Response(
                200,
                json={"data": [{"traceId": rows[0]["traceId"]}], "meta": {"cursor": "next"}},
            )
        if params.get("fields") == "core":
            return httpx.Response(200, json={"data": [], "meta": {"cursor": None}})
        return httpx.Response(200, json={"data": rows, "meta": {"cursor": None}})

    client = httpx.Client(
        base_url="http://langfuse.test",
        transport=httpx.MockTransport(handler),
    )
    source = LangfuseSource("http://langfuse.test", "pk", "sk", client=client)
    start = datetime(2026, 8, 3, 12, tzinfo=UTC)
    traces = list(source.iter_traces(start, start.replace(hour=13)))

    assert len(traces) == 1
    assert traces[0].span_count == 2
    assert len(calls) == 3
    assert calls[1]["cursor"] == "next"
    assert calls[2]["traceId"] == rows[0]["traceId"]
