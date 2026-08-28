from __future__ import annotations

import json
from urllib.parse import parse_qs

import httpx
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceResponse

from otel2dbx.config import DatabricksTarget, UnityCatalogTarget
from otel2dbx.databricks import ResolvedDestination, ZerobusOTLPSink
from tests.otlp_fixtures import sample_request


def test_export_posts_protobuf_to_zerobus_otlp_endpoint() -> None:
    captured: dict[str, object] = {"token_requests": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oidc/v1/token":
            captured["token_requests"] = int(captured["token_requests"]) + 1
            captured["token_body"] = request.content.decode()
            return httpx.Response(200, json={"access_token": "test-token", "expires_in": 3600})
        captured["url"] = str(request.url)
        captured["headers"] = request.headers
        captured["body"] = request.content
        return httpx.Response(200, content=ExportTraceServiceResponse().SerializeToString())

    target = DatabricksTarget(
        uc_target=UnityCatalogTarget("catalog", "schema", "prefix"),
        zerobus_client_id="client-id",
        zerobus_client_secret="client-secret",
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))
    sink = ZerobusOTLPSink(target, client=client, config=object())
    sink._resolved = ResolvedDestination(
        host="https://example-workspace.cloud.databricks.com",
        experiment_id=target.experiment_id,
        uc_target=target.uc_target,
        workspace_id="1234567890123456",
        region="us-east-1",
    )
    request = sample_request()

    sink.export(request)

    assert (
        captured["url"]
        == "https://1234567890123456.zerobus.us-east-1.cloud.databricks.com/v1/traces"
    )
    headers = captured["headers"]
    assert headers["x-databricks-zerobus-table-name"] == "catalog.schema.prefix_otel_spans"
    assert headers["authorization"] == "Bearer test-token"
    assert headers["content-type"] == "application/x-protobuf"
    assert captured["body"] == request.SerializeToString()
    assert captured["token_requests"] == 1
    token_form = parse_qs(str(captured["token_body"]))
    assert token_form["resource"] == [
        "api://databricks/workspaces/1234567890123456/zerobusDirectWriteApi"
    ]
    authorization_details = json.loads(token_form["authorization_details"][0])
    assert authorization_details[-1] == {
        "type": "unity_catalog_privileges",
        "privileges": ["SELECT", "MODIFY"],
        "object_type": "TABLE",
        "object_full_path": "catalog.schema.prefix_otel_spans",
    }


def test_export_refreshes_zerobus_token_after_unauthorized() -> None:
    calls = {"token": 0, "export": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oidc/v1/token":
            calls["token"] += 1
            return httpx.Response(
                200,
                json={"access_token": f"token-{calls['token']}", "expires_in": 3600},
            )
        calls["export"] += 1
        if calls["export"] == 1:
            return httpx.Response(401, text="expired")
        assert request.headers["authorization"] == "Bearer token-2"
        return httpx.Response(200, content=ExportTraceServiceResponse().SerializeToString())

    target = DatabricksTarget(
        uc_target=UnityCatalogTarget("catalog", "schema", "prefix"),
        zerobus_client_id="client-id",
        zerobus_client_secret="client-secret",
    )
    sink = ZerobusOTLPSink(
        target,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        config=object(),
        max_attempts=2,
    )
    sink._resolved = ResolvedDestination(
        host="https://example-workspace.cloud.databricks.com",
        experiment_id=target.experiment_id,
        uc_target=target.uc_target,
        workspace_id="1234567890123456",
    )

    sink.export(sample_request())

    assert calls == {"token": 2, "export": 2}
