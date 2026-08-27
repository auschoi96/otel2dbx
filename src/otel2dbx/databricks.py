from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

import httpx
import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from mlflow.entities.trace_location import UnityCatalog
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

from otel2dbx.config import (
    DEFAULT_DATABRICKS_HOST,
    DEFAULT_ZEROBUS_REGION,
    DEFAULT_ZEROBUS_WORKSPACE_ID,
    DatabricksTarget,
    UnityCatalogTarget,
)
from otel2dbx.errors import ConfigurationError, DestinationError, VerificationError
from otel2dbx.otel import iter_spans


@dataclass(frozen=True)
class ResolvedDestination:
    host: str
    experiment_id: str
    uc_target: UnityCatalogTarget
    workspace_id: str = DEFAULT_ZEROBUS_WORKSPACE_ID
    region: str = DEFAULT_ZEROBUS_REGION

    @property
    def endpoint(self) -> str:
        return (
            f"https://{self.workspace_id}.zerobus.{self.region}.cloud.databricks.com"
            ":443/v1/traces"
        )

    @property
    def token_url(self) -> str:
        return f"{self.host.rstrip('/')}/oidc/v1/token"

    @property
    def oauth_resource(self) -> str:
        return f"api://databricks/workspaces/{self.workspace_id}/zerobusDirectWriteApi"


def _confirm_uc_binding(experiment_id: str, target: UnityCatalogTarget) -> None:
    """Guard the permanent experiment -> Unity Catalog trace-location binding.

    Interactive runs must confirm explicitly; non-interactive contexts (bundle jobs,
    the Claude hook) proceed with a warning because they were invoked deliberately.
    """
    message = (
        f"Binding MLflow experiment {experiment_id} permanently to "
        f"{target.trace_location} for OTEL trace storage."
    )
    if sys.stdin.isatty():
        reply = input(f"{message}\nThis cannot be undone. Continue? [y/N] ")
        if reply.strip().lower() not in {"y", "yes"}:
            raise ConfigurationError("Aborted before binding the experiment.")
    else:
        print(f"WARNING: {message}", file=sys.stderr)


class ZerobusOTLPSink:
    """Write OTLP protobuf through Zerobus and verify it through managed MLflow."""

    def __init__(
        self,
        target: DatabricksTarget,
        *,
        client: httpx.Client | None = None,
        max_attempts: int = 5,
        config: Any | None = None,
    ) -> None:
        self.target = target
        self._config = config
        self._owns_client = client is None
        self.client = client or httpx.Client(timeout=60.0)
        self.max_attempts = max_attempts
        self._resolved: ResolvedDestination | None = None
        self._access_token: str | None = None
        self._access_token_expiry = 0.0

    def _auth_hint(self) -> str:
        if self.target.profile:
            host = DEFAULT_DATABRICKS_HOST or "https://<your-workspace>.cloud.databricks.com"
            return (
                f"Databricks profile {self.target.profile!r} is not authenticated. "
                f"Run: databricks auth login {host} "
                f"--profile {self.target.profile}"
            )
        return (
            "No Databricks credentials available. Pass --profile (or set "
            "OTEL2DBX_DATABRICKS_PROFILE) when running locally; on Databricks "
            "compute the ambient identity is used automatically."
        )

    def _get_config(self) -> Config:
        if self._config is not None:
            return self._config
        try:
            self._config = (
                Config(profile=self.target.profile) if self.target.profile else Config()
            )
        except Exception as exc:
            raise ConfigurationError(self._auth_hint()) from exc
        return self._config

    def close(self) -> None:
        if self._owns_client:
            self.client.close()

    def workspace_auth_headers(self) -> dict[str, str]:
        try:
            headers = self._get_config().authenticate()
        except Exception as exc:
            raise ConfigurationError(self._auth_hint()) from exc
        if not headers or "Authorization" not in headers:
            raise ConfigurationError(
                f"Databricks profile {self.target.profile!r} did not provide "
                "an Authorization header"
            )
        return dict(headers)

    def _zerobus_credentials(self) -> tuple[str, str]:
        client_id = self.target.zerobus_client_id or os.getenv("ZEROBUS_CLIENT_ID", "")
        client_secret = self.target.zerobus_client_secret or os.getenv(
            "ZEROBUS_CLIENT_SECRET", ""
        )
        if not client_id or not client_secret:
            raise ConfigurationError(
                "Zerobus requires service-principal OAuth credentials. Set "
                "ZEROBUS_CLIENT_ID and ZEROBUS_CLIENT_SECRET."
            )
        return client_id, client_secret

    def _authorization_details(self, table_name: str) -> str:
        catalog, schema, _ = table_name.split(".", 2)
        return json.dumps(
            [
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["USE CATALOG"],
                    "object_type": "CATALOG",
                    "object_full_path": catalog,
                },
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["USE SCHEMA"],
                    "object_type": "SCHEMA",
                    "object_full_path": f"{catalog}.{schema}",
                },
                {
                    "type": "unity_catalog_privileges",
                    "privileges": ["SELECT", "MODIFY"],
                    "object_type": "TABLE",
                    "object_full_path": table_name,
                },
            ],
            separators=(",", ":"),
        )

    def zerobus_access_token(self, *, force_refresh: bool = False) -> str:
        destination = self.resolve()
        if (
            not force_refresh
            and self._access_token
            and time.monotonic() < self._access_token_expiry - 60
        ):
            return self._access_token

        client_id, client_secret = self._zerobus_credentials()
        response = self.client.post(
            destination.token_url,
            auth=httpx.BasicAuth(client_id, client_secret),
            data={
                "grant_type": "client_credentials",
                "scope": "all-apis",
                "resource": destination.oauth_resource,
                "authorization_details": self._authorization_details(
                    destination.uc_target.spans_table
                ),
            },
        )
        if response.status_code >= 400:
            raise ConfigurationError(
                f"Zerobus OAuth token request failed ({response.status_code}): "
                f"{response.text[:500]}"
            )
        try:
            payload = response.json()
            token = str(payload["access_token"])
            expires_in = max(int(payload.get("expires_in", 3600)), 1)
        except (KeyError, TypeError, ValueError) as exc:
            raise ConfigurationError("Zerobus OAuth returned an invalid token response") from exc
        self._access_token = token
        self._access_token_expiry = time.monotonic() + expires_in
        return token

    def validate_zerobus_auth(self) -> dict[str, str]:
        destination = self.resolve()
        client_id, _ = self._zerobus_credentials()
        self.zerobus_access_token()
        return {
            "client_id": client_id,
            "endpoint": destination.endpoint,
            "table": destination.uc_target.spans_table,
        }

    def resolve(self) -> ResolvedDestination:
        if self._resolved:
            return self._resolved
        config = self._get_config()
        host = (config.host or "").rstrip("/")
        if not host:
            raise ConfigurationError(self._auth_hint())

        self.workspace_auth_headers()
        if self.target.profile:
            # UC trace reads currently construct a Databricks SDK client internally;
            # pin the same profile so they do not fall back to an ambiguous DEFAULT.
            os.environ["DATABRICKS_CONFIG_PROFILE"] = self.target.profile
            mlflow.set_tracking_uri(f"databricks://{self.target.profile}")
        else:
            mlflow.set_tracking_uri("databricks")
        os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = self.target.warehouse_id
        if not self.target.experiment_id:
            raise ConfigurationError(
                "No MLflow experiment configured. Set OTEL2DBX_EXPERIMENT_ID in your .env "
                "or pass --experiment-id (create one with `otel2dbx setup --experiment-name ...`)."
            )
        try:
            experiment = mlflow.get_experiment(self.target.experiment_id)
        except Exception as exc:
            raise ConfigurationError(
                f"Cannot load Databricks MLflow experiment {self.target.experiment_id}: {exc}"
            ) from exc
        if experiment is None:
            raise ConfigurationError(
                f"Databricks MLflow experiment {self.target.experiment_id} does not exist"
            )

        location = experiment.trace_location
        if location is None:
            if self.target.uc_target is None:
                raise ConfigurationError(
                    f"Experiment {self.target.experiment_id} has no Unity Catalog trace location. "
                    "Provide --uc-catalog, --uc-schema, and --table-prefix together."
                )
            requested = self.target.uc_target
            _confirm_uc_binding(self.target.experiment_id, requested)
            try:
                experiment = mlflow.set_experiment(
                    experiment_id=self.target.experiment_id,
                    trace_location=UnityCatalog(
                        catalog_name=requested.catalog,
                        schema_name=requested.schema,
                        table_prefix=requested.table_prefix,
                    ),
                )
            except Exception as exc:
                raise ConfigurationError(
                    "Failed to bind the experiment to Unity Catalog. Confirm the tracing previews "
                    "are enabled and that you have USE CATALOG, USE SCHEMA, CREATE TABLE, SELECT, "
                    f"and MODIFY privileges: {exc}"
                ) from exc
            location = experiment.trace_location

        if not isinstance(location, UnityCatalog):
            raise ConfigurationError(
                f"Experiment {self.target.experiment_id} is not bound to "
                "a Unity Catalog trace location"
            )
        uc_target = UnityCatalogTarget(
            catalog=location.catalog_name,
            schema=location.schema_name,
            table_prefix=location.table_prefix,
        )
        try:
            table = WorkspaceClient(config=config).tables.get(uc_target.spans_table)
        except Exception as exc:
            raise ConfigurationError(
                f"Cannot load Zerobus target table {uc_target.spans_table}: {exc}"
            ) from exc
        required_columns = {
            "record_id",
            "time",
            "date",
            "service_name",
            "trace_id",
            "span_id",
            "name",
            "kind",
            "attributes",
            "events",
            "links",
            "resource",
            "instrumentation_scope",
        }
        column_names = {str(column.name) for column in table.columns or []}
        missing = sorted(required_columns - column_names)
        properties = table.properties or {}
        if "MANAGED" not in str(table.table_type).upper():
            raise ConfigurationError(
                f"Zerobus target {uc_target.spans_table} must be a managed Delta table"
            )
        if properties.get("otel.schemaVersion") != "v2" or missing:
            detail = f"; missing columns: {', '.join(missing)}" if missing else ""
            raise ConfigurationError(
                f"Zerobus target {uc_target.spans_table} is not an OTEL v2 spans table{detail}"
            )
        workspace_id = self.target.zerobus_workspace_id or getattr(
            config, "workspace_id", None
        )
        if not workspace_id:
            raise ConfigurationError(
                "Cannot determine the Zerobus workspace ID. Set ZEROBUS_WORKSPACE_ID "
                "(the o= number in workspace URLs)."
            )
        self._resolved = ResolvedDestination(
            host=host,
            experiment_id=self.target.experiment_id,
            uc_target=uc_target,
            workspace_id=str(workspace_id),
            region=self.target.zerobus_region or DEFAULT_ZEROBUS_REGION,
        )
        mlflow.set_experiment(experiment_id=self.target.experiment_id)
        return self._resolved

    def export(self, request: ExportTraceServiceRequest) -> None:
        destination = self.resolve()
        payload = request.SerializeToString()
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                token = self.zerobus_access_token()
                headers = {
                    "authorization": f"Bearer {token}",
                    "content-type": "application/x-protobuf",
                    "x-databricks-zerobus-table-name": destination.uc_target.spans_table,
                }
                response = self.client.post(destination.endpoint, content=payload, headers=headers)
                if response.status_code == 401:
                    self._access_token = None
                    self._access_token_expiry = 0.0
                    if attempt < self.max_attempts:
                        continue
                if response.status_code in {408, 429} or response.status_code >= 500:
                    response.raise_for_status()
                if response.status_code >= 400:
                    raise DestinationError(
                        f"Zerobus OTLP rejected the trace ({response.status_code}): "
                        f"{response.text[:500]}"
                    )
                parsed = ExportTraceServiceResponse()
                if response.content:
                    parsed.ParseFromString(response.content)
                    partial = parsed.partial_success
                    if partial.rejected_spans:
                        raise DestinationError(
                            f"Zerobus rejected {partial.rejected_spans} spans: "
                            f"{partial.error_message}"
                        )
                return
            except DestinationError:
                raise
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    break
                time.sleep(min(2 ** (attempt - 1), 8))
        raise DestinationError(
            f"Zerobus OTLP export failed after {self.max_attempts} attempts: {last_error}"
        )

    def trace_uri(self, trace_id: str) -> str:
        location = self.resolve().uc_target
        return f"trace:/{location.trace_location}/{trace_id}"

    def validate_warehouse(self) -> dict[str, str]:
        try:
            warehouse = WorkspaceClient(config=self._get_config()).warehouses.get(
                self.target.warehouse_id
            )
        except Exception as exc:
            raise ConfigurationError(
                f"Cannot use SQL warehouse {self.target.warehouse_id}: {exc}"
            ) from exc
        return {
            "id": str(warehouse.id),
            "name": str(warehouse.name),
            "state": str(warehouse.state),
        }

    def get_trace(self, trace_id: str) -> Any | None:
        self.resolve()
        # UC trace visibility is eventually consistent; an expected "not found"
        # during preflight or post-export polling should not alarm the operator.
        fluent_logger = logging.getLogger("mlflow.tracing.fluent")
        previous_level = fluent_logger.level
        fluent_logger.setLevel(logging.ERROR)
        try:
            return mlflow.get_trace(self.trace_uri(trace_id))
        except Exception as exc:
            message = str(exc).lower()
            if any(
                token in message
                for token in ("not found", "does not exist", "resource_does_not_exist")
            ):
                return None
            raise ConfigurationError(f"Failed to query Databricks trace {trace_id}: {exc}") from exc
        finally:
            fluent_logger.setLevel(previous_level)

    def trace_exists(self, trace_id: str) -> bool:
        return self.get_trace(trace_id) is not None

    def verify(
        self,
        trace_id: str,
        source_request: ExportTraceServiceRequest,
        *,
        timeout_seconds: int = 120,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        trace = None
        while time.monotonic() < deadline:
            # Poll after a pause: the first instant check almost always misses
            # while the Zerobus write becomes queryable.
            time.sleep(2)
            trace = self.get_trace(trace_id)
            if trace is not None:
                break
        if trace is None:
            raise VerificationError(
                f"Trace {trace_id} did not become queryable in Databricks within {timeout_seconds}s"
            )

        source_spans = list(iter_spans(source_request))
        destination_spans = list(trace.data.spans)
        source_names = sorted(span.name for span in source_spans)
        destination_names = sorted(span.name for span in destination_spans)
        if len(source_spans) != len(destination_spans):
            raise VerificationError(
                f"Trace {trace_id} span count mismatch: source={len(source_spans)}, "
                f"destination={len(destination_spans)}"
            )
        if source_names != destination_names:
            raise VerificationError(f"Trace {trace_id} span names differ after migration")
        return {
            "trace_id": trace_id,
            "span_count": len(source_spans),
            "names_match": True,
            "state": str(trace.info.state),
            "trace_uri": self.trace_uri(trace_id),
        }
