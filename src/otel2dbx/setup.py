from __future__ import annotations

import base64

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from otel2dbx.errors import ConfigurationError

# Secret-scope keys (lowercase) mapped to the environment variables the CLI reads.
SECRET_SCOPE_KEYS = {
    "zerobus_client_id": "ZEROBUS_CLIENT_ID",
    "zerobus_client_secret": "ZEROBUS_CLIENT_SECRET",
    "zerobus_workspace_id": "ZEROBUS_WORKSPACE_ID",
    "zerobus_region": "ZEROBUS_REGION",
}


def _workspace_client(profile: str | None) -> WorkspaceClient:
    config = Config(profile=profile) if profile else Config()
    return WorkspaceClient(config=config)


def ensure_experiment(profile: str | None, experiment_name: str) -> str:
    """Return the ID of the named MLflow experiment, creating it if needed."""
    mlflow.set_tracking_uri(f"databricks://{profile}" if profile else "databricks")
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is not None:
        return str(experiment.experiment_id)
    return str(mlflow.create_experiment(experiment_name))


def ensure_secret_scope(
    scope: str, values: dict[str, str], profile: str | None
) -> None:
    """Create the scope if needed and store each non-empty value."""
    workspace = _workspace_client(profile)
    try:
        workspace.secrets.create_scope(scope=scope)
    except Exception as exc:
        if "already exists" not in str(exc).lower():
            raise ConfigurationError(f"Cannot create secret scope {scope!r}: {exc}") from exc
    for key, value in values.items():
        if value:
            workspace.secrets.put_secret(scope=scope, key=key, string_value=value)


def read_secret_scope(scope: str, profile: str | None) -> dict[str, str]:
    """Read known Zerobus keys from a secret scope into environment-variable names."""
    workspace = _workspace_client(profile)
    values: dict[str, str] = {}
    for key, env_name in SECRET_SCOPE_KEYS.items():
        try:
            response = workspace.secrets.get_secret(scope=scope, key=key)
        except Exception:
            continue
        raw = response.value or b""
        if isinstance(raw, str):
            raw = raw.encode()
        try:
            decoded = base64.b64decode(raw, validate=True).decode()
        except Exception:
            decoded = raw.decode(errors="replace")
        values[env_name] = decoded.strip()
    return values
