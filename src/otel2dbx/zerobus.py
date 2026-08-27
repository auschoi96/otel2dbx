from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from dotenv import dotenv_values, set_key

from otel2dbx.databricks import ResolvedDestination
from otel2dbx.errors import ConfigurationError


@dataclass(frozen=True)
class ZerobusBootstrapResult:
    application_id: str
    display_name: str
    created: bool
    secret_created: bool
    table_name: str
    # The OAuth secret itself, so callers can store it (e.g. in a secret scope).
    # Never print this field.
    client_secret: str


def _execute_grant(
    workspace: WorkspaceClient,
    warehouse_id: str,
    statement: str,
) -> None:
    response = workspace.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
    )
    state = str(response.status.state) if response.status else "UNKNOWN"
    if not state.endswith("SUCCEEDED"):
        error = response.status.error if response.status else None
        raise ConfigurationError(f"Zerobus grant failed ({state}): {error}")


def bootstrap_zerobus(
    *,
    profile: str | None,
    warehouse_id: str,
    destination: ResolvedDestination,
    env_path: Path | None,
    display_name: str = "otel2dbx-zerobus-demo",
    rotate_secret: bool = False,
) -> ZerobusBootstrapResult:
    """Create or reuse a Zerobus service principal, grant access, and save its secret.

    When env_path is None (e.g. inside a bundle job) the secret is only returned in
    the result object and exported to this process's environment.
    """
    if not re.fullmatch(r"[A-Za-z0-9_. -]{1,100}", display_name):
        raise ConfigurationError("Service-principal name contains unsupported characters")

    config = Config(profile=profile) if profile else Config()
    workspace = WorkspaceClient(config=config)
    matches = list(
        workspace.service_principals.list(filter=f'displayName eq "{display_name}"')
    )
    created = not matches
    principal = (
        workspace.service_principals.create(display_name=display_name)
        if created
        else matches[0]
    )
    if not principal.id or not principal.application_id:
        raise ConfigurationError("Databricks did not return the service-principal identifiers")

    existing = dotenv_values(env_path) if env_path and env_path.exists() else {}
    existing_client_id = str(existing.get("ZEROBUS_CLIENT_ID") or "")
    existing_secret = str(existing.get("ZEROBUS_CLIENT_SECRET") or "")
    secret_created = rotate_secret or not (
        existing_client_id == principal.application_id and existing_secret
    )
    client_secret = existing_secret
    if secret_created:
        secret_response = workspace.service_principal_secrets_proxy.create(
            str(principal.id),
            lifetime="31536000s",
            scopes=["all-apis"],
        )
        client_secret = str(secret_response.secret or "")
        if not client_secret:
            raise ConfigurationError("Databricks did not return the new OAuth secret")

    catalog = destination.uc_target.catalog
    schema = destination.uc_target.schema
    table = destination.uc_target.spans_table
    principal_name = principal.application_id
    for statement in (
        f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{principal_name}`",
        f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{schema}` TO `{principal_name}`",
        f"GRANT MODIFY, SELECT ON TABLE `{catalog}`.`{schema}`."
        f"`{destination.uc_target.table_prefix}_otel_spans` TO `{principal_name}`",
    ):
        _execute_grant(workspace, warehouse_id, statement)

    values = {
        "ZEROBUS_WORKSPACE_ID": destination.workspace_id,
        "ZEROBUS_REGION": destination.region,
        "ZEROBUS_CLIENT_ID": principal.application_id,
        "ZEROBUS_CLIENT_SECRET": client_secret,
    }
    for key, value in values.items():
        os.environ[key] = value
    if env_path is not None:
        env_path.touch(mode=0o600, exist_ok=True)
        for key, value in values.items():
            set_key(env_path, key, value, quote_mode="always")
        env_path.chmod(0o600)

    return ZerobusBootstrapResult(
        application_id=principal.application_id,
        display_name=display_name,
        created=created,
        secret_created=secret_created,
        table_name=table,
        client_secret=client_secret,
    )
