from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Nothing here is workspace-specific: every destination value comes from the environment
# (a local .env, exported vars, or a Databricks secret scope), a CLI flag, or a bundle
# target in databricks.yml. The empty defaults make an unconfigured command fail with a
# clear message instead of silently targeting someone else's workspace. Set these in your
# gitignored .env to run the same commands zero-config on your own workspace.
DEFAULT_DATABRICKS_HOST = os.getenv("OTEL2DBX_DATABRICKS_HOST", "")
DEFAULT_DATABRICKS_PROFILE = os.getenv("OTEL2DBX_DATABRICKS_PROFILE", "DEFAULT")
DEFAULT_EXPERIMENT_ID = os.getenv("OTEL2DBX_EXPERIMENT_ID", "")
DEFAULT_WAREHOUSE_ID = os.getenv("OTEL2DBX_WAREHOUSE_ID", "")
# Left unset by default so it is auto-derived from the authenticated workspace; override
# with the o= number from your workspace URL only when derivation is not possible.
DEFAULT_ZEROBUS_WORKSPACE_ID = os.getenv("ZEROBUS_WORKSPACE_ID", "")
DEFAULT_ZEROBUS_REGION = os.getenv("ZEROBUS_REGION", "us-east-1")
DEFAULT_LANGFUSE_URL = "http://localhost:3000"
# Set as the user.id span attribute on demo agent root spans so the MLflow
# Traces UI can group/filter by user.
DEFAULT_USER_ID = os.getenv("OTEL2DBX_USER_ID", "demo@example.com")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = PROJECT_ROOT / ".otel2dbx"
RUNS_DIR = STATE_DIR / "runs"
GENERATED_DIR = STATE_DIR / "generated"
ARCHIVE_DIR = STATE_DIR / "archive"


def running_on_databricks() -> bool:
    """True on Databricks compute (job, notebook, serverless), where auth is ambient."""
    return bool(
        os.getenv("DATABRICKS_RUNTIME_VERSION")
        or os.getenv("DATABRICKS_JOB_ID")
        or os.getenv("DATABRICKS_WORKSPACE_ID")
    )


def effective_profile(explicit: str | None) -> str | None:
    """Resolve the CLI/SDK profile: flag > env > ambient on Databricks > demo default."""
    if explicit:
        return explicit
    env = os.getenv("OTEL2DBX_DATABRICKS_PROFILE")
    if env:
        return env
    if running_on_databricks():
        return None
    return DEFAULT_DATABRICKS_PROFILE


@dataclass(frozen=True)
class UnityCatalogTarget:
    catalog: str
    schema: str
    table_prefix: str

    @property
    def spans_table(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_prefix}_otel_spans"

    @property
    def trace_location(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_prefix}"


@dataclass(frozen=True)
class DatabricksTarget:
    profile: str | None = None
    experiment_id: str = DEFAULT_EXPERIMENT_ID
    warehouse_id: str = DEFAULT_WAREHOUSE_ID
    uc_target: UnityCatalogTarget | None = None
    zerobus_workspace_id: str | None = None
    zerobus_region: str | None = None
    zerobus_client_id: str | None = None
    zerobus_client_secret: str | None = None
