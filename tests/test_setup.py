from __future__ import annotations

import base64

import pytest

from otel2dbx import setup as setup_lib
from otel2dbx.config import DEFAULT_DATABRICKS_PROFILE, effective_profile


def test_effective_profile_prefers_explicit_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL2DBX_DATABRICKS_PROFILE", "env-profile")
    assert effective_profile("flag-profile") == "flag-profile"


def test_effective_profile_uses_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL2DBX_DATABRICKS_PROFILE", "env-profile")
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("DATABRICKS_JOB_ID", raising=False)
    assert effective_profile(None) == "env-profile"


def test_effective_profile_is_ambient_on_databricks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OTEL2DBX_DATABRICKS_PROFILE", raising=False)
    monkeypatch.setenv("DATABRICKS_JOB_ID", "123")
    assert effective_profile(None) is None


def test_effective_profile_falls_back_to_demo_default(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("OTEL2DBX_DATABRICKS_PROFILE", "DATABRICKS_RUNTIME_VERSION", "DATABRICKS_JOB_ID"):
        monkeypatch.delenv(name, raising=False)
    assert effective_profile(None) == DEFAULT_DATABRICKS_PROFILE


class _FakeSecrets:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def get_secret(self, *, scope: str, key: str):  # noqa: ANN202
        if key not in self.values:
            raise RuntimeError("SECRET_DOES_NOT_EXIST")
        raw = self.values[key]
        return type(
            "GetSecretResponse",
            (),
            {"value": base64.b64encode(raw.encode())},
        )()


def test_read_secret_scope_decodes_and_maps_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeSecrets(
        {
            "zerobus_client_id": "app-id",
            "zerobus_client_secret": "s3cret",
            "zerobus_workspace_id": "12345",
            "zerobus_region": "eu-west-1",
        }
    )
    workspace = type("WorkspaceClient", (), {"secrets": fake})()
    monkeypatch.setattr(setup_lib, "_workspace_client", lambda profile: workspace)

    values = setup_lib.read_secret_scope("otel2dbx", None)

    assert values == {
        "ZEROBUS_CLIENT_ID": "app-id",
        "ZEROBUS_CLIENT_SECRET": "s3cret",
        "ZEROBUS_WORKSPACE_ID": "12345",
        "ZEROBUS_REGION": "eu-west-1",
    }


def test_read_secret_scope_tolerates_missing_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeSecrets({"zerobus_client_id": "app-id"})
    workspace = type("WorkspaceClient", (), {"secrets": fake})()
    monkeypatch.setattr(setup_lib, "_workspace_client", lambda profile: workspace)

    values = setup_lib.read_secret_scope("otel2dbx", None)

    assert values == {"ZEROBUS_CLIENT_ID": "app-id"}
