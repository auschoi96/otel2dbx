from __future__ import annotations

import base64
import json
import random
import secrets
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Literal

from dotenv import dotenv_values

from otel2dbx.config import (
    DEFAULT_DATABRICKS_PROFILE,
    DEFAULT_EXPERIMENT_ID,
    DEFAULT_WAREHOUSE_ID,
    DEFAULT_ZEROBUS_REGION,
    DEFAULT_ZEROBUS_WORKSPACE_ID,
    GENERATED_DIR,
    PROJECT_ROOT,
)
from otel2dbx.databricks import ResolvedDestination
from otel2dbx.errors import ConfigurationError

DEFAULT_DEMO_PROMPT = (
    "Work only in this demo directory. Run `uv run pytest -q`, diagnose the failing "
    "price parser test, make the smallest correct fix, rerun the tests, and summarize the result."
)

TASK_BANKS = {
    "claude": PROJECT_ROOT / "demo_assets" / "tasks.json",
    "langgraph": PROJECT_ROOT / "demo_assets" / "langgraph_tasks.json",
}


def load_task_bank(agent: str = "claude") -> list[str]:
    """Prompts from the agent's JSON bank in demo_assets; editable without touching code."""
    path = TASK_BANKS.get(agent)
    if path is None:
        raise ConfigurationError(
            f"Unknown agent {agent!r}; expected one of {sorted(TASK_BANKS)}"
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ConfigurationError(
            f"Cannot read the demo task bank at {path}: {exc}"
        ) from exc
    prompts = [str(item["prompt"]) for item in data if item.get("prompt")]
    if not prompts:
        raise ConfigurationError(f"The demo task bank at {path} is empty")
    return prompts


def sample_tasks(count: int, *, seed: int | None = None, agent: str = "claude") -> list[str]:
    """Draw count unique task prompts at random; deterministic when seed is given."""
    bank = load_task_bank(agent)
    if count < 1:
        raise ConfigurationError("--count must be at least 1")
    if count > len(bank):
        raise ConfigurationError(
            f"The demo task bank has {len(bank)} tasks; cannot sample {count}"
        )
    return random.Random(seed).sample(bank, count)


def initialize_env(*, force: bool = False) -> tuple[Path, str]:
    path = PROJECT_ROOT / ".env"
    existing = dotenv_values(path) if path.exists() else {}
    if existing.get("LANGFUSE_USER_PASSWORD") and not force:
        return path, str(existing["LANGFUSE_USER_PASSWORD"])

    public_key = f"lf_pk_{secrets.token_hex(16)}"
    secret_key = f"lf_sk_{secrets.token_hex(24)}"
    auth = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()
    user_password = secrets.token_urlsafe(18)
    values = {
        "POSTGRES_PASSWORD": secrets.token_urlsafe(24),
        "CLICKHOUSE_PASSWORD": secrets.token_urlsafe(24),
        "REDIS_AUTH": secrets.token_urlsafe(24),
        "MINIO_ROOT_PASSWORD": secrets.token_urlsafe(24),
        "SALT": secrets.token_hex(24),
        "ENCRYPTION_KEY": secrets.token_hex(32),
        "NEXTAUTH_SECRET": secrets.token_urlsafe(32),
        "LANGFUSE_PUBLIC_KEY": public_key,
        "LANGFUSE_SECRET_KEY": secret_key,
        "LANGFUSE_AUTH_HEADER": f"Basic {auth}",
        "LANGFUSE_USER_EMAIL": "demo@example.com",
        "LANGFUSE_USER_PASSWORD": user_password,
        "LANGFUSE_BASE_URL": "http://localhost:3000",
        "ZEROBUS_WORKSPACE_ID": DEFAULT_ZEROBUS_WORKSPACE_ID,
        "ZEROBUS_REGION": DEFAULT_ZEROBUS_REGION,
        "ZEROBUS_CLIENT_ID": "",
        "ZEROBUS_CLIENT_SECRET": "",
    }
    for key in (
        "ZEROBUS_WORKSPACE_ID",
        "ZEROBUS_REGION",
        "ZEROBUS_CLIENT_ID",
        "ZEROBUS_CLIENT_SECRET",
    ):
        if existing.get(key):
            values[key] = str(existing[key])
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()), encoding="utf-8")
    path.chmod(0o600)
    return path, user_password


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    if shutil.which("docker") is None:
        raise ConfigurationError(
            "Docker is not installed. Install and start Docker Desktop, then rerun this command."
        )
    if shutil.which("docker-compose"):
        command = ["docker-compose", "--project-directory", str(PROJECT_ROOT)]
    else:
        command = ["docker", "compose", "--project-directory", str(PROJECT_ROOT)]
    try:
        return subprocess.run(
            [*command, *args],
            cwd=PROJECT_ROOT,
            check=check,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise ConfigurationError(
            f"{' '.join(command)} {' '.join(args)} failed (exit {exc.returncode}). If the "
            "error above says the Docker daemon is unreachable, start it first "
            "(Colima: `colima start`; Docker Desktop: open the app), then rerun."
        ) from exc


def up() -> None:
    initialize_env()
    (PROJECT_ROOT / ".otel2dbx" / "archive").mkdir(parents=True, exist_ok=True)
    compose("up", "-d", "--wait")


def down() -> None:
    compose("down")


def status() -> None:
    compose("ps")


def reset_fixture(*, clear_archive: bool = True) -> None:
    source = PROJECT_ROOT / "demo_assets"
    destination = PROJECT_ROOT / "demo_workspace"
    destination.mkdir(parents=True, exist_ok=True)
    for name in ("price_parser.py", "test_price_parser.py"):
        shutil.copy2(source / name, destination / name)
    if clear_archive:
        archive = PROJECT_ROOT / ".otel2dbx" / "archive" / "claude-traces.jsonl"
        archive.unlink(missing_ok=True)


def _hook_env(
    target: Literal["langfuse", "zerobus"],
    profile: str | None,
    experiment_id: str,
    warehouse_id: str,
) -> dict[str, str]:
    values = {
        "OTEL2DBX_HOOK_TARGET": target,
        "OTEL2DBX_COLLECTOR_ENDPOINT": "http://localhost:4318/v1/traces",
        "OTEL2DBX_HOOK_STATE": str(PROJECT_ROOT / ".otel2dbx" / "claude-hook-state.json"),
        "OTEL2DBX_HOOK_LOG": str(PROJECT_ROOT / ".otel2dbx" / "claude-hook.log"),
        "MLFLOW_CLAUDE_TRACING_ENABLED": "false",
    }
    if target == "zerobus":
        values.update(
            {
                "OTEL2DBX_DATABRICKS_PROFILE": profile or DEFAULT_DATABRICKS_PROFILE,
                "OTEL2DBX_EXPERIMENT_ID": experiment_id,
                "OTEL2DBX_WAREHOUSE_ID": warehouse_id,
            }
        )
    return values


def write_claude_settings(
    target: Literal["langfuse", "zerobus"],
    *,
    destination: ResolvedDestination | None = None,
    profile: str | None = None,
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
) -> Path:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    if target == "zerobus":
        if destination is None or profile is None:
            raise ValueError("A resolved Databricks destination and profile are required")
    hook_command = " ".join(
        shlex.quote(part) for part in (sys.executable, "-m", "otel2dbx.claude_hook")
    )
    hook = {"type": "command", "command": hook_command, "timeout": 60}
    settings: dict[str, object] = {
        "env": _hook_env(
            target,
            profile,
            destination.experiment_id if destination else DEFAULT_EXPERIMENT_ID,
            warehouse_id,
        ),
        "hooks": {
            "Stop": [{"hooks": [hook]}],
            "SessionEnd": [{"hooks": [hook]}],
        },
    }
    path = GENERATED_DIR / f"claude-{target}.settings.json"
    path.write_text(json.dumps(settings, indent=2), encoding="utf-8")
    return path


def run_claude(
    *,
    target: Literal["langfuse", "zerobus"],
    prompt: str = DEFAULT_DEMO_PROMPT,
    destination: ResolvedDestination | None = None,
    profile: str | None = None,
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
    interactive: bool = False,
) -> int:
    if shutil.which("claude") is None:
        raise ConfigurationError("Claude Code is not installed")
    settings = write_claude_settings(
        target,
        destination=destination,
        profile=profile,
        warehouse_id=warehouse_id,
    )
    command = [
        "claude",
        "--settings",
        str(settings),
        "--setting-sources",
        "project",
    ]
    if interactive:
        command.append(prompt)
    else:
        command.extend(
            [
                "--permission-mode",
                "acceptEdits",
                "--allowedTools",
                "Read,Edit,Bash",
                "--max-budget-usd",
                "1.00",
                "-p",
                prompt,
            ]
        )
    result = subprocess.run(command, cwd=PROJECT_ROOT / "demo_workspace", check=False)
    return result.returncode
