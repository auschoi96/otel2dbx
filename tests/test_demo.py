from __future__ import annotations

import pytest

from otel2dbx.demo import load_task_bank, sample_tasks
from otel2dbx.errors import ConfigurationError


def test_task_bank_has_enough_unique_guarded_tasks() -> None:
    bank = load_task_bank()
    assert len(bank) >= 10
    assert len(set(bank)) == len(bank)
    assert all(prompt.startswith("Work only in this demo directory.") for prompt in bank)


def test_langgraph_task_bank_is_separate_and_simple() -> None:
    bank = load_task_bank("langgraph")
    assert len(bank) >= 10
    assert len(set(bank)) == len(bank)
    assert bank != load_task_bank("claude")


def test_sample_tasks_from_langgraph_bank() -> None:
    tasks = sample_tasks(5, seed=7, agent="langgraph")
    assert len(tasks) == 5
    assert set(tasks) <= set(load_task_bank("langgraph"))


def test_unknown_agent_bank_is_rejected() -> None:
    with pytest.raises(ConfigurationError):
        load_task_bank("not-an-agent")


def test_sample_tasks_is_deterministic_with_seed() -> None:
    assert sample_tasks(5, seed=42) == sample_tasks(5, seed=42)


def test_sample_tasks_draws_without_replacement() -> None:
    tasks = sample_tasks(10, seed=1)
    assert len(tasks) == 10
    assert len(set(tasks)) == 10


def test_sample_tasks_rejects_oversized_count() -> None:
    with pytest.raises(ConfigurationError):
        sample_tasks(len(load_task_bank()) + 1)
