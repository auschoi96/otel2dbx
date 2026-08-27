from __future__ import annotations

import pytest

from otel2dbx.langgraph_agent import (
    _final_answer,
    _format_sql_result,
    _is_read_only,
    _safe_eval,
)


def test_safe_eval_handles_arithmetic_and_precedence() -> None:
    assert _safe_eval("47 * 89") == 4183
    assert _safe_eval("(144 / 12) * 9") == 108
    assert _safe_eval("19.99 + 45.01") == pytest.approx(65.0)
    assert _safe_eval("-5 + 2") == -3
    assert _safe_eval("2 ** 16") == 65536


def test_safe_eval_rejects_unsafe_expressions() -> None:
    for expression in (
        "__import__('os')",
        "open('x')",
        "x + 1",
        "2 ** 100000",
        "().__class__",
    ):
        with pytest.raises((ValueError, SyntaxError)):
            _safe_eval(expression)


class _Message:
    def __init__(self, content: object) -> None:
        self.content = content


def test_final_answer_extracts_string_and_block_content() -> None:
    assert _final_answer({"messages": [_Message("42")]}) == "42"
    blocks = [{"type": "text", "text": "hello"}, {"type": "tool_use", "name": "x"}]
    assert _final_answer({"messages": [_Message(blocks)]}) == "hello"
    assert _final_answer({"messages": []}) == ""


def test_read_only_guard_accepts_and_rejects() -> None:
    for statement in (
        "SELECT 1",
        "  select * from samples.tpch.nation",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "SHOW TABLES",
        "DESCRIBE samples.tpch.nation",
        "EXPLAIN SELECT 1",
    ):
        assert _is_read_only(statement), statement
    for statement in (
        "INSERT INTO t VALUES (1)",
        "DROP TABLE t",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "GRANT SELECT ON t TO u",
        "",
    ):
        assert not _is_read_only(statement), statement


def test_format_sql_result_renders_and_truncates() -> None:
    assert _format_sql_result(["a"], []) == "(no rows)"
    rendered = _format_sql_result(["name", "score"], [["alice", 3], ["bob", None]])
    assert rendered.splitlines() == ["name, score", "alice, 3", "bob, "]
    big = _format_sql_result(["n"], [[i] for i in range(60)])
    assert big.splitlines()[-1] == "... truncated to 50 of 60 rows"
