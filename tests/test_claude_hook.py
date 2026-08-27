from __future__ import annotations

from otel2dbx.claude_hook import build_trace
from otel2dbx.otel import attributes_to_dict, iter_spans


def test_claude_hook_builds_agent_llm_and_tool_tree() -> None:
    rows = [
        {
            "type": "user",
            "uuid": "prompt-1",
            "timestamp": "2026-08-03T12:00:00Z",
            "message": {"role": "user", "content": "Run the safe test"},
        },
        {
            "type": "assistant",
            "uuid": "assistant-1",
            "timestamp": "2026-08-03T12:00:01Z",
            "message": {
                "id": "message-1",
                "role": "assistant",
                "model": "claude-sonnet",
                "usage": {"input_tokens": 10, "output_tokens": 3},
                "content": [
                    {"type": "text", "text": "I will run it."},
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "Bash",
                        "input": {"command": "pytest -q"},
                    },
                ],
            },
        },
        {
            "type": "user",
            "uuid": "result-1",
            "timestamp": "2026-08-03T12:00:02Z",
            "message": {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "tool-1",
                        "content": "2 passed",
                    }
                ],
            },
        },
    ]

    trace_id, request = build_trace("session-1", rows)
    spans = list(iter_spans(request))
    root = next(span for span in spans if not span.parent_span_id)
    child_operations = {
        attributes_to_dict(span.attributes).get("gen_ai.operation.name")
        for span in spans
        if span.parent_span_id
    }

    assert len(trace_id) == 32
    assert len(spans) == 3
    assert attributes_to_dict(root.attributes)["gen_ai.usage.input_tokens"] == 10
    assert child_operations == {"chat", "execute_tool"}
