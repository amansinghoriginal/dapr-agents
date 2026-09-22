#
# Copyright 2026 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Offline regressions for the harness, separate from real-runtime coverage."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from . import model_app
from .runtime import Images, Runtime


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[Runtime]:
    harness = Runtime(tmp_path, Images(agent="unused-agent", router="unused-router"))
    try:
        yield harness
    finally:
        harness.client.close()


@pytest.fixture
def start_calls(harness: Runtime, monkeypatch: pytest.MonkeyPatch) -> dict[str, Mock]:
    calls = {
        "compose": Mock(return_value="container logs"),
        "url": Mock(return_value="http://127.0.0.1:12345"),
        "request": Mock(return_value={"inbox": "inbox", "dlt": "dlt"}),
        "record_versions": Mock(),
        "scheduling": Mock(return_value=[]),
        "evidence": Mock(return_value={"calls": [], "records": []}),
        "stream": Mock(return_value=[]),
    }
    for name, call in calls.items():
        monkeypatch.setattr(harness, name, call)
    return calls


@pytest.mark.parametrize("failure", ["ports", "readiness", "versions"])
def test_partial_startup_captures_only_logs(
    harness: Runtime, start_calls: dict[str, Mock], failure: str
) -> None:
    error = RuntimeError(f"startup failed during {failure}")
    if failure == "ports":
        start_calls["url"].side_effect = ["http://127.0.0.1:12345", error]
    elif failure == "readiness":
        start_calls["request"].side_effect = error
    else:
        start_calls["record_versions"].side_effect = error
    for name in ("scheduling", "evidence", "stream"):
        start_calls[name].side_effect = AssertionError(
            "Artifact endpoint used after incomplete startup."
        )

    with pytest.raises(RuntimeError, match=f"startup failed during {failure}"):
        harness.start()
    assert harness.agent_url
    harness.capture()

    assert (harness.directory / "containers.log").read_text() == "container logs\n"
    assert not (harness.directory / "artifacts.json").exists()
    for name in ("scheduling", "evidence", "stream"):
        start_calls[name].assert_not_called()


def test_completed_startup_captures_runtime_artifacts(
    harness: Runtime, start_calls: dict[str, Mock]
) -> None:
    harness.start()
    harness.capture()

    start_calls["record_versions"].assert_called_once()
    start_calls["stream"].assert_any_call("inbox")
    start_calls["stream"].assert_any_call("dlt")
    assert json.loads((harness.directory / "artifacts.json").read_text()) == {
        "scheduling": [],
        "receiver": {"calls": [], "records": []},
        "inbox": [],
        "dlt": [],
    }


def _tool_message(name: str) -> dict[str, Any]:
    return {
        "role": "assistant",
        "tool_calls": [
            {
                "id": f"call_{name}",
                "type": "function",
                "function": {"name": name, "arguments": "{}"},
            }
        ],
    }


@pytest.mark.parametrize(
    ("current_calls", "expected_tool"),
    [
        ((), "list_drasi_subscriptions"),
        (("list_drasi_subscriptions",), "record_event"),
        (("list_drasi_subscriptions", "record_event"), None),
    ],
)
def test_scripted_model_uses_only_current_user_turn(
    monkeypatch: pytest.MonkeyPatch,
    current_calls: tuple[str, ...],
    expected_tool: str | None,
) -> None:
    monkeypatch.setattr(model_app, "_calls", [])
    task = "The same event delivered again."
    messages = [
        {"role": "user", "content": task},
        _tool_message("list_drasi_subscriptions"),
        _tool_message("record_event"),
        {"role": "assistant", "content": "Recorded."},
        {"role": "user", "content": task},
        *(_tool_message(name) for name in current_calls),
    ]
    response = model_app.completion(
        model_app.ChatRequest(
            messages=messages,
            tools=[
                {"type": "function", "function": {"name": name}}
                for name in ("list_drasi_subscriptions", "record_event")
            ],
        )
    )
    choice = response["choices"][0]
    if expected_tool is None:
        assert choice["finish_reason"] == "stop"
        assert not choice["message"].get("tool_calls")
    else:
        assert choice["finish_reason"] == "tool_calls"
        function = choice["message"]["tool_calls"][0]["function"]
        assert function["name"] == expected_tool
        assert json.loads(function["arguments"]) == (
            {"task": task} if expected_tool == "record_event" else {}
        )
