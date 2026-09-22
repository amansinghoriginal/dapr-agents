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

"""Unit tests for the DurableAgent.add_activation registration API."""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Optional
from unittest.mock import MagicMock, Mock

import pytest

from dapr_agents.agents.configs import (
    AgentPubSubConfig,
    AgentRegistryConfig,
    AgentStateConfig,
)
from dapr_agents.agents.durable import DurableAgent
from dapr_agents.agents.base import AgentBase
from dapr_agents.llm import OpenAIChatClient
from dapr_agents.storage.daprstores.stateservice import StateStoreService


@pytest.fixture(autouse=True)
def patch_dapr_check(monkeypatch):
    import dapr.ext.workflow as wf

    mock_runtime = Mock(spec=wf.WorkflowRuntime)
    monkeypatch.setattr(wf, "WorkflowRuntime", lambda: mock_runtime)

    class MockRetryPolicy:
        def __init__(
            self,
            max_number_of_attempts=1,
            first_retry_interval=timedelta(seconds=1),
            max_retry_interval=timedelta(seconds=60),
            backoff_coefficient=2.0,
            retry_timeout: Optional[timedelta] = None,
        ):
            self.max_number_of_attempts = max_number_of_attempts

    monkeypatch.setattr(wf, "RetryPolicy", MockRetryPolicy)
    yield


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    os.environ["OPENAI_API_KEY"] = "test-api-key"
    monkeypatch.setattr(
        "dapr_agents.storage.daprstores.base.default_dapr_client_factory",
        lambda: MagicMock(),
    )
    yield
    os.environ.pop("OPENAI_API_KEY", None)


@pytest.fixture
def agent() -> DurableAgent:
    llm = Mock(spec=OpenAIChatClient)
    llm.prompt_template = None
    llm.__class__.__name__ = "MockLLMClient"
    llm.provider = "MockOpenAIProvider"
    llm.api = "MockOpenAIAPI"
    llm.model = "gpt-4o-mock"
    return DurableAgent(
        name="ApiAgent",
        role="Test Assistant",
        goal="Help with testing",
        llm=llm,
        pubsub=AgentPubSubConfig(pubsub_name="testpubsub", agent_topic="ApiAgent"),
        state=AgentStateConfig(store=StateStoreService(store_name="teststatestore")),
        registry=AgentRegistryConfig(
            store=StateStoreService(store_name="testregistry")
        ),
    )


def test_fresh_agent_has_no_activations(agent):
    assert agent.activations == []


def test_add_activation_stores_in_registration_order(agent):
    def a(ctx):
        return None

    def b(ctx):
        return None

    agent.add_activation(a)
    agent.add_activation(b)

    assert agent.activations == [a, b]


def test_activations_property_returns_a_copy(agent):
    agent.add_activation(lambda ctx: None)
    snapshot = agent.activations
    snapshot.append("intruder")

    assert len(agent.activations) == 1  # internal list untouched


def test_add_activation_rejects_non_callable(agent):
    with pytest.raises(TypeError):
        agent.add_activation("not-callable")


def test_add_activation_after_hosting_window_closed_raises(agent):
    # The runner closes this window on first attach; simulate that here.
    agent._activation_window_open = False

    with pytest.raises(RuntimeError, match=r"run_stream\(\)"):
        agent.add_activation(lambda ctx: None)


def test_pre_start_registration_is_opt_in_and_returns_a_copy(agent):
    before, after = Mock(), Mock()
    agent.add_activation(before, before_start=True)
    agent.add_activation(after)

    assert agent.pre_start_activations == [before]
    assert agent.activations == [after]
    agent.pre_start_activations.clear()
    assert agent.pre_start_activations == [before]


def test_pre_start_registration_rejects_an_already_started_agent(agent):
    agent._started = True
    with pytest.raises(RuntimeError, match="runtime has started"):
        agent.add_activation(lambda ctx: None, before_start=True)
    assert agent.pre_start_activations == []


def test_pre_start_registration_rejects_a_closed_window(agent):
    agent._activation_window_open = False
    with pytest.raises(RuntimeError, match="has been hosted"):
        agent.add_activation(lambda ctx: None, before_start=True)


def test_prepared_agent_cannot_start_without_the_runner_preparation_boundary(agent):
    agent.add_activation(lambda ctx: None, before_start=True)
    with pytest.raises(RuntimeError, match="host it through AgentRunner"):
        agent.start()


def test_start_prepares_after_configuration_and_registration_before_worker(
    agent, monkeypatch
):
    order = []
    monkeypatch.setattr(agent, "_runtime", Mock())
    monkeypatch.setattr(AgentBase, "start", lambda self: order.append("configuration"))
    monkeypatch.setattr(agent, "_restore_pending_approvals", Mock())
    monkeypatch.setattr(
        agent, "register_workflows", lambda runtime: order.append("registration")
    )
    agent.runtime.start.side_effect = lambda: order.append("worker")

    agent.start(prepare=lambda: order.append("preparation"))

    assert order == ["configuration", "registration", "preparation", "worker"]


def test_start_preparation_failure_cleans_base_resources_without_starting_worker(
    agent, monkeypatch
):
    monkeypatch.setattr(agent, "_runtime", Mock())
    monkeypatch.setattr(AgentBase, "start", Mock())
    cleanup = Mock()
    monkeypatch.setattr(AgentBase, "stop", cleanup)
    monkeypatch.setattr(agent, "_restore_pending_approvals", Mock())
    monkeypatch.setattr(agent, "register_workflows", Mock())

    with pytest.raises(ValueError, match="preparation failed"):
        agent.start(prepare=Mock(side_effect=ValueError("preparation failed")))

    agent.runtime.start.assert_not_called()
    cleanup.assert_called_once()
    assert not agent.is_started


def test_prepared_start_does_not_disguise_worker_failure_as_already_running(
    agent, monkeypatch
):
    runtime = Mock()
    runtime.start.side_effect = RuntimeError("worker unavailable")
    monkeypatch.setattr(agent, "_runtime", runtime)
    monkeypatch.setattr(AgentBase, "start", Mock())
    cleanup = Mock()
    monkeypatch.setattr(AgentBase, "stop", cleanup)
    monkeypatch.setattr(agent, "_restore_pending_approvals", Mock())
    monkeypatch.setattr(agent, "register_workflows", Mock())
    prepare = Mock()

    with pytest.raises(RuntimeError, match="worker unavailable"):
        agent.start(prepare=prepare)

    prepare.assert_called_once()
    runtime.shutdown.assert_called_once()
    cleanup.assert_called_once()
    assert not agent.is_started


@pytest.mark.parametrize("passed_to_start", (False, True))
def test_preparation_rejects_borrowed_runtime_before_registering_workflows(
    agent, monkeypatch, passed_to_start
):
    borrowed = Mock()
    configure = Mock()
    register = Mock()
    prepare = Mock()
    monkeypatch.setattr(AgentBase, "start", configure)
    monkeypatch.setattr(agent, "register_workflows", register)
    if not passed_to_start:
        agent._runtime = borrowed
        agent._runtime_owned = False

    with pytest.raises(RuntimeError, match="agent-owned workflow runtime"):
        agent.start(
            runtime=borrowed if passed_to_start else None,
            prepare=prepare,
        )

    configure.assert_not_called()
    register.assert_not_called()
    prepare.assert_not_called()
    borrowed.start.assert_not_called()
    borrowed.shutdown.assert_not_called()


def test_prepared_stop_does_not_claim_a_failed_shutdown_succeeded(agent, monkeypatch):
    runtime = Mock()
    monkeypatch.setattr(agent, "_runtime", runtime)
    monkeypatch.setattr(AgentBase, "start", Mock())
    monkeypatch.setattr(AgentBase, "stop", Mock())
    monkeypatch.setattr(agent, "_restore_pending_approvals", Mock())
    monkeypatch.setattr(agent, "register_workflows", Mock())
    agent.start(prepare=Mock())
    runtime.shutdown.side_effect = RuntimeError("shutdown failed")

    with pytest.raises(RuntimeError, match="shutdown failed"):
        agent.stop()

    assert agent.is_started
    runtime.shutdown.side_effect = None
    agent.stop()
    assert not agent.is_started
