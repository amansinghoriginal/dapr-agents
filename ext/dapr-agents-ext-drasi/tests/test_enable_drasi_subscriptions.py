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

"""Compose the dynamic Drasi lifecycle over scripted SDK and HTTP transports."""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID

import pytest
from dapr.clients import DaprClient
from dapr.clients.grpc._response import TopicEventResponseStatus
from dapr.ext.workflow import DaprWorkflowClient, WorkflowRuntime
from drasi_agent_router_contracts import (
    AgentDelivery,
    ListQueriesResponse,
    parse,
    to_wire,
)
from fastapi import FastAPI

from dapr_agents.agents.configs import (
    AgentExecutionConfig,
    AgentMCPConfig,
    AgentObservabilityConfig,
    AgentPubSubConfig,
    RuntimeSubscriptionConfig,
)
from dapr_agents.agents.durable import DurableAgent
from dapr_agents.ext.drasi import (
    DrasiLifecycleError,
    drasi_subscription_lifecycle,
    register_drasi_trigger,
)
from dapr_agents.ext.drasi import router_client, subscription_manager, subscriptions
from dapr_agents.ext.drasi._models import (
    IntentDocument,
    SubscriptionIntent,
    SubscriptionScope,
)
from dapr_agents.ext.drasi._registration import (
    DrasiApplicationConflictError,
    DrasiModeConflictError,
)
from dapr_agents.ext.drasi.delivery import DrasiDeliveryError
from dapr_agents.ext.drasi.intent_store import DaprIntentRepository
from dapr_agents.llm import OpenAIChatClient
from dapr_agents.tool import AgentTool
from dapr_agents.workflow.runners.agent import AgentRunner

from .test_delivery import _Stream, _message
from .test_intent_store import _Backend, _state_config
from .test_router_client import _Server, _subscribe_response, _success


@dataclass
class _Component:
    name: str
    type: str


@dataclass
class _Metadata:
    application_id: str
    registered_components: list[_Component]
    mcp_servers: list[str] = field(default_factory=list)


class _RetryInbox:
    def __init__(self) -> None:
        self.allow_stop = False
        self.close_calls = 0
        self.is_stopped = False
        self.is_closed = False

    def close(self) -> None:
        self.close_calls += 1
        if not self.allow_stop:
            raise DrasiDeliveryError("The Drasi inbox consumer did not stop in time.")
        self.is_stopped = True
        self.is_closed = True


@dataclass
class _Harness:
    scope: SubscriptionScope
    agent: DurableAgent
    runtime: MagicMock
    workflow: MagicMock
    backend: _Backend
    server: _Server
    metadata: _Metadata
    clients: list[MagicMock]
    streams: list[_Stream]
    make_agent: Callable[..., DurableAgent]
    client_factory: Callable[..., MagicMock]
    mount_service_routes: Mock
    mount_hitl_routes: Mock

    def scope_for(self, agent: DurableAgent | None = None) -> SubscriptionScope:
        target = agent or self.agent
        return SubscriptionScope(
            router_id=self.scope.router_id,
            namespace=self.scope.namespace,
            app_id=target.appid,
            agent_name=target.name,
        )

    def repository(self, agent: DurableAgent | None = None) -> DaprIntentRepository:
        target = agent or self.agent
        return DaprIntentRepository(
            scope=self.scope_for(target), store=target.state_store
        )

    def lifecycle(
        self,
        *,
        agent: DurableAgent | None = None,
        app: FastAPI | None = None,
        pubsub: str | None = None,
        router_timeout_seconds: float = 30.0,
        startup_timeout_seconds: float = 30.0,
    ):
        return drasi_subscription_lifecycle(
            agent or self.agent,
            app=app,
            workflow_client=self.workflow,
            router_id=self.scope.router_id,
            namespace=self.scope.namespace,
            pubsub=pubsub,
            router_timeout_seconds=router_timeout_seconds,
            startup_timeout_seconds=startup_timeout_seconds,
        )


@pytest.fixture
def harness(
    scope: SubscriptionScope,
    catalog: ListQueriesResponse,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[_Harness]:
    backend = _Backend()
    server = _Server(catalog)
    clients: list[MagicMock] = []
    streams: list[_Stream] = []
    agents: list[DurableAgent] = []
    runtimes: list[MagicMock] = []
    metadata = _Metadata(
        application_id=scope.app_id,
        registered_components=[
            _Component("custom-runtime", "state.redis"),
            _Component("application-bus", "pubsub.redis"),
            _Component("override-bus", "pubsub.redis"),
        ],
    )

    def open_stream(**kwargs: object) -> _Stream:
        del kwargs
        assert any(runtime.wait_for_worker_ready.called for runtime in runtimes)
        stream = _Stream()
        streams.append(stream)
        return stream

    def client_factory(**kwargs: object) -> MagicMock:
        del kwargs
        client = MagicMock(spec=DaprClient)
        client.__enter__.return_value = client
        client.get_metadata.return_value = metadata
        client.subscribe.side_effect = open_stream
        clients.append(client)
        return client

    monkeypatch.setattr("dapr_agents.agents.base.DaprClient", client_factory)
    monkeypatch.setattr(router_client.httpx, "AsyncClient", server.client)
    monkeypatch.delenv("DAPR_API_MAX_RETRIES", raising=False)
    incarnations = itertools.count(1)
    monkeypatch.setattr(
        subscription_manager, "uuid4", lambda: UUID(int=next(incarnations))
    )

    def make_agent(
        name: str,
        tools: list[AgentTool],
        *,
        tool_choice: str | None = "auto",
    ) -> DurableAgent:
        runtime = MagicMock(spec=WorkflowRuntime)
        runtime.wait_for_worker_ready.return_value = True
        runtimes.append(runtime)
        monkeypatch.setattr(
            "dapr_agents.agents.durable.wf.WorkflowRuntime", lambda: runtime
        )
        llm = Mock(spec=OpenAIChatClient)
        llm.prompt_template = None
        llm.provider = "test"
        llm.api = "chat"
        llm.model = "test-model"
        agent = DurableAgent(
            name=name,
            role="Monitor service health",
            system_prompt="Keep the author's policy unchanged.",
            llm=llm,
            tools=tools,
            pubsub=AgentPubSubConfig(
                pubsub_name="application-bus",
                agent_topic="framework-inbox",
                broadcast_topic="framework-broadcast",
            ),
            state=_state_config(backend),
            execution=AgentExecutionConfig(builtin_tools=[], tool_choice=tool_choice),
            agent_observability=AgentObservabilityConfig(enabled=False),
            mcp=AgentMCPConfig(enabled=False),
        )
        agent._client_factory = client_factory
        agents.append(agent)
        return agent

    workflow = MagicMock(spec=DaprWorkflowClient)
    workflow.schedule_new_workflow.side_effect = lambda name, *, input, instance_id: (
        instance_id
    )

    wire_pubsub_routes = Mock()
    wire_http_routes = Mock()
    mount_service_routes = Mock()
    mount_hitl_routes = Mock()
    monkeypatch.setattr(AgentRunner, "_wire_pubsub_routes", wire_pubsub_routes)
    monkeypatch.setattr(AgentRunner, "_wire_http_routes", wire_http_routes)
    monkeypatch.setattr(AgentRunner, "_mount_service_routes", mount_service_routes)
    monkeypatch.setattr(AgentRunner, "_mount_hitl_routes", mount_hitl_routes)

    ordinary = AgentTool(
        name="record_assessment",
        description="Record an assessment.",
        func=lambda: "recorded",
    )
    agent = make_agent(scope.agent_name, [ordinary])
    try:
        yield _Harness(
            scope=scope,
            agent=agent,
            runtime=runtimes[0],
            workflow=workflow,
            backend=backend,
            server=server,
            metadata=metadata,
            clients=clients,
            streams=streams,
            make_agent=make_agent,
            client_factory=client_factory,
            mount_service_routes=mount_service_routes,
            mount_hitl_routes=mount_hitl_routes,
        )
    finally:
        for created in agents:
            created.stop()
        for stream in streams:
            stream.close()
        assert all(http.is_closed for http in server.clients)
        assert all(not http.owner[0].is_alive() for http in server.clients)


def _tool(agent: DurableAgent, prefix: str) -> AgentTool:
    return next(
        tool
        for tool in agent.tool_executor.list_tools()
        if tool.name.startswith(prefix)
    )


def _confirm_subscribe(
    harness: _Harness,
    *,
    incarnation: str = UUID(int=1).hex,
    operations: tuple[str, ...] = ("i", "u"),
) -> None:
    harness.server.queue(
        "subscribe",
        _success(
            _subscribe_response(
                harness.scope,
                subscription_incarnation=incarnation,
                operations=list(operations),
            )
        ),
    )


@pytest.mark.asyncio
async def test_lifecycle_prepares_before_hosting_and_opens_inbox_after_readiness(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mcp_connected = False
    original_list = router_client.MCPRouterClient.list_queries

    async def connect_mcpservers() -> None:
        nonlocal mcp_connected
        mcp_connected = True
        harness.agent._mcp_tools_connected = True

    def list_queries(client):
        assert mcp_connected
        return original_list(client)

    harness.agent.connect_mcpservers = AsyncMock(side_effect=connect_mcpservers)
    monkeypatch.setattr(router_client.MCPRouterClient, "list_queries", list_queries)

    def starting_worker() -> None:
        snapshot = harness.repository().load()
        assert snapshot is not None and snapshot.document.intents == {}
        assert len(harness.agent.get_llm_tools()) == 6
        assert harness.streams == []

    harness.runtime.start.side_effect = starting_worker
    app = FastAPI()
    async with harness.lifecycle(app=app) as runner:
        assert isinstance(runner, AgentRunner)
        assert len(harness.streams) == 1
        assert harness.runtime.wait_for_worker_ready.call_args.kwargs == {
            "timeout": 30.0
        }
        harness.mount_service_routes.assert_called_once()
        harness.mount_hitl_routes.assert_called_once()
        assert [call["name"] for call in harness.server.calls] == ["list_queries"]
        assert harness.clients[-1].subscribe.call_args.kwargs == {
            "pubsub_name": "application-bus",
            "topic": harness.scope.inbox_topic,
            "dead_letter_topic": harness.scope.dead_letter_topic,
        }

    assert harness.clients[-1].close.called
    assert harness.streams[0].closed.is_set()


@pytest.mark.asyncio
async def test_tools_intent_admission_and_delivery_work_together(
    harness: _Harness,
    insert_delivery: AgentDelivery,
) -> None:
    async with harness.lifecycle():
        _confirm_subscribe(harness)
        instructions = "Record an assessment for each newly matching service error."
        result = _tool(harness.agent, "subscribe_service-errors_").run(
            operations=["i", "u"], instructions=instructions
        )
        assert not result.isError
        intent = harness.repository().get("service-errors")
        assert intent is not None and intent.status == "active"
        assert intent.instructions == instructions
        assert "instructions" not in harness.server.calls[-1]["arguments"]

        document = to_wire(insert_delivery)
        document["subscriptionIncarnation"] = intent.incarnation
        stream = harness.streams[0]
        stream.messages.put(_message(parse(AgentDelivery, document)))
        _, status = stream.responses.get(timeout=3)
        assert status == TopicEventResponseStatus.success
        call = harness.workflow.schedule_new_workflow.call_args
        assert call.args == (harness.agent.agent_workflow_name,)
        assert set(call.kwargs["input"]) == {"task"}
        assert instructions in call.kwargs["input"]["task"]
        assert "BEGIN_UNTRUSTED_DRASI_EVENT_JSON" in call.kwargs["input"]["task"]
        assert _tool(harness.agent, "record_assessment").run() == "recorded"


@pytest.mark.asyncio
async def test_cleanup_closes_inbox_then_drains_worker_then_closes_router(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    original_close = router_client.MCPRouterClient.close

    def close_router(client) -> None:
        order.append("router")
        original_close(client)

    monkeypatch.setattr(router_client.MCPRouterClient, "close", close_router)
    lifecycle = harness.lifecycle()
    async with lifecycle:
        client = harness.clients[-1]
        stream = harness.streams[-1]
        client.close.side_effect = lambda: order.append("inbox-client")
        original_stream_close = stream.close

        def close_stream() -> None:
            order.append("inbox-stream")
            original_stream_close()

        stream.close = close_stream
        harness.runtime.shutdown.side_effect = lambda: order.append("runtime")

    assert order[:2] == ["inbox-client", "inbox-stream"]
    assert "runtime" in order
    assert order[-1] == "router"
    assert harness.repository().load() is not None
    assert all(call["name"] != "unsubscribe" for call in harness.server.calls)


@pytest.mark.asyncio
async def test_primary_exception_and_cancellation_are_preserved(
    harness: _Harness,
) -> None:
    lifecycle = harness.lifecycle()
    with pytest.raises(ValueError, match="application failed") as failure:
        async with lifecycle:
            harness.clients[-1].close.side_effect = RuntimeError("close failed")
            raise ValueError("application failed")
    assert any("cleanup failed" in note.lower() for note in failure.value.__notes__)
    harness.clients[-1].close.side_effect = None
    await lifecycle.aclose()

    fresh = harness.make_agent("CancellationAgent", [])
    entered = asyncio.Event()

    async def run() -> None:
        async with harness.lifecycle(agent=fresh):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(run())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


@pytest.mark.asyncio
async def test_cleanup_errors_aggregate_only_without_primary(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_close = router_client.MCPRouterClient.close

    def fail_router(client) -> None:
        original_close(client)
        raise RuntimeError("router close failed")

    monkeypatch.setattr(router_client.MCPRouterClient, "close", fail_router)
    with pytest.raises(ExceptionGroup) as failure:
        async with harness.lifecycle():
            harness.clients[-1].close.side_effect = RuntimeError(
                "inbox client close failed"
            )
    assert len(failure.value.exceptions) == 2


@pytest.mark.asyncio
async def test_inbox_timeout_retains_dependent_resources_and_application_guard(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inbox = _RetryInbox()
    monkeypatch.setattr(
        subscriptions,
        "subscribe_drasi_inbox",
        lambda **kwargs: inbox,
    )
    lifecycle = harness.lifecycle()
    await lifecycle.__aenter__()
    router = lifecycle._router
    runner = lifecycle._runner
    contender = harness.make_agent("InboxContender", [])

    with pytest.raises(DrasiDeliveryError, match="did not stop"):
        await lifecycle.aclose()

    assert lifecycle._inbox is inbox
    assert lifecycle._runner is runner
    assert lifecycle._router is router
    harness.runtime.shutdown.assert_not_called()
    assert harness.workflow.close.call_count == 0
    with pytest.raises(DrasiApplicationConflictError, match="active"):
        async with harness.lifecycle(agent=contender):
            pytest.fail("Application ownership was released while inbox work remained.")

    inbox.allow_stop = True
    await lifecycle.aclose()
    assert inbox.close_calls == 2
    assert lifecycle._cleanup_complete

    replacement = harness.make_agent("InboxReplacement", [])
    async with harness.lifecycle(agent=replacement):
        assert replacement.is_started


@pytest.mark.asyncio
async def test_runtime_shutdown_failure_retains_runner_router_and_guard_for_retry(
    harness: _Harness,
) -> None:
    lifecycle = harness.lifecycle()
    await lifecycle.__aenter__()
    router = lifecycle._router
    runner = lifecycle._runner
    harness.runtime.shutdown.side_effect = [
        RuntimeError("runtime shutdown failed"),
        None,
        None,
    ]
    contender = harness.make_agent("RuntimeContender", [])

    with pytest.raises(RuntimeError, match="runtime shutdown failed"):
        await lifecycle.aclose()

    assert lifecycle._runner is runner
    assert lifecycle._router is router
    assert not lifecycle._runtime_drained
    with pytest.raises(DrasiApplicationConflictError, match="active"):
        async with harness.lifecycle(agent=contender):
            pytest.fail("Application ownership was released before runtime drainage.")

    await lifecycle.aclose()
    assert lifecycle._runner is None
    assert lifecycle._router is None
    assert lifecycle._cleanup_complete

    replacement = harness.make_agent("RuntimeReplacement", [])
    async with harness.lifecycle(agent=replacement):
        assert replacement.is_started


@pytest.mark.asyncio
async def test_worker_readiness_failure_prevents_inbox_and_cleans_resources(
    harness: _Harness,
) -> None:
    harness.runtime.wait_for_worker_ready.return_value = False
    with pytest.raises(DrasiLifecycleError, match="did not become ready"):
        async with harness.lifecycle(startup_timeout_seconds=1.25):
            pytest.fail("Lifecycle unexpectedly became ready.")

    assert harness.runtime.wait_for_worker_ready.call_args.kwargs == {"timeout": 1.25}
    assert harness.streams == []
    assert harness.clients[-1].close.called
    assert all(http.is_closed for http in harness.server.clients)


@pytest.mark.parametrize("choice", ("none", "required", "auto"))
@pytest.mark.asyncio
async def test_initially_toolless_agent_preserves_issue_819_tool_choice(
    harness: _Harness,
    empty_catalog: ListQueriesResponse,
    choice: str,
) -> None:
    harness.server.catalog = to_wire(empty_catalog)
    agent = harness.make_agent("ToollessAgent", [], tool_choice=choice)

    async with harness.lifecycle(agent=agent):
        expected = "auto" if choice == "auto" else choice
        assert agent.execution.tool_choice == expected
        assert agent.tool_executor.get_tool_names() == ["list_drasi_subscriptions"]


@pytest.mark.parametrize("first_mode", ("static", "dynamic"))
def test_static_dynamic_mode_exclusion_in_both_orders(
    harness: _Harness, first_mode: str
) -> None:
    if first_mode == "static":
        register_drasi_trigger(harness.agent, query_id="service-errors")
        with pytest.raises(DrasiModeConflictError, match="already registered"):
            harness.lifecycle()
    else:
        harness.lifecycle()
        with pytest.raises(DrasiModeConflictError, match="already registered"):
            register_drasi_trigger(harness.agent, query_id="service-errors")


def test_duplicate_dynamic_lifecycle_is_rejected(harness: _Harness) -> None:
    harness.lifecycle()
    with pytest.raises(DrasiModeConflictError, match="already registered"):
        harness.lifecycle()


@pytest.mark.asyncio
async def test_lifecycle_closed_before_entry_cannot_start_later(
    harness: _Harness,
) -> None:
    lifecycle = harness.lifecycle()
    await lifecycle.aclose()

    with pytest.raises(DrasiLifecycleError, match="closed before entry"):
        async with lifecycle:
            pytest.fail("A closed lifecycle unexpectedly entered.")

    harness.runtime.start.assert_not_called()
    harness.runtime.shutdown.assert_not_called()
    assert harness.streams == []
    assert harness.server.calls == []


@pytest.mark.asyncio
async def test_dynamic_application_guard_is_active_only_for_open_context(
    harness: _Harness,
) -> None:
    second = harness.make_agent("SecondAgent", [])
    async with harness.lifecycle():
        with pytest.raises(DrasiApplicationConflictError, match="active"):
            async with harness.lifecycle(agent=second):
                pytest.fail("Second lifecycle unexpectedly became active.")

    third = harness.make_agent("ThirdAgent", [])
    async with harness.lifecycle(agent=third):
        assert third.is_started


@pytest.mark.asyncio
async def test_collision_fails_before_intent_mutation_or_hosting(
    harness: _Harness,
) -> None:
    harness.agent.tool_executor.register_tool(
        AgentTool(
            name="LIST DRASI SUBSCRIPTIONS",
            description="Conflicting author tool.",
            func=lambda: None,
        )
    )
    with pytest.raises(ValueError, match="conflicts with an existing tool"):
        async with harness.lifecycle():
            pytest.fail("Lifecycle unexpectedly became ready.")

    assert harness.backend.writes == []
    assert harness.runtime.start.call_count == 0
    assert harness.streams == []


@pytest.mark.parametrize("restriction", ("configuration", "activation"))
def test_minimal_dynamic_profile_restrictions(
    harness: _Harness, restriction: str
) -> None:
    if restriction == "configuration":
        harness.agent.configuration = RuntimeSubscriptionConfig(store_name="config")
        expected = "RuntimeSubscriptionConfig"
    else:
        harness.agent.add_activation(lambda context: None)
        expected = "activation callbacks"

    with pytest.raises(ValueError, match=expected):
        harness.lifecycle()
    assert harness.server.calls == []
    assert harness.backend.writes == []


@pytest.mark.parametrize("mutation", ("configuration", "activation", "started"))
@pytest.mark.asyncio
async def test_profile_is_revalidated_between_construction_and_entry(
    harness: _Harness,
    mutation: str,
) -> None:
    lifecycle = harness.lifecycle()
    if mutation == "configuration":
        harness.agent.configuration = RuntimeSubscriptionConfig(store_name="config")
        expected = "RuntimeSubscriptionConfig"
    elif mutation == "activation":
        harness.agent.add_activation(lambda context: None)
        expected = "activation callbacks"
    else:
        harness.agent._started = True
        expected = "hosted or started"

    with pytest.raises(ValueError, match=expected):
        await lifecycle.__aenter__()
    assert harness.server.calls == []
    assert harness.backend.writes == []


@pytest.mark.parametrize("mutation", ("configuration", "activation", "started"))
@pytest.mark.asyncio
async def test_profile_is_revalidated_after_mcp_discovery(
    harness: _Harness,
    mutation: str,
) -> None:
    lifecycle = harness.lifecycle()

    async def mutate_during_mcp() -> None:
        if mutation == "configuration":
            harness.agent.configuration = RuntimeSubscriptionConfig(store_name="config")
        elif mutation == "activation":
            harness.agent.add_activation(lambda context: None)
        else:
            harness.agent._started = True
        harness.agent._mcp_tools_connected = True

    harness.agent.connect_mcpservers = AsyncMock(side_effect=mutate_during_mcp)
    expected = {
        "configuration": "RuntimeSubscriptionConfig",
        "activation": "activation callbacks",
        "started": "hosted or started",
    }[mutation]

    with pytest.raises(ValueError, match=expected):
        await lifecycle.__aenter__()
    assert harness.server.calls == []
    assert harness.backend.writes == []


@pytest.mark.asyncio
async def test_profile_and_config_are_revalidated_after_catalog_before_mutation(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = harness.lifecycle()
    original_list = router_client.MCPRouterClient.list_queries

    def mutate_after_catalog(client):
        catalog = original_list(client)
        harness.agent.add_activation(lambda context: None)
        return catalog

    monkeypatch.setattr(
        router_client.MCPRouterClient,
        "list_queries",
        mutate_after_catalog,
    )
    with pytest.raises(ValueError, match="activation callbacks"):
        await lifecycle.__aenter__()
    assert harness.backend.writes == []
    assert harness.runtime.start.call_count == 0


@pytest.mark.parametrize("missing", ("pubsub", "state", "sidecar_identity"))
@pytest.mark.asyncio
async def test_missing_or_mismatched_sidecar_configuration_fails_before_catalog(
    harness: _Harness, missing: str
) -> None:
    if missing == "sidecar_identity":
        harness.metadata.application_id = "another-app"
    elif missing == "pubsub":
        harness.metadata.registered_components = [
            component
            for component in harness.metadata.registered_components
            if not component.type.startswith("pubsub.")
        ]
    else:
        harness.metadata.registered_components = [
            component
            for component in harness.metadata.registered_components
            if not component.type.startswith("state.")
        ]

    with pytest.raises((ValueError, DrasiLifecycleError)):
        async with harness.lifecycle():
            pytest.fail("Lifecycle unexpectedly became ready.")
    assert harness.server.calls == []
    assert harness.backend.writes == []


@pytest.mark.asyncio
async def test_post_host_configuration_change_is_detected_before_inbox(
    harness: _Harness,
) -> None:
    harness.runtime.start.side_effect = lambda: setattr(
        harness.agent.state_store, "store_name", "changed-store"
    )
    with pytest.raises(DrasiLifecycleError, match="changed during startup"):
        async with harness.lifecycle():
            pytest.fail("Lifecycle unexpectedly became ready.")
    assert harness.streams == []


@pytest.mark.asyncio
async def test_pubsub_and_router_timeout_overrides_are_applied(
    harness: _Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[float] = []
    original_init = router_client.MCPRouterClient.__init__

    def init(client, config, *, timeout_seconds=30.0):
        observed.append(timeout_seconds)
        original_init(client, config, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(router_client.MCPRouterClient, "__init__", init)
    async with harness.lifecycle(pubsub="override-bus", router_timeout_seconds=4.5):
        assert harness.clients[-1].subscribe.call_args.kwargs["pubsub_name"] == (
            "override-bus"
        )
    assert observed == [4.5]


@pytest.mark.asyncio
async def test_lifecycle_is_one_shot_and_fresh_objects_recover_persisted_intent(
    harness: _Harness,
    active_intent: SubscriptionIntent,
) -> None:
    harness.repository().initialize(
        IntentDocument(
            format_version=1,
            scope=harness.scope,
            intents={active_intent.query_id: active_intent},
        )
    )
    _confirm_subscribe(harness, incarnation=active_intent.incarnation)
    lifecycle = harness.lifecycle()
    async with lifecycle:
        pass
    with pytest.raises(DrasiLifecycleError, match="one-shot"):
        async with lifecycle:
            pytest.fail("Stopped lifecycle unexpectedly restarted.")

    restored = harness.make_agent(harness.scope.agent_name, [])
    _confirm_subscribe(harness, incarnation=active_intent.incarnation)
    async with harness.lifecycle(agent=restored):
        intent = harness.repository(restored).get(active_intent.query_id)
        assert intent is not None
        assert intent.incarnation == active_intent.incarnation


@pytest.mark.asyncio
async def test_unavailable_intent_guidance_reaches_generated_tool(
    harness: _Harness,
    active_intent: SubscriptionIntent,
) -> None:
    unavailable = active_intent.model_copy(update={"status": "unavailable"}, deep=True)
    harness.repository().initialize(
        IntentDocument(
            format_version=1,
            scope=harness.scope,
            intents={unavailable.query_id: unavailable},
        )
    )
    async with harness.lifecycle():
        result = _tool(harness.agent, "subscribe_service-errors_").run(
            operations=["i"], instructions="Monitor again."
        )
        assert result.isError
        assert "unsubscribe first, then subscribe again" in result.content[0].text
        assert all(call["name"] != "subscribe" for call in harness.server.calls)
