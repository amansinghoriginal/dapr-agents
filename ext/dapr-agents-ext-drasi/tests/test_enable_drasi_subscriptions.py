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

"""Compose the real Drasi components over scripted SDK and HTTP transports."""

from __future__ import annotations

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
from grpc import StatusCode

from dapr_agents.agents.base import AgentBase
from dapr_agents.agents.configs import (
    AgentExecutionConfig,
    AgentMCPConfig,
    AgentObservabilityConfig,
    AgentPubSubConfig,
)
from dapr_agents.agents.durable import DurableAgent
from dapr_agents.ext.drasi import (
    enable_drasi_subscriptions,
    register_drasi_trigger,
)
from dapr_agents.ext.drasi import _registration, router_client, subscription_manager
from dapr_agents.ext.drasi._models import (
    IntentDocument,
    SubscriptionIntent,
    SubscriptionScope,
)
from dapr_agents.ext.drasi._registration import (
    DrasiApplicationConflictError,
    DrasiModeConflictError,
    register_activation,
)
from dapr_agents.ext.drasi.intent_store import DaprIntentRepository
from dapr_agents.llm import OpenAIChatClient
from dapr_agents.tool import AgentTool
from dapr_agents.tool.executor import AgentToolExecutor
from dapr_agents.workflow.runners.agent import AgentRunner

from .test_delivery import _Stream, _message
from .test_intent_store import _Backend, _grpc_error, _state_config
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


@dataclass
class _Harness:
    scope: SubscriptionScope
    agent: DurableAgent
    runner: AgentRunner
    runtime: MagicMock
    workflow: MagicMock
    backend: _Backend
    server: _Server
    metadata: _Metadata
    clients: list[MagicMock]
    streams: list[_Stream]
    make_agent: Callable[[str, list[AgentTool]], DurableAgent]
    client_factory: Callable[..., MagicMock]
    open_stream: Callable[..., _Stream]

    @property
    def repository(self) -> DaprIntentRepository:
        return DaprIntentRepository(scope=self.scope, store=self.agent.state_store)

    def enable(self, *, pubsub: str | None = None) -> None:
        enable_drasi_subscriptions(
            self.agent,
            router_id=self.scope.router_id,
            namespace=self.scope.namespace,
            pubsub=pubsub,
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
        stream = _Stream()
        streams.append(stream)
        return stream

    def client_factory(**kwargs: object) -> MagicMock:
        client = MagicMock(spec=DaprClient)
        client.__enter__.return_value = client
        client.get_metadata.return_value = metadata
        client.subscribe.side_effect = open_stream
        clients.append(client)
        return client

    monkeypatch.setattr("dapr_agents.agents.base.DaprClient", client_factory)
    monkeypatch.setattr(router_client.httpx, "AsyncClient", server.client)
    monkeypatch.setattr(_registration, "_APPLICATION_OWNERS", {})
    monkeypatch.delenv("DAPR_API_MAX_RETRIES", raising=False)
    incarnations = itertools.count(1)
    monkeypatch.setattr(
        subscription_manager, "uuid4", lambda: UUID(int=next(incarnations))
    )

    def make_agent(name: str, tools: list[AgentTool]) -> DurableAgent:
        runtime = MagicMock(spec=WorkflowRuntime)
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
            execution=AgentExecutionConfig(builtin_tools=[]),
            agent_observability=AgentObservabilityConfig(enabled=False),
            mcp=AgentMCPConfig(enabled=False),
        )
        agents.append(agent)
        return agent

    ordinary = AgentTool(
        name="record_assessment",
        description="Record an assessment.",
        func=lambda: "recorded",
    )
    agent = make_agent(scope.agent_name, [ordinary])
    workflow = MagicMock(spec=DaprWorkflowClient)
    workflow.schedule_new_workflow.side_effect = lambda name, *, input, instance_id: (
        instance_id
    )
    runner = AgentRunner(wf_client=workflow, client_factory=client_factory)
    runner._wire_pubsub_routes = Mock()
    runner._wire_http_routes = Mock()
    runner._mount_service_routes = Mock()
    runner._mount_hitl_routes = Mock()
    runner.run_workflow_async = AsyncMock(return_value="normal-task")
    try:
        yield _Harness(
            scope=scope,
            agent=agent,
            runner=runner,
            runtime=runtimes[0],
            workflow=workflow,
            backend=backend,
            server=server,
            metadata=metadata,
            clients=clients,
            streams=streams,
            make_agent=make_agent,
            client_factory=client_factory,
            open_stream=open_stream,
        )
    finally:
        runner.shutdown()
        for created in agents:
            created.stop()
        for stream in streams:
            stream.close()
        assert all(http.is_closed for http in server.clients)
        assert all(not http.owner[0].is_alive() for http in server.clients)


def _tool(harness: _Harness, prefix: str) -> AgentTool:
    return next(
        tool
        for tool in harness.agent.tool_executor.list_tools()
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


def test_registration_is_inert_and_startup_prepares_before_worker_execution(
    harness: _Harness,
) -> None:
    agent = harness.agent
    executor = agent.tool_executor
    ordinary = executor.list_tools()[0]
    prompt = agent.prompt_template
    policy = agent.profile.system_prompt
    store = agent.state_store
    harness.enable()
    assert harness.server.calls == []
    assert harness.backend.reads == []
    assert harness.streams == []
    assert executor.list_tools() == [ordinary]

    def starting_worker() -> None:
        snapshot = harness.repository.load()
        assert snapshot is not None and snapshot.etag
        assert snapshot.document.intents == {}
        assert len(agent.get_llm_tools()) == 6
        assert len(harness.streams) == 1

    harness.runtime.start.side_effect = starting_worker
    harness.runner.workflow(agent)

    assert agent.tool_executor is executor
    assert executor.get_tool(ordinary.name) is ordinary
    assert agent.prompt_template is prompt
    assert agent.profile.system_prompt == policy
    assert agent.state_store is store
    agent.llm.generate.assert_not_called()
    harness.clients[-1].subscribe.assert_called_once_with(
        pubsub_name="application-bus",
        topic=harness.scope.inbox_topic,
        dead_letter_topic=harness.scope.dead_letter_topic,
    )
    assert [call["name"] for call in harness.server.calls] == ["list_queries"]
    assert ordinary.run() == "recorded"


@pytest.mark.parametrize(
    "host", ("workflow", "subscribe", "register_routes", "serve", "run", "run_stream")
)
@pytest.mark.asyncio
async def test_all_host_paths_prepare_once_and_shutdown_without_unsubscribing(
    harness: _Harness, host: str
) -> None:
    harness.enable()
    runner, agent = harness.runner, harness.agent
    if host == "run":
        await runner.run(agent, "Monitor health.", wait=False)
    elif host == "run_stream":
        consumer = MagicMock()
        consumer.astart = AsyncMock()
        consumer.aclose = AsyncMock()
        consumer.__aiter__.return_value = iter(())
        runner._resolve_default_listener = Mock(return_value={"type": "in_process"})
        runner._build_stream_consumer = Mock(return_value=consumer)
        assert [chunk async for chunk in runner.run_stream(agent, "Monitor.")] == []
    elif host == "serve":
        runner.serve(agent, app=FastAPI())
    elif host == "register_routes":
        runner.register_routes(agent, fastapi_app=FastAPI())
    else:
        getattr(runner, host)(agent)
    runner.workflow(agent)
    assert len(harness.streams) == 1
    assert len(harness.server.calls) == 1
    assert len(agent.tool_executor.list_tools()) == 6

    runner.shutdown(agent)

    assert harness.streams[0].closed.is_set()
    assert len(agent.tool_executor.list_tools()) == 1
    assert harness.repository.load() is not None
    assert all(call["name"] != "unsubscribe" for call in harness.server.calls)
    harness.workflow.close.assert_not_called()


def test_real_tools_manager_store_admission_and_delivery_work_together(
    harness: _Harness, insert_delivery: AgentDelivery
) -> None:
    harness.enable()
    harness.runner.workflow(harness.agent)
    _confirm_subscribe(harness)
    instructions = "Record an assessment for each newly matching service error."
    result = _tool(harness, "subscribe_service-errors_").run(
        operations=["i", "u"], instructions=instructions
    )
    assert not result.isError
    intent = harness.repository.get("service-errors")
    assert intent is not None and intent.status == "active"
    assert intent.instructions == instructions
    assert "instructions" not in harness.server.calls[-1]["arguments"]

    document = to_wire(insert_delivery)
    document["subscriptionIncarnation"] = intent.incarnation
    event = parse(AgentDelivery, document)
    stream = harness.streams[0]
    stream.messages.put(_message(event))
    _, status = stream.responses.get(timeout=3)
    assert status == TopicEventResponseStatus.success
    call = harness.workflow.schedule_new_workflow.call_args
    assert call.args == (harness.agent.agent_workflow_name,)
    assert set(call.kwargs["input"]) == {"task"}
    assert instructions in call.kwargs["input"]["task"]
    assert "BEGIN_UNTRUSTED_DRASI_EVENT_JSON" in call.kwargs["input"]["task"]
    assert len(call.kwargs["instance_id"]) == 70
    assert len(harness.agent.get_llm_tools()) == 6
    assert _tool(harness, "subscribe_rollout-status_") is not None
    assert _tool(harness, "record_assessment").run() == "recorded"

    harness.server.queue(
        "unsubscribe", _success({"query_id": "service-errors", "removed": True})
    )
    assert not _tool(harness, "unsubscribe_service-errors_").run().isError
    assert harness.repository.get("service-errors") is None
    harness.runner.shutdown()
    assert sum(call["name"] == "unsubscribe" for call in harness.server.calls) == 1


@pytest.mark.parametrize("per_agent", (False, True))
def test_rehosting_reconciles_and_rebinds_tools_without_closed_clients(
    harness: _Harness,
    active_intent: SubscriptionIntent,
    per_agent: bool,
) -> None:
    harness.repository.initialize(
        IntentDocument(
            format_version=1,
            scope=harness.scope,
            intents={active_intent.query_id: active_intent},
        )
    )
    harness.enable()
    _confirm_subscribe(harness, incarnation=active_intent.incarnation)
    harness.runner.workflow(harness.agent)
    old_tools = harness.agent.tool_executor.list_tools()
    harness.runner.shutdown(harness.agent if per_agent else None)
    assert harness.repository.get(active_intent.query_id).status == "active"
    assert harness.streams[0].closed.is_set()

    _confirm_subscribe(harness, incarnation=active_intent.incarnation)
    harness.runner.workflow(harness.agent)
    assert len(harness.agent.get_llm_tools()) == 6
    assert harness.agent.get_llm_tools()[0] is old_tools[0]
    assert harness.agent.get_llm_tools()[1] is not old_tools[1]
    assert len(harness.streams) == 2
    _confirm_subscribe(
        harness, incarnation=active_intent.incarnation, operations=("d",)
    )
    updated = _tool(harness, "subscribe_service-errors_").run(
        operations=["d"], instructions="Handle departures."
    )
    assert not updated.isError
    assert (
        harness.repository.get(active_intent.query_id).incarnation
        == active_intent.incarnation
    )


def test_empty_catalog_and_initially_toolless_agent_still_expose_local_inspection(
    harness: _Harness, empty_catalog: ListQueriesResponse
) -> None:
    original = harness.agent.tool_executor.list_tools()[0]
    harness.agent.tool_executor.unregister_tool(original)
    harness.agent.execution.tool_choice = None
    harness.server.catalog = to_wire(empty_catalog)
    harness.enable()
    harness.runner.workflow(harness.agent)

    assert harness.agent.execution.tool_choice == "auto"
    assert harness.agent.tool_executor.get_tool_names() == ["list_drasi_subscriptions"]
    result = _tool(harness, "list_drasi_subscriptions").run()
    assert not result.isError
    assert result.structuredContent == {
        "result": {"source": "local_intent", "subscriptions": []}
    }
    harness.runner.shutdown()
    assert harness.agent.tool_executor.list_tools() == []
    assert harness.agent.execution.tool_choice is None


def test_explicit_tool_policy_and_pubsub_override_are_preserved(harness: _Harness):
    harness.agent.execution.tool_choice = "none"
    harness.agent._infra._pubsub = None
    harness.enable(pubsub="override-bus")
    harness.runner.workflow(harness.agent)
    assert harness.agent.execution.tool_choice == "none"
    assert (
        harness.clients[-1].subscribe.call_args.kwargs["pubsub_name"] == "override-bus"
    )
    harness.runner.shutdown()
    assert harness.agent.execution.tool_choice == "none"


@pytest.mark.parametrize("first_mode", ("static", "dynamic"))
def test_mixed_modes_fail_in_both_orders(harness: _Harness, first_mode: str):
    if first_mode == "static":
        register_drasi_trigger(harness.agent, query_id="service-errors")
        with pytest.raises(DrasiModeConflictError, match="already registered"):
            harness.enable()
    else:
        harness.enable()
        with pytest.raises(DrasiModeConflictError, match="already registered"):
            register_drasi_trigger(harness.agent, query_id="service-errors")
    assert harness.server.calls == []
    assert harness.backend.writes == []


def test_duplicate_enablement_is_not_a_second_continuation(harness: _Harness):
    harness.enable()
    with pytest.raises(DrasiModeConflictError, match="already registered"):
        harness.enable()
    assert len(harness.agent.pre_start_activations) == 1


def test_another_local_agent_is_rejected_until_the_owner_shuts_down(
    harness: _Harness,
) -> None:
    harness.enable()
    harness.runner.workflow(harness.agent)
    other = harness.make_agent("OtherAgent", [])
    enable_drasi_subscriptions(
        other, router_id=harness.scope.router_id, namespace=harness.scope.namespace
    )
    runner = AgentRunner(
        wf_client=harness.workflow, client_factory=harness.client_factory
    )
    try:
        with pytest.raises(RuntimeError, match="one Drasi-enabled"):
            runner.workflow(other)
        assert len(harness.streams) == 1
        assert not other.is_started
        harness.runner.shutdown(harness.agent)
        runner.workflow(other)
        assert len(harness.streams) == 2
    finally:
        runner.shutdown()


def test_static_application_ownership_survives_until_all_registrations_close(
    harness: _Harness,
) -> None:
    for _ in range(2):
        register_activation(harness.agent, mode="static", callback=lambda ctx: None)
    harness.runner.workflow(harness.agent)
    closers = harness.runner._activation_closers[id(harness.agent)]
    closers[0]()
    assert _registration._APPLICATION_OWNERS[harness.scope.app_id].registrations == 1
    other = harness.make_agent("OtherAgent", [])
    with pytest.raises(DrasiApplicationConflictError, match="one Drasi-enabled"):
        _registration._claim_application(other)
    harness.runner.shutdown()
    assert harness.scope.app_id not in _registration._APPLICATION_OWNERS


@pytest.mark.parametrize("phase", ("catalog", "store", "inbox", "worker"))
def test_failed_preparation_unwinds_and_can_be_retried(
    harness: _Harness, phase: str
) -> None:
    harness.agent.execution.tool_choice = None
    harness.enable()
    if phase == "catalog":
        harness.server.initialization_error = 503
    elif phase == "store":
        harness.backend.read_error = _grpc_error(StatusCode.UNAVAILABLE)
    elif phase == "worker":
        harness.runtime.start.side_effect = RuntimeError("worker unavailable")
    else:
        original_factory = harness.runner._client_factory

        def failing_factory():
            client = original_factory()
            client.subscribe.side_effect = RuntimeError("subscription failed")
            return client

        harness.runner._client_factory = failing_factory

    error = "worker unavailable" if phase == "worker" else "failed during hosting"
    with pytest.raises(RuntimeError, match=error):
        harness.runner.workflow(harness.agent)
    if phase == "worker":
        harness.runtime.shutdown.assert_called_once()
    else:
        harness.runtime.start.assert_not_called()
    assert not harness.agent.is_started
    assert len(harness.agent.tool_executor.list_tools()) == 1
    assert harness.agent.execution.tool_choice is None
    assert harness.scope.app_id not in _registration._APPLICATION_OWNERS
    assert all(not http.owner[0].is_alive() for http in harness.server.clients)
    assert all(call["name"] != "unsubscribe" for call in harness.server.calls)

    harness.server.initialization_error = None
    harness.backend.read_error = None
    harness.runtime.start.side_effect = None
    harness.runner._client_factory = harness.client_factory
    if harness.runner._dapr_client is not None:
        harness.runner._dapr_client.subscribe.side_effect = harness.open_stream
    harness.runner.workflow(harness.agent)
    assert len(harness.agent.get_llm_tools()) == 6
    assert harness.agent.is_started


def test_partial_tool_attachment_is_rolled_back(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    executor = harness.agent.tool_executor
    register = AgentToolExecutor.register_tool
    attempts = 0

    def fail_second(target: AgentToolExecutor, tool: AgentTool) -> None:
        nonlocal attempts
        if target is executor:
            attempts += 1
            if attempts == 2:
                raise RuntimeError("second registration failed")
        register(target, tool)

    harness.enable()
    with monkeypatch.context() as patch:
        patch.setattr(AgentToolExecutor, "register_tool", fail_second)
        with pytest.raises(RuntimeError, match="second registration failed"):
            harness.runner.workflow(harness.agent)
    assert executor.get_tool_names() == ["record_assessment"]
    assert harness.streams == []
    assert all(not http.owner[0].is_alive() for http in harness.server.clients)
    harness.runner.workflow(harness.agent)
    assert len(executor.list_tools()) == 6


def test_tool_name_collision_is_detected_before_state_or_router_mutation(
    harness: _Harness,
) -> None:
    ordinary = AgentTool(
        name="LIST DRASI SUBSCRIPTIONS",
        description="A conflicting ordinary tool.",
        func=lambda: "ordinary",
    )
    harness.agent.tool_executor.register_tool(ordinary)
    harness.enable()
    with pytest.raises(RuntimeError, match="conflicts with an existing tool"):
        harness.runner.workflow(harness.agent)
    assert harness.backend.writes == []
    assert [call["name"] for call in harness.server.calls] == ["list_queries"]
    assert harness.agent.tool_executor.get_tool(ordinary.name) is ordinary
    assert harness.streams == []


@pytest.mark.parametrize(
    "missing",
    (
        "state",
        "bus",
        "identity",
        "namespace",
        "router",
        "port",
        "executor",
        "orchestrator",
    ),
)
def test_invalid_registration_does_not_claim_mode(
    harness: _Harness, missing: str
) -> None:
    router_id, namespace, port = harness.scope.router_id, harness.scope.namespace, 3500
    if missing == "state":
        harness.agent._infra.state_store = None
    elif missing == "bus":
        harness.agent._infra._pubsub = None
    elif missing == "identity":
        harness.agent.appid = None
    elif missing == "namespace":
        namespace = ""
    elif missing == "router":
        router_id = "not-a-scoped-router"
    elif missing == "port":
        port = 0
    elif missing == "executor":
        harness.agent.executor = Mock()
    else:
        harness.agent._orchestration_strategy = Mock()
    with pytest.raises(ValueError):
        enable_drasi_subscriptions(
            harness.agent,
            router_id=router_id,
            namespace=namespace,
            dapr_http_port=port,
        )
    assert harness.agent.pre_start_activations == []
    assert not hasattr(harness.agent, "_dapr_agents_ext_drasi_mode")
    assert harness.server.calls == []


@pytest.mark.parametrize("topic", ("inbox_topic", "dead_letter_topic"))
def test_derived_topic_must_not_collide_with_framework_topics(
    harness: _Harness, topic: str
) -> None:
    harness.agent._infra._pubsub.agent_topic = getattr(harness.scope, topic)
    with pytest.raises(ValueError, match="distinct"):
        harness.enable()


@pytest.mark.parametrize("missing", ("pubsub", "state", "sidecar_identity"))
def test_missing_or_mismatched_sidecar_configuration_fails_before_catalog(
    harness: _Harness, missing: str
) -> None:
    harness.enable()
    if missing == "sidecar_identity":
        harness.metadata.application_id = "another-app"
    else:
        harness.metadata.registered_components = [
            component
            for component in harness.metadata.registered_components
            if not component.type.startswith(missing + ".")
        ]
    with pytest.raises(RuntimeError, match="failed during hosting"):
        harness.runner.workflow(harness.agent)
    assert harness.server.calls == []
    assert harness.backend.writes == []
    harness.runtime.start.assert_not_called()


def test_configuration_changes_applied_at_start_are_checked_before_worker_execution(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness.enable()

    def load_changed_configuration(agent: AgentBase) -> None:
        agent._infra._pubsub.pubsub_name = "override-bus"

    monkeypatch.setattr(AgentBase, "start", load_changed_configuration)
    with pytest.raises(RuntimeError, match="changed after registration"):
        harness.runner.workflow(harness.agent)
    harness.runtime.start.assert_not_called()
    assert harness.server.calls == []


@pytest.mark.parametrize("hosted", (False, True))
def test_late_enablement_is_rejected(harness: _Harness, hosted: bool) -> None:
    if hosted:
        harness.runner.workflow(harness.agent)
    else:
        harness.agent.start()
    with pytest.raises(ValueError, match="before the agent is hosted or started"):
        harness.enable()
    assert harness.agent.pre_start_activations == []


def test_unavailable_intent_recovery_guidance_reaches_the_generated_tool(
    harness: _Harness, active_intent: SubscriptionIntent
) -> None:
    unavailable = active_intent.model_copy(update={"status": "unavailable"}, deep=True)
    harness.repository.initialize(
        IntentDocument(
            format_version=1,
            scope=harness.scope,
            intents={unavailable.query_id: unavailable},
        )
    )
    harness.enable()
    harness.runner.workflow(harness.agent)
    result = _tool(harness, "subscribe_service-errors_").run(
        operations=["i"], instructions="Monitor again."
    )
    assert result.isError
    assert "unsubscribe first, then subscribe again" in result.content[0].text
    assert all(call["name"] != "subscribe" for call in harness.server.calls)
