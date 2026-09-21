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

"""Robustness tests for the generic activation hook.

Proves an extension callback registered via ``DurableAgent.add_activation`` fires
exactly once when the agent is hosted via ANY AgentRunner entry point
(``run``/``workflow``/``register_routes``/``subscribe``/``serve``), receives the
right context, is torn down on shutdown, and never double-fires.
"""

from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event, get_ident
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from fastapi import FastAPI

from dapr_agents.agents.configs import (
    AgentExecutionConfig,
    AgentPubSubConfig,
    AgentRegistryConfig,
    AgentStateConfig,
)
from dapr_agents.agents.durable import DurableAgent
from dapr_agents.llm import OpenAIChatClient
from dapr_agents.storage.daprstores.stateservice import StateStoreService
from dapr_agents.types.activation import ActivationContext
from dapr_agents.workflow.runners.agent import AgentRunner


# ---------------------------------------------------------------------------
# Fixtures (mirror tests/agents/durableagent/test_durable_agent.py)
# ---------------------------------------------------------------------------
class MockDaprClient:
    """Context-manager Dapr client whose state reads return ``data=None`` so the
    agent registry falls back to its default dict (mirrors test_durable_agent.py)."""

    def __init__(self, *args, **kwargs):
        self.get_state = MagicMock(return_value=Mock(data=None))
        self.save_state = MagicMock()
        self.delete_state = MagicMock()
        self.query_state = MagicMock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def __call__(self, *args, **kwargs):
        return self

    def get_metadata(self):
        response = MagicMock()
        response.registered_components = []
        response.application_id = "test-app-id"
        return response


@pytest.fixture(autouse=True)
def patch_dapr_check(monkeypatch):
    """Mock the workflow runtime so no live Dapr instance is required."""
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
    yield mock_runtime


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    """Provide an API key and a mock Dapr client factory."""
    os.environ["OPENAI_API_KEY"] = "test-api-key"
    monkeypatch.setattr(
        "dapr_agents.storage.daprstores.base.default_dapr_client_factory",
        lambda: MockDaprClient(),
    )
    yield
    os.environ.pop("OPENAI_API_KEY", None)


@pytest.fixture(autouse=True)
def stub_agent_lifecycle(monkeypatch):
    """Stub agent start/stop (they touch Dapr) so tests isolate activation."""

    def start(self, *args, prepare=None, **kwargs):
        if prepare is not None:
            prepare()

    monkeypatch.setattr(DurableAgent, "start", start)
    monkeypatch.setattr(DurableAgent, "stop", lambda self, *a, **k: None)


@pytest.fixture(autouse=True)
def stub_route_wiring(monkeypatch):
    """Stub pub/sub + HTTP route wiring so host methods isolate activation."""
    monkeypatch.setattr(
        "dapr_agents.workflow.runners.agent.register_message_routes",
        MagicMock(return_value=[]),
    )
    monkeypatch.setattr(
        "dapr_agents.workflow.runners.agent.register_http_routes",
        MagicMock(return_value=[]),
    )


def _make_agent(name: str = "ActivationAgent") -> DurableAgent:
    llm = Mock(spec=OpenAIChatClient)
    llm.prompt_template = None
    llm.__class__.__name__ = "MockLLMClient"
    llm.provider = "MockOpenAIProvider"
    llm.api = "MockOpenAIAPI"
    llm.model = "gpt-4o-mock"
    return DurableAgent(
        name=name,
        role="Test Assistant",
        goal="Help with testing",
        llm=llm,
        pubsub=AgentPubSubConfig(
            pubsub_name="testpubsub",
            agent_topic=name,
            broadcast_topic=f"{name}.broadcast",
        ),
        state=AgentStateConfig(store=StateStoreService(store_name="teststatestore")),
        registry=AgentRegistryConfig(
            store=StateStoreService(store_name="testregistry")
        ),
        execution=AgentExecutionConfig(max_iterations=5),
    )


def _make_runner() -> AgentRunner:
    """An AgentRunner whose clients are mocked and whose serve()-only HTTP mounts
    are stubbed, so every host path exercises the activation seam in isolation."""
    runner = AgentRunner(wf_client=MagicMock(), client_factory=lambda: MockDaprClient())
    # serve()-only mounts are irrelevant to activation; stub to keep tests focused.
    runner._wire_http_routes = lambda **k: None
    runner._mount_service_routes = lambda **k: None
    runner._mount_hitl_routes = lambda **k: None
    # run() scheduling internals are out of scope; isolate them.
    runner.discover_entry = MagicMock(return_value=lambda *a, **k: None)
    runner.run_workflow_async = AsyncMock(return_value="instance-1")
    return runner


def _spy():
    """Return (callback, calls, closer_calls). The callback records each context
    and returns a closer that records each teardown invocation."""
    calls: list[ActivationContext] = []
    closer_calls: list[int] = []

    def callback(ctx: ActivationContext):
        calls.append(ctx)
        return lambda: closer_calls.append(1)

    return callback, calls, closer_calls


# ---------------------------------------------------------------------------
# Fires exactly once under each host entry point
# ---------------------------------------------------------------------------
def test_fires_once_under_run():
    agent, runner = _make_agent("RunAgent"), _make_runner()
    cb, calls, _ = _spy()
    agent.add_activation(cb)

    asyncio.run(runner.run(agent, payload={"task": "x"}, wait=False))

    assert len(calls) == 1
    ctx = calls[0]
    assert ctx.agent is agent and ctx.runner is runner
    assert ctx.app is None
    assert ctx.dapr_client is not None and ctx.wf_client is not None


def test_fires_once_under_workflow():
    agent, runner = _make_agent("WfAgent"), _make_runner()
    cb, calls, _ = _spy()
    agent.add_activation(cb)

    runner.workflow(agent)

    assert len(calls) == 1
    # workflow() never wires pub/sub, yet a dapr client must be ensured.
    assert calls[0].app is None
    assert calls[0].dapr_client is not None


def test_fires_once_under_subscribe():
    agent, runner = _make_agent("SubAgent"), _make_runner()
    cb, calls, _ = _spy()
    agent.add_activation(cb)

    runner.subscribe(agent)

    assert len(calls) == 1
    assert calls[0].app is None
    assert calls[0].dapr_client is not None


def test_fires_once_under_register_routes():
    agent, runner = _make_agent("RegAgent"), _make_runner()
    app = FastAPI()
    cb, calls, _ = _spy()
    agent.add_activation(cb)

    runner.register_routes(agent, fastapi_app=app)

    assert len(calls) == 1
    assert calls[0].app is app  # app threaded through when present


def test_fires_once_under_serve():
    agent, runner = _make_agent("ServeAgent"), _make_runner()
    app = FastAPI()
    cb, calls, _ = _spy()
    agent.add_activation(cb)

    runner.serve(agent, app=app)

    assert len(calls) == 1
    assert calls[0].app is app


# ---------------------------------------------------------------------------
# No double-fire / fire-once semantics
# ---------------------------------------------------------------------------
def test_serve_does_not_double_fire():
    """serve() calls subscribe() internally; the guard must prevent two fires."""
    agent, runner = _make_agent("ServeOnce"), _make_runner()
    cb, calls, _ = _spy()
    agent.add_activation(cb)

    runner.serve(agent, app=FastAPI())

    assert len(calls) == 1


def test_attach_twice_fires_once():
    """Hosting the same agent via two entry points fires activations once total."""
    agent, runner = _make_agent("Twice"), _make_runner()
    cb, calls, _ = _spy()
    agent.add_activation(cb)

    runner.subscribe(agent)
    runner.workflow(agent)

    assert len(calls) == 1


def test_multiple_activations_fire_in_registration_order():
    agent, runner = _make_agent("Ordered"), _make_runner()
    order: list[str] = []
    agent.add_activation(lambda ctx: order.append("a"))
    agent.add_activation(lambda ctx: order.append("b"))
    agent.add_activation(lambda ctx: order.append("c"))

    runner.subscribe(agent)

    assert order == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
def test_closer_invoked_on_full_shutdown():
    agent, runner = _make_agent("CloseFull"), _make_runner()
    cb, _, closer_calls = _spy()
    agent.add_activation(cb)

    runner.subscribe(agent)
    assert closer_calls == []
    runner.shutdown()

    assert closer_calls == [1]


def test_closer_invoked_on_per_agent_shutdown():
    agent, runner = _make_agent("ClosePer"), _make_runner()
    cb, _, closer_calls = _spy()
    agent.add_activation(cb)

    runner.subscribe(agent)
    runner.shutdown(agent)

    assert closer_calls == [1]


def test_closer_not_double_invoked_across_shutdowns():
    """The runner pops closers, so a second shutdown must not re-invoke them."""
    agent, runner = _make_agent("CloseTwice"), _make_runner()
    cb, _, closer_calls = _spy()
    agent.add_activation(cb)

    runner.subscribe(agent)
    runner.shutdown(agent)
    runner.shutdown()  # second, global shutdown

    assert closer_calls == [1]


def test_guard_reset_allows_reattach():
    """After shutdown the guard resets, so re-hosting re-activates."""
    agent, runner = _make_agent("Reattach"), _make_runner()
    cb, calls, _ = _spy()
    agent.add_activation(cb)

    runner.subscribe(agent)
    runner.shutdown()
    runner.subscribe(agent)

    assert len(calls) == 2


# ---------------------------------------------------------------------------
# app/client invariants and error handling
# ---------------------------------------------------------------------------
def test_app_none_but_client_present_under_workflow():
    agent, runner = _make_agent("NoApp"), _make_runner()
    captured: dict = {}
    agent.add_activation(
        lambda ctx: captured.update(app=ctx.app, client=ctx.dapr_client)
    )

    runner.workflow(agent)

    assert captured["app"] is None
    assert captured["client"] is not None


def test_raising_activation_surfaces_error_and_rolls_back():
    agent, runner = _make_agent("Boom"), _make_runner()
    cb1, _, closer_calls = _spy()  # returns a closer, runs first

    def cb2_raises(ctx):
        raise ValueError("boom")

    agent.add_activation(cb1)
    agent.add_activation(cb2_raises)

    with pytest.raises(RuntimeError) as exc:
        runner.subscribe(agent)

    # Clear error names the failing callback...
    assert "cb2_raises" in str(exc.value)
    # ...the earlier callback's closer was rolled back (no leak)...
    assert closer_calls == [1]
    # ...and the guard was released so a corrected retry can re-attach.
    assert id(agent) not in runner._activated_agent_ids


def test_non_callable_closer_is_rejected():
    agent, runner = _make_agent("BadCloser"), _make_runner()
    agent.add_activation(lambda ctx: "not-callable")

    with pytest.raises(TypeError):
        runner.subscribe(agent)


def test_guard_released_when_dapr_client_init_fails():
    """If Dapr client init fails after the guard is claimed, the guard must be
    released so a later hosting attempt re-activates (Copilot PR #638 finding)."""
    agent = _make_agent("ClientBoom")
    calls: list = []
    agent.add_activation(lambda ctx: calls.append(ctx))

    # Client factory fails the first time, then succeeds.
    factory = MagicMock(side_effect=[RuntimeError("no sidecar"), MockDaprClient()])
    runner = AgentRunner(wf_client=MagicMock(), client_factory=factory)
    runner._wire_http_routes = lambda **k: None
    runner._mount_service_routes = lambda **k: None
    runner._mount_hitl_routes = lambda **k: None

    with pytest.raises(RuntimeError):
        runner.subscribe(agent)
    assert calls == []  # activation never ran (client init failed first)
    assert id(agent) not in runner._activated_agent_ids  # guard released

    runner.subscribe(agent)  # recovery: hosting again re-activates
    assert len(calls) == 1


def test_failed_attach_removes_agent_from_managed_agents():
    """A failed activation must roll back the _managed_agents append this call
    made — the crux of PR #638 review comment #3."""
    agent, runner = _make_agent("RollbackManaged"), _make_runner()

    def cb_raises(ctx):
        raise ValueError("deliberate")

    agent.add_activation(cb_raises)

    with pytest.raises(RuntimeError):
        runner.subscribe(agent)

    assert agent not in runner._managed_agents
    assert id(agent) not in runner._activated_agent_ids


def test_stop_called_on_rollback_when_this_call_started_the_agent(monkeypatch):
    """If THIS call started the agent, a failed attach stops it on rollback."""
    agent, runner = _make_agent("StopOnRollback"), _make_runner()
    stop_calls: list = []
    # Scope the spy to THIS agent: sibling runners GC'd mid-test also call stop()
    # (WorkflowRunner.__del__ -> shutdown), which would otherwise pollute the list.
    monkeypatch.setattr(DurableAgent, "start", lambda self, *a, **k: None)
    monkeypatch.setattr(
        DurableAgent,
        "stop",
        lambda self, *a, **k: stop_calls.append(self) if self is agent else None,
    )

    def cb_raises(ctx):
        raise ValueError("fail after start")

    agent.add_activation(cb_raises)

    with pytest.raises(RuntimeError):
        runner.subscribe(agent)

    assert stop_calls == [agent]
    assert agent not in runner._managed_agents


def test_stop_not_called_on_rollback_when_agent_was_already_started(monkeypatch):
    """If the agent was already running (start() raised), rollback must NOT stop it."""
    agent, runner = _make_agent("NoStopOnRollback"), _make_runner()
    stop_calls: list = []

    def _already_started(self, *a, **k):
        raise RuntimeError("already started")

    # Scope the spy to THIS agent: sibling runners GC'd mid-test also call stop().
    monkeypatch.setattr(DurableAgent, "start", _already_started)  # started_here=False
    monkeypatch.setattr(
        DurableAgent,
        "stop",
        lambda self, *a, **k: stop_calls.append(self) if self is agent else None,
    )

    def cb_raises(ctx):
        raise ValueError("fail")

    agent.add_activation(cb_raises)

    with pytest.raises(RuntimeError):
        runner.subscribe(agent)

    assert stop_calls == []  # not our start -> not ours to stop


def test_abort_attach_leaves_preexisting_host_intact():
    """_abort_attach must not evict/stop an agent it did not add or start
    (added_here/started_here both False) — contract check on the rollback scoping."""
    agent, runner = _make_agent("KeepManaged"), _make_runner()
    cb, _, closer_calls = _spy()
    agent.add_activation(cb)
    runner.subscribe(agent)  # succeeds; agent now managed
    assert agent in runner._managed_agents

    runner._abort_attach(agent, [], started_here=False, added_here=False)

    assert agent in runner._managed_agents  # not removed (added_here=False)
    assert closer_calls == []  # nothing torn down


# ---------------------------------------------------------------------------
# Regression: agents without activations are unaffected
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("host", ["subscribe", "workflow", "serve", "register_routes"])
def test_no_activations_regression(host):
    agent, runner = _make_agent(f"NoAct{host}"), _make_runner()

    if host == "subscribe":
        runner.subscribe(agent)
    elif host == "workflow":
        runner.workflow(agent)
    elif host == "serve":
        runner.serve(agent, app=FastAPI())
    else:
        runner.register_routes(agent, fastapi_app=FastAPI())

    assert agent in runner._managed_agents
    assert runner._activation_closers == {}
    runner.shutdown()  # must not raise


def test_preparation_precedes_start_and_preserves_post_start_callbacks(monkeypatch):
    agent, runner = _make_agent("Prepared"), _make_runner()
    order = []

    def start(*, prepare=None):
        if prepare is not None:
            prepare()
        order.append("start")

    monkeypatch.setattr(agent, "start", start)
    agent.add_activation(lambda ctx: order.append("after"))
    agent.add_activation(lambda ctx: order.append("before"), before_start=True)

    runner.workflow(agent)

    assert order == ["before", "start", "after"]
    runner.shutdown()


def test_preparation_failure_unwinds_without_starting_and_allows_retry(monkeypatch):
    agent, runner = _make_agent("PrepareFailure"), _make_runner()
    runtime_start = Mock()
    close = Mock()
    fail = True

    def start(*, prepare=None):
        if prepare is not None:
            prepare()
        runtime_start()

    monkeypatch.setattr(agent, "start", start)
    agent.add_activation(lambda ctx: close, before_start=True)

    def prepare(ctx):
        if fail:
            raise ValueError("preparation failed")

    agent.add_activation(prepare, before_start=True)
    with pytest.raises(RuntimeError, match="preparation failed"):
        runner.workflow(agent)

    runtime_start.assert_not_called()
    close.assert_called_once()
    assert agent not in runner._managed_agents
    assert id(agent) not in runner._preparing_agent_ids
    fail = False
    runner.workflow(agent)
    runtime_start.assert_called_once()
    runner.shutdown()
    assert close.call_count == 2


def test_post_start_failure_also_unwinds_preparation(monkeypatch):
    agent, runner = _make_agent("PostFailure"), _make_runner()
    order = []

    def start(*, prepare=None):
        if prepare is not None:
            prepare()
        order.append("start")

    monkeypatch.setattr(agent, "start", start)
    monkeypatch.setattr(agent, "stop", lambda: order.append("stop"))
    agent.add_activation(
        lambda ctx: lambda: order.append("close preparation"), before_start=True
    )
    agent.add_activation(lambda ctx: lambda: order.append("close activation"))

    def fail(ctx):
        raise ValueError("post-start failure")

    agent.add_activation(fail)
    with pytest.raises(RuntimeError, match="post-start failure"):
        runner.workflow(agent)
    assert order == ["start", "stop", "close activation", "close preparation"]


def test_preparation_cannot_attach_after_manual_runtime_start():
    agent, runner = _make_agent("LatePrepare"), _make_runner()
    prepare = Mock()
    agent.add_activation(prepare, before_start=True)
    agent._started = True

    with pytest.raises(RuntimeError, match="requires preparation"):
        runner.workflow(agent)
    prepare.assert_not_called()
    assert agent not in runner._managed_agents


def test_preparation_reentrancy_cannot_admit_unprepared_work():
    agent, runner = _make_agent("ReentrantPrepare"), _make_runner()
    agent.add_activation(lambda ctx: runner.workflow(agent), before_start=True)

    with pytest.raises(RuntimeError, match="preparation is running"):
        runner.workflow(agent)
    assert agent not in runner._managed_agents


def test_existing_post_start_reentrancy_still_works():
    agent, runner = _make_agent("ReentrantActivation"), _make_runner()
    calls = []

    def activate(ctx):
        calls.append(ctx)
        runner.workflow(agent)

    agent.add_activation(activate)
    runner.workflow(agent)
    assert len(calls) == 1
    runner.shutdown()


@pytest.mark.parametrize("per_agent", (False, True))
def test_preparation_resources_outlive_worker_shutdown(monkeypatch, per_agent):
    agent, runner = _make_agent("PreparedShutdown"), _make_runner()
    order = []
    monkeypatch.setattr(agent, "stop", lambda: order.append("stop"))
    agent.add_activation(
        lambda ctx: lambda: order.append("close resources"), before_start=True
    )
    runner.workflow(agent)

    runner.shutdown(agent if per_agent else None)

    assert order == ["stop", "close resources"]


def test_concurrent_hosts_wait_for_one_completed_preparation():
    agent, runner = _make_agent("ConcurrentPrepare"), _make_runner()
    entered, release = Event(), Event()
    calls = []

    def prepare(ctx):
        calls.append(ctx)
        entered.set()
        assert release.wait(3)

    agent.add_activation(prepare, before_start=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(runner.workflow, agent)
        try:
            assert entered.wait(3)
            second = pool.submit(runner.workflow, agent)
            assert not second.done()
        finally:
            release.set()
        first.result(timeout=3)
        second.result(timeout=3)
    assert len(calls) == 1
    runner.shutdown()


@pytest.mark.asyncio
async def test_async_run_awaits_offloaded_preparation():
    agent, runner = _make_agent("AsyncPrepare"), _make_runner()
    entered, release = Event(), Event()
    caller_thread = get_ident()
    preparation_threads = []

    def prepare(ctx):
        preparation_threads.append(get_ident())
        entered.set()
        assert release.wait(3)

    agent.add_activation(prepare, before_start=True)
    pending = asyncio.create_task(runner.run(agent, wait=False))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        runner.run_workflow_async.assert_not_awaited()
        assert len(preparation_threads) == 1
        assert preparation_threads[0] != caller_thread
    finally:
        release.set()
    await pending
    runner.run_workflow_async.assert_awaited_once()
    runner.shutdown()


@pytest.mark.asyncio
async def test_cancelled_run_joins_preparation_without_orphaning_resources():
    agent, runner = _make_agent("CancelledPrepare"), _make_runner()
    entered, release = Event(), Event()
    close = Mock()

    def prepare(ctx):
        entered.set()
        assert release.wait(3)
        return close

    agent.add_activation(prepare, before_start=True)
    pending = asyncio.create_task(runner.run(agent, wait=False))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    runner.run_workflow_async.assert_not_awaited()
    assert agent in runner._managed_agents
    runner.shutdown()
    close.assert_called_once()


@pytest.mark.asyncio
async def test_streaming_uses_both_activation_phases_and_normal_cleanup():
    agent, runner = _make_agent("StreamingPrepare"), _make_runner()
    before, before_calls, before_closes = _spy()
    after, after_calls, after_closes = _spy()
    agent.add_activation(before, before_start=True)
    agent.add_activation(after)
    consumer = MagicMock()
    consumer.astart = AsyncMock()
    consumer.aclose = AsyncMock()
    consumer.__aiter__.return_value = iter(())
    runner._resolve_default_listener = Mock(return_value={"type": "in_process"})
    runner._build_stream_consumer = Mock(return_value=consumer)

    assert [chunk async for chunk in runner.run_stream(agent, "task")] == []

    assert len(before_calls) == len(after_calls) == 1
    assert before_calls[0].wf_client is runner._wf_client
    consumer.astart.assert_awaited_once()
    consumer.aclose.assert_awaited_once()
    runner.shutdown()
    assert before_closes == after_closes == [1]
