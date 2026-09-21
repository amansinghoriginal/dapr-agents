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

"""Lifecycle composition for agent-managed Drasi subscriptions."""

from __future__ import annotations

import logging
from contextlib import ExitStack
from typing import Callable, NoReturn

from dapr.clients.exceptions import DaprInternalError
from dapr.conf import settings
from grpc import RpcError
from pydantic import ValidationError

from dapr_agents.agents.durable import DurableAgent
from dapr_agents.storage.daprstores.stateservice import StateStoreService
from dapr_agents.types.activation import ActivationContext

from ._models import IntentDocument, ResolvedDrasiConfig, SubscriptionScope
from ._registration import register_activation
from .admission import DrasiAdmissionHandler
from .delivery import subscribe_drasi_inbox
from .intent_store import DaprIntentRepository
from .router_client import MCPRouterClient
from .subscription_manager import DrasiSubscriptionManager
from .subscription_tools import build_subscription_tools

logger = logging.getLogger(__name__)


def _invalid(message: str) -> NoReturn:
    logger.error("%s", message)
    raise ValueError(message)


def _resolve_config(
    agent: DurableAgent,
    *,
    router_id: str,
    namespace: str,
    pubsub: str | None,
    dapr_http_port: int | None,
) -> tuple[ResolvedDrasiConfig, StateStoreService]:
    if agent.executor is not None or agent.orchestrator or agent.llm is None:
        _invalid(
            "Agent-managed Drasi subscriptions require the ordinary DurableAgent "
            "chat/tool loop, not executor= or orchestration mode."
        )
    store = agent.state_store
    if not isinstance(store, StateStoreService):
        _invalid(
            "Drasi subscriptions require a configured AgentStateConfig.store; "
            "conversational memory is not durable subscription intent."
        )
    pubsub_name = agent.message_bus_name if pubsub is None else pubsub
    if pubsub_name is None:
        _invalid(
            "Drasi subscriptions require a Pub/Sub component. Configure the agent's "
            "bus or pass pubsub= explicitly."
        )

    port = dapr_http_port
    if port is None:
        try:
            port = int(settings.DAPR_HTTP_PORT)
        except (TypeError, ValueError):
            _invalid("DAPR_HTTP_PORT must be an integer TCP port.")
    try:
        config = ResolvedDrasiConfig(
            scope=SubscriptionScope(
                router_id=router_id,
                namespace=namespace,
                app_id=agent.appid,
                agent_name=agent.name,
            ),
            pubsub_name=pubsub_name,
            state_store_name=store.store_name,
            workflow_name=agent.agent_workflow_name,
            dapr_http_port=port,
        )
    except ValidationError:
        _invalid(
            "Invalid Drasi configuration. Supply router_id='<namespace>/<app-id>', "
            "the application's namespace, a valid Dapr application/agent identity, "
            "non-blank component names, and a TCP port from 1 to 65535."
        )

    if config.pubsub_name == agent.message_bus_name:
        framework_topics = {agent.topic_name, agent.broadcast_topic_name}
        if framework_topics.intersection(
            (config.scope.inbox_topic, config.scope.dead_letter_topic)
        ):
            _invalid(
                "The derived Drasi inbox and dead-letter topic must be distinct "
                "from the agent's framework topics on the same Pub/Sub component."
            )
    return config, store


def _check_sidecar(context: ActivationContext, config: ResolvedDrasiConfig) -> None:
    try:
        metadata = context.dapr_client.get_metadata()
    except (RpcError, DaprInternalError, OSError) as error:
        logger.error(
            "Drasi sidecar metadata retrieval failed (%s).", type(error).__name__
        )
        raise RuntimeError(
            "Could not read Dapr metadata during Drasi preparation."
        ) from None
    if metadata.application_id != config.scope.app_id:
        _invalid(
            "The hosting runner's Dapr application identity does not match the agent."
        )
    components = {
        component.name: component.type for component in metadata.registered_components
    }
    for name, kind in (
        (config.pubsub_name, "pubsub."),
        (config.state_store_name, "state."),
    ):
        if not components.get(name, "").startswith(kind):
            _invalid(f"Required Drasi {kind[:-1]} component {name!r} is not loaded.")


def enable_drasi_subscriptions(
    agent: DurableAgent,
    *,
    router_id: str,
    namespace: str,
    pubsub: str | None = None,
    dapr_http_port: int | None = None,
) -> None:
    """Enable persistent, agent-selected subscriptions before hosting an agent.

    ``router_id`` is the router's ``<namespace>/<app-id>`` identity. ``namespace``
    is the subscriber application's namespace; the application ID and exact
    logical name come from the agent. Pub/Sub defaults to the agent's bus, and
    the local sidecar HTTP port defaults to the Dapr SDK setting.

    Registration performs no network I/O. AgentRunner completes preparation,
    reconciliation, tool attachment and inbox subscription before starting the
    workflow worker. Shutdown closes runtime resources and detaches only these
    tools, preserving durable intent and router rules.
    """
    if not isinstance(agent, DurableAgent):
        _invalid("Drasi subscriptions require a DurableAgent.")
    if agent.is_started or not agent._activation_window_open:
        _invalid("Enable Drasi subscriptions before the agent is hosted or started.")
    config, store = _resolve_config(
        agent,
        router_id=router_id,
        namespace=namespace,
        pubsub=pubsub,
        dapr_http_port=dapr_http_port,
    )

    def prepare(context: ActivationContext) -> Callable[[], None]:
        current_config, current_store = _resolve_config(
            agent,
            router_id=router_id,
            namespace=namespace,
            pubsub=pubsub,
            dapr_http_port=dapr_http_port,
        )
        if current_config != config or current_store is not store:
            _invalid(
                "Drasi identity or infrastructure changed after registration. "
                "Configure a new agent before hosting it."
            )
        _check_sidecar(context, config)

        with ExitStack() as resources:
            router = MCPRouterClient(config)
            resources.callback(router.close)
            catalog = router.list_queries()
            repository = DaprIntentRepository(scope=config.scope, store=store)
            manager = DrasiSubscriptionManager(repository=repository, router=router)
            tools = build_subscription_tools(catalog, manager)
            executor = agent.tool_executor
            for tool in tools:
                if executor.get_tool(tool.name) is not None:
                    _invalid(
                        f"Drasi tool {tool.name!r} conflicts with an existing tool. "
                        "Rename the existing tool before enabling subscriptions."
                    )

            if repository.load() is None:
                repository.initialize(
                    IntentDocument(format_version=1, scope=config.scope, intents={})
                )
            manager.reconcile(catalog)

            for tool in tools:
                executor.register_tool(tool)
                resources.callback(executor.unregister_tool, tool)
            if agent.execution.tool_choice is None:
                agent.execution.tool_choice = "auto"

                def restore_tool_choice() -> None:
                    if agent.execution.tool_choice == "auto":
                        agent.execution.tool_choice = None

                resources.callback(restore_tool_choice)

            admission = DrasiAdmissionHandler(scope=config.scope, intents=repository)
            close_inbox = subscribe_drasi_inbox(
                config=config,
                admission=admission,
                dapr_client=context.dapr_client,
                workflow_client=context.wf_client,
            )
            resources.callback(close_inbox)
            return resources.pop_all().close

    register_activation(agent, mode="dynamic", callback=prepare, before_start=True)
