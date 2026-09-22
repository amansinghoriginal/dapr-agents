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

from __future__ import annotations

import logging
from threading import Lock
from typing import Callable, Literal, TypeAlias

from dapr_agents.agents.durable import DurableAgent
from dapr_agents.types.activation import ActivationCallback, ActivationContext

logger = logging.getLogger(__name__)

DrasiMode: TypeAlias = Literal["static", "dynamic"]

_MODE_ATTRIBUTE = "_dapr_agents_ext_drasi_mode"
_MODE_LOCK = Lock()


_APPLICATION_OWNERS: dict[str, DurableAgent] = {}


class DrasiModeConflictError(ValueError):
    """An agent already has an incompatible Drasi registration."""


class DrasiApplicationConflictError(ValueError):
    """Another local Drasi agent is already hosted for this Dapr application."""


def _claim_application(agent: DurableAgent) -> None:
    app_id = agent.appid
    if not isinstance(app_id, str) or not app_id:
        # Static registrations predate an application-identity requirement.
        return

    with _MODE_LOCK:
        owner = _APPLICATION_OWNERS.get(app_id)
        if owner is not None and owner is not agent:
            message = (
                f"Dapr application {app_id!r} already hosts another Drasi agent. "
                "Use one Drasi-enabled logical agent per Dapr application."
            )
            logger.error("%s", message)
            raise DrasiApplicationConflictError(message)
        # One owner for this process lifetime; shutdown is not an agent handoff.
        _APPLICATION_OWNERS[app_id] = agent


def _guard_application(callback: ActivationCallback) -> ActivationCallback:
    lock = Lock()
    attempted = False

    def activate(context: ActivationContext) -> Callable[[], None] | None:
        nonlocal attempted
        with lock:
            if attempted:
                message = (
                    "This Drasi hosting lifecycle has ended or failed. "
                    "Restart the application; do not reuse its agent or runner."
                )
                logger.error("%s", message)
                raise DrasiApplicationConflictError(message)
            _claim_application(context.agent)
            attempted = True
            return callback(context)

    return activate


def register_activation(
    agent: DurableAgent,
    *,
    mode: DrasiMode,
    callback: ActivationCallback,
    before_start: bool = False,
) -> None:
    """Register an activation without mixing Drasi modes on an agent.

    Mode ownership lasts as long as the registered callbacks, including across
    failed hosting attempts and subscription shutdown.
    """
    with _MODE_LOCK:
        previous_mode = getattr(agent, _MODE_ATTRIBUTE, None)
        if previous_mode is not None and (previous_mode != mode or mode == "dynamic"):
            message = (
                f"Cannot register Drasi {mode} mode for agent {agent.name!r}: "
                f"{previous_mode} mode is already registered."
            )
            logger.error("%s", message)
            raise DrasiModeConflictError(message)

        guarded = _guard_application(callback)
        if before_start:
            agent.add_activation(guarded, before_start=True)
        else:
            agent.add_activation(guarded)
        setattr(agent, _MODE_ATTRIBUTE, mode)
