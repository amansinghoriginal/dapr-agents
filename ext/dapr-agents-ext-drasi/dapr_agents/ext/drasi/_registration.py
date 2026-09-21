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
from dataclasses import dataclass
from threading import Lock
from typing import Callable, Literal, TypeAlias
from weakref import ReferenceType, ref

from dapr_agents.agents.durable import DurableAgent
from dapr_agents.types.activation import ActivationCallback, ActivationContext

logger = logging.getLogger(__name__)

DrasiMode: TypeAlias = Literal["static", "dynamic"]

_MODE_ATTRIBUTE = "_dapr_agents_ext_drasi_mode"
_MODE_LOCK = Lock()


@dataclass
class _ApplicationOwner:
    agent: ReferenceType[DurableAgent]
    registrations: int = 0


_APPLICATION_OWNERS: dict[str, _ApplicationOwner] = {}


class DrasiModeConflictError(ValueError):
    """An agent already has an incompatible Drasi registration."""


class DrasiApplicationConflictError(ValueError):
    """Another local Drasi agent is already hosted for this Dapr application."""


def _claim_application(agent: DurableAgent) -> Callable[[], None]:
    app_id = agent.appid
    if not isinstance(app_id, str) or not app_id:
        # Static registrations predate an application-identity requirement.
        return lambda: None

    with _MODE_LOCK:
        owner = _APPLICATION_OWNERS.get(app_id)
        if owner is not None and owner.agent() not in (None, agent):
            message = (
                f"Dapr application {app_id!r} already hosts another Drasi agent. "
                "Use one Drasi-enabled logical agent per Dapr application."
            )
            logger.error("%s", message)
            raise DrasiApplicationConflictError(message)
        if owner is None or owner.agent() is None:
            owner = _ApplicationOwner(ref(agent))
            _APPLICATION_OWNERS[app_id] = owner
        owner.registrations += 1

    def release() -> None:
        with _MODE_LOCK:
            if _APPLICATION_OWNERS.get(app_id) is owner:
                owner.registrations -= 1
                if owner.registrations == 0:
                    del _APPLICATION_OWNERS[app_id]

    return release


def _guard_application(callback: ActivationCallback) -> ActivationCallback:
    lock = Lock()
    active = False

    def activate(context: ActivationContext) -> Callable[[], None]:
        nonlocal active
        with lock:
            if active:
                message = (
                    "This Drasi registration is already hosted; shut it down first."
                )
                logger.error("%s", message)
                raise DrasiApplicationConflictError(message)
            release = _claim_application(context.agent)
            try:
                closer = callback(context)
                if closer is not None and not callable(closer):
                    raise TypeError("A Drasi activation must return a closer or None.")
            except BaseException:
                release()
                raise
            active = True

        def close() -> None:
            nonlocal active
            with lock:
                if not active:
                    return
                if closer is not None:
                    closer()
                release()
                active = False

        return close

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
