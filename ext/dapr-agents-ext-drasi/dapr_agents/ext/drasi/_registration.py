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
from typing import Literal, TypeAlias

from dapr_agents.agents.durable import DurableAgent
from dapr_agents.types.activation import ActivationCallback

logger = logging.getLogger(__name__)

DrasiMode: TypeAlias = Literal["static", "dynamic"]

_MODE_ATTRIBUTE = "_dapr_agents_ext_drasi_mode"
_MODE_LOCK = Lock()


class DrasiModeConflictError(ValueError):
    """An agent already has registrations in the other Drasi mode."""


def register_activation(
    agent: DurableAgent,
    *,
    mode: DrasiMode,
    callback: ActivationCallback,
) -> None:
    """Register an activation without mixing Drasi modes on an agent.

    Mode ownership lasts as long as the registered callbacks, including across
    failed hosting attempts and subscription shutdown.
    """
    with _MODE_LOCK:
        previous_mode = getattr(agent, _MODE_ATTRIBUTE, None)
        if previous_mode is not None and previous_mode != mode:
            message = (
                f"Cannot register Drasi {mode} mode for agent {agent.name!r}: "
                f"{previous_mode} mode is already registered."
            )
            logger.error("%s", message)
            raise DrasiModeConflictError(message)

        agent.add_activation(callback)
        setattr(agent, _MODE_ATTRIBUTE, mode)
