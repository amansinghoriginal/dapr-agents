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

"""Verify that editable core and extension packages remain importable together."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

_CORE_FIRST = """
import dapr_agents
from dapr_agents import DurableAgent
import dapr_agents.ext.drasi as drasi

assert dapr_agents.__file__ is not None
assert callable(DurableAgent)
assert callable(drasi.register_drasi_trigger)
assert callable(drasi.enable_drasi_subscriptions)
"""

_EXTENSION_FIRST = """
import dapr_agents.ext.drasi as drasi
import dapr_agents
from dapr_agents import DurableAgent

assert dapr_agents.__file__ is not None
assert callable(DurableAgent)
assert callable(drasi.register_drasi_trigger)
assert callable(drasi.enable_drasi_subscriptions)
"""


@pytest.mark.parametrize("script", (_CORE_FIRST, _EXTENSION_FIRST))
@pytest.mark.parametrize("from_checkout", (True, False))
def test_editable_core_and_extension_imports(
    script: str,
    from_checkout: bool,
    tmp_path: Path,
) -> None:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=_REPOSITORY_ROOT if from_checkout else tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
