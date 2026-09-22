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

"""Opt-in, credential-free integration fixtures with precise project ownership."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from uuid import uuid4

import pytest

from .runtime import HERE, Images, Runtime, command


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-drasi-managed",
        action="store_true",
        help="Build and run the isolated real-Dapr Drasi integration suite.",
    )


def pytest_sessionstart(session: pytest.Session) -> None:
    if session.config.getoption("--run-drasi-managed") and (
        "dapr" in sys.modules and not isinstance(sys.modules["dapr"], ModuleType)
    ):
        raise pytest.UsageError(
            "Core conftest.py mocks the Dapr SDK. Run this suite separately with "
            "--confcutdir=tests/integration/drasi_managed."
        )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if not config.getoption("--run-drasi-managed"):
        for item in items:
            if Path(item.path).is_relative_to(HERE) and item.get_closest_marker(
                "integration"
            ):
                item.add_marker(pytest.mark.skip(reason="Use --run-drasi-managed."))


@pytest.fixture(scope="session")
def images(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Images]:
    tag = f"drasi-i2-{uuid4().hex}"
    images = Images(agent=f"{tag}-agent:local", router=f"{tag}-router:local")
    builder = Runtime(tmp_path_factory.mktemp("drasi-build"), images)
    try:
        builder.compose("config", "--quiet")
        builder.compose("build", "--quiet", "router", "model", timeout=1200)
        yield images
    finally:
        try:
            for image in (images.agent, images.router):
                existing = command(
                    ["docker", "image", "ls", "--quiet", image],
                    environment=builder.environment,
                )
                if existing:
                    command(
                        ["docker", "image", "rm", image],
                        environment=builder.environment,
                    )
        finally:
            builder.client.close()


@pytest.fixture
def runtime(tmp_path: Path, images: Images) -> Iterator[Runtime]:
    runtime = Runtime(tmp_path, images)
    try:
        runtime.start()
        yield runtime
    finally:
        try:
            runtime.capture()
        finally:
            runtime.close()
