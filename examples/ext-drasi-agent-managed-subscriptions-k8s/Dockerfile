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

FROM python:3.12.11-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.11.27 /uv /bin/uv
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    SETUPTOOLS_SCM_PRETEND_VERSION_FOR_DAPR_AGENTS=1.0.6

WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY dapr_agents ./dapr_agents
COPY ext/dapr-agents-ext-drasi ./ext/dapr-agents-ext-drasi
COPY examples/ext-drasi-agent-managed-subscriptions-k8s/pyproject.toml \
    examples/ext-drasi-agent-managed-subscriptions-k8s/uv.lock \
    ./examples/ext-drasi-agent-managed-subscriptions-k8s/
WORKDIR /src/examples/ext-drasi-agent-managed-subscriptions-k8s
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

FROM python:3.12.11-slim-bookworm
WORKDIR /app
COPY --from=build /src/examples/ext-drasi-agent-managed-subscriptions-k8s/.venv ./.venv
COPY examples/ext-drasi-agent-managed-subscriptions-k8s/app.py \
    examples/ext-drasi-agent-managed-subscriptions-k8s/actions.py \
    examples/ext-drasi-agent-managed-subscriptions-k8s/settings.py ./
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["python", "app.py"]
