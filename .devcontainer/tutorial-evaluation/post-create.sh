#!/usr/bin/env bash
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

set -euo pipefail

readonly DAPR_RUNTIME_VERSION="${DAPR_RUNTIME_VERSION:-1.18.0}"
readonly OLLAMA_MODEL="${OLLAMA_MODEL:-qwen3:0.6b}"
readonly OLLAMA_TOOL_MODEL="${OLLAMA_TOOL_MODEL:-qwen2.5:3b}"
readonly OLLAMA_VERSION="0.32.15"
readonly UV_VERSION="0.12.5"
readonly COPILOT_CLI_VERSION="1.0.80"

echo "Installing tutorial evaluation dependencies..."
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  jq \
  lsof \
  procps \
  python-is-python3 \
  python3-pip \
  python3-venv \
  redis-tools \
  zstd

python3 -m pip install \
  --break-system-packages \
  --disable-pip-version-check \
  "uv==$UV_VERSION"
npm install -g "@github/copilot@$COPILOT_CLI_VERSION"

if ! command -v ollama >/dev/null 2>&1; then
  case "$(uname -m)" in
    x86_64)
      readonly OLLAMA_ARCH="amd64"
      readonly OLLAMA_SHA256="50539c5fe9bf85887733355098dcdb266b433cb8c73fa180713417e9ed6e42bb"
      ;;
    aarch64 | arm64)
      readonly OLLAMA_ARCH="arm64"
      readonly OLLAMA_SHA256="c898270b1690eab0f51aa9e9197686b7b4c6a7d88b83967763818f3127e477e9"
      ;;
    *)
      echo "Unsupported Ollama architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac

  readonly OLLAMA_ARCHIVE="/tmp/ollama-linux-${OLLAMA_ARCH}.tar.zst"
  curl -fsSL \
    "https://github.com/ollama/ollama/releases/download/v${OLLAMA_VERSION}/ollama-linux-${OLLAMA_ARCH}.tar.zst" \
    -o "$OLLAMA_ARCHIVE"
  echo "$OLLAMA_SHA256  $OLLAMA_ARCHIVE" | sha256sum --check -
  zstd -dc "$OLLAMA_ARCHIVE" | sudo tar -xf - -C /usr/local
fi

echo "Waiting for Docker..."
for attempt in $(seq 1 60); do
  if docker info >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "Docker did not become ready." >&2
    exit 1
  fi
  sleep 2
done

echo "Initializing Dapr runtime ${DAPR_RUNTIME_VERSION}..."
dapr uninstall --all >/dev/null 2>&1 || true
dapr init --runtime-version "$DAPR_RUNTIME_VERSION"

echo "Starting Ollama..."
if ! pgrep -f "ollama serve" >/dev/null 2>&1; then
  nohup ollama serve >/tmp/ollama-serve.log 2>&1 &
fi

for attempt in $(seq 1 60); do
  if curl -fsS http://localhost:11434/api/tags >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 60 ]; then
    echo "Ollama did not become ready." >&2
    cat /tmp/ollama-serve.log >&2 || true
    exit 1
  fi
  sleep 2
done

ollama pull "$OLLAMA_MODEL"
ollama pull "$OLLAMA_TOOL_MODEL"

echo "Tutorial evaluation environment is ready."
