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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <case>" >&2
  exit 2
fi

readonly CASE_NAME="$1"
readonly REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly STAGING_ROOT="$REPOSITORY_ROOT/.tutorial-evaluation/staging/$CASE_NAME"
readonly RESULT_ROOT="$REPOSITORY_ROOT/.tutorial-evaluation/results/$CASE_NAME"
readonly CASE_FILE="$STAGING_ROOT/tutorial-case.md"
readonly TUTORIAL_DOC="$STAGING_ROOT/tutorial-docs/_index.md"
readonly BASE_PROMPT="$REPOSITORY_ROOT/.github/prompts/tutorial-evaluation.md"
readonly COPILOT_OUTPUT="$RESULT_ROOT/copilot-output.log"
readonly COPILOT_HOME_DIR="$(mktemp -d "/tmp/dapr-agents-copilot-${CASE_NAME}.XXXXXX")"

case "$CASE_NAME" in
  echo)
    readonly CASE_WORKDIR="$REPOSITORY_ROOT/examples/01-llm-call-dapr"
    ;;
  *)
    readonly CASE_WORKDIR="$REPOSITORY_ROOT/quickstarts"
    ;;
esac

cleanup() {
  case "$CASE_NAME" in
    durable-workflow)
      dapr stop --app-id weather-agent >/dev/null 2>&1 || true
      dapr stop --app-id workflow-trigger >/dev/null 2>&1 || true
      dapr stop --app-id agent-orchestrator >/dev/null 2>&1 || true
      ;;
    pubsub-agent)
      dapr stop --app-id durable-agent-subscriber >/dev/null 2>&1 || true
      ;;
    agent-workflow)
      (
        cd "$REPOSITORY_ROOT/quickstarts"
        dapr stop -f 06_workflow_agents.yaml >/dev/null 2>&1 || true
      )
      ;;
    hot-reload)
      dapr stop --app-id hot-reload-agent >/dev/null 2>&1 || true
      ;;
    getting-started)
      dapr stop --app-id durable-agent >/dev/null 2>&1 || true
      ;;
  esac
  rm -rf "$COPILOT_HOME_DIR"
}
trap cleanup EXIT

if [ ! -f "$CASE_FILE" ]; then
  echo "Case descriptor is missing: $CASE_FILE" >&2
  exit 1
fi
if [ -z "${COPILOT_GITHUB_TOKEN:-}" ]; then
  echo "COPILOT_GITHUB_TOKEN is not set." >&2
  exit 1
fi
readonly COPILOT_TOKEN="$COPILOT_GITHUB_TOKEN"
readonly TUTORIAL_OLLAMA_ENDPOINT="${OLLAMA_ENDPOINT:-http://localhost:11434/v1}"
TUTORIAL_OLLAMA_MODEL="${OLLAMA_MODEL:-qwen3:0.6b}"
TUTORIAL_DAPR_API_TIMEOUT_SECONDS="120"
case "$CASE_NAME" in
  durable-workflow | getting-started | pubsub-agent | agent-workflow | tracing)
    TUTORIAL_OLLAMA_MODEL="${OLLAMA_TOOL_MODEL:-qwen2.5:3b}"
    ;;
esac
if [ "$CASE_NAME" = "agent-workflow" ]; then
  TUTORIAL_DAPR_API_TIMEOUT_SECONDS="300"
fi
readonly TUTORIAL_OLLAMA_MODEL
readonly TUTORIAL_DAPR_API_TIMEOUT_SECONDS
readonly TUTORIAL_BASH_ENV="$COPILOT_HOME_DIR/tutorial-env.sh"
unset \
  COPILOT_GITHUB_TOKEN \
  OLLAMA_ENDPOINT \
  OLLAMA_HOST \
  OLLAMA_MODEL \
  OLLAMA_TOOL_MODEL \
  OLLAMA_NUM_PARALLEL \
  OPENAI_API_KEY \
  OPENAI_BASE_URL

mkdir -p "$RESULT_ROOT"

export NODE_PATH="$(npm root -g)"
export COPILOT_HOME="$COPILOT_HOME_DIR"

cat >"$TUTORIAL_BASH_ENV" <<EOF
export OLLAMA_ENDPOINT="$TUTORIAL_OLLAMA_ENDPOINT"
export OLLAMA_MODEL="$TUTORIAL_OLLAMA_MODEL"
export OLLAMA_NUM_PARALLEL="1"
export OPENAI_API_KEY="ollama"
export OPENAI_BASE_URL="$TUTORIAL_OLLAMA_ENDPOINT"
export DAPR_API_TIMEOUT_SECONDS="$TUTORIAL_DAPR_API_TIMEOUT_SECONDS"
EOF
chmod 600 "$TUTORIAL_BASH_ENV"

docker info >/dev/null
redis-cli ping | grep -Fx PONG >/dev/null
curl -fsS http://localhost:11434/api/tags >/dev/null

readonly PROMPT="$(
  cat "$BASE_PROMPT"
  printf '\nCase descriptor:\n\n'
  cat "$CASE_FILE"
  printf '\nAbsolute repository root: `%s`\n' "$REPOSITORY_ROOT"
  printf 'Absolute tutorial document: `%s`\n' "$TUTORIAL_DOC"
  printf 'Absolute case result root: `%s`\n' "$RESULT_ROOT"
)"

set +e
BASH_ENV="$TUTORIAL_BASH_ENV" COPILOT_GITHUB_TOKEN="$COPILOT_TOKEN" \
  timeout 1800 copilot \
  -C "$CASE_WORKDIR" \
  --prompt "$PROMPT" \
  --model gpt-5.6-sol \
  --effort xhigh \
  --bash-env=on \
  --secret-env-vars=COPILOT_GITHUB_TOKEN \
  --no-ask-user \
  --no-auto-update \
  --disable-builtin-mcps \
  --allow-all-tools \
  --allow-all-paths \
  --allow-url http://localhost \
  --allow-url http://127.0.0.1 \
  --deny-tool fetch \
  --deny-tool websearch \
  --deny-tool 'shell(git push)' \
  --deny-tool 'shell(gh:*)' \
  --deny-tool 'shell(ssh:*)' \
  --deny-tool 'shell(scp:*)' \
  --deny-tool 'shell(nc:*)' \
  --deny-tool 'shell(telnet:*)' \
  --deny-tool 'shell(ftp:*)' \
  --deny-tool 'shell(wget:*)' \
  --deny-tool 'shell(dd:*)' \
  2>&1 | tee "$COPILOT_OUTPUT"
readonly COPILOT_STATUS="${PIPESTATUS[0]}"
set -e

bash "$REPOSITORY_ROOT/scripts/tutorial-evaluation/validate-report.sh" \
  "$CASE_NAME" "$RESULT_ROOT"

if [ "$COPILOT_STATUS" -ne 0 ]; then
  echo "Copilot CLI exited with status $COPILOT_STATUS." >&2
  exit "$COPILOT_STATUS"
fi
