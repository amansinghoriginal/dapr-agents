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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <case> <docs-repository-path>" >&2
  exit 2
fi

readonly CASE_NAME="$1"
readonly DOCS_REPOSITORY_PATH="$2"
readonly REPOSITORY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly EVALUATION_ROOT="$REPOSITORY_ROOT/.tutorial-evaluation"
readonly STAGING_ROOT="$EVALUATION_ROOT/staging/$CASE_NAME"
readonly RESULT_ROOT="$EVALUATION_ROOT/results/$CASE_NAME"
readonly CASE_FILE="$STAGING_ROOT/tutorial-case.md"
readonly TUTORIAL_DOC="$STAGING_ROOT/tutorial-docs/_index.md"

rm -rf "$STAGING_ROOT" "$RESULT_ROOT"
mkdir -p \
  "$EVALUATION_ROOT/cache/ollama" \
  "$STAGING_ROOT/tutorial-docs" \
  "$RESULT_ROOT"

case "$CASE_NAME" in
  llm-client)
    cp "$REPOSITORY_ROOT/quickstarts/README.md" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: LLM Client

- Working directory: \`quickstarts\`
- Tutorial document: \`.tutorial-evaluation/staging/llm-client/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/llm-client\`

## Scope

Evaluate the shared Python environment setup, the default local Ollama
configuration, and "1. LLM Client". Stop before "2. Durable Agent Workflow".
Docker, Dapr, Python, uv, Ollama, and \`qwen3:0.6b\` are available; Python
dependencies are not preinstalled.

## Required evidence

- \`01_llm-client-output.txt\`: complete combined output from the documented
  LLM Client command.

The evidence must show the \`llm-provider\` component loading, a successful
application exit, and a non-empty \`Response:\` line.
EOF
    ;;
  durable-workflow)
    cp "$REPOSITORY_ROOT/quickstarts/README.md" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: Durable Agent Workflow

- Working directory: \`quickstarts\`
- Tutorial document: \`.tutorial-evaluation/staging/durable-workflow/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/durable-workflow\`

## Scope

Evaluate the shared Python environment setup, local Ollama configuration, and
"2. Durable Agent Workflow" using Option B, the \`call_agent\` orchestrator
path. Skip Option A. Use \`qwen2.5:3b\`, which the document recommends for more
reliable tool calling, instead of the smaller default model. Docker, Dapr,
Python, uv, Ollama, and that model are available; Python dependencies are not
preinstalled.

Run the Terminal 1 agent command as a background process, wait for its workflow
runtime to report \`Workflow engine started\` and establish its work-item stream,
then run the documented Option B Terminal 2 command. Do not wait for a named
WeatherAgent instance before triggering; that instance is created by Terminal 2.
After capturing the result, terminate only the Terminal 1 process you started.

## Required evidence

- \`01_durable-workflow-agent.txt\`: complete output from the Terminal 1 agent.
- \`02_durable-workflow-trigger.txt\`: complete output from the Option B trigger.

The evidence must show the WeatherAgent workflow registering, the weather tool
running for London, and a non-empty \`Result:\` from the trigger.
EOF
    ;;
  pubsub-agent)
    cp "$REPOSITORY_ROOT/quickstarts/README.md" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: Durable Agent Subscribe

- Working directory: \`quickstarts\`
- Tutorial document: \`.tutorial-evaluation/staging/pubsub-agent/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/pubsub-agent\`

## Scope

Evaluate the shared Python environment setup, local Ollama configuration, and
"4. Durable Agent Subscribe". Use \`qwen2.5:3b\`, which the document recommends
for more reliable tool calling, instead of the smaller default model. Docker,
Dapr, Redis, Python, uv, Ollama, and that model are available; Python
dependencies are not preinstalled.

Run the documented subscriber as a background process. Wait until the
\`weather.requests\` subscription is ready, publish the documented message from
a second terminal, and wait up to 300 seconds for that message's workflow to
emit the exact \`ORCHESTRATION_STATUS_COMPLETED\` marker. A final assistant
response alone is not completion evidence. Then terminate only the subscriber
process you started.

## Required evidence

- \`01_pubsub-agent.txt\`: subscriber output through workflow completion.
- \`02_publish-output.txt\`: output from the documented \`dapr publish\` command.

The evidence must show a successful publish, receipt of the weather request,
the weather tool running for London, and completed workflow execution.
EOF
    ;;
  llm-workflow)
    cp "$REPOSITORY_ROOT/quickstarts/README.md" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: Workflow with LLM Activities

- Working directory: \`quickstarts\`
- Tutorial document: \`.tutorial-evaluation/staging/llm-workflow/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/llm-workflow\`

## Scope

Evaluate the shared Python environment setup, the default local Ollama
configuration, and "5. Workflow with LLM Activities". Docker, Dapr, Python, uv,
Ollama, and \`qwen3:0.6b\` are available; Python dependencies are not
preinstalled.

## Required evidence

- \`01_llm-workflow-output.txt\`: complete combined output from the documented
  workflow command.

The evidence must show a workflow instance starting, a non-empty outline, a
non-empty blog post, and a non-empty final blog post after completed workflow
execution.
EOF
    ;;
  agent-workflow)
    cp "$REPOSITORY_ROOT/quickstarts/README.md" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: Workflow with Agent Activities

- Working directory: \`quickstarts\`
- Tutorial document: \`.tutorial-evaluation/staging/agent-workflow/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/agent-workflow\`

## Scope

Evaluate the shared Python environment setup, local Ollama configuration, and
"6. Workflow with Agent Activities". Use \`qwen2.5:3b\`, which the document
recommends for more reliable tool calling, instead of the smaller default
model. Docker, Dapr, Redis, Python, uv, Ollama, and that model are available;
Python dependencies are not preinstalled.

Run the documented multi-app command as a background terminal process. Wait for
the support workflow's final recommendation, then terminate only the multi-app
processes you started.

## Required evidence

- \`01_agent-workflow-output.txt\`: complete multi-app output through the final
  recommendation.

The evidence must show the triage and expert agents starting, a triage result
for Alice, an expert recommendation, and a non-empty final recommendation from
a completed support workflow.
EOF
    ;;
  tracing)
    cp "$REPOSITORY_ROOT/quickstarts/README.md" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: Durable Agent Trace

- Working directory: \`quickstarts\`
- Tutorial document: \`.tutorial-evaluation/staging/tracing/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/tracing\`

## Scope

Evaluate the shared Python environment setup, local Ollama configuration, and
"7. Durable Agent Trace (Zipkin)". Use \`qwen2.5:3b\`, which the document
recommends for more reliable tool calling, instead of the smaller default
model. Docker, Dapr, Zipkin, Python, uv, Ollama, and that model are available;
Python dependencies are not preinstalled. The browser UI is outside this first
automated scope. Use Zipkin's localhost API only as an evidence-capture
equivalent of inspecting the documented UI.

## Required evidence

- \`01_tracing-output.txt\`: complete output from the documented tracing command.
- \`02_zipkin-services.json\`: real response from Zipkin's services endpoint.
- \`03_zipkin-traces.json\`: real traces retrieved for a service emitted by this
  run.

The application evidence must show the weather tool and completed workflow. The
Zipkin evidence must contain at least one trace and span data for workflow,
LLM, and tool operations.
EOF
    ;;
  echo)
    cp "$REPOSITORY_ROOT/examples/01-llm-call-dapr/README.md" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: Dapr Chat Client with Echo

- Working directory: \`examples/01-llm-call-dapr\`
- Tutorial document: \`.tutorial-evaluation/staging/echo/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/echo\`

## Scope

Evaluate only "Environment Setup" and "1. Using the Echo Component". Stop before
"2. Switching to OpenAI". Docker and Dapr are initialized already. Dependencies
are not preinstalled; execute the in-scope setup and run commands as a user
would. No API key is required.

## Required evidence

- \`01_echo-output.txt\`: complete combined output from the documented
  \`text_completion.py\` run through Dapr.

The evidence must show a successful command, at least one \`Response:\` line,
the fixed prompt \`Name a famous dog!\`, and the echoed \`hello\` user input.
EOF
    ;;
  hot-reload)
    cp "$REPOSITORY_ROOT/quickstarts/README.md" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: Durable Agent Hot-Reload

- Working directory: \`quickstarts\`
- Tutorial document: \`.tutorial-evaluation/staging/hot-reload/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/hot-reload\`

## Scope

Evaluate only the shared environment setup that is relevant to Python
dependencies and "8. Durable Agent Hot-Reload". Skip LLM configuration and all
other numbered quickstarts. Docker, Dapr, Redis, Python, and uv are available;
the tutorial's Python dependencies are not preinstalled.

Run the documented agent command as a background terminal process so that you
can execute the documented Redis update from a second terminal. Wait only long
enough to observe the update, then terminate the process you started.

## Required evidence

- \`01_hot-reload-agent.txt\`: complete agent output from startup through the
  observed configuration change.
- \`02_redis-update.txt\`: output from the documented Redis SET command.

The evidence must show \`Original Role\`, \`New Hot-Reloaded Role\`, and a
successful Redis update.
EOF
    ;;
  getting-started)
    readonly SOURCE_DOC="$DOCS_REPOSITORY_PATH/daprdocs/content/en/developing-ai/dapr-agents/dapr-agents-getting-started.md"
    if [ ! -f "$SOURCE_DOC" ]; then
      echo "Getting Started source not found: $SOURCE_DOC" >&2
      exit 1
    fi
    cp "$SOURCE_DOC" "$TUTORIAL_DOC"
    cat >"$CASE_FILE" <<EOF
# Tutorial evaluation case: Durable HTTP Agent and Recovery

- Working directory: \`quickstarts\`
- Tutorial document: \`.tutorial-evaluation/staging/getting-started/tutorial-docs/_index.md\`
- Result root: \`.tutorial-evaluation/results/getting-started\`

## Scope

The repository, Dapr CLI/runtime, Docker services, uv, Ollama, and
\`qwen2.5:3b\` are already installed. Verify the documented environment where
the guide gives a verification command, but do not reinstall those tools or
clone the repository.

Evaluate from "Prepare your environment" through "Test durability by
interrupting the agent". Skip the alternative OpenAI configuration, Diagrid Dev
Dashboard, Redis Insight, and Next Steps.

Run the documented durable agent command in a background terminal and preserve
its output. Trigger the workflow, capture the returned workflow ID, stop the
documented agent process during execution, restart the same command, and query
the same workflow ID until it reaches a terminal state. Terminate only the
processes you started after collecting evidence.

After the POST, wait until the first agent log shows the
\`Function name: slow_weather_func\` call. During that documented 30-second
tool delay, obtain the valid \`RUNNING\` status and stop the first process
immediately. Fail if the tool call does not appear or its tool-result line has
already appeared before interruption.

After the POST, use a bounded readiness poll of up to 30 seconds to obtain the
first valid JSON workflow status. A transient 404, 500, or non-JSON response
while workflow state materializes is not the status evidence. Fail if no valid
state appears, or if the first valid state is already terminal instead of
\`RUNNING\`. Check the response's exact \`runtime_status\` field; there is no
\`status\` field in this API response.

## Required evidence

- \`01_agent-before-restart.txt\`: output from the first agent process.
- \`02_trigger-response.json\`: JSON body returned by POST \`/agent/run\`.
- \`workflow-id.txt\`: only the returned workflow ID.
- \`03_status-before-restart.json\`: JSON returned by GET
  \`/agent/instances/{WORKFLOW_ID}\` immediately before stopping the first process.
- \`04_restart-marker.txt\`: a short record containing the same workflow ID and
  confirming that the first documented process was stopped before restart.
- \`05_agent-after-restart.txt\`: output from the restarted agent process.
- \`06_status-after-restart.json\`: final JSON returned by GET
  \`/agent/instances/{WORKFLOW_ID}\`.

The trigger response must contain a non-empty \`instance_id\`. The status
immediately before interruption must be \`RUNNING\`; if it has already completed,
the durability test has not been performed and must fail. The first process must
then be stopped. Before writing the restart marker or starting a replacement,
wait up to 60 seconds for the first process tree to exit and for localhost port
8001 to reject connections. Fail instead of restarting if either remains active.
The final status must be \`COMPLETED\`, and every identifier-bearing file must
refer to the same workflow ID.
EOF
    ;;
  *)
    echo "Unknown tutorial evaluation case: $CASE_NAME" >&2
    exit 2
    ;;
esac

echo "Prepared tutorial evaluation case '$CASE_NAME'."
