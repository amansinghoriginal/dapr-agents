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
  echo "Usage: $0 <case> <result-root>" >&2
  exit 2
fi

readonly CASE_NAME="$1"
readonly RESULT_ROOT="$2"

REPORTS=()
while IFS= read -r report; do
  REPORTS+=("$report")
done < <(
  find "$RESULT_ROOT" -type f -name report.md -path "*/evaluation-*/*" | sort
)

if [ "${#REPORTS[@]}" -ne 1 ]; then
  echo "Expected exactly one report.md, found ${#REPORTS[@]}." >&2
  exit 1
fi

readonly REPORT="${REPORTS[0]}"
readonly EVIDENCE_DIR="$(dirname "$REPORT")"

python3 - "$REPORT" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8-sig").replace("\r\n", "\n")
path.write_text(text, encoding="utf-8")
PY

readonly STATUS_COUNT="$(
  grep -Ec '^## STATUS: (SUCCESS|FAILURE)$' "$REPORT" || true
)"
readonly LAST_NONEMPTY_LINE="$(awk 'NF { line=$0 } END { print line }' "$REPORT")"

if [ "$STATUS_COUNT" -ne 1 ]; then
  echo "The report must contain exactly one STATUS line." >&2
  exit 1
fi

if [ "$LAST_NONEMPTY_LINE" != "## STATUS: SUCCESS" ]; then
  echo "Tutorial evaluation did not finish successfully." >&2
  cat "$REPORT"
  exit 1
fi

require_file() {
  local filename="$1"
  if [ ! -s "$EVIDENCE_DIR/$filename" ]; then
    echo "Required evidence file is missing or empty: $filename" >&2
    exit 1
  fi
}

require_text() {
  local filename="$1"
  local text="$2"
  if ! grep -Fqi "$text" "$EVIDENCE_DIR/$filename"; then
    echo "Required evidence '$text' not found in $filename." >&2
    exit 1
  fi
}

require_nonempty_after_marker() {
  local filename="$1"
  local marker="$2"
  if ! awk -v marker="$marker" '
    index($0, marker) {
      found = 1
      rest = substr($0, index($0, marker) + length(marker))
      if (rest ~ /[^[:space:]]/) {
        ok = 1
        exit
      }
      next
    }
    found && /[^[:space:]]/ {
      ok = 1
      exit
    }
    END { exit(ok ? 0 : 1) }
  ' "$EVIDENCE_DIR/$filename"; then
    echo "Required non-empty output after '$marker' not found in $filename." >&2
    exit 1
  fi
}

case "$CASE_NAME" in
  llm-client)
    require_file "01_llm-client-output.txt"
    require_text "01_llm-client-output.txt" "Component loaded: llm-provider"
    require_text "01_llm-client-output.txt" "Exited App successfully"
    require_nonempty_after_marker "01_llm-client-output.txt" "Response:"
    ;;
  durable-workflow)
    require_file "01_durable-workflow-agent.txt"
    require_file "02_durable-workflow-trigger.txt"
    require_text "01_durable-workflow-agent.txt" "dapr.agents.WeatherAgent.workflow"
    require_text "01_durable-workflow-agent.txt" "slow_weather_func"
    require_text "02_durable-workflow-trigger.txt" "Result:"
    require_text "02_durable-workflow-trigger.txt" "London"
    require_nonempty_after_marker "02_durable-workflow-trigger.txt" "Result:"
    ;;
  pubsub-agent)
    require_file "01_pubsub-agent.txt"
    require_file "02_publish-output.txt"
    require_text "02_publish-output.txt" "Published"
    require_text "01_pubsub-agent.txt" "weather.requests"
    require_text "01_pubsub-agent.txt" "slow_weather_func"
    require_text "01_pubsub-agent.txt" "ORCHESTRATION_STATUS_COMPLETED"
    ;;
  llm-workflow)
    require_file "01_llm-workflow-output.txt"
    require_text "01_llm-workflow-output.txt" "Workflow started:"
    require_nonempty_after_marker "01_llm-workflow-output.txt" "Outline:"
    require_nonempty_after_marker "01_llm-workflow-output.txt" "Blog post:"
    require_nonempty_after_marker "01_llm-workflow-output.txt" "Final Blog Post:"
    ;;
  agent-workflow)
    require_file "01_agent-workflow-output.txt"
    require_text "01_agent-workflow-output.txt" "triage-agent"
    require_text "01_agent-workflow-output.txt" "expert-agent"
    require_text "01_agent-workflow-output.txt" "Alice"
    require_nonempty_after_marker "01_agent-workflow-output.txt" "Triage result:"
    require_nonempty_after_marker "01_agent-workflow-output.txt" "Recommendation:"
    require_nonempty_after_marker "01_agent-workflow-output.txt" "Final Recommendation:"
    ;;
  tracing)
    require_file "01_tracing-output.txt"
    require_file "02_zipkin-services.json"
    require_file "03_zipkin-traces.json"
    require_text "01_tracing-output.txt" "slow_weather_func"
    require_text "01_tracing-output.txt" "ORCHESTRATION_STATUS_COMPLETED"
    jq -e 'type == "array" and length > 0' \
      "$EVIDENCE_DIR/02_zipkin-services.json" >/dev/null
    jq -e 'type == "array" and length > 0 and (.[0] | type == "array" and length > 0)' \
      "$EVIDENCE_DIR/03_zipkin-traces.json" >/dev/null
    require_text "03_zipkin-traces.json" "workflow"
    require_text "03_zipkin-traces.json" "llm"
    require_text "03_zipkin-traces.json" "tool"
    ;;
  echo)
    require_file "01_echo-output.txt"
    require_text "01_echo-output.txt" "Response:"
    require_text "01_echo-output.txt" "Name a famous dog!"
    require_text "01_echo-output.txt" "hello"
    ;;
  hot-reload)
    require_file "01_hot-reload-agent.txt"
    require_file "02_redis-update.txt"
    require_text "01_hot-reload-agent.txt" "Original Role"
    require_text "01_hot-reload-agent.txt" "Current role: New Hot-Reloaded Role"
    require_text "02_redis-update.txt" "OK"
    ;;
  getting-started)
    require_file "01_agent-before-restart.txt"
    require_file "02_trigger-response.json"
    require_file "workflow-id.txt"
    require_file "03_status-before-restart.json"
    require_file "04_restart-marker.txt"
    require_file "05_agent-after-restart.txt"
    require_file "06_status-after-restart.json"

    readonly INSTANCE_ID="$(
      jq -er '.instance_id | select(type == "string" and length > 0)' \
        "$EVIDENCE_DIR/02_trigger-response.json"
    )"
    readonly WORKFLOW_ID="$(tr -d '[:space:]' <"$EVIDENCE_DIR/workflow-id.txt")"
    readonly PRE_RESTART_ID="$(
      jq -er '.instance_id | select(type == "string" and length > 0)' \
        "$EVIDENCE_DIR/03_status-before-restart.json"
    )"
    readonly PRE_RESTART_STATUS="$(
      jq -er '.runtime_status | select(type == "string")' \
        "$EVIDENCE_DIR/03_status-before-restart.json"
    )"
    readonly FINAL_ID="$(
      jq -er '.instance_id | select(type == "string" and length > 0)' \
        "$EVIDENCE_DIR/06_status-after-restart.json"
    )"
    readonly RUNTIME_STATUS="$(
      jq -er '.runtime_status | select(type == "string")' \
        "$EVIDENCE_DIR/06_status-after-restart.json"
    )"
    readonly ORIGINAL_TASK="$(
      jq -er '.serialized_input | fromjson | .task | select(type == "string")' \
        "$EVIDENCE_DIR/06_status-after-restart.json"
    )"
    readonly FIRST_APP_PID="$(
      grep -m1 -Eo 'appPID: [0-9]+' \
        "$EVIDENCE_DIR/01_agent-before-restart.txt" | awk '{print $2}' || true
    )"
    readonly SECOND_APP_PID="$(
      grep -m1 -Eo 'appPID: [0-9]+' \
        "$EVIDENCE_DIR/05_agent-after-restart.txt" | awk '{print $2}' || true
    )"

    if [ "$INSTANCE_ID" != "$WORKFLOW_ID" ] ||
       [ "$INSTANCE_ID" != "$PRE_RESTART_ID" ] ||
       [ "$INSTANCE_ID" != "$FINAL_ID" ]; then
      echo "The workflow evidence contains inconsistent instance IDs." >&2
      exit 1
    fi
    if [ "$PRE_RESTART_STATUS" != "RUNNING" ]; then
      echo "Expected RUNNING immediately before interruption, found: $PRE_RESTART_STATUS" >&2
      exit 1
    fi
    if [ -z "$FIRST_APP_PID" ] || [ -z "$SECOND_APP_PID" ] ||
       [ "$FIRST_APP_PID" = "$SECOND_APP_PID" ]; then
      echo "The evidence does not show two distinct application processes." >&2
      exit 1
    fi
    if grep -F "$INSTANCE_ID" "$EVIDENCE_DIR/01_agent-before-restart.txt" |
       grep -Fq "COMPLETED"; then
      echo "The workflow completed before the first process was interrupted." >&2
      exit 1
    fi
    if [[ "$ORIGINAL_TASK" != *London* ]]; then
      echo "The completed workflow does not contain the original London task." >&2
      exit 1
    fi
    require_text "04_restart-marker.txt" "$INSTANCE_ID"
    if [ "$RUNTIME_STATUS" != "COMPLETED" ]; then
      echo "Expected COMPLETED workflow status, found: $RUNTIME_STATUS" >&2
      exit 1
    fi
    require_text "01_agent-before-restart.txt" "You're up and running"
    require_text "01_agent-before-restart.txt" "$INSTANCE_ID"
    require_text "01_agent-before-restart.txt" "Function name: slow_weather_func"
    if grep -Fq "slow_weather_func(tool)" \
      "$EVIDENCE_DIR/01_agent-before-restart.txt"; then
      echo "The weather tool completed before the first process was interrupted." >&2
      exit 1
    fi
    require_text "05_agent-after-restart.txt" "You're up and running"
    require_text "05_agent-after-restart.txt" "$INSTANCE_ID"
    require_text "05_agent-after-restart.txt" "slow_weather_func"
    require_text "05_agent-after-restart.txt" "ORCHESTRATION_STATUS_COMPLETED"
    ;;
  *)
    echo "Unknown tutorial evaluation case: $CASE_NAME" >&2
    exit 2
    ;;
esac

echo "Tutorial evaluation evidence is valid: $CASE_NAME"
