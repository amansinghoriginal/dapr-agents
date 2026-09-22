<!--
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Managed Drasi integration

Credential-free integration coverage for [amansinghoriginal/dapr-agents#15](https://github.com/amansinghoriginal/dapr-agents/issues/15). This suite exercises `enable_drasi_subscriptions()` with the actual built-in DaprAgentRouter, shared protocol package, Dapr service invocation, streaming Pub/Sub, persistent state, and workflow execution.

The model is a local scripted HTTP fixture, not an external LLM. It deterministically invokes the generated subscription-list tool and the agent's ordinary `record_event` tool through the normal durable chat/tool loop. The temporary receiver records actions for inspection; it is not the autonomous demonstration or a production idempotency service.

The suite injects packed, non-aggregation query-result fixtures through the router's real inbound Pub/Sub component. It does not install the Drasi query engine or validate Kubernetes installation. Real query generation, autonomous query choice, and an idempotent external business action belong to I3.

## Run

Requirements: Python supported by the workspace, uv, Git, and a running Docker engine with Compose/Buildx support for additional build contexts. No Dapr CLI, Kubernetes context, existing cluster, Azure deployment, or API key is needed.

From the repository root:

```sh
uv sync --frozen --group dev --group test --extra drasi
uv run --frozen --no-sync --extra drasi pytest \
  --confcutdir=tests/integration/drasi_managed \
  tests/integration/drasi_managed \
  --run-drasi-managed \
  -o log_cli_level=WARNING
```

Run one scenario by appending `-k catalog_tools`, `-k interrupted`, or another pytest selector. Without `--run-drasi-managed`, these tests skip. They also carry the existing `integration` marker.

**Run this suite in its own pytest process with `--confcutdir`.** Core `tests/conftest.py` replaces SDK modules globally, and the shared integration fixtures can initialize or reuse developer-wide Dapr resources. Neither is appropriate here. An explicitly requested run fails rather than skips if Docker, image builds, or a runtime dependency is unavailable.

Images build once per pytest session. Each case then creates a fresh, randomly named Compose project, a private bridge network, temporary query files, and project-owned state volumes. Every published port binds to an ephemeral loopback port. The agent, router, and broker never adopt an existing container or application state.

Compose is invoked with an empty environment file, and the agent build has an explicit file allowlist that excludes `.env`, `.env.*`, virtual environments, and Git metadata. The repository's Azure/model configuration is not loaded or copied into either application.

## Reference pins and evidence

| Boundary | Selection |
|---|---|
| Dapr runtime, placement, scheduler | `1.18.4` |
| Redis Streams and Redis state stores | `7.4.2-alpine` |
| Built-in router and Reaction SDK | Drasi Platform revision `cb959f665dbb190143161964df69edeb7013712e` |
| Shared Python contract | Extension's pinned revision `49e8df694a16520519a1ba559f99c1a13a668431` |
| Agent SDK and dependencies | Repository `uv.lock`, installed with `--frozen` |
| Agent container Python / uv | `3.12.12` / `0.11.27` |

The router image is built directly from the Platform's own Dockerfile and Reaction SDK build context. There is no copied router or local replacement protocol. The agent image installs the checked-out core and extension as non-editable packages, so the source-tree namespace bootstrap and unit-test SDK mocks cannot mask packaging/import problems. Its core build metadata is set to `1.0.6`; the checked-out Git revision and actual image IDs, not that build-time version, identify the source under test.

Each case retains these files in its pytest temporary directory:

| File | Contents |
|---|---|
| `compose.env` | Non-secret project/image/query coordinates, written before startup for scoped recovery |
| `versions.json` | Actual runtime/Redis/package versions, Git revisions, project name, and container image IDs |
| `containers.log` | Router, agent, sidecar, broker, placement, and scheduler logs |
| `artifacts.json` | Scheduling observations, model/tool evidence, and inbox/DLT CloudEvents |

Logs are also captured when startup fails; later artifacts require a successfully started application. Pytest reports its temporary directory with normal verbose output. To choose an artifact location, use a **new, dedicated** `--basetemp` directory: pytest replaces an existing directory supplied through that option.

## Scenarios

| Scenario | Observable boundary |
|---|---|
| Startup catalog | Real MCP through Dapr invocation; shared parsing and omitted optional fields |
| Subscribe/update/unsubscribe | Generated tools, durable agent intent, and real router rules |
| Operation filtering | Router filters a mixed packed batch; admission also rejects a retained delivery excluded by a later filter |
| Router and agent restart | Durable rules/instructions survive; incarnation is retained; no startup model turn |
| Interrupted mutations | Stop the real router, observe persisted pending intent, kill the agent, then restart and reconcile subscribe/update/unsubscribe |
| Inactive/stale/unavailable input | Already-published deliveries are consumed without scheduling or model work |
| ACK boundary | Hold scheduling before acceptance and observe Redis pending state; then accept while the model remains blocked and observe broker acknowledgement before workflow completion |
| Frozen instructions | Updating a subscription while scheduling is held cannot rewrite an already admitted task |
| Intent failures | Invalid persisted intent and an actual store outage request retry, not success/absence |
| Scheduler failure | A real SDK client connects to a reserved, non-listening local port; restoring the normal client allows retry |
| Poison input and retry exhaustion | Original data reaches the configured DLT; poison never invokes the model |
| Active/terminal duplicates | Active execution is not replaced; a terminal ID can be scheduled again without application-local suppression |
| Mode exclusion and tools | Both static/dynamic registration orders reject mixing; ordinary tools and all generated tools remain available in event workflows |

The only scheduling instrumentation is a `DaprWorkflowClient` subclass that records calls, optionally holds a call before acceptance, or selects the deliberately unreachable real client. Successful calls always delegate to the native scheduler. There are no fabricated workflow IDs, fake state clients, mocked router responses, or synthetic broker acknowledgements.

For retirement/admission checks, a test changes only its temporary startup catalog or its own scoped intent document through Dapr's state API. CAS and strong reads remain enabled. Restart recovery uses fresh application processes, not reuse of a stopped registration.

## Bounded guarantees

The fixture configures three inbound retries at two-second intervals. Redis broker redelivery and SDK retries are separate layers; a configured retry count is not a universal bound on all possible attempts. The suite observes actual Redis consumer cursors and pending entries instead of treating `Subscription.respond()` as a durable broker receipt.

Poison is intentionally dropped with a configured dead-letter topic. Retriable failures can reach that same DLT after the selected runtime's retry budget is exhausted. DLT publication itself can fail; this suite does not promise unconditional redelivery, loss-free transport, exactly-once execution, permanent deduplication, or preservation across destruction of the reference environment.

Incarnations fence already-published inbox messages, not upstream query generations. Fresh query IDs remain necessary when recreating a query or changing its meaning. No migration or backward-compatibility layer is provided for this pre-release contract.

This is one logical agent, one router, and one application replica per isolated project. The state components use Redis and support ETags; this does not certify every Dapr state component or production broker configuration.

## Cleanup and ownership

Fixture teardown runs `docker compose down --volumes` for the exact generated project and verifies that no containers, networks, or volumes with that project's label remain. Session teardown removes only the two uniquely tagged images it built. Existing Docker images/build cache, clusters, Dapr installations, and unrelated resources are never pruned or uninstalled. Temporary evidence is retained for diagnosis.

A forcibly killed pytest process cannot run teardown. Copy the exact `COMPOSE_PROJECT_NAME` from that case's `compose.env` into `--project-name` below:

```sh
docker compose \
  --env-file /absolute/path/to/test-artifacts/compose.env \
  --project-name drasi-i2-<exact-project-id> \
  --file tests/integration/drasi_managed/compose.yaml \
  down --volumes --remove-orphans
```

Once all of that session's projects are down, abandoned test images can be removed with `docker image rm` using only the two exact image references from `compose.env`. Do not delete resources by a broad prefix or use `docker system prune`, `dapr uninstall`, or cluster-wide cleanup.

This directory owns its harness, fixtures, and local operating instructions. I4 owns any root workspace/lockfile changes, shared CI wiring, and broader documentation links. The I3 example directory is not modified.

Shared-tooling handoff to I4: pin or align mypy's automatically installed stub dependencies with its configured Python 3.11 target. Repeated type checks under a newer interpreter can install Python-3.12-only stubs and upgrade locked runtime packages. The repository checks for this change use Python 3.11; the real-runtime suite also runs from Python 3.13 and hosts the applications in Python 3.12 containers.

## Prior art

The Compose topology, packed-change shape, and Redis acknowledgement observations follow the real-runtime tests in the [Platform router package](https://github.com/drasi-project/drasi-platform/tree/cb959f665dbb190143161964df69edeb7013712e/reactions/dapr/agent-router/tests). That Apache-2.0 implementation remains authoritative for router behavior; these tests add the composed Dapr Agents boundary.
