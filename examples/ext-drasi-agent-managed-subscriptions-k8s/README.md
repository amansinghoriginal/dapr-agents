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

# Agent-managed Drasi subscriptions

One SRE agent chooses persistent monitoring from two Drasi queries, reacts to real PostgreSQL changes in independent workflows, and maintains one useful service-assessment record. Application code enables the capability but never selects a query or calls a subscription tool on the agent's behalf.

This is a pre-release, trusted, single-replica reference environment. It deliberately has one deployment path and no scaling machinery, migration layer, compatibility shims, dashboard, or general-purpose replay/deduplication service.

## What runs

```text
PostgreSQL service_errors / rollout_status
    -> Drasi PostgreSQL Source
    -> two ordinary Continuous Queries
    -> built-in DaprAgentRouter
    -> application-facing Redis / Dapr Pub/Sub
    -> CheckoutSRE independent workflows
    -> PostgreSQL service_assessments[checkout]
```

The catalog offers individual checkout HTTP 5xx error records and individual checkout rollout-status records. Neither query aggregates. Insert/update/delete operations describe projected result membership and changes, not necessarily database insert/update/delete operations.

The router uses Platform-managed state and its provided one-replica `Recreate` deployment policy. Its catalog and control endpoint are the implemented shared protocol, reached through private Dapr service invocation. There is no bespoke router Service, extra router state Component, old MCP tool name, or copied wire schema.

The agent-facing Redis is separate from Drasi's internal broker. Namespace-local `agent-egress` and `agent-pubsub` Components point at that application broker. Agent workflow state and subscription intent use its separate Redis database 1. PostgreSQL's assessment table is not included in the Source, so assessment writes do not cause a feedback loop.

## Prerequisites and pins

Use macOS or Linux on arm64 or amd64 with Docker running, Docker Buildx, Git, Make, kubectl, k3d, Helm, and uv. The first source build can take significant time and disk space. No host Rust, Go, Drasi CLI, Dapr CLI, or Python package installation outside the example is required.

| Input | Reference version or source pin |
|---|---|
| k3d | `v5.8.3` |
| Kubernetes node | `rancher/k3s:v1.32.5-k3s1` |
| Helm | `v3.17.3` |
| Dapr chart and sidecars | `1.18.1` |
| uv | `0.11.27` |
| Agent Python image | `python:3.12.11-slim-bookworm` |
| Local Python | Python 3.12, selected by uv |
| PostgreSQL | `postgres:15.8-alpine3.20` |
| Application Redis | `redis:7.4.1-alpine3.20` |
| Platform source and CLI | [`cb959f665dbb190143161964df69edeb7013712e`](https://github.com/drasi-project/drasi-platform/tree/cb959f665dbb190143161964df69edeb7013712e) |
| Platform images | `m2-cb959f665dbb-azure-linux`, built from that checkout and its pinned Core submodule |
| CLI builder | `golang:1.24.4-bookworm` |
| Agent/router Python contract | The extension's immutable public Git dependency, resolved in this example's `uv.lock` |

`cluster.py` pins the Platform-provided MongoDB and internal Redis images by digest without replacing their configuration or provisioning another router state store. It builds only the Platform components this example uses, selecting the provided Azure Linux variant. The agent image gets a tag derived from its built image ID, not `latest`.

Use the listed CLI versions when reproducing this environment; setup checks tool availability, not exact installed versions. Other images use explicit version tags, which registries can republish. This reference setup does not claim hermetic or bit-for-bit image reproducibility.

This directory has an isolated uv workspace and lockfile so it can run before the later shared workspace/CI/documentation handoff. It installs the core and extension from this repository checkout. Do not replace them with a released package that lacks `enable_drasi_subscriptions()`.

## Model configuration

Use a tool-capable Azure OpenAI deployment with its OpenAI-compatible v1 API. Keep the Azure deployment on a fixed model version for repeatable runs; deployment names and credentials are operator configuration, not repository constants.

From this directory:

```bash
cp .env.example .env
```

Fill in these four values in the ignored `.env`:

```dotenv
LLM_PROVIDER=azure
LLM_CHAT_URL=https://YOUR-RESOURCE.openai.azure.com/openai/v1/
LLM_API_KEY=YOUR-KEY
LLM_MODEL=YOUR-DEPLOYMENT
```

An existing v1 `/responses` or `/chat/completions` URL is also accepted and normalized to the SDK's base URL. This agent uses **Chat Completions with tool calling**, so the deployment must support that API. `LLM_PROVIDER=openai` also works with a compatible v1 endpoint.

Only these four settings are copied into the example's Kubernetes Secret. Unrelated `.env` values are not copied. The file is not sourced as shell code, credentials are sent to kubectl on standard input rather than command-line arguments, and the Docker build context excludes `.env` files. Process environment values, including empty values, take precedence over file values. Empty required settings fail explicitly rather than falling back to credentials or configuration from the file.

If your configuration is already in the repository root, use `--env-file ../../.env` below instead of making another copy. Do not commit credentials.

## Set up

```bash
uv sync --locked
uv run --locked python cluster.py setup --env-file .env
```

Setup builds the pinned Platform components, its CLI, and the actual agent image. It creates a dedicated `drasi-agent-managed-reference` k3d cluster, installs Dapr and the built-in Platform providers, and deploys the source, queries, router, and agent. PostgreSQL credentials are generated for this environment.

The application namespace is `drasi-m2-demo`; Drasi uses `drasi-system`. The script checks the router's one-replica `Recreate` policy and Platform-created state Component before hosting the agent.

**Setup does not submit a model task or create dynamic router rules.** `enable_drasi_subscriptions()` prepares and reconciles the capability before the worker and HTTP readiness are exposed. Query selection happens later, during an ordinary user task.

Your default kubeconfig/current context and existing Drasi CLI configuration are not changed. This example stores its kubeconfig, ownership marker, private Drasi configuration, source checkout, and build outputs under ignored `.runtime/`. Setup refuses to reuse or delete an existing cluster with the same name.

## Run the complete walkthrough

```bash
uv run --locked python demo.py exercise
```

The walkthrough requires a fresh example. It prints the user task, workflow completion, confirmed router rules, and actual database assessments. It fails rather than silently choosing tools for the LLM, treating a prose answer as a successful subscription, or accepting a missing external effect.

| Step | What you observe |
|---|---|
| Empty starting state | No dynamic rules and no assessment object. A pre-existing synthetic error is acknowledged with no subscriber. There is no historical catch-up. |
| Autonomous selection | A broad monitoring task, without tool names or query IDs, causes the LLM to select error monitoring with operation `i` and store future handling instructions. The ordinary task completes before event work is produced. |
| Unselected query | A new rollout record is processed but does not reach the agent because rollout monitoring has not been selected. |
| Excluded operation | Updating the pre-existing error produces a query-result `u`, which is filtered by the selected `i` rule. The assessment table remains empty. |
| Independent event work | A new checkout error mentioning an ongoing rollout causes an event workflow to write an investigating assessment and establish follow-up rollout monitoring with operation `u`. No second user request establishes that monitoring. |
| Follow-up work | Updating the rollout to healthy produces another event workflow and a healthy assessment of the same service. |
| Duplicate execution | Two fresh workflows receive the same already-observed recovery and perform the assessment action again. Their workflow IDs differ, the database confirms new writes, and there is still exactly one assessment object. |
| Inspection and unsubscribe | Ordinary model tasks inspect and stop monitoring. Router inspection confirms the rules are absent, and a subsequent source error is acknowledged without delivery to the agent. |

Negative observations are checked after the matching packed input is visible in Drasi's real Redis stream and the router consumer has acknowledged it. Both dead-letter streams must remain empty; a dead-lettered input is not reported as successfully filtered. The walkthrough does not use an arbitrary sleep as proof that a filtered change was processed.

The duplicate-observation step intentionally uses fresh workflows rather than depending on whether a repeated inbox message's workflow ID is reused or suppressed. The property being demonstrated is the destination's business identity, not a deduplication promise.

## Why the external action is safe to repeat

The agent's only business action is `record_service_assessment(status, summary)`. The service key comes from trusted application configuration (`ASSESSMENT_SERVICE=checkout`), not tool arguments or event fields. There is no model-generated idempotency token.

PostgreSQL enforces `PRIMARY KEY (service)`. The action uses a parameterized `INSERT ... ON CONFLICT (service) DO UPDATE` in one transaction. Repeated or concurrent executions may change the assessment text or timestamp, but cannot create multiple objects for that service. Writes are last-writer-wins; this is not event ordering or exactly-once reasoning.

The ordinary assessment tool and all generated subscription tools remain available in event workflows. Stored handling instructions authorize work; projected event data is explicitly untrusted and cannot choose the action's destination or business key.

Establishing follow-up monitoring and recording the error assessment are independent actions in this example. Neither ordering nor atomicity between those tool calls is required.

## Inspect and control the agent

The walkthrough stops monitoring at its end. While the environment is still running, you can submit another ordinary task or inspect it:

```bash
uv run --locked python demo.py task "Keep monitoring newly appearing checkout server errors and record an assessment when one arrives."
uv run --locked python demo.py inspect
uv run --locked python demo.py unsubscribe
```

Inspection shows both agent-reported intent and the router's confirmed routing snapshot. The latter is not a fresh state-store health check. Unsubscribe does not cancel workflows already scheduled.

All client port-forwards bind to dynamic loopback ports and are closed when the command exits. To inspect application or router logs directly:

```bash
kubectl --kubeconfig .runtime/kubeconfig.yaml -n drasi-m2-demo logs deployment/checkout-sre -c agent
kubectl --kubeconfig .runtime/kubeconfig.yaml -n drasi-system logs deployment/sre-router-reaction -c reaction
```

Do not scale the agent or router. An ordinary application restart preserves intent and router rules; it does not unsubscribe. This example uses a single hosting lifecycle per process, so recover failed preparation or a stopped consumer by restarting the application, not by trying to rehost the same agent object.

## Failure boundaries

- The router ACKs after its selected broker publications are accepted. The agent ACKs after workflow scheduling acceptance, not after local enqueue or workflow completion.
- Model/tool failures after scheduling are workflow failures, not a reason to promise inbox replay. Inspect the workflow status and logs.
- Both inbound paths have finite retry policies. The router's DLT belongs to Drasi's internal broker; the agent's derived DLT belongs to the application broker. Neither is an automatic replay service.
- Delivery can duplicate, arrive late, or be lost under bounded retry, publication, and dead-letter failure conditions. There is no permanent deduplication ledger, source-event-time subscription cutoff, or universal loss-free guarantee.
- Incarnations reject already-published inbox messages carrying an old incarnation. They do not fence every old upstream event. Use fresh query IDs for recreated queries or changed semantics.
- This is synthetic data in a private cluster, not production authorization, network isolation, a real deployment action, or a ticketing integration.

If the model does not choose the expected monitoring, inspect its completed response and the actual rules. Do not add a Python fallback subscription or force a tool name to make the demonstration appear to pass. Correct the configuration or task and rerun from a clean reference environment.

## Clean up

```bash
uv run --locked python cluster.py cleanup
```

Cleanup validates local runtime paths before any deletion and verifies the local ownership record and matching Docker label before deleting an existing cluster. It removes that cluster's applications, volumes, rules, intent, generated Secrets, kubeconfig, and private Drasi client configuration. It does not delete another cluster, alter your default context, remove your original `.env`, or touch the Azure model deployment.

If creation failed before a cluster exists, the ownership marker may remain. Run the same cleanup command: it removes orphaned local state only after both k3d and Docker confirm that no matching cluster or node containers remain. Failed inventory queries or resources whose ownership cannot be verified preserve the marker and credentials. Fix unsafe local path types or symlinks before retrying cleanup; do not manually discard the ownership marker for a live cluster.

The pinned source checkout, built images, and build caches remain for reuse. No global Docker prune, namespace wildcard, cluster-wide unsubscribe, or host-wide process kill is used.

## Example-local development

```bash
uv sync --locked --group dev
uv run --locked ruff format app.py actions.py settings.py cluster.py demo.py tests
uv run --locked flake8 app.py actions.py settings.py cluster.py demo.py tests --ignore=E501,F401,W503,E203,E704
uv run --locked mypy --config-file pyproject.toml app.py actions.py settings.py cluster.py demo.py
uv run --locked pytest tests -m "not integration"
DEMO_TEST_POSTGRES=1 uv run --locked pytest tests/test_assessment_database.py
```

The PostgreSQL test creates and removes its own uniquely named container with a loopback-only random port. It proves repeated and concurrent writes retain one object and that a duplicate raw insert is rejected by the database. It does not connect to an existing database. The full model/Drasi walkthrough is `demo.py exercise`, not the unit suite.

Shared workspace registration, root lockfiles, CI, and top-level documentation are deliberately unchanged here and belong to the composition epic's later handoff.

## Prior art

The application/state/pubsub layout builds on the repository's [author-configured Drasi example](../ext-drasi-change-driven-agents-k8s/README.md). The separate-broker deployment and acknowledged-input observations adapt the [Platform's pinned DaprAgentRouter end-to-end scenario](https://github.com/drasi-project/drasi-platform/tree/cb959f665dbb190143161964df69edeb7013712e/e2e-tests/11-dapr-agent-router-scenario). The catalog and service-invocation setup follow the [implemented router documentation](https://github.com/drasi-project/drasi-platform/tree/cb959f665dbb190143161964df69edeb7013712e/reactions/dapr/agent-router).
