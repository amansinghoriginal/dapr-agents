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

# dapr-agents-ext-drasi

[Drasi](https://drasi.io/) extension for Dapr Agents, enabling resilient, scalable, and business event-driven AI agents through Drasi's change notification capabilities.

## Features

- Directly trigger agents from Drasi change events via Dapr pub/sub

## Getting Started

### Prerequisites

This extension is installed as an optional dependency on the core `dapr-agents` package; see the `Getting Started` section in the [root README](../../README.md) for a list of prerequisites.

### Installation

```bash
uv add dapr-agents[drasi]
```

Installing this source version requires Git and access to the public Drasi Platform repository. The extension pins the [shared router contract package](https://github.com/drasi-project/drasi-platform/tree/49e8df694a16520519a1ba559f99c1a13a668431/typespec/dapr-agent-router/python) to an immutable revision; a local Platform checkout and schema-generation tools are not required. This dependency is not installed by the core framework unless the Drasi extension is selected.

### Public API

```python
from dapr_agents.ext.drasi import (
    register_drasi_trigger,         # Register author-configured Drasi query triggers
    DrasiChangeEvent,               # Type for Drasi change events
    DrasiOperation,                 # Operation type for Drasi change events
)
```

`register_drasi_trigger()` replaces `drasi_trigger()` without an old-name alias. It preserves the existing unpacked change-event format and task-mapping behavior. The future agent-managed entry point, `enable_drasi_subscriptions()`, is not yet exported.

### Usage

Register a Drasi query subscription on an agent before hosting. Call the helper several times to register different fixed queries on the same agent. Static triggers and agent-managed subscriptions cannot be combined on one agent.

```python
agent = DurableAgent(...)

register_drasi_trigger(
    agent,
    query_id="<YOUR_DRASI_QUERY_ID>",
    task_mapper=lambda event, ctx: TriggerAction(task="<AGENT_TASK_MESSAGE>")
)

runner = AgentRunner()
try:
    runner.subscribe(agent)
    await wait_for_shutdown()
finally:
    runner.shutdown(agent)
```

### Examples
- [Drasi Change-Driven Agents on Kubernetes](../../examples/ext-drasi-change-driven-agents-k8s/README.md) — demonstrates how to subscribe an agent to Drasi queries in a Kubernetes environment.

## Development

### Install extension in editable mode

From the project root:

```bash
uv venv
source .venv/bin/activate
uv sync --active --group dev --group test --extra drasi
```

### Run extension tests

```bash
uv run --group test --extra drasi pytest ext/dapr-agents-ext-drasi -m "not integration" -v
```

### Extension code quality

See the `Code Quality` section in the [development README](../../docs/development/README.md) for code quality commands.

### Agent-managed internal contracts

`_models.py` and `_interfaces.py` define the private F2 baseline for independent implementation lanes. They do not implement agent-managed subscriptions or export `enable_drasi_subscriptions()`. Changes to these shared contracts and fixtures belong in one coordinated foundation follow-up, rather than conflicting edits in individual lanes.

#### Configuration and ownership

`SubscriptionScope(router_id, namespace, app_id, agent_name)` is immutable and preserves exact identity strings. It validates identities through the shared router package and returns fresh shared `Subscriber` models. Its `inbox_topic` and `dead_letter_topic` properties use the shared identity helpers. Queries, operation filters, and incarnations do not change those topics.

`ResolvedDrasiConfig` contains that scope, `pubsub_name`, `state_store_name`, the already-registered `workflow_name`, and `dapr_http_port`. All values are resolved before construction; this model does not read environment variables, discover resources, or create clients. Its `router_mcp_url` addresses the configured router through the local Dapr sidecar.

All domain methods are synchronous and return completed results, not tasks, futures, or enqueue acknowledgements. Blocking state/network I/O belongs in preparation, a subscription worker, or an ordinary tool activity. Async callers must offload blocking calls rather than block their application event loop. Deterministic workflow bodies and replayed hooks must not perform this I/O. R1 owns adaptation to asynchronous MCP machinery, including session/task/loop affinity; this foundation supplies no threading framework and does not assume a session can move between arbitrary event loops.

I1 owns preparation and resource lifetime: prepare the router catalog, initialize/load intent, reconcile the manager, attach tools, and only then admit work. It closes partially prepared resources on failure and closes the inbox and router client on shutdown. Tools and the manager borrow their dependencies; the agent continues to own its configured state-store primitive. `RouterClient.close()` is idempotent and releases runtime resources only. Shutdown never removes intent or router rules.

#### Component signatures

The complete signatures are in `_interfaces.py`. `Operation` below is imported from the shared contract package, not the static trigger's separate operation enum.

| Boundary | Arguments and result | Semantics |
|---|---|---|
| `RouterClient.list_queries()` | Returns shared `ListQueriesResponse` | Prepare/cache the complete startup catalog for `scope`; an empty catalog is valid, a failed fetch is not an empty catalog. |
| `RouterClient.subscribe(request)` | Shared `SubscribeRequest` -> `SubscribeResponse` | A validated confirmation of the query, subscriber scope, incarnation, effective operation set, and derived topic. Both `created` and `updated` succeed. |
| `RouterClient.unsubscribe(request)` | Shared `UnsubscribeRequest` -> `UnsubscribeResponse` | A validated confirmation for the request; both `removed=True` and `removed=False` confirm absence. Retired queries remain removable. |
| `RouterClient.close()` | Returns `None` | I1 owns cleanup, including after failed preparation. Calls after close are invalid lifecycle usage, not successful no-ops. |
| `IntentReader.get(query_id: str)` | Returns `SubscriptionIntent` or `None` | An owned local snapshot; `None` means genuinely absent intent. Reading may perform persistent I/O. |
| `IntentRepository.load()` | Returns `IntentSnapshot` or `None` | The whole scoped document and its ETag, including queries absent from the current catalog. `None` means the document is absent. |
| `IntentRepository.initialize(document)` | `IntentDocument` -> `None` | Unconditional initialization only during single-owner preparation, after checking absence and before admitting work. Not atomic create-if-absent. |
| `IntentRepository.save(document, *, expected_etag: str)` | Returns `None` | Conditional replacement of the whole document. Reload to obtain its next ETag; the underlying state primitive does not return one from save. |
| `SubscriptionManager.subscribe(query_id: str, *, operations: tuple[Operation, ...], instructions: str)` | Returns persisted, active `SubscriptionIntent` | New subscription or same-query update. Require a non-empty unique operation subset and non-blank, self-contained instructions. |
| `SubscriptionManager.unsubscribe(query_id: str)` | Returns `None` | Success only after router absence is confirmed and local intent is removed; already absent local intent is a successful no-op. |
| `SubscriptionManager.list_subscriptions()` | Returns `tuple[SubscriptionIntent, ...]` | Local intent, filters, instructions and status, not a claim about live router health. |
| `SubscriptionManager.reconcile(catalog)` | Shared `ListQueriesResponse` -> `None` | Preparation-time reconciliation; retain an owned catalog copy for later commands. No periodic refresh or startup model turn. |
| `AdmissionHandler.admit(data: object)` | Returns `AdmissionResult` | Decoded M2 CloudEvent **data**, not the outer CloudEvent; no scheduling or acknowledgement occurs here. |

The gateway consumes `parse`, `parse_catalog`, and `to_wire` from `drasi_agent_router_contracts`; generated Pydantic validation alone is insufficient. It inspects MCP `isError` before success parsing and treats operation arrays as sets. It never receives handling instructions. The current catalog contains `protocol_version`, `router_id`, and `queries`; there is no capability-negotiation or separate delivery-version field.

`router_client.MCPRouterClient(config, timeout_seconds=30.0)` implements that private gateway. Preparation calls `list_queries()` to validate and cache the catalog; mutations also establish the configured router identity before their first write if preparation has not already run. Each MCP exchange has a deadline and creates, initializes, and closes a stateless HTTP session on the client's worker thread. The SDK's typed request API leaves result validation to the shared contract so both structured and JSON-text results work. Calls are serialized with `close()`, and shutdown waits for the current exchange without removing router rules. The client does not retry mutations; reconciliation and retry decisions remain with the subscription manager.

The client honors the Dapr SDK's `DAPR_API_TOKEN` setting through the `dapr-api-token` header. Environment HTTP proxies are disabled because requests target the local sidecar. Tokens are not included in the client's URLs or diagnostic messages.

`RouterError.category` distinguishes `invalid_arguments`, `unknown_query`, `incarnation_conflict`, `state_unavailable`, `transport`, and `invalid_response`. The first three mean a rejected mutation. The latter three mean an uncertain mutation: a write may already have committed. A malformed or mismatched success response is not evidence of rejection. `mutation_outcome` makes this distinction explicit; it does not decide retry policy. For a catalog read, these categories describe retrieval failure, not a state mutation.

`IntentStoreError.category` distinguishes `unavailable`, `corrupt`, `unsupported_version`, and `conflict`. None may be converted into absent/empty state. An existing document without a usable ETag is a storage failure. `SubscriptionCommandError.category` distinguishes local `invalid_input`, `unknown_query`, `pending_operation`, and `unavailable` rejections. Manager commands may also propagate classified router/store errors. These errors carry safe categories, not raw responses, rows, or instructions; callers log operation context without logging sensitive payloads. Programming errors, cancellation, and invalid lifecycle usage are not converted into successful results.

#### Intent document and concurrency

`IntentDocument` has required `format_version=1`, a `SubscriptionScope`, and an `intents` mapping keyed by query ID. Each `SubscriptionIntent` contains its query ID, selected operations, handling instructions, historical full catalog snapshot, opaque non-empty incarnation, status, and optional `RouterOperationOutcome`. Record keys and catalog/router identities must agree. The format version is local persistence metadata, not a new router wire field.

`IntentSnapshot.etag` belongs to the **entire router/subscriber document**, never an individual query. A successful write to query A invalidates the token held by a simultaneous writer for query B. On conflict, S2 must reread the document, reevaluate its targeted transition, and merge only that query's intended change into the fresh document before conditionally saving again. It must not resubmit an unchanged stale document or overwrite unrelated queries. S1 must surface the conflict rather than silently perform that overwrite.

Initial creation is serialized by single-owner preparation. `initialize` is explicitly an unconditional write, not an `If-None-Match` operation and not a way around a failed CAS. Preparation reloads the created document to obtain an ETag before admitting work. Every subsequent update, including removal of a completed subscription, uses the whole-document ETag. An empty document can remain after the last unsubscribe. This design needs no state-store query API, per-query index, transaction framework, or distributed lease.

Statuses are `pending_subscribe`, `pending_update`, `active`, `pending_unsubscribe`, and `unavailable`. S2 persists pending intent before calling the router and reports success only after its final local commit. Updates and retries retain incarnation; unsubscribe followed by a new subscription changes it. A failed or uncertain mutation retains pending intent. A failed local write can also have committed, so recovery must reload rather than assume the previous state.

Startup reasserts active/pending subscriptions, completes pending unsubscriptions even when a query has left the catalog, and otherwise marks removed queries unavailable while retaining their historical metadata. Query reappearance does not automatically revive unavailable intent. Ordinary shutdown does not unsubscribe. The models validate data shapes; S2, not these models or the fakes, implements transition ordering and same-query serialization.

#### Durable intent storage

The private `intent_store.DaprIntentRepository(scope=..., store=...)` implements the repository boundary using the agent's configured `AgentStateConfig.store` (also exposed as `agent.state_store`). It borrows the existing raw Dapr state primitive and its configured factory, retaining the service's key prefix without applying its optional workflow model, local mirroring, or blanket retries. It does not change or close the configured service.

The storage key is `<service key prefix>drasi:intent:<scope.inbox_topic>`. The shared inbox identity includes the router, subscriber namespace, application, and exact agent name; queries live inside that document. Reads also validate the embedded scope. The format version remains in the document so an unsupported version cannot be mistaken for a new, absent key. The component must support ETags; nonempty data without a usable ETag fails as unavailable, and empty data with a nonempty ETag is corrupt. The native SDK represents absence with empty data and `etag=""`, not `None`. Its GetState response has no separate existence flag, so a backend with empty, untagged records cannot be distinguished from absence and is unsupported. Reads always consult storage and return detached snapshots, including pending and retired-query intent.

Initialization is unconditional and restricted by the caller to exclusive preparation. Subsequent saves pass the expected document ETag with first-write-wins and strong write consistency. A gRPC `ABORTED` conditional save is a conflict; other transport failures are unavailable, and may follow a committed write. Each write uses a fresh client from the configured primitive's factory and explicitly disables SDK retries on that client. Otherwise, a committed write with a lost reply could be retried with its stale ETag and incorrectly reported as a conflict. The configured factory, read retry policy, and runtime-side resiliency policies are unchanged. The adapter does not retry or repair writes: callers reload after success or uncertain failure and merge against fresh state after conflicts. It adds no TTL, automatic deletion, router calls, or catalog filtering.

Malformed JSON, invalid records, mismatched scope, and missing or incorrectly typed versions fail as corrupt; an unsupported integer format version fails separately. Error messages and logs contain safe categories rather than backend responses or stored instructions. S1 coverage is in `tests/test_intent_store.py` and exercises the real state wrappers with an injected SDK boundary.

#### Subscription commands and recovery

The private `subscription_manager.DrasiSubscriptionManager(repository=..., router=...)` borrows matching scoped dependencies. Preparation initializes the repository and calls `reconcile(catalog)` before admitting work. Reconciliation retains an owned startup catalog, finishes pending unsubscriptions first (including retired queries), reasserts active/pending subscriptions with their existing incarnations, and marks other retired intent unavailable without deleting its historical context. It fails explicitly if any operation fails, and subscribe commands remain unavailable until preparation completes. Listing and unsubscribe do not need a current catalog, so retained intent can still be inspected and removed.

Subscribe and update persist pending intent before the router call and report success only after the active state is committed. An identical active command is a local no-op, not a router-health check. An identical pending command resumes the existing lifecycle; conflicting commands are rejected while that operation is unresolved. Updates retain incarnation. Unsubscribe persists pending-unsubscribe before the router call and removes intent only after confirmed absence, including an already-absent router response. Router failures retain pending intent and record a safe rejected/uncertain outcome; local write failures remain visible and may require reloading after an uncertain commit.

An unavailable intent is never revived by catalog reappearance or a subscribe call. Subscribe returns an actionable error to unsubscribe first, then subscribe again. Unsubscribe uses the retained incarnation and the normal durable deletion flow; failed deletion retains pending-unsubscribe. Only confirmed router deletion or absence permits removing local intent. The subsequent subscribe persists a fresh incarnation, while ordinary active-subscription updates retain their incarnation. This adds no automatic cleanup, replacement state machine, or permanent tombstones.

A fresh incarnation rejects already-published inbox messages carrying the old incarnation. It does not fence query generations: an upstream packed event retried later can be routed through the new rule and stamped with the new incarnation. Continue using fresh query IDs for deleted/recreated queries or changed query semantics. Explicit unsubscribe/re-subscribe is subscription-lifecycle hygiene, not query-generation fencing.

The single-owner POC manager serializes mutating commands within its process; listing reads persisted snapshots without waiting for a router call. Whole-document ETag conflicts reload and merge only the targeted query, with at most ten write attempts. A changed same-query intent is never overwritten. The manager adds no router discovery, periodic refresh, startup model turn, resource cleanup, or shutdown unsubscribe; I1 owns composition and resource lifetimes. Its synchronous methods perform I/O and belong in preparation or ordinary tool activities, never deterministic workflow bodies or replayed hooks.

#### Snapshot and admission boundaries

Return values are detached caller-owned snapshots. Components deep-copy models they return and inputs they retain; callers may modify their own copies without changing stored or prepared state. The configuration's immutable scalar identity is separate from these mutable owned snapshots. There is no second recursively immutable copy of the router model hierarchy.

`SubscriptionIntent` parses an owned catalog through the shared package and serializes it with `to_wire`, preserving omitted optional fields such as `usage`. Do not replace that serialization with an ordinary nested `model_dump()` that introduces unsupported explicit nulls. Persist documents using their model serialization and validate on load; do not bypass shared wire validation.

| Condition after protocol parsing | D1 result |
|---|---|
| No intent, unavailable intent, pending unsubscribe, or stale incarnation | `Discard` |
| Matching pending subscribe/update | `Retry`, before evaluating the requested operation filter |
| Intent storage unavailable, corrupt, or unsupported | `Retry`, not absent intent |
| Active intent excludes the operation | `Discard` |
| Malformed/unsupported delivery or row, or wrong router | `Poison` |
| Valid active matching intent | `SchedulingInput(instance_id, event_id, task)` |

`SchedulingInput.task` is complete, owned text containing the admitted instructions, catalog context, operation, event identity and canonical event data. D1 delimits projected data as untrusted and derives a bounded deterministic instance ID from the router/subscriber scope, incarnation and canonical row event ID. Later intent changes cannot rewrite that text. No current time, model-generated token, or outer publication ID changes the agreed row identity.

D2 maps these results to transport responses and schedules the configured workflow. A `SchedulingInput` is **not** an acknowledgement: success follows scheduling acceptance or an explicit existing-active-instance conflict, not local enqueue or workflow completion. Arbitrary scheduler errors retry. Duplicate workflows after terminal ID reuse remain possible.

#### Shared fixtures and lane ownership

Extension-local `tests/conftest.py` and `tests/fakes.py` provide contract-backed catalogs/deliveries, pending-state examples, a scripted router, a scripted manager, and a document-level in-memory repository. Their purpose is to let T1, S2 and D1 exercise their own components without waiting for real dependencies. They are not a second subscription engine. Scripted outcomes must be explicit; storage errors are not empty results, stale ETags conflict, initialization is not create-only, and returned mutable values are detached.

| Owner | Production files and responsibilities |
|---|---|
| F2 / coordinated foundation follow-up | `_models.py`, `_interfaces.py`, shared fixtures/fakes and their contract coverage |
| R1 | `router_client.py`: MCP lifecycle, parsing, reply validation, async adaptation |
| S1 | `intent_store.py`: configured runtime-store adapter, document codec and conditional writes |
| S2 | `subscription_manager.py`: commands, serialization of transitions and reconciliation |
| T1 | `subscription_tools.py`: schemas/descriptions and delegation to the manager |
| D1 | `admission.py`, `task_builder.py`: persistent read, classification and detached event task |
| D2 | `delivery.py`: inbox/DLT lifecycle, transport dispositions and scheduling acceptance |
| I1 | Completed activation/composition, tool attachment, mode exclusion and public export |

Two known integration risks remain with their later owners. **I1:** `AgentRunner.run_stream()` bypasses activation attachment; supporting it needs an explicit preparation solution. **D2:** the generic filter/mapper path collapses some errors into non-matches/`DROP`, and its asynchronous path may acknowledge local enqueue. Neither core runner nor generic routing/scheduling behavior is changed by F2.

#### Generated subscription tools

The private `subscription_tools.build_subscription_tools(catalog, manager)` factory returns ordinary synchronous `AgentTool` objects without performing network/state I/O or changing an agent. Each cached query gets subscribe/unsubscribe tools with bounded, deterministic names containing a digest of the exact query ID. Subscribe accepts only explicit, distinct `i/u/d` operations and non-blank, self-contained handling instructions. `list_drasi_subscriptions()` is always present, including for an empty catalog, and reports local intent rather than live router state.

The tools borrow the F2 management interface and preserve classified command failures as error results. Their diagnostics exclude handling instructions and raw payloads. This is an extension-tool logging boundary, not an end-to-end redaction guarantee: existing core console output, debug logging, and tracing may include tool arguments and results. I1 owns attachment to the existing executor, collision checks against existing tools, and lifecycle preparation. The factory is not publicly re-exported and does not enable dynamic subscriptions by itself.

### Regenerate Drasi models

See the [provenance file](./PROVENANCE.md) for context.

```bash
./scripts/regen-drasi-models.sh
```