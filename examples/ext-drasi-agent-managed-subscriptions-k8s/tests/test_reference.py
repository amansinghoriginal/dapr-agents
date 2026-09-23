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

import json
import subprocess
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import httpx
import psycopg
import pytest
import yaml
from pydantic import ValidationError
from redis import ResponseError

from dapr_agents.types import ToolResult

import actions
import cluster
import demo as demo_module
import multi_agent_demo
from actions import AssessmentError, AssessmentInput, AssessmentRecord
from demo import (
    Demo,
    MONITORING_TASK,
    cloud_event_data,
    consumer_group,
    packed_contains,
    stream_id,
    wait_for,
)
from settings import AGENT_ENV_KEYS, MODEL_ENV_KEYS, AgentSettings, ModelSettings


def model_environment() -> dict[str, str]:
    return {
        "LLM_PROVIDER": "azure",
        "LLM_CHAT_URL": "https://example.openai.azure.com/openai/v1/",
        "LLM_API_KEY": "synthetic-test-key",
        "LLM_MODEL": "test-deployment",
    }


@pytest.fixture
def owned_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> cluster.Ownership:
    monkeypatch.setattr(cluster, "RUNTIME", tmp_path)
    monkeypatch.setattr(cluster, "OWNERSHIP", tmp_path / "ownership.json")
    monkeypatch.setattr(cluster, "KUBECONFIG", tmp_path / "kubeconfig.yaml")
    monkeypatch.setattr(cluster, "require_tools", Mock())
    owner = cluster.Ownership(cluster=cluster.CLUSTER_NAME, owner=uuid4())
    cluster.OWNERSHIP.write_text(owner.model_dump_json())
    cluster.KUBECONFIG.write_text("private credentials")
    home = tmp_path / "drasi-home"
    home.mkdir()
    (home / "registration").write_text("private registration")
    return owner


def test_default_agent_settings_preserve_the_single_agent_walkthrough() -> None:
    settings = AgentSettings.from_env({})
    assert settings.name == "CheckoutSRE"
    assert settings.namespace == "drasi-m2-demo"
    assert settings.router_id == "drasi-system/sre-router-reaction"
    assert settings.pubsub_name == "agent-pubsub"
    assert settings.state_store_name == "agent-state"
    assert settings.request_topic == "checkout-sre.requests"
    assert settings.broadcast_topic == "checkout-sre.broadcast"
    assert settings.assessment_service == "checkout"


def test_agent_settings_support_distinct_application_personas() -> None:
    environment = {
        "AGENT_NAME": "ReleaseGuardian",
        "AGENT_ROLE": "Release guardian",
        "AGENT_GOAL": "Assess rollout transitions.",
        "AGENT_NAMESPACE": "applications",
        "DRASI_ROUTER_ID": "drasi-system/router-reaction",
        "AGENT_PUBSUB_NAME": "application-pubsub",
        "AGENT_STATE_STORE_NAME": "application-state",
        "AGENT_REQUEST_TOPIC": "release.requests",
        "AGENT_BROADCAST_TOPIC": "release.broadcast",
        "ASSESSMENT_SERVICE": "checkout-release",
    }
    settings = AgentSettings.from_env(environment)
    assert settings.name == "ReleaseGuardian"
    assert settings.role == "Release guardian"
    assert settings.goal == "Assess rollout transitions."
    assert settings.namespace == "applications"
    assert settings.router_id == "drasi-system/router-reaction"
    assert settings.pubsub_name == "application-pubsub"
    assert settings.state_store_name == "application-state"
    assert settings.request_topic == "release.requests"
    assert settings.broadcast_topic == "release.broadcast"
    assert settings.assessment_service == "checkout-release"


@pytest.mark.parametrize("key", AGENT_ENV_KEYS)
def test_empty_agent_setting_does_not_fall_back_to_default(key: str) -> None:
    with pytest.raises(ValidationError):
        AgentSettings.from_env({key: ""})


@pytest.mark.parametrize("suffix", ["", "responses", "chat/completions"])
def test_model_endpoint_normalization(suffix: str) -> None:
    environment = model_environment()
    environment["LLM_CHAT_URL"] += suffix
    settings = ModelSettings.from_env(environment)
    assert settings.base_url == model_environment()["LLM_CHAT_URL"]
    assert settings.secret_data() == model_environment()
    assert environment["LLM_API_KEY"] not in repr(settings)
    assert environment["LLM_CHAT_URL"] not in repr(settings)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.invalid/v1",
        "https://example.invalid/v1?api-key=synthetic-test-key",
        "https://synthetic-test-key@example.invalid/v1",
        "https://example.invalid/v1#fragment",
        "https://example.invalid/not-an-api",
    ],
)
def test_invalid_model_urls_fail_without_secret_values(url: str) -> None:
    environment = model_environment()
    environment["LLM_CHAT_URL"] = url
    with pytest.raises(ValidationError) as error:
        ModelSettings.from_env(environment)
    assert "synthetic-test-key" not in str(error.value)
    assert url not in str(error.value)


def test_missing_configuration_is_not_replaced_by_a_default_model() -> None:
    with pytest.raises(ValueError, match="LLM_MODEL"):
        ModelSettings.from_env({**model_environment(), "LLM_MODEL": ""})


def test_only_model_settings_are_copied_from_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in MODEL_ENV_KEYS:
        monkeypatch.delenv(name, raising=False)
    path = tmp_path / ".env"
    values = {**model_environment(), "UNRELATED_SECRET": "do-not-copy"}
    path.write_text("\n".join(f"{key}={value}" for key, value in values.items()))
    settings = cluster.load_model_settings(path)
    assert set(settings.secret_data()) == set(MODEL_ENV_KEYS)
    assert "do-not-copy" not in settings.secret_data().values()


@pytest.mark.parametrize("key", MODEL_ENV_KEYS)
def test_empty_environment_values_do_not_fall_back_to_file_settings(
    key: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in MODEL_ENV_KEYS:
        monkeypatch.delenv(name, raising=False)
    path = tmp_path / ".env"
    path.write_text(
        "\n".join(f"{name}={value}" for name, value in model_environment().items())
    )
    monkeypatch.setenv(key, "")
    with pytest.raises(ValueError, match=key):
        cluster.load_model_settings(path)


def test_nonempty_environment_values_override_file_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in MODEL_ENV_KEYS:
        monkeypatch.delenv(name, raising=False)
    path = tmp_path / ".env"
    path.write_text(
        "\n".join(f"{name}={value}" for name, value in model_environment().items())
    )
    monkeypatch.setenv("LLM_MODEL", "environment-deployment")
    assert cluster.load_model_settings(path).model == "environment-deployment"


def test_action_arguments_cannot_choose_identity_or_destination() -> None:
    assert set(AssessmentInput.model_fields) == {"status", "summary"}
    for extra in ("service", "idempotency_key", "database", "sql"):
        with pytest.raises(ValidationError):
            AssessmentInput.model_validate(
                {"status": "healthy", "summary": "Recovered", extra: "model-chosen"}
            )


def test_action_uses_the_bound_service(monkeypatch: pytest.MonkeyPatch) -> None:
    result = AssessmentRecord(
        service="checkout",
        status="healthy",
        summary="Synthetic recovery",
        updated_at=datetime.now(timezone.utc),
    )
    save = Mock(return_value=result)
    monkeypatch.setattr(actions, "save_assessment", save)
    tool = actions.make_assessment_tool("checkout")
    returned = tool.func(status="healthy", summary="Synthetic recovery")
    save.assert_called_once_with(
        "checkout", AssessmentInput(status="healthy", summary="Synthetic recovery")
    )
    assert returned == ToolResult.success(
        result.model_dump(mode="json"),
        text="Updated the single assessment record for checkout.",
    )


def test_database_failure_is_not_reported_as_a_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = Mock(side_effect=psycopg.OperationalError("synthetic internal detail"))
    monkeypatch.setattr(actions.psycopg, "connect", connection)
    with pytest.raises(AssessmentError) as error:
        actions.save_assessment(
            "checkout", AssessmentInput(status="healthy", summary="Recovered")
        )
    assert "synthetic internal detail" not in str(error.value)
    tool = actions.make_assessment_tool("checkout")
    assert tool.func(status="healthy", summary="Recovered") == ToolResult.error(
        "The assessment database did not confirm the write."
    )


def test_missing_owned_cluster_never_invokes_deletion(
    owned_runtime: cluster.Ownership, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster.OWNERSHIP.unlink()
    commands = Mock()
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(cluster.DemoError, match="ownership record"):
        cluster.cleanup()
    commands.assert_not_called()


@pytest.mark.parametrize("labels", [None, {}, {cluster.OWNER_LABEL: "other-owner"}])
def test_foreign_or_null_cluster_labels_never_invoke_deletion(
    labels: dict[str, str] | None,
    owned_runtime: cluster.Ownership,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cluster, "cluster_present", Mock(return_value=True))
    commands = Mock(return_value=json.dumps(labels))
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(cluster.DemoError, match="does not match"):
        cluster.cleanup()
    assert commands.call_count == 1
    assert commands.call_args.args[0][:2] == ["docker", "inspect"]
    assert cluster.OWNERSHIP.exists()
    assert cluster.KUBECONFIG.exists()


def test_cleanup_preserves_builds_and_other_files(
    owned_runtime: cluster.Ownership,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_home = tmp_path / "drasi-home"
    retained = tmp_path / "agent-image.json"
    retained.write_text("keep built image metadata")
    monkeypatch.setattr(cluster, "cluster_present", Mock(return_value=True))
    commands = Mock(
        side_effect=[json.dumps({cluster.OWNER_LABEL: str(owned_runtime.owner)}), ""]
    )
    monkeypatch.setattr(cluster, "run", commands)
    cluster.cleanup()
    assert commands.call_count == 2
    assert commands.call_args_list[0].args[0][:2] == ["docker", "inspect"]
    assert commands.call_args.args[0] == [
        "k3d",
        "cluster",
        "delete",
        cluster.CLUSTER_NAME,
    ]
    assert commands.call_args.kwargs["env"]["KUBECONFIG"] == str(cluster.KUBECONFIG)
    assert not cluster.OWNERSHIP.exists()
    assert not cluster.KUBECONFIG.exists()
    assert not private_home.exists()
    assert retained.read_text() == "keep built image metadata"


def test_cleanup_removes_orphaned_state_only_after_confirming_absence(
    owned_runtime: cluster.Ownership, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Mock(side_effect=["[]", ""])
    monkeypatch.setattr(cluster, "run", commands)
    cluster.cleanup()
    assert commands.call_count == 2
    assert commands.call_args_list[0].args[0][:3] == ["k3d", "cluster", "list"]
    assert commands.call_args_list[1].args[0][:3] == ["docker", "container", "ls"]
    assert not cluster.OWNERSHIP.exists()
    assert not cluster.KUBECONFIG.exists()
    assert not (cluster.RUNTIME / "drasi-home").exists()


def test_failed_create_can_be_recovered_through_guarded_cleanup(
    owned_runtime: cluster.Ownership, monkeypatch: pytest.MonkeyPatch
) -> None:
    cluster.OWNERSHIP.unlink()
    monkeypatch.setattr(cluster, "build", Mock())
    monkeypatch.setattr(
        cluster,
        "load_model_settings",
        Mock(return_value=ModelSettings.from_env(model_environment())),
    )
    failure = subprocess.CalledProcessError(1, ["k3d", "cluster", "create"])
    commands = Mock(side_effect=["[]", "", failure])
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(subprocess.CalledProcessError):
        cluster.setup(cluster.RUNTIME / ".env")
    assert cluster.OWNERSHIP.exists()
    assert commands.call_args.args[0][:3] == ["k3d", "cluster", "create"]
    assert commands.call_args.kwargs["env"]["KUBECONFIG"] == str(cluster.KUBECONFIG)

    commands.reset_mock(side_effect=True)
    commands.side_effect = ["[]", ""]
    cluster.cleanup()
    assert not cluster.OWNERSHIP.exists()


@pytest.mark.parametrize("failed_probe", ["k3d", "docker"])
def test_failed_inventory_does_not_discard_ownership(
    failed_probe: str,
    owned_runtime: cluster.Ownership,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure = subprocess.CalledProcessError(1, [failed_probe])
    commands = Mock(side_effect=[failure] if failed_probe == "k3d" else ["[]", failure])
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(subprocess.CalledProcessError):
        cluster.cleanup()
    assert cluster.OWNERSHIP.exists()
    assert cluster.KUBECONFIG.exists()
    assert (cluster.RUNTIME / "drasi-home" / "registration").exists()


def test_unowned_colliding_node_is_not_treated_as_an_absent_cluster(
    owned_runtime: cluster.Ownership, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Mock(side_effect=["[]", "container-id\n", "null"])
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(cluster.DemoError, match="does not match"):
        cluster.cleanup()
    assert commands.call_count == 3
    assert cluster.OWNERSHIP.exists()


def test_partial_cluster_without_an_owned_server_is_preserved(
    owned_runtime: cluster.Ownership, monkeypatch: pytest.MonkeyPatch
) -> None:
    commands = Mock(
        side_effect=[
            json.dumps([{"name": cluster.CLUSTER_NAME}]),
            subprocess.CalledProcessError(1, ["docker", "inspect"]),
        ]
    )
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(subprocess.CalledProcessError):
        cluster.cleanup()
    assert commands.call_count == 2
    assert cluster.OWNERSHIP.exists()


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("drasi-home", "symlink"),
        ("drasi-home", "file"),
        ("kubeconfig.yaml", "symlink"),
        ("kubeconfig.yaml", "directory"),
        ("ownership.json", "symlink"),
        ("ownership.json", "directory"),
    ],
)
def test_unsafe_runtime_paths_fail_before_cluster_deletion(
    name: str,
    kind: str,
    owned_runtime: cluster.Ownership,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = cluster.RUNTIME / name
    target = cluster.RUNTIME / "untouched"
    target.write_text("keep this file")
    if path.is_dir():
        (path / "registration").unlink()
        path.rmdir()
    else:
        path.unlink()
    if kind == "symlink":
        path.symlink_to(target)
    elif kind == "file":
        path.write_text("not a directory")
    else:
        path.mkdir()
    commands = Mock()
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(cluster.DemoError, match="Expected a"):
        cluster.cleanup()
    commands.assert_not_called()
    assert target.read_text() == "keep this file"


def test_failed_cluster_deletion_preserves_local_recovery_state(
    owned_runtime: cluster.Ownership, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cluster, "cluster_present", Mock(return_value=True))
    commands = Mock(
        side_effect=[
            json.dumps({cluster.OWNER_LABEL: str(owned_runtime.owner)}),
            subprocess.CalledProcessError(1, ["k3d", "cluster", "delete"]),
        ]
    )
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(subprocess.CalledProcessError):
        cluster.cleanup()
    assert cluster.OWNERSHIP.exists()
    assert cluster.KUBECONFIG.exists()
    assert (cluster.RUNTIME / "drasi-home" / "registration").exists()


def test_platform_manifest_pins_preserve_managed_infrastructure() -> None:
    documents = [
        {
            "kind": "StatefulSet",
            "metadata": {"name": name},
            "spec": {"template": {"spec": {"containers": [{"image": "original"}]}}},
        }
        for name in cluster.PLATFORM_INFRASTRUCTURE_IMAGES
    ]
    cluster.pin_platform_manifests(documents)
    for document in documents:
        name = document["metadata"]["name"]
        assert document["metadata"]["namespace"] == "drasi-system"
        assert (
            document["spec"]["template"]["spec"]["containers"][0]["image"]
            == (cluster.PLATFORM_INFRASTRUCTURE_IMAGES[name])
        )
    with pytest.raises(cluster.DemoError, match="expected infrastructure"):
        cluster.pin_platform_manifests([])


def test_mixed_namespace_apply_keeps_only_the_owned_cluster_context() -> None:
    command = cluster.kubectl_command("apply", "-f", "components.yaml", namespace=None)
    assert "--namespace" not in command
    assert command[command.index("--context") + 1] == f"k3d-{cluster.CLUSTER_NAME}"
    assert command[command.index("--kubeconfig") + 1] == str(cluster.KUBECONFIG)
    scoped = cluster.kubectl_command("get", "pods")
    assert scoped[scoped.index("--namespace") + 1] == "drasi-m2-demo"


def test_duplicate_rules_cannot_satisfy_expected_subscription_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rule = demo_module.Rule(
        query_id="checkout-server-errors",
        subscriber=demo_module.SUBSCRIBER,
        operations=[demo_module.Operation("i")],
        subscription_incarnation="test-incarnation",
        topic_name=demo_module.INBOX,
    )
    snapshot = demo_module.RulesSnapshot(
        router_id=demo_module.ROUTER_ID,
        view="routing_snapshot",
        rules=[rule, rule],
    )
    demo = Demo(Mock(), Mock(), Mock(), Mock())
    monkeypatch.setattr(demo, "subscriptions", Mock(return_value=snapshot))
    monkeypatch.setattr(demo_module, "wait_for", partial(wait_for, timeout=0))
    with pytest.raises(cluster.DemoError, match="Timed out"):
        demo.expect_subscriptions({"checkout-server-errors": {"i"}})


def test_example_uses_real_router_and_separate_namespace_local_broker() -> None:
    example = Path(__file__).resolve().parent.parent
    router = yaml.safe_load((example / "drasi/router.yaml").read_text())
    assert router["spec"]["kind"] == "DaprAgentRouter"
    assert len(router["spec"]["queries"]) == 2
    components = list(
        yaml.safe_load_all((example / "kubernetes/components.yaml").read_text())
    )
    buses = [
        item
        for item in components
        if item["kind"] == "Component" and item["spec"]["type"] == "pubsub.redis"
    ]
    assert {item["metadata"]["namespace"] for item in buses} == {
        "drasi-system",
        "drasi-m2-demo",
    }
    assert len({item["spec"]["metadata"][0]["value"] for item in buses}) == 1
    assert all(
        "agent-redis.drasi-m2-demo" in item["spec"]["metadata"][0]["value"]
        for item in buses
    )
    assert not any(
        item["metadata"]["name"].startswith("drasi-statestore") for item in components
    )
    expected_agent_scopes = {"checkout-sre"}
    application_resources = [
        item
        for item in components
        if item["metadata"]["namespace"] == "drasi-m2-demo"
        and item["metadata"]["name"]
        in {"agent-pubsub", "agent-state", "sre-agent-retries"}
    ]
    assert len(application_resources) == 3
    assert all(
        set(item["scopes"]) == expected_agent_scopes for item in application_resources
    )
    assert "subscribe_" not in MONITORING_TASK
    assert "checkout-server-errors" not in MONITORING_TASK
    assert "checkout-rollout-status" not in MONITORING_TASK
    assert "independent actions" in MONITORING_TASK
    assert "either order is acceptable" in MONITORING_TASK


def test_multi_agent_setup_expands_scopes_before_applications_start() -> None:
    default_documents = cluster.application_component_documents(multi_agent=False)
    multi_documents = cluster.application_component_documents(multi_agent=True)
    names = {"agent-pubsub", "agent-state", "sre-agent-retries"}

    def scopes(documents: list[dict[str, object]]) -> dict[str, set[str]]:
        return {
            str(document["metadata"]["name"]): set(document["scopes"])
            for document in documents
            if document["metadata"]["namespace"] == "drasi-m2-demo"
            and document["metadata"]["name"] in names
        }

    assert scopes(default_documents) == {name: {"checkout-sre"} for name in names}
    assert scopes(multi_documents) == {
        name: {"checkout-sre", *multi_agent_demo.OPTIONAL_AGENT_APP_IDS}
        for name in names
    }


def test_multi_agent_walkthrough_uses_distinct_identities_and_natural_tasks() -> None:
    agents = multi_agent_demo.AGENTS
    assert tuple(spec.app_id for spec in agents[1:]) == (
        multi_agent_demo.OPTIONAL_AGENT_APP_IDS
    )
    assert len({spec.app_id for spec in agents}) == len(agents)
    assert len({spec.agent_name for spec in agents}) == len(agents)
    assert len({spec.service for spec in agents}) == len(agents)
    assert len({spec.inbox for spec in agents}) == len(agents)
    tasks = (
        multi_agent_demo.INCIDENT_TASK,
        multi_agent_demo.RELEASE_TASK,
        multi_agent_demo.AUDITOR_TASK,
        multi_agent_demo.SECURITY_TASK,
    )
    for task in tasks:
        assert "subscribe_" not in task
        assert "checkout-server-errors" not in task
        assert "checkout-rollout-status" not in task


def test_multi_agent_deployment_binds_each_persona_to_its_app() -> None:
    spec = multi_agent_demo.RELEASE
    manifest = multi_agent_demo.deployment(spec, "example/image:test")
    template = manifest["spec"]["template"]
    assert template["metadata"]["annotations"]["dapr.io/app-id"] == spec.app_id
    container = template["spec"]["containers"][0]
    assert container["image"] == "example/image:test"
    environment = {
        item["name"]: item["value"] for item in container["env"] if "value" in item
    }
    assert environment["AGENT_NAME"] == spec.agent_name
    assert environment["AGENT_ROLE"] == spec.role
    assert environment["AGENT_GOAL"] == spec.goal
    assert environment["ASSESSMENT_SERVICE"] == spec.service
    assert environment["AGENT_REQUEST_TOPIC"] == f"{spec.app_id}.requests"
    assert environment["AGENT_BROADCAST_TOPIC"] == f"{spec.app_id}.broadcast"


def test_cloud_event_and_packed_marker_inspection() -> None:
    data = {
        "updatedResults": [{"before": {"marker": "old"}, "after": {"marker": "new"}}]
    }
    for representation in (data, json.dumps(data)):
        assert cloud_event_data(json.dumps({"data": representation})) == data
    assert packed_contains(data, "u", "new")
    assert packed_contains(data, "u", "old")
    assert not packed_contains(data, "i", "new")
    assert stream_id("100-10") > stream_id("100-2")
    with pytest.raises(cluster.DemoError):
        cloud_event_data('{"data":null}')


def test_missing_stream_is_distinct_from_broker_failure() -> None:
    broker = Mock()
    broker.xinfo_groups.side_effect = ResponseError("no such key")
    assert consumer_group(broker, "inbox", "reader") is None
    broker.xinfo_groups.side_effect = ResponseError("NOAUTH Authentication required")
    with pytest.raises(ResponseError, match="NOAUTH"):
        consumer_group(broker, "inbox", "reader")


@pytest.mark.parametrize("failed_broker", ["internal_broker", "agent_broker"])
def test_dead_lettered_input_is_not_reported_as_filtered(
    failed_broker: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    internal = Mock()
    internal.xlen.return_value = 0
    agent = Mock()
    agent.xlen.return_value = 0
    demo = Demo(Mock(), Mock(), internal, agent)
    getattr(demo, failed_broker).xlen.return_value = 1
    monkeypatch.setattr(demo, "wait_source_ack", Mock())
    monkeypatch.setattr(demo, "deliveries", Mock(return_value=[]))
    with pytest.raises(cluster.DemoError, match="dead-letter stream"):
        demo.expect_filtered("checkout-server-errors", "i", "marker")


def test_polling_times_out_instead_of_claiming_success() -> None:
    with pytest.raises(cluster.DemoError, match="Timed out"):
        wait_for("an impossible condition", lambda: False, bool, timeout=0)


def test_task_waits_for_completed_workflow_and_surfaces_failure() -> None:
    status = "COMPLETED"

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            assert json.loads(request.content) == {"task": "Monitor service errors"}
            return httpx.Response(200, json={"instance_id": "normal-task"})
        return httpx.Response(
            200, json={"instance_id": "normal-task", "runtime_status": status}
        )

    with httpx.Client(
        base_url="http://reference", transport=httpx.MockTransport(handle)
    ) as client:
        demo = Demo(client, client, Mock(), Mock())
        assert demo.task("Monitor service errors") == "normal-task"
        status = "FAILED"
        with pytest.raises(cluster.DemoError, match="did not complete"):
            demo.task("Monitor service errors")
