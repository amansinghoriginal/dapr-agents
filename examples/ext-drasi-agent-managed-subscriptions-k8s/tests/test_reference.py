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
from settings import MODEL_ENV_KEYS, ModelSettings


def model_environment() -> dict[str, str]:
    return {
        "LLM_PROVIDER": "azure",
        "LLM_CHAT_URL": "https://example.openai.azure.com/openai/v1/",
        "LLM_API_KEY": "synthetic-test-key",
        "LLM_MODEL": "test-deployment",
    }


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
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cluster, "OWNERSHIP", tmp_path / "missing.json")
    monkeypatch.setattr(cluster, "require_tools", Mock())
    commands = Mock()
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(cluster.DemoError, match="ownership record"):
        cluster.cleanup()
    commands.assert_not_called()


def test_foreign_cluster_label_never_invokes_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "owner.json"
    path.write_text(
        cluster.Ownership(cluster=cluster.CLUSTER_NAME, owner=uuid4()).model_dump_json()
    )
    monkeypatch.setattr(cluster, "OWNERSHIP", path)
    monkeypatch.setattr(cluster, "require_tools", Mock())
    commands = Mock(return_value=json.dumps({cluster.OWNER_LABEL: str(uuid4())}))
    monkeypatch.setattr(cluster, "run", commands)
    with pytest.raises(cluster.DemoError, match="does not match"):
        cluster.cleanup()
    assert commands.call_count == 1
    assert commands.call_args.args[0][:2] == ["docker", "inspect"]


def test_cleanup_preserves_builds_and_other_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner_file = tmp_path / "owner.json"
    owner_file.write_text("owned")
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("private credentials")
    private_home = tmp_path / "drasi-home"
    private_home.mkdir()
    (private_home / "registration").write_text("private registration")
    retained = tmp_path / "agent-image.json"
    retained.write_text("keep built image metadata")
    monkeypatch.setattr(cluster, "RUNTIME", tmp_path)
    monkeypatch.setattr(cluster, "OWNERSHIP", owner_file)
    monkeypatch.setattr(cluster, "KUBECONFIG", kubeconfig)
    monkeypatch.setattr(cluster, "require_tools", Mock())
    monkeypatch.setattr(cluster, "check_ownership", Mock())
    commands = Mock()
    monkeypatch.setattr(cluster, "run", commands)
    cluster.cleanup()
    commands.assert_called_once()
    assert commands.call_args.args[0] == [
        "k3d",
        "cluster",
        "delete",
        cluster.CLUSTER_NAME,
    ]
    assert commands.call_args.kwargs["env"]["KUBECONFIG"] == str(kubeconfig)
    assert not owner_file.exists()
    assert not kubeconfig.exists()
    assert not private_home.exists()
    assert retained.read_text() == "keep built image metadata"


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
    assert "subscribe_" not in MONITORING_TASK
    assert "checkout-server-errors" not in MONITORING_TASK
    assert "checkout-rollout-status" not in MONITORING_TASK


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
