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

"""Build and provision only this example's explicitly owned local cluster."""

from __future__ import annotations

import argparse
import json
import logging
import os
import platform
import secrets
import shutil
import socket
import subprocess
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict

from settings import (
    APP_ID,
    MODEL_ENV_KEYS,
    NAMESPACE,
    OPTIONAL_AGENT_APP_IDS,
    ROUTER_APP_ID,
    ROUTER_NAMESPACE,
    ModelSettings,
)

logger = logging.getLogger(__name__)

EXAMPLE = Path(__file__).resolve().parent
REPOSITORY = EXAMPLE.parent.parent
RUNTIME = EXAMPLE / ".runtime"
KUBECONFIG = RUNTIME / "kubeconfig.yaml"
PLATFORM = RUNTIME / "drasi-platform"
PLATFORM_REVISION = "7e3f4f2aec6574ba1f487d910315e4265670ddc8"
PLATFORM_BUILD = "azure-linux"
PLATFORM_TAG_BASE = f"m2-{PLATFORM_REVISION[:12]}"
PLATFORM_TAG = f"{PLATFORM_TAG_BASE}-{PLATFORM_BUILD}"
DAPR_VERSION = "1.18.1"
K3S_IMAGE = "rancher/k3s:v1.32.5-k3s1"
CLUSTER_NAME = "drasi-agent-managed-reference"
OWNER_LABEL = "io.dapr-agents.drasi-m2-owner"
OWNERSHIP = RUNTIME / "ownership.json"
AGENT_IMAGE_RECORD = RUNTIME / "agent-image.json"
PLATFORM_INFRASTRUCTURE_IMAGES = {
    "drasi-mongo": (
        "ghcr.io/drasi-project/mongo@"
        "sha256:10182c9bee7868ed1a490b9734658130cdcf2453901edcbcb27f0be2817bd55a"
    ),
    "drasi-redis": (
        "ghcr.io/drasi-project/redis@"
        "sha256:3b73847e72874be07e6657b129a94761662b79bc0f679273757d4218573b2a98"
    ),
}
PLATFORM_COMPONENTS = {
    "control-planes/mgmt_api": "api",
    "control-planes/kubernetes_provider": "kubernetes-provider",
    "query-container/publish-api": "query-container-publish-api",
    "query-container/query-host": "query-container-query-host",
    "query-container/view-svc": "query-container-view-svc",
    "sources/shared/change-dispatcher": "source-change-dispatcher",
    "sources/shared/change-router": "source-change-router",
    "sources/shared/query-api": "source-query-api",
    "sources/relational/debezium-reactivator": "source-debezium-reactivator",
    "sources/relational/sql-proxy": "source-sql-proxy",
    "reactions/dapr/agent-router": "reaction-dapr-agent-router",
}


class DemoError(Exception):
    """An explicit reference-environment or demonstration failure."""


class Ownership(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cluster: str
    owner: UUID


def run(
    arguments: list[str],
    *,
    capture: bool = False,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 1200,
) -> str:
    result = subprocess.run(
        arguments,
        check=True,
        text=True,
        input=input_text,
        stdout=subprocess.PIPE if capture else None,
        env=env,
        timeout=timeout,
    )
    return result.stdout if capture else ""


def require_tools(*names: str) -> None:
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        raise DemoError(f"Missing prerequisites: {', '.join(missing)}. See README.md.")


def kubectl_command(*arguments: str, namespace: str | None = NAMESPACE) -> list[str]:
    command = [
        "kubectl",
        "--kubeconfig",
        str(KUBECONFIG),
        "--context",
        f"k3d-{CLUSTER_NAME}",
    ]
    if namespace is not None:
        command.extend(["--namespace", namespace])
    return [*command, *arguments]


def kubectl(
    *arguments: str,
    namespace: str | None = NAMESPACE,
    capture: bool = False,
    input_text: str | None = None,
) -> str:
    return run(
        kubectl_command(*arguments, namespace=namespace),
        capture=capture,
        input_text=input_text,
    )


def drasi(*arguments: str) -> None:
    home = RUNTIME / "drasi-home"
    home.mkdir(parents=True, exist_ok=True)
    run(
        [str(RUNTIME / "bin" / "drasi"), *arguments],
        env={**os.environ, "HOME": str(home), "KUBECONFIG": str(KUBECONFIG)},
    )


def load_model_settings(env_file: Path) -> ModelSettings:
    file_values = (
        dotenv_values(env_file, interpolate=False) if env_file.is_file() else {}
    )
    selected = {
        key: os.environ[key] if key in os.environ else file_values.get(key)
        for key in MODEL_ENV_KEYS
    }
    return ModelSettings.from_env(
        {key: value for key, value in selected.items() if value is not None}
    )


def load_ownership() -> Ownership:
    if not OWNERSHIP.is_file() or OWNERSHIP.is_symlink():
        raise DemoError(
            "No local ownership record; refusing to use or delete a cluster."
        )
    owner = Ownership.model_validate_json(OWNERSHIP.read_text())
    if owner.cluster != CLUSTER_NAME:
        raise DemoError("The local ownership record names a different cluster.")
    return owner


def validate_runtime_paths() -> None:
    for directory in (RUNTIME, RUNTIME / "drasi-home"):
        if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
            raise DemoError(
                f"Expected a real runtime directory, not a link or file: {directory}."
            )
    for file in (OWNERSHIP, KUBECONFIG):
        if file.is_symlink() or (file.exists() and not file.is_file()):
            raise DemoError(
                f"Expected a regular runtime file, not a link or directory: {file}."
            )


def cluster_present() -> bool:
    clusters = json.loads(
        run(["k3d", "cluster", "list", "--output", "json"], capture=True)
    )
    if any(item["name"] == CLUSTER_NAME for item in clusters):
        return True
    # Also reject colliding or partial nodes that k3d cannot identify as a cluster.
    return bool(
        run(
            [
                "docker",
                "container",
                "ls",
                "--all",
                "--filter",
                f"name=^/k3d-{CLUSTER_NAME}-(server-[0-9]+|agent-[0-9]+|serverlb)$",
                "--format",
                "{{.ID}}",
            ],
            capture=True,
        ).strip()
    )


def check_ownership() -> Ownership:
    owner = load_ownership()
    labels = json.loads(
        run(
            [
                "docker",
                "inspect",
                f"k3d-{CLUSTER_NAME}-server-0",
                "--format",
                "{{json .Config.Labels}}",
            ],
            capture=True,
        )
    )
    if not isinstance(labels, dict) or labels.get(OWNER_LABEL) != str(owner.owner):
        raise DemoError(
            "Cluster ownership does not match this checkout; refusing access."
        )
    return owner


def platform_images() -> list[str]:
    return [
        f"drasi-project/{name}:{PLATFORM_TAG}" for name in PLATFORM_COMPONENTS.values()
    ]


def build() -> None:
    require_tools("docker", "git", "make")
    RUNTIME.mkdir(parents=True, exist_ok=True)
    if not PLATFORM.exists():
        run(["git", "init", "--quiet", str(PLATFORM)])
        run(
            [
                "git",
                "-C",
                str(PLATFORM),
                "remote",
                "add",
                "origin",
                "https://github.com/drasi-project/drasi-platform.git",
            ]
        )
        run(
            [
                "git",
                "-C",
                str(PLATFORM),
                "fetch",
                "--quiet",
                "--depth=1",
                "origin",
                PLATFORM_REVISION,
            ]
        )
        run(["git", "-C", str(PLATFORM), "switch", "--quiet", "--detach", "FETCH_HEAD"])
    revision = run(
        ["git", "-C", str(PLATFORM), "rev-parse", "HEAD"], capture=True
    ).strip()
    changes = run(
        ["git", "-C", str(PLATFORM), "status", "--porcelain"], capture=True
    ).strip()
    if revision != PLATFORM_REVISION or changes:
        raise DemoError(
            "The cached Platform checkout must match the clean pinned revision."
        )
    run(
        [
            "git",
            "-C",
            str(PLATFORM),
            "submodule",
            "update",
            "--init",
            "--recursive",
            "--depth=1",
        ]
    )

    for directory in PLATFORM_COMPONENTS:
        logger.info("Building pinned Platform component %s", directory)
        run(
            [
                "make",
                "-C",
                str(PLATFORM / directory),
                "docker-build",
            ],
            env={
                **os.environ,
                "IMAGE_PREFIX": "drasi-project",
                "DOCKER_TAG_VERSION": PLATFORM_TAG_BASE,
                "BUILD_CONFIG": PLATFORM_BUILD,
                "TAG_SUFFIX": "",
                "DOCKERX_OPTS": "--load",
            },
        )

    operating_system = platform.system().lower()
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(
        platform.machine()
    )
    if operating_system not in {"darwin", "linux"} or architecture is None:
        raise DemoError("The reference setup supports Linux/macOS on amd64 or arm64.")
    run(
        [
            "docker",
            "buildx",
            "build",
            str(PLATFORM / "cli"),
            "-f",
            str(EXAMPLE / "Dockerfile.cli"),
            "--build-arg",
            f"CLI_OS={operating_system}",
            "--build-arg",
            f"CLI_ARCH={architecture}",
            "--output",
            f"type=local,dest={RUNTIME / 'bin'}",
        ]
    )

    image_id_file = RUNTIME / "agent-image-id"
    run(
        [
            "docker",
            "buildx",
            "build",
            str(REPOSITORY),
            "-f",
            str(EXAMPLE / "Dockerfile"),
            "--load",
            "--iidfile",
            str(image_id_file),
        ]
    )
    image_id = image_id_file.read_text().strip()
    if not image_id.startswith("sha256:") or len(image_id) != 71:
        raise DemoError("Docker did not return a valid agent image identity.")
    image = f"dapr-agents/agent-managed-drasi:build-{image_id.removeprefix('sha256:')}"
    run(["docker", "tag", image_id, image])
    AGENT_IMAGE_RECORD.write_text(json.dumps({"image": image, "id": image_id}) + "\n")


def pin_platform_manifests(documents: list[dict[str, Any]]) -> None:
    pinned = set()
    cluster_kinds = {"Namespace", "ClusterRole", "ClusterRoleBinding", "PriorityClass"}
    for document in documents:
        kind = document["kind"]
        metadata = document.setdefault("metadata", {})
        if kind not in cluster_kinds:
            metadata["namespace"] = ROUTER_NAMESPACE
        if kind == "StatefulSet" and metadata["name"] in PLATFORM_INFRASTRUCTURE_IMAGES:
            name = metadata["name"]
            containers = document["spec"]["template"]["spec"]["containers"]
            if len(containers) != 1:
                raise DemoError(f"Unexpected Platform container layout for {name}.")
            containers[0]["image"] = PLATFORM_INFRASTRUCTURE_IMAGES[name]
            pinned.add(name)
    if pinned != set(PLATFORM_INFRASTRUCTURE_IMAGES):
        raise DemoError(
            "The pinned Platform installer did not emit its expected infrastructure."
        )


def apply_secret(name: str, namespace: str, values: dict[str, str]) -> None:
    kubectl(
        "apply",
        "--server-side",
        "-f",
        "-",
        namespace=namespace,
        input_text=json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": name, "namespace": namespace},
                "type": "Opaque",
                "stringData": values,
            }
        ),
    )


def application_component_documents(*, multi_agent: bool) -> list[dict[str, Any]]:
    documents = [
        document
        for document in yaml.safe_load_all(
            (EXAMPLE / "kubernetes" / "components.yaml").read_text()
        )
        if document is not None
    ]
    if not multi_agent:
        return documents

    scopes = [APP_ID, *OPTIONAL_AGENT_APP_IDS]
    application_resources = {"agent-pubsub", "agent-state", "sre-agent-retries"}
    updated = set()
    for document in documents:
        metadata = document["metadata"]
        if (
            metadata.get("namespace") == NAMESPACE
            and metadata.get("name") in application_resources
        ):
            document["scopes"] = scopes
            updated.add(metadata["name"])
    if updated != application_resources:
        raise DemoError("The application Component scope resources changed.")
    return documents


def setup(env_file: Path, *, multi_agent: bool = False) -> None:
    require_tools("docker", "git", "make", "kubectl", "k3d", "helm")
    validate_runtime_paths()
    settings = load_model_settings(env_file)
    if cluster_present() or OWNERSHIP.exists():
        raise DemoError(
            "A cluster or ownership record already exists. Inspect it and use this "
            "example's cleanup command before setting up a fresh demonstration."
        )
    build()
    owner = Ownership(cluster=CLUSTER_NAME, owner=uuid4())
    OWNERSHIP.write_text(owner.model_dump_json() + "\n")
    OWNERSHIP.chmod(0o600)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        api_port = listener.getsockname()[1]
    run(
        [
            "k3d",
            "cluster",
            "create",
            CLUSTER_NAME,
            "--image",
            K3S_IMAGE,
            "--api-port",
            f"127.0.0.1:{api_port}",
            "--kubeconfig-update-default=false",
            "--kubeconfig-switch-context=false",
            "--runtime-label",
            f"{OWNER_LABEL}={owner.owner}@server:0",
            "--k3s-arg",
            "--disable=traefik@server:0",
            "--wait",
        ],
        env={**os.environ, "KUBECONFIG": str(KUBECONFIG)},
    )
    check_ownership()
    KUBECONFIG.write_text(run(["k3d", "kubeconfig", "get", CLUSTER_NAME], capture=True))
    KUBECONFIG.chmod(0o600)
    image = json.loads(AGENT_IMAGE_RECORD.read_text())["image"]
    run(
        ["k3d", "image", "import", *platform_images(), image, "--cluster", CLUSTER_NAME]
    )
    run(
        [
            "helm",
            "upgrade",
            "--install",
            "dapr",
            "dapr",
            "--repo",
            "https://dapr.github.io/helm-charts/",
            "--version",
            DAPR_VERSION,
            "--namespace",
            "dapr-system",
            "--create-namespace",
            "--kubeconfig",
            str(KUBECONFIG),
            "--set",
            f"global.tag={DAPR_VERSION}",
            "--wait",
            "--timeout",
            "5m",
        ]
    )
    manifests = RUNTIME / "platform-manifests"
    drasi(
        "init",
        "--manifest",
        str(manifests),
        "--local",
        "--version",
        PLATFORM_TAG,
        "--dapr-runtime-version",
        DAPR_VERSION,
        "--dapr-sidecar-version",
        DAPR_VERSION,
    )
    documents = [
        document
        for document in yaml.safe_load_all(
            (manifests / "kubernetes-resources.yaml").read_text()
        )
        if document is not None
    ]
    pin_platform_manifests(documents)
    kubectl(
        "apply",
        "-f",
        "-",
        namespace=ROUTER_NAMESPACE,
        input_text=yaml.safe_dump_all(documents),
    )
    for resource in (
        "statefulset/drasi-mongo",
        "statefulset/drasi-redis",
        "deployment/drasi-api",
        "deployment/drasi-resource-provider",
    ):
        kubectl(
            "rollout", "status", resource, "--timeout=300s", namespace=ROUTER_NAMESPACE
        )
    drasi("env", "kube")
    drasi("apply", "-f", str(manifests / "drasi-resources.yaml"))

    kubectl("apply", "-f", str(EXAMPLE / "kubernetes" / "namespace.yaml"))
    password = secrets.token_urlsafe(32)
    apply_secret("sre-postgres", NAMESPACE, {"password": password})
    apply_secret("sre-postgres", ROUTER_NAMESPACE, {"password": password})
    apply_secret("sre-model", NAMESPACE, settings.secret_data())
    kubectl(
        "apply",
        "-f",
        "-",
        input_text=json.dumps(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "sre-database-init", "namespace": NAMESPACE},
                "data": {"schema.sql": (EXAMPLE / "database.sql").read_text()},
            }
        ),
    )
    kubectl("apply", "-f", str(EXAMPLE / "kubernetes" / "infrastructure.yaml"))
    for resource in ("statefulset/postgres", "statefulset/agent-redis"):
        kubectl("rollout", "status", resource, "--timeout=300s")
    kubectl(
        "apply",
        "-f",
        "-",
        namespace=None,
        input_text=yaml.safe_dump_all(
            application_component_documents(multi_agent=multi_agent)
        ),
    )
    for name in ("source.yaml", "queries.yaml", "router.yaml"):
        resource_file = str(EXAMPLE / "drasi" / name)
        drasi("apply", "-f", resource_file)
        drasi("wait", "-f", resource_file, "-t", "300")

    router = json.loads(
        kubectl(
            "get",
            f"deployment/{ROUTER_APP_ID}",
            "-o",
            "json",
            namespace=ROUTER_NAMESPACE,
            capture=True,
        )
    )
    if (
        router["spec"]["replicas"] != 1
        or router["spec"]["strategy"]["type"] != "Recreate"
    ):
        raise DemoError(
            "The built-in router did not receive its single-instance deployment contract."
        )
    kubectl(
        "get",
        "component/drasi-statestore-sre-router",
        namespace=ROUTER_NAMESPACE,
        capture=True,
    )
    agent = yaml.safe_load((EXAMPLE / "kubernetes" / "agent.yaml").read_text())
    agent["spec"]["template"]["spec"]["containers"][0]["image"] = image
    kubectl("apply", "-f", "-", input_text=yaml.safe_dump(agent))
    kubectl("rollout", "status", f"deployment/{APP_ID}", "--timeout=300s")
    logger.info("Reference environment ready. No monitoring task has been submitted.")


def cleanup() -> None:
    require_tools("docker", "k3d")
    validate_runtime_paths()
    load_ownership()
    if cluster_present():
        check_ownership()
        run(
            ["k3d", "cluster", "delete", CLUSTER_NAME],
            env={**os.environ, "KUBECONFIG": str(KUBECONFIG)},
        )
    else:
        logger.info(
            "Reference cluster is absent; clearing orphaned local ownership state."
        )
    KUBECONFIG.unlink(missing_ok=True)
    home = RUNTIME / "drasi-home"
    if home.exists():
        shutil.rmtree(home)
    OWNERSHIP.unlink()
    logger.info(
        "Cleared the reference cluster's local ownership and client credentials. "
        "Source checkouts, built images, build caches, and the original .env remain."
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["build", "setup", "cleanup"])
    parser.add_argument(
        "--env-file",
        type=Path,
        default=EXAMPLE / ".env"
        if (EXAMPLE / ".env").is_file()
        else REPOSITORY / ".env",
    )
    parser.add_argument(
        "--multi-agent",
        action="store_true",
        help="Configure application Components for the optional multi-agent walkthrough.",
    )
    arguments = parser.parse_args()
    try:
        if arguments.command == "build":
            build()
        elif arguments.command == "setup":
            setup(arguments.env_file, multi_agent=arguments.multi_agent)
        else:
            cleanup()
    except (DemoError, ValueError, OSError, subprocess.SubprocessError) as error:
        logger.error("Reference environment operation failed: %s", error)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
