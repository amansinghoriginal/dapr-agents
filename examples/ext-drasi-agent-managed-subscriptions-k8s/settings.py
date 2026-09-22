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

"""Operator-owned identities and model configuration for this reference app."""

import os
from collections.abc import Mapping
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

NAMESPACE = "drasi-m2-demo"
APP_ID = "checkout-sre"
AGENT_NAME = "CheckoutSRE"
ROUTER_NAME = "sre-router"
ROUTER_NAMESPACE = "drasi-system"
ROUTER_APP_ID = f"{ROUTER_NAME}-reaction"
ROUTER_ID = f"{ROUTER_NAMESPACE}/{ROUTER_APP_ID}"
PUBSUB_NAME = "agent-pubsub"
STATE_STORE_NAME = "agent-state"
ERROR_QUERY = "checkout-server-errors"
ROLLOUT_QUERY = "checkout-rollout-status"
MODEL_ENV_KEYS = ("LLM_PROVIDER", "LLM_CHAT_URL", "LLM_API_KEY", "LLM_MODEL")


class ModelSettings(BaseModel):
    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)

    provider: Literal["openai", "azure", "azure-openai", "azure_openai"]
    base_url: str = Field(repr=False)
    api_key: SecretStr
    model: str = Field(min_length=1, pattern=r"\S", repr=False)

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        path = parsed.path.rstrip("/")
        for endpoint in ("/chat/completions", "/responses"):
            if path.endswith(endpoint):
                path = path.removesuffix(endpoint)
                break
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or not path.endswith("/v1")
        ):
            raise ValueError(
                "LLM_CHAT_URL must identify an HTTPS OpenAI-compatible v1 API, "
                "without credentials, query parameters, or fragments."
            )
        return parsed._replace(path=path + "/").geturl()

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("LLM_API_KEY must not be empty.")
        return value

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> "ModelSettings":
        environment = os.environ if values is None else values
        missing = [key for key in MODEL_ENV_KEYS if not environment.get(key)]
        if missing:
            raise ValueError(f"Missing model settings: {', '.join(missing)}.")
        return cls.model_validate(
            {
                "provider": environment["LLM_PROVIDER"].lower(),
                "base_url": environment["LLM_CHAT_URL"],
                "api_key": environment["LLM_API_KEY"],
                "model": environment["LLM_MODEL"],
            }
        )

    def secret_data(self) -> dict[str, str]:
        return {
            "LLM_PROVIDER": self.provider,
            "LLM_CHAT_URL": self.base_url,
            "LLM_API_KEY": self.api_key.get_secret_value(),
            "LLM_MODEL": self.model,
        }
