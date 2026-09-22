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

"""One business object per configured service, enforced by PostgreSQL."""

import logging
from datetime import datetime
from typing import Literal

import psycopg
from psycopg.rows import dict_row
from pydantic import BaseModel, ConfigDict, Field

from dapr_agents.tool import AgentTool
from dapr_agents.types import ToolResult

logger = logging.getLogger(__name__)

AssessmentStatus = Literal["investigating", "recovering", "healthy"]


class AssessmentInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    status: AssessmentStatus = Field(
        description="The checkout service's assessed condition."
    )
    summary: str = Field(
        min_length=1,
        max_length=2000,
        pattern=r"\S",
        description="A concise assessment grounded in the observed event data.",
    )


class AssessmentRecord(AssessmentInput):
    service: str
    updated_at: datetime


class AssessmentError(Exception):
    """The destination did not confirm the assessment write."""


def save_assessment(service: str, assessment: AssessmentInput) -> AssessmentRecord:
    try:
        # libpq reads only the operator-supplied PG* connection settings.
        with psycopg.connect(connect_timeout=5, row_factory=dict_row) as connection:
            row = connection.execute(
                """
                INSERT INTO service_assessments (service, status, summary)
                VALUES (%s, %s, %s)
                ON CONFLICT (service) DO UPDATE
                SET status = EXCLUDED.status,
                    summary = EXCLUDED.summary,
                    updated_at = clock_timestamp()
                RETURNING service, status, summary, updated_at
                """,
                (service, assessment.status, assessment.summary),
            ).fetchone()
    except psycopg.Error as error:
        logger.error(
            "Assessment write for %s failed (%s).", service, type(error).__name__
        )
        raise AssessmentError(
            "The assessment database did not confirm the write."
        ) from None

    if row is None:
        logger.error("Assessment write for %s returned no record.", service)
        raise AssessmentError("The assessment database returned no record.")
    return AssessmentRecord.model_validate(row)


def make_assessment_tool(service: str) -> AgentTool:
    if not service.strip():
        logger.error("The configured assessment service is empty.")
        raise ValueError("ASSESSMENT_SERVICE must not be empty.")

    def record_service_assessment(status: AssessmentStatus, summary: str) -> ToolResult:
        try:
            record = save_assessment(
                service, AssessmentInput(status=status, summary=summary)
            )
        except AssessmentError as error:
            return ToolResult.error(str(error))
        return ToolResult.success(
            record.model_dump(mode="json"),
            text=f"Updated the single assessment record for {service}.",
        )

    return AgentTool(
        name="record_service_assessment",
        description=(
            f"Record an assessment of the configured service {service!r}. "
            "The database atomically upserts the service's one assessment record. "
            "Repeated or concurrent calls cannot create additional objects for "
            "that service. The service identity and destination are operator "
            "configuration, not tool arguments. Call this only when instructed "
            "to assess an actual observation; it does not establish monitoring."
        ),
        args_model=AssessmentInput,
        func=record_service_assessment,
    )
