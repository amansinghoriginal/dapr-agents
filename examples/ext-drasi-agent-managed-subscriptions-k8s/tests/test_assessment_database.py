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

import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from actions import AssessmentInput, save_assessment


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("DEMO_TEST_POSTGRES") != "1",
    reason="Set DEMO_TEST_POSTGRES=1 to run the isolated Docker/PostgreSQL proof.",
)
def test_repeated_and_concurrent_actions_cannot_create_duplicate_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = f"drasi-m2-assessment-test-{uuid4().hex}"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--label",
            "io.dapr-agents.example=drasi-m2-assessment-test",
            "--env",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "--env",
            "POSTGRES_USER=sre",
            "--env",
            "POSTGRES_DB=sre",
            "--publish",
            "127.0.0.1::5432",
            "postgres:15.8-alpine3.20",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    try:
        address = subprocess.check_output(
            ["docker", "port", name, "5432"], text=True, timeout=10
        ).strip()
        host, port = address.rsplit(":", 1)
        for key, value in {
            "PGHOST": host,
            "PGPORT": port,
            "PGUSER": "sre",
            "PGDATABASE": "sre",
            "PGSSLMODE": "disable",
        }.items():
            monkeypatch.setenv(key, value)
        monkeypatch.delenv("PGSERVICE", raising=False)
        monkeypatch.delenv("PGOPTIONS", raising=False)
        deadline = time.monotonic() + 60
        while True:
            try:
                connection = psycopg.connect(connect_timeout=2, autocommit=True)
                break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    pytest.fail(
                        "The owned PostgreSQL test container did not become ready."
                    )
                time.sleep(0.25)
        with connection:
            schema = Path(__file__).resolve().parent.parent / "database.sql"
            connection.execute(schema.read_text())
            observation = AssessmentInput(
                status="healthy", summary="Same synthetic recovery"
            )
            save_assessment("checkout", observation)
            save_assessment("checkout", observation)
            with ThreadPoolExecutor(max_workers=4) as pool:
                records = list(
                    pool.map(
                        lambda _: save_assessment("checkout", observation),
                        range(8),
                    )
                )
            assert all(record.service == "checkout" for record in records)
            assert connection.execute(
                "SELECT count(*) FROM service_assessments"
            ).fetchone() == (1,)
            assert connection.execute(
                "SELECT service, status, summary FROM service_assessments"
            ).fetchone() == ("checkout", "healthy", "Same synthetic recovery")
            with pytest.raises(psycopg.errors.UniqueViolation):
                connection.execute(
                    "INSERT INTO service_assessments (service, status, summary) "
                    "VALUES ('checkout', 'healthy', 'duplicate object')"
                )
    finally:
        subprocess.run(
            ["docker", "rm", "--force", name],
            check=True,
            capture_output=True,
            timeout=30,
        )
