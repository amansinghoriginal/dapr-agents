--
-- Copyright 2026 The Dapr Authors
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--     http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE TABLE service_errors (
    error_id text PRIMARY KEY,
    service text NOT NULL,
    status_code integer NOT NULL,
    message text NOT NULL,
    rollout_id text,
    marker text NOT NULL
);

CREATE TABLE rollout_status (
    rollout_id text PRIMARY KEY,
    service text NOT NULL,
    status text NOT NULL,
    message text NOT NULL,
    marker text NOT NULL
);

CREATE TABLE service_assessments (
    service text PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('investigating', 'recovering', 'healthy')),
    summary text NOT NULL CHECK (length(summary) BETWEEN 1 AND 2000),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE service_errors REPLICA IDENTITY FULL;
ALTER TABLE rollout_status REPLICA IDENTITY FULL;
