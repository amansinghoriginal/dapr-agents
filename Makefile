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

UV_RUN := uv run --frozen --no-sync
DRASI_TESTS := ext/dapr-agents-ext-drasi/tests

# Test targets
.PHONY: test test-core test-extension
test: test-core test-extension

test-core:
	@echo "Running core tests..."
	$(UV_RUN) pytest tests -m "not integration" -v --tb=short

test-extension:
	@echo "Running Drasi extension tests..."
	$(UV_RUN) pytest $(DRASI_TESTS) -m "not integration" -v --tb=short

.PHONY: test-cov
test-cov:
	@echo "Running tests with coverage..."
	$(UV_RUN) pytest tests -m "not integration" -v --cov=dapr_agents --cov-report=
	$(UV_RUN) pytest $(DRASI_TESTS) -m "not integration" -v \
		--cov=dapr_agents.ext.drasi --cov-append \
		--cov-report=term-missing --cov-report=html

.PHONY: test-install
test-install:
	@echo "Installing test dependencies..."
	uv sync --frozen --group test --extra drasi \
		--config-settings-package dapr-agents:editable_mode=strict \
		--reinstall-package dapr-agents

.PHONY: test-all
test-all: test-install test-cov
	@echo "All tests completed!"

# Pre-commit hook targets
.PHONY: hooks-install
hooks-install:
	@echo "Installing pre-push hooks..."
	pre-commit install --hook-type pre-push

.PHONY: hooks-uninstall
hooks-uninstall:
	@echo "Uninstalling pre-push hooks..."
	pre-commit uninstall --hook-type pre-push

.PHONY: hooks-run
hooks-run:
	@echo "Running all pre-push hooks..."
	pre-commit run --all-files --hook-stage pre-push

.PHONY: hooks-run-all
hooks-run-all:
	@echo "Running all pre-push hooks plus integration tests..."
	@echo "Step 1/2: Running pre-push hooks (format, lint, type check, unit tests)..."
	pre-commit run --all-files --hook-stage pre-push
	@echo "Step 2/2: Running integration tests..."
	$(UV_RUN) pytest tests -m integration -v

.PHONY: format
format:
	@echo "Formatting code with ruff..."
	$(UV_RUN) ruff format dapr_agents tests ext

.PHONY: lint
lint:
	@echo "Linting with flake8..."
	$(UV_RUN) flake8 dapr_agents tests ext --ignore=E501,F401,W503,E203,E704

.PHONY: typecheck
typecheck:
	@echo "Type checking with mypy..."
	$(UV_RUN) mypy --config-file mypy.ini

.PHONY: test-unit
test-unit: test
