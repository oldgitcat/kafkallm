SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

VENV ?= .venv
PYTHON ?= python3
COMPOSE ?= podman compose -f compose.yaml
CONTAINER ?= podman
WORKER_IMAGE ?= gaensestall-worker:latest
GUI_IMAGE ?= gaensestall-gui:latest
MLFLOW_IMAGE ?= gaensestall-mlflow:latest
export GAENSESTALL_WORKER_IMAGE := $(WORKER_IMAGE)
export GAENSESTALL_GUI_IMAGE := $(GUI_IMAGE)
export GAENSESTALL_MLFLOW_IMAGE := $(MLFLOW_IMAGE)
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
VENV_STAMP := $(VENV)/.gaensestall-installed

.PHONY: help setup gui-auth gui-auth-force doctor bootstrap network images llm-proxy-image worker-image gui-image mlflow-image rea-image up down restart logs ps test lint docs-check check format venv clean prune lock reset

help: ## Show this help
	@printf "%-14s %s\n" "target" "description"
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_.-]+:.*## / {printf "%-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Create .env, generate the proxy token, and check required settings
	./scripts/setup.sh

gui-auth: ## Set the GUI username/password for the Caddy basic auth
	./scripts/gui-auth.sh

gui-auth-force: ## Change the existing GUI username/password
	./scripts/gui-auth.sh --force

doctor: ## Check host prerequisites, configuration, and services
	./scripts/doctor.sh

bootstrap: setup network images up ## One-command first start (after .env is valid)

network: ## Create the isolated IPv6 Podman network (run once per host)
	./scripts/init-job-network.sh

images: llm-proxy-image ## Build the default sandbox and LLM proxy images
	./scripts/build-goose-image.sh

llm-proxy-image: ## Build the distroless allowlisting LLM proxy image
	./scripts/build-llm-proxy-image.sh

worker-image: ## Build the worker image
	$(CONTAINER) build -f backend/Dockerfile -t $(WORKER_IMAGE) .

gui-image: ## Build the standalone distroless GUI image
	$(CONTAINER) build -f frontend/Dockerfile -t $(GUI_IMAGE) .

mlflow-image: ## Build the MLflow tracking server with the AI Gateway
	$(CONTAINER) build -f infra/mlflow/Dockerfile -t $(MLFLOW_IMAGE) .

rea-image: ## Build the optional REA + Ghidra job image
	./scripts/build-rea-image.sh

up: gui-auth llm-proxy-image gui-image mlflow-image ## Build images and start the control plane
	./scripts/prepare-job-root.sh
	$(CONTAINER) build -f backend/Dockerfile -t $(WORKER_IMAGE) .
	-$(COMPOSE) --profile storage down --remove-orphans
	$(COMPOSE) --profile storage up -d --remove-orphans

down: ## Stop the stack without deleting volumes
	$(COMPOSE) --profile storage down --remove-orphans

restart: up ## Rebuild the worker image and recreate the stack

logs: ## Follow logs for the whole stack
	$(COMPOSE) logs -f --tail=100

ps: ## Show service status
	$(COMPOSE) ps

venv: $(VENV_STAMP) ## Create the local development environment

$(VENV_STAMP): pyproject.toml frontend/pyproject.toml
	@test -x $(VENV)/bin/python || $(PYTHON) -m venv $(VENV)
	@$(VENV)/bin/python -m pip --version >/dev/null 2>&1 || \
		$(VENV)/bin/python -m ensurepip --upgrade
	$(VENV)/bin/python -m pip install -c constraints.txt -e '.[dev]' -e './frontend'
	@touch $(VENV_STAMP)

test: venv ## Run the unit tests
	$(PYTEST)

lint: venv ## Check formatting and lint rules
	$(RUFF) check backend tests frontend
	$(RUFF) format --check backend tests frontend

docs-check: venv ## Check Markdown links and documented repository paths
	$(PYTEST) tests/test_docs.py

check: lint test ## Run lint and the full test suite (including docs checks)

format: venv ## Auto-format and fix lint findings
	$(RUFF) check --fix backend tests frontend
	$(RUFF) format backend tests frontend

reset: ## Stop the stack and delete all Kafka, trace, and storage data
	@printf 'This deletes all jobs, agents, sandboxes, and traces. Type "reset" to continue: '; \
	read -r answer; [ "$$answer" = "reset" ] || { echo "aborted"; exit 1; }
	$(COMPOSE) --profile storage down --remove-orphans -v

clean: ## Remove local caches and build output (keeps .env and .venv)
	rm -rf build dist .pytest_cache .ruff_cache
	find backend tests -name '__pycache__' -type d -prune -exec rm -rf {} +
	find backend . -maxdepth 1 -name '*.egg-info' -type d -prune -exec rm -rf {} +

prune: ## Remove dangling images and local caches
	$(CONTAINER) image prune -f
	$(MAKE) clean

lock: ## Regenerate constraints.txt from the current app image
	{ printf '%s\n' '# Locked transitive dependencies for the gaensestall app image.' '# Regenerate intentionally with: make lock'; \
	  $(CONTAINER) run --rm --entrypoint pip $(WORKER_IMAGE) freeze | grep -v '^gaensestall'; } > constraints.txt
