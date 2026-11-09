SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

VENV ?= .venv
PYTHON ?= python3
ENGINE ?= podman
COMPOSE ?= $(ENGINE) compose
CONTAINER ?= $(ENGINE)
APP_IMAGE ?= kafkallm-app:latest
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
VENV_STAMP := $(VENV)/.kafkallm-installed

.PHONY: help setup doctor bootstrap network images app-image rea-image up down restart logs ps test lint format venv clean reset

help: ## Show this help
	@printf "%-14s %s\n" "target" "description"
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_.-]+:.*## / {printf "%-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Create .env, generate the gateway key, and check required settings
	./scripts/setup.sh

doctor: ## Check host prerequisites, configuration, and services
	./scripts/doctor.sh

bootstrap: setup network images up ## One-command first start (after .env is valid)

network: ## Create the isolated IPv6 Podman network (run once per host)
	./scripts/init-job-network.sh

images: ## Build the default goose job image into rootless Podman
	./scripts/build-goose-image.sh

app-image: ## Build the worker/GUI image with the selected engine
	$(CONTAINER) build -t $(APP_IMAGE) .

rea-image: ## Build the optional REA + Ghidra job image
	./scripts/build-rea-image.sh

up: ## Build the app image and start the control plane
	$(CONTAINER) build -t $(APP_IMAGE) .
	-$(COMPOSE) --profile storage down --remove-orphans
	$(COMPOSE) --profile storage up -d --remove-orphans

down: ## Stop the stack without deleting volumes
	$(COMPOSE) --profile storage down --remove-orphans

restart: up ## Rebuild the app image and recreate the stack

logs: ## Follow logs for the whole stack
	$(COMPOSE) logs -f --tail=100

ps: ## Show service status
	$(COMPOSE) ps

venv: $(VENV_STAMP) ## Create the local development environment

$(VENV_STAMP): pyproject.toml
	@test -x $(VENV)/bin/python || $(PYTHON) -m venv $(VENV)
	@$(VENV)/bin/python -m pip --version >/dev/null 2>&1 || \
		$(VENV)/bin/python -m ensurepip --upgrade
	$(VENV)/bin/python -m pip install -e '.[dev]'
	@touch $(VENV_STAMP)

test: venv ## Run the unit tests
	$(PYTEST)

lint: venv ## Check formatting and lint rules
	$(RUFF) check src tests
	$(RUFF) format --check src tests

format: venv ## Auto-format and fix lint findings
	$(RUFF) check --fix src tests
	$(RUFF) format src tests

reset: ## Stop the stack and delete all Kafka, trace, and storage data
	@printf 'This deletes all jobs, agents, sandboxes, and traces. Type "reset" to continue: '; \
	read -r answer; [ "$$answer" = "reset" ] || { echo "aborted"; exit 1; }
	$(COMPOSE) --profile storage down --remove-orphans -v

clean: ## Remove local caches and build output (keeps .env and .venv)
	rm -rf build dist .pytest_cache .ruff_cache
	find src tests -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -maxdepth 1 -name '*.egg-info' -type d -prune -exec rm -rf {} +
