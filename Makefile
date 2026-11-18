# Convenience targets. Everything also works with plain docker compose.
COMPOSE ?= docker compose

.PHONY: help build up down logs ps demo demo-failure demo-integrity test test-api test-failures test-integrity test-worker test-cli local-up local-down clean

help:          ## show this help
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

build:         ## build the controller, worker and client images
	$(COMPOSE) build
	$(COMPOSE) --profile client build client

up:            ## start 1 controller + 4 workers in the background
	$(COMPOSE) up -d --build

down:          ## stop the cluster (data volumes are kept)
	$(COMPOSE) down

logs:          ## follow controller + worker logs
	$(COMPOSE) logs -f

ps:            ## show container status
	$(COMPOSE) ps

demo:          ## run every required demonstration against the running cluster
	scripts/demo.sh all

demo-failure:  ## only the worker-failure demonstration
	scripts/demo.sh failure

demo-integrity: ## only the data-integrity demonstration
	scripts/demo.sh integrity

test:          ## run the automated test-suite (no Docker needed)
	scripts/test.sh

test-api:      ## run client REST API and metadata tests
	scripts/test.sh api

test-failures: ## run worker failure and automatic-repair tests
	scripts/test.sh failures

test-integrity: ## run corruption detection and restoration tests
	scripts/test.sh integrity

test-worker:   ## run worker persistent-storage tests
	scripts/test.sh worker

test-cli:      ## run command-line client tests
	scripts/test.sh cli

local-up:      ## run the cluster as local processes instead of containers
	scripts/local_cluster.sh up

local-down:    ## stop the local-process cluster
	scripts/local_cluster.sh down

clean:         ## stop the cluster and DELETE all stored data
	$(COMPOSE) down -v
	rm -rf demo-data .local-data
