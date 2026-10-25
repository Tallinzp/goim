export CGO_ENABLED := 1

# Host ports for the Tier 2 Redis; unusual defaults avoid clashing with a local Redis.
export GOIM_REDIS_PORT ?= 16379
export GOIM_REDIS_REPLICA_PORT ?= 16380

COMPOSE_REDIS := docker compose -f deploy/compose/redis-test.yml
TIER2_GUARD := test "$$GOIM_TEST_TIER" = 2 || { echo "Tier 2 host only: set GOIM_TEST_TIER=2"; exit 1; }

.PHONY: test test-race-suite test-scripts test-load-probe redis-server test-redis-local test-redis-cluster-local proto redis-up redis-down test-redis test-redis-cluster test-tier2 deploy deploy-down deploy-status deploy-logs deploy-token

# Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
test:
	go test -race -count=1 ./...

# Tier 1b (dev machine, before every commit): the race integration suite. It
# builds the binaries with -race and runs them as local processes, with a
# redis-server primary and replica per scenario (Plan 5b). GOIM_RACE_LOGS=dir
# keeps every scenario's logs.
test-race-suite:
	GOIM_RACE_SUITE=1 GOIM_REDIS_SERVER=$(REDIS_SERVER) go test -count=1 -timeout 30m ./internal/racetest/

# The Tier 2 and load-run scripts (Plans 6a1, 6a2), on any machine: lib.sh's
# guard, result.json, recording and host-setup checks, loadgen.sh's JSON
# readers, and a real stack.sh stack.
# Needs python3, curl and redis-server (make redis-server).
test-scripts:
	scripts/tier2/lib_test.sh
	scripts/tier2/floods_test.sh
	scripts/tier2/reconnect_test.sh
	scripts/loadgen_test.sh
	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
	GOIM_REDIS_SERVER=$(REDIS_SERVER) scripts/deploy_test.sh

# The Tier 1a load probe (Plan 6a2, spec: ≤ 5k connections), on the dev
# machine: a 2-gateway stack.sh stack and cmd/loadgen, light then sdk with a
# gateway killed by loadgen. Not in make test (about 2 minutes). Needs
# redis-server (make redis-server), curl and python3.
test-load-probe:
	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/load-probe.sh

# The race suite's Redis: redis-server on PATH, else the pinned 7.4.11 that
# make redis-server builds into .cache/redis (cc and make, no Docker).
REDIS_SERVER ?= $(or $(shell command -v redis-server 2>/dev/null),$(CURDIR)/.cache/redis/bin/redis-server)
redis-server:
	scripts/redis-build.sh .cache/redis

# Tier 1a on real Redis, on the dev machine: every test, including the ones
# that skip as "TODO: Tier 2 integration", against a local redis-server
# primary and replica (no Docker). The tests make test-redis runs on the Tier 2 host.
test-redis-local:
	scripts/redis-local.sh $(REDIS_SERVER) go test -race -count=1 ./...

# The same on a local Redis Cluster (P5bI-4, Plan 6c1): 3 masters and 3
# replicas from scripts/redis-cluster-local.sh, no Docker.
test-redis-cluster-local:
	scripts/redis-cluster-local.sh $(REDIS_SERVER) go test -race -count=1 ./...

# Regenerate api/internalpb and api/clientpb from api/internal.proto and
# api/client.proto. Needs protoc 33.1 and the two pinned plugins (Plan 2,
# Task 1); the generated code is committed.
PROTOC ?= protoc
GOBIN_DIR := $(shell go env GOPATH)/bin
proto:
	PATH="$(GOBIN_DIR):$$PATH" $(PROTOC) --go_out=. --go_opt=module=github.com/Tallinzp/goim \
		--go-grpc_out=. --go-grpc_opt=module=github.com/Tallinzp/goim api/internal.proto api/client.proto

# Tier 2 host only: real Redis primary + replica for the tests that skip at Tier 1a.
redis-up:
	@$(TIER2_GUARD)
	$(COMPOSE_REDIS) up -d --wait

redis-down:
	$(COMPOSE_REDIS) down

test-redis:
	@$(TIER2_GUARD)
	GOIM_TEST_REDIS=127.0.0.1:$(GOIM_REDIS_PORT) GOIM_TEST_REDIS_REPLICA=127.0.0.1:$(GOIM_REDIS_REPLICA_PORT) go test -race -count=1 ./...

# Tier 2 host: every test on the pinned 7.4.11 as a Redis Cluster (P5bI-4).
test-redis-cluster:
	@$(TIER2_GUARD)
	scripts/redis-cluster-local.sh $(REDIS_SERVER) go test -race -count=1 ./...

# Tier 2 scenario entry point (spec: automation contract). The caller sets
# GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
# scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, floods, reconnect, probe (read-only, no guard).
# The script's exit code is 0 (pass), 1 (a check failed or the run stopped
# early) or 2 (host refused); make exits 2 on any failure, so the recipe
# prints the script's code. Automation that needs it runs the script directly.
test-tier2:
	@scripts/tier2/$(SCENARIO).sh; rc=$$?; [ $$rc -eq 0 ] || { echo "test-tier2: $(SCENARIO) exited $$rc" >&2; exit $$rc; }

# A single-node stack for a real client app, on Ubuntu or macOS (scripts/deploy.sh,
# README: Run a node on your machine). Data and keys live in $GOIM_HOME (~/.goim).
# Redis: on PATH, else the one make redis-server builds.
DEPLOY_REDIS := $(if $(wildcard $(REDIS_SERVER)),GOIM_REDIS_SERVER=$(REDIS_SERVER))

deploy:
	$(DEPLOY_REDIS) scripts/deploy.sh up

# FORCE=1 sends SIGKILL to a process that does not stop in time.
deploy-down:
	FORCE=$(FORCE) scripts/deploy.sh down

deploy-status:
	scripts/deploy.sh status

# NAME is one of redis, devtoken, logic, gateway (default: all).
deploy-logs:
	scripts/deploy.sh logs $(NAME)

# make deploy-token USER_ID=alice: a login token for the app.
deploy-token:
	@test -n "$(USER_ID)" || { echo "USER_ID=<uid> is required"; exit 2; }
	@scripts/deploy.sh token "$(USER_ID)"
