export CGO_ENABLED := 1

# Host ports for the Tier 2 Redis; unusual defaults avoid clashing with a local Redis.
export GOIM_REDIS_PORT ?= 16379
export GOIM_REDIS_REPLICA_PORT ?= 16380

COMPOSE_REDIS := docker compose -f deploy/compose/redis-test.yml
TIER2_GUARD := test "$$GOIM_TEST_TIER" = 2 || { echo "Tier 2 host only: set GOIM_TEST_TIER=2"; exit 1; }

.PHONY: test redis-up redis-down test-redis test-tier2

# Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
test:
	go test -race -count=1 ./...

# Tier 2 host only: real Redis primary + replica for the tests that skip at Tier 1a.
redis-up:
	@$(TIER2_GUARD)
	$(COMPOSE_REDIS) up -d --wait

redis-down:
	$(COMPOSE_REDIS) down

test-redis:
	@$(TIER2_GUARD)
	GOIM_TEST_REDIS=127.0.0.1:$(GOIM_REDIS_PORT) GOIM_TEST_REDIS_REPLICA=127.0.0.1:$(GOIM_REDIS_REPLICA_PORT) go test -race -count=1 ./...

# Tier 2 scenario entry point (spec: automation contract), e.g. make test-tier2 SCENARIO=store
test-tier2:
	scripts/tier2/$(SCENARIO).sh
