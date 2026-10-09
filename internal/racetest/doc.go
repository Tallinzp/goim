// Package racetest is the race integration suite (spec: Verification → Race
// integration tests; Tier 1b; Plan 5b). Each scenario starts the real
// binaries as local processes, built with -race (choice #47): a Redis,
// devtoken serving the JWKS, one Logic and four gateways, all on the test
// timing profile and with the test-only fault API on. The scenario drives
// scripted clients (internal/clientconn) or SDK clients, kills, pauses and
// restarts processes with signals, injects faults through each process's
// /fault/ endpoint, and checks the outcome in Redis and in the metrics.
//
// The suite runs only with GOIM_RACE_SUITE=1 (make test-race-suite); a
// plain `go test ./...` skips it. Redis is a redis-server primary and
// replica started for each scenario (GOIM_REDIS_SERVER or redis-server on
// PATH; make redis-server builds the pinned 7.4.11), on the dev machine and
// on the Tier 2 host (make test-tier2 SCENARIO=race) alike. Every file but
// this one is a _test.go file, so nothing here ships.
package racetest
