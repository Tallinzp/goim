#!/usr/bin/env bash
# Tests for scripts/loadgen.sh's JSON readers on any machine (make
# test-scripts): a result that is missing or broken, e.g. loadgen died
# before writing it, reads as null and false, so a scenario still writes a
# valid result.json with a failed check. Needs python3.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/loadgen.sh
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
fails=0
expect() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1: got [$2], want [$3]"; fails=$((fails + 1)); fi; }

echo '{"mode": "light", "online": 3, "lost": {"stopped": 3}, "pong_rtt": {"p99_ms": 4}}' >"$dir/r.json"
expect "py: a number" "$(lg_py "$dir/r.json" 'd["online"]')" 3
expect "py: an object as JSON" "$(lg_py "$dir/r.json" 'd["pong_rtt"]')" '{"p99_ms": 4}'
expect "py: a string as JSON" "$(lg_py "$dir/r.json" 'd["mode"]')" '"light"'
expect "py: stdin" "$(lg_py - 'd["online"]' <"$dir/r.json")" 3
expect "py: a missing file is null" "$(lg_py "$dir/none.json" 'd["online"]')" null
echo '{"mode": ' >"$dir/half.json"
expect "py: half a file is null" "$(lg_py "$dir/half.json" 'd["mode"]')" null
expect "py: a missing key is null" "$(lg_py "$dir/r.json" 'd["nope"]')" null
expect "true: a true expression" "$(lg_true "$dir/r.json" 'set(d["lost"]) <= {"stopped"}')" true
expect "true: a false expression" "$(lg_true "$dir/r.json" 'd["online"] > 3')" false
expect "true: a missing file is false" "$(lg_true "$dir/none.json" 'd["online"] == 3')" false
expect "true: a non-bool value is judged by truth" "$(lg_true "$dir/r.json" 'd["online"]')" true

[ "$fails" -eq 0 ] && echo "loadgen_test: all passed" || { echo "loadgen_test: $fails failed"; exit 1; }
