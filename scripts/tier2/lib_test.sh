#!/usr/bin/env bash
# Tests for scripts/tier2/lib.sh that run on any machine (make test-scripts).
# The guard's verdicts are pure functions; result.json and the report are
# checked on a fake run that skips t2_init. Needs python3 to validate JSON.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
lib="$PWD/scripts/tier2/lib.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
contains() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1: [$2] lacks [$3]" ;; esac; }
command -v python3 >/dev/null || { echo "lib_test: python3 is needed to validate JSON" >&2; exit 2; }
valid_json() { python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$1" 2>&1; }
# shellcheck source=lib.sh
. "$lib"

# The guard.
why=$(t2_guard_verdict "" 24 26); expect "guard: no GOIM_TEST_TIER is refused" "$?" 1
contains "guard: names GOIM_TEST_TIER" "$why" "GOIM_TEST_TIER=2"
t2_guard_verdict 1 24 26 >/dev/null; expect "guard: tier 1 is refused" "$?" 1
why=$(t2_guard_verdict 2 2 2); expect "guard: the dev machine is refused" "$?" 1
contains "guard: names the minimum" "$why" ">= 12 cores"
t2_guard_verdict 2 24 23 >/dev/null; expect "guard: 23GB is refused" "$?" 1
t2_guard_verdict 2 "" 26 >/dev/null; expect "guard: unknown cores are refused" "$?" 1
t2_guard_verdict 2 24 26 >/dev/null; expect "guard: the Tier 2 host passes" "$?" 0
t2_kind_verdict "" "" >/dev/null; expect "kind: none passes" "$?" 0
t2_kind_verdict "" "kind-control-plane" >/dev/null; expect "kind: a leftover container is refused" "$?" 1
t2_kind_verdict "goim" "" >/dev/null; expect "kind: a cluster is refused" "$?" 1

# t2_init refuses before writing anything.
err=$(cd "$tmp" && git init -q . && GOIM_TEST_TIER= bash -c ". '$lib'; t2_init x" 2>&1); rc=$?
expect "init: refused without GOIM_TEST_TIER exits 2" "$rc" 2
contains "init: says why" "$err" "GOIM_TEST_TIER=2"
expect "init: nothing written" "$(ls "$tmp")" ""
if [ "$(getconf _NPROCESSORS_ONLN)" -lt 12 ]; then
  (cd "$tmp" && GOIM_TEST_TIER=2 bash -c ". '$lib'; t2_init x" >/dev/null 2>&1); expect "init: this small host is refused" "$?" 2
fi

# JSON strings.
expect "json: escapes" "$(t2_json_str $'a"b\\c\nd\te\001')" '"a\"b\\c\nd\te"'

# fake_run BODY: a scenario run without t2_init; prints its exit code.
fake_run() {
  rm -rf "$tmp/run" && mkdir -p "$tmp/run/out"
  (
    T2_SCENARIO=fake T2_OUT="$tmp/run/out" T2_REPORT="$tmp/run/out.report.txt" T2_CORES=24 T2_MEM_GB=26 T2_T0=$SECONDS
    t2_traps
    t2_report_extra() { t2_section "extra"; echo "extra section"; }
    eval "$1"
  ) >"$tmp/run/stdout" 2>&1
  echo $?
}

rc=$(fake_run 't2_check a true "\"n\": 3"; t2_check b true; t2_duration tests 4; t2_field redis "\"7.4.11\""; t2_not_covered "x \"y\""; t2_finish')
expect "finish: all pass exits 0" "$rc" 0
expect "finish: result.json is JSON" "$(valid_json "$tmp/run/out/result.json")" ""
r=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d["schema"], d["pass"], d["checks"]["a"]["n"], d["duration_s"]["tests"], d["redis"], d["not_covered"][0])' "$tmp/run/out/result.json")
expect "finish: result fields" "$r" '1 True 3 4 7.4.11 x "y"'
contains "report: has result.json" "$(cat "$tmp/run/out.report.txt")" '"scenario": "fake"'
contains "report: has the extra section" "$(cat "$tmp/run/out.report.txt")" "extra section"

rc=$(fake_run 't2_check a true; t2_check b false "\"why\": \"x\""; t2_finish')
expect "finish: a failed check exits 1" "$rc" 1
expect "finish: failed run is JSON" "$(valid_json "$tmp/run/out/result.json")" ""
contains "finish: pass false" "$(cat "$tmp/run/out/result.json")" '"pass": false,'

rc=$(fake_run 't2_finish'); expect "finish: no checks exits 1" "$rc" 1
expect "finish: empty run is JSON" "$(valid_json "$tmp/run/out/result.json")" ""

rc=$(fake_run 't2_check a maybe'); expect "check: a bad pass value stops the run" "$rc" 1

rc=$(fake_run 't2_on_exit "echo first >>$tmp/run/hooks"; t2_on_exit "echo second >>$tmp/run/hooks"; exit 7')
expect "exit: an early stop keeps its exit code" "$rc" 7
expect "exit: hooks run newest first" "$(paste -sd' ' "$tmp/run/hooks")" "second first"
contains "exit: the report says the run stopped early" "$(cat "$tmp/run/out.report.txt")" "the run stopped early"
rc=$(fake_run 't2_refuse "refused: no room"; t2_check a true')
expect "refuse: exits 2" "$rc" 2
contains "refuse: the report says why" "$(cat "$tmp/run/out.report.txt")" "refused: no room"
for sig in TERM INT; do
  rc=$(fake_run 't2_on_exit "echo hook >>$tmp/run/hooks"; kill -'$sig' $BASHPID; sleep 5')
  expect "exit: SIG$sig runs the hooks" "$(cat "$tmp/run/hooks" 2>/dev/null)" hook
  [ -s "$tmp/run/out.report.txt" ] && ok "exit: SIG$sig writes the report" || bad "exit: SIG$sig wrote no report"
done

# Run recording.
expect "cpulist: expands ranges" "$(t2_cpulist_expand 4-6,9,16-17)" "4 5 6 9 16 17"
t2_mem_verdict 10 12 >/dev/null; expect "mem: enough passes" "$?" 0
why=$(t2_mem_verdict 10 5); expect "mem: too little is refused" "$?" 1
contains "mem: says what to do" "$why" "close the desktop apps"
printf '%s\n' "10 7 1 100 50 java" "11 7 1 200 10 chrome" "12 99 1 300 5 ours" "13 7 2 400 3 kworker" >"$tmp/before"
printf '%s\n' "10 7 1 100 250 java" "11 7 1 200 60 chrome" "12 99 1 300 900 ours" "13 7 2 400 103 kworker" \
  "14 7 1 500 20 new_proc" "15 7 1 999 5 reused_pid" >"$tmp/after"
expect "cpu delta: outside, kernel threads, top" "$(t2_cpu_delta "$tmp/before" "$tmp/after" 99 100)" \
  '2.8 1.0 [{"comm": "java", "cpu_s": 2.0}, {"comm": "chrome", "cpu_s": 0.5}, {"comm": "new_proc", "cpu_s": 0.2}, {"comm": "reused_pid", "cpu_s": 0.1}]'
expect "procs: our group is this shell's" "$(t2_pgid $$)" "$(ps -o pgid= -p $$ | tr -d ' ')"

# The real thing: 1.2s of CPU in another process group counts, 1.2s in
# ours does not (EPOCHREALTIME: SECONDS is too coarse). Named copies of
# bash tell them apart from whatever else runs; both outlive the window,
# since a process that exits inside it is not seen.
ln -s "$(command -v bash)" "$tmp/t2outside"
ln -s "$(command -v bash)" "$tmp/t2ours"
rc=$(fake_run '
  t2_record_start
  setsid "$tmp/t2outside" -c "end=\$((\${EPOCHREALTIME/./} + 1200000)); while ((\${EPOCHREALTIME/./} < end)); do :; done; sleep 30; true" & other=$!
  "$tmp/t2ours" -c "end=\$((\${EPOCHREALTIME/./} + 1200000)); while ((\${EPOCHREALTIME/./} < end)); do :; done; sleep 30; true" & mine=$!
  sleep 2.7
  t2_record_end
  kill $other $mine
  t2_finish')
expect "record: result.json is JSON" "$(valid_json "$tmp/run/out/result.json")" ""
r=$(python3 -c '
import json,sys
d = json.load(open(sys.argv[1])); b = d["background"]
top = {t["comm"]: t["cpu_s"] for t in b["top_outside"]}
print(top.get("t2outside", 0) >= 0.5, "t2ours" not in top, b["outside_user_cpu_s"] >= 0.5, "no_swap_in" in d["checks"], b["window_s"] >= 2)' "$tmp/run/out/result.json")
expect "record: the other group counts, ours does not" "$r" "True True True True True"
[ -s "$tmp/run/out/background-before.txt" ] && ok "record: background-before.txt" || bad "record: background-before.txt missing"

[ "$fails" -eq 0 ] && echo "lib_test: all passed" || { echo "lib_test: $fails failed"; exit 1; }
