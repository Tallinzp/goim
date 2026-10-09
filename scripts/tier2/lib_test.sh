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
expect "json: invalid UTF-8 is dropped" "$(t2_json_str $'a\xffb\xc3')" '"ab"'
expect "json: UTF-8 is kept" "$(t2_json_str 'é—x')" '"é—x"'

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
# A second Ctrl-C or SIGTERM during teardown must not cut it short.
rc=$(fake_run 't2_on_exit "echo first >>$tmp/run/hooks"; t2_on_exit "kill -TERM \$BASHPID; echo second >>$tmp/run/hooks"; exit 3')
expect "exit: a signal during teardown keeps the exit code" "$rc" 3
expect "exit: a signal during teardown runs every hook" "$(paste -sd' ' "$tmp/run/hooks" 2>/dev/null)" "second first"
[ -s "$tmp/run/out.report.txt" ] && ok "exit: a signal during teardown writes the report" || bad "exit: a signal during teardown wrote no report"

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
  echo "[${T2_SAMPLER:-}]" >"$tmp/run/sampler"
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
expect "record: no sampler pid is left for the exit hook to kill" "$(cat "$tmp/run/sampler")" "[]"

# Host setup.
mkdir -p "$tmp/hs"
(cd "$tmp/hs" && "$OLDPWD/scripts/tier2/host-setup.sh" --root "$tmp/hs/root" --user alice) >"$tmp/hs/out" 2>&1
expect "setup: test mode exits 0" "$?" 0
expect "setup: sudoers line" "$(cat "$tmp/hs/root/etc/sudoers.d/goim-netdrop")" "alice ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop"
expect "setup: sudoers mode" "$(stat -c %a "$tmp/hs/root/etc/sudoers.d/goim-netdrop")" 440
expect "setup: helper mode" "$(stat -c %a "$tmp/hs/root/usr/local/sbin/goim-netdrop")" 755
again=$(scripts/tier2/host-setup.sh --root "$tmp/hs/root" --user alice | grep -c '^unchanged')
expect "setup: idempotent" "$again" 4
scripts/tier2/host-setup.sh --root "$tmp/hs/root" --user 'a b' >/dev/null 2>&1; expect "setup: odd user refused" "$?" 2
scripts/tier2/host-setup.sh --root "$tmp/hs/root" >/dev/null 2>&1 </dev/null; rc=$?
[ -n "${SUDO_USER:-}" ] || expect "setup: no user refused" "$rc" 2
# A fake /proc/sys and /sys/module that match sysctl.conf, then break them.
fake=$tmp/fake
while IFS='=' read -r k v; do
  k=${k// /}; mkdir -p "$fake/proc/$(dirname "${k//.//}")"; echo "${v# }" >"$fake/proc/${k//.//}"
done < <(grep -vE '^[[:space:]]*(#|$)' scripts/tier2/sysctl.conf)
for m in $(grep -vE '^[[:space:]]*(#|$)' scripts/tier2/modules.conf); do mkdir -p "$fake/mod/$m"; done
cp scripts/tier2/goim-netdrop "$fake/netdrop"
expect "setup check: a set-up host" "$(t2_host_setup_missing scripts/tier2 "$fake/proc" "$fake/mod" "$fake/netdrop")" ""
printf '1048576\t1000000\t2097152\n' >"$fake/proc/net/ipv4/tcp_mem"
echo 262144 >"$fake/proc/net/netfilter/nf_conntrack_max"
rmdir "$fake/mod/ip_vs"
echo "# old" >>"$fake/netdrop"
m=$(t2_host_setup_missing scripts/tier2 "$fake/proc" "$fake/mod" "$fake/netdrop")
contains "setup check: low conntrack" "$m" "sysctl net.netfilter.nf_conntrack_max is 262144, want >= 4194304"
contains "setup check: one low tcp_mem field" "$m" "sysctl net.ipv4.tcp_mem is 1048576 1000000 2097152"
contains "setup check: module" "$m" "module ip_vs is not loaded"
contains "setup check: outdated helper" "$m" "differs from scripts/tier2/goim-netdrop"
expect "setup check: nothing else" "$(echo "$m" | wc -l)" 4
t2_netdrop_leftover "" >/dev/null; expect "leftover: no rules pass" "$?" 0
why=$(t2_netdrop_leftover "-A INPUT -i lo -p tcp -m tcp --dport 17100 -m comment --comment goim-netdrop -j DROP"); expect "leftover: a rule is refused" "$?" 1
contains "leftover: says how to remove it" "$why" "sudo -n /usr/local/sbin/goim-netdrop flush"

# goim-netdrop prints its rules and refuses anything else.
expect "netdrop: add" "$(scripts/tier2/goim-netdrop --print add 21100)" \
"iptables -w -I INPUT -i lo -p tcp --dport 21100 -m comment --comment goim-netdrop -j DROP
iptables -w -I INPUT -i lo -p tcp --sport 21100 -m comment --comment goim-netdrop -j DROP"
contains "netdrop: del" "$(scripts/tier2/goim-netdrop --print del 21100)" "iptables -w -D INPUT -i lo -p tcp --dport 21100"
for bad_args in "add 80" "add 01167" "add 70000" "add 21100x" "add" "add 1 2" "drop 21100" "--print flush"; do
  # shellcheck disable=SC2086
  scripts/tier2/goim-netdrop $bad_args >/dev/null 2>&1; expect "netdrop: refuses '$bad_args'" "$?" 2
done
# Without root, iptables fails: list and flush must say so, not exit 0.
if [ "$(id -u)" != 0 ]; then
  for c in list flush; do
    scripts/tier2/goim-netdrop $c >/dev/null 2>&1 && bad "netdrop: $c exits 0 when iptables fails" || ok "netdrop: $c fails when iptables fails"
  done
fi

# make test-tier2 shows the script's own exit code (make itself exits 2 on any failure).
out=$(GOIM_TEST_TIER= make -s test-tier2 SCENARIO=race 2>&1)
contains "make: shows the scenario's exit code" "$out" "test-tier2: race exited 2"

[ "$fails" -eq 0 ] && echo "lib_test: all passed" || { echo "lib_test: $fails failed"; exit 1; }
