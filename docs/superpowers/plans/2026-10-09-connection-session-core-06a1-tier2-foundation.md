# Plan 6a1: Tier 2 Foundation (scripts) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the script foundation every Tier 2 load scenario stands on: a shared library with the host guard, `result.json` and the report; run recording (background load, swap, CPU clocks); the one-time root host setup with its narrow DROP helper; and a process stack pinned by core type. `store.sh` and `race.sh` move onto the library.

**Architecture:** Bash only, no Go changes. `scripts/tier2/lib.sh` is sourced by every Tier 2 scenario (P5bI-5). `scripts/stack.sh` starts redis-server, devtoken, Logic and N gateways as local processes from the calling shell (P5bI-1), pinned with `taskset` on Tier 2 (#71) and unpinned for the Tier 1a probe that Plan 6a2 adds. `scripts/tier2/host-setup.sh` is the one root step (#68). A new Tier 2 scenario, `foundation`, proves all of it on the host before any load exists. Each piece has a test script that runs on the dev machine (`make test-scripts`).

**Tech Stack:** bash 5.2 (Ubuntu 24.04), coreutils/util-linux (`taskset`, `setsid`), `curl`, `iptables` (nf_tables backend), `sudo`/`visudo`, the pinned `redis-server` 7.4.11 (`make redis-server`), python3 in the tests only (to validate JSON).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Verification → Test execution model* (Tier 2 row, *CPU isolation*, *Automation contract*). Roadmap choices #65–#71 and P5bI-1, P5bI-5 (`docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`, Plan 5b1 *Execution Notes*).

**Branch:** `feat/plan6a-foundation`, from `feat/plan5b-race` (head `1f4c3ce` or later); the branches keep stacking, as decided before 5b.

**Split (choice #72, decided 2026-10-09):** #65's Plan 6a is written as two plans, each in its own session. **6a1** (this plan) is the scripts. **6a2** is `internal/loadgen` + `cmd/loadgen` (clientconn with a light pinger, and the SDK, per P5I-2), a control API, per-client SLO stats, pprof on the admin ports, the Tier 1a load probe (≤ 5k connections) on `scripts/stack.sh`, and a Tier 2 smoke load. 6a2 is written after 6a1 runs on the Tier 2 host.

## Global Constraints

- Tier 2 scenarios are non-interactive, have one entry point each (`GOIM_TEST_TIER=2 make test-tier2 SCENARIO=<name>`), and exit 0 (every check passed), 1 (a check failed or the run stopped early) or 2 (host refused) (spec: *Automation contract*).
- The guard exits 2 unless the **caller** set `GOIM_TEST_TIER=2`, and on hosts with fewer than 12 cores or 24GB (P5bI-5; spec: *Host guard*). They must not run on the dev machine.
- The guard refuses while a kind cluster or its containers exist (#67).
- Teardown always runs: exit hooks run on any exit, including Ctrl-C and SIGTERM.
- CPU sets (#71): gateway `4-9,16-21`, Logic + Redis `10,11,22,23`, loadgen `0-3,12-15`.
- Root is used only through `sudo scripts/tier2/host-setup.sh` (once, by hand) and `sudo -n /usr/local/sbin/goim-netdrop` (#68). No other script calls sudo.
- Loadgen sources are 127.0.0.x and the ephemeral range stays at its default (#69): nothing in this plan adds IP aliases.
- Swap is recorded, not turned off; a measured window with any page swapped in fails `no_swap_in` (#70).
- No Docker on the dev machine; `store.sh` keeps the compose Redis on the Tier 2 host (unchanged).
- No Go code changes in this plan.

## Review Focus

1. **A run that dies half way leaves processes or a DROP rule behind**: Ctrl-C, SIGTERM or a failing step must still stop the stack and flush `goim-netdrop` rules. Task 1 tests that the exit hooks and the report run on SIGTERM and SIGINT (bash runs the EXIT trap on those signals by itself; the explicit traps fix the exit codes at 130/143, so removing them does not fail the test). Task 5's `foundation.sh` registers `stack_down` and `goim-netdrop flush` as hooks.
2. **The host setup goes stale**: someone edits `sysctl.conf` or `goim-netdrop` and forgets to re-run the setup, so a run silently uses old limits or an old helper. Task 4's check compares the running values with the checkout's files (every field of `tcp_mem`) and the installed helper byte for byte, and refuses with the command to run.
3. **A leftover stack from a SIGKILLed run holds the ports**: the next run must refuse instead of fighting over them. Task 5 tests that `stack_up` refuses busy ports before it starts anything.
4. **Desktop load on the host distorts numbers**: the guard can't prevent it, so it must be visible. Task 3 tests that CPU used by another process group is counted, and that this run's own group is not (named processes, robust to other load on the machine).
5. **Strings from the run break `result.json`**: failing test names, error text from `stack_up`, or odd process names (a quote, a newline, a control byte) must not corrupt the JSON. Task 1 tests `t2_json_str`; Task 3's process names are reduced to `[A-Za-z0-9._:/-]`; every test that writes `result.json` validates it with python3.

## Implementation choices this plan makes

Numbered after the roadmap's #71. **#73–#77 decided 2026-10-09** (plan review by Claude at the user's request; #75 and #76 amended, see *Execution Notes → Plan review*):

72. **Plan 6a split** into 6a1 (scripts) and 6a2 (loadgen), decided by the user 2026-10-09 (see *Split* above).
73. **`result.json` schema 1**: `schema`, `scenario`, `tier`, `commit`, `dirty`, `host {cores, mem_gb, kernel}`, `pass`, `duration_s {total, …}`, `checks {name: {pass, …}}`, members a scenario adds (`background`, `mem_budget`, `redis`, …), `not_covered`. A run passes only if it recorded at least one check and all passed. Next to it, `<dir>.report.txt` is the one file to send back. Built in bash without jq (jq is not known to be on the Tier 2 host); the tests validate it with python3.
74. **"Ours" is the run's process group**: make, the scenario script and every process it starts (none calls `setpgid`; the race harness only sets `Pdeathsig`). `background` reports host busy CPU, CPU used by other user processes (with the ten biggest), and kernel threads apart, since they do our network work. A process that starts and exits inside the window is not seen. Clocks are sampled every 5s from `scaling_cur_freq` and summarized per CPU set (`null` where the host reports none, e.g. the dev VM).
75. **Host setup values live in files** that the setup installs and the guard checks: `scripts/tier2/sysctl.conf` (`nf_conntrack_max` 4,194,304 and buckets 1,048,576; `somaxconn` and `tcp_max_syn_backlog` 65,535; `tcp_mem` 4/6/8GB; inotify 8192 instances / 1,048,576 watches) and `scripts/tier2/modules.conf` (`nf_conntrack`, `ip_vs` with `rr`, `wrr`, `sh`, `lc`). `goim-netdrop` drops on `lo` in `INPUT`, by destination port (client to gateway) and by source port (gateway to client), tagged `goim-netdrop`; it validates the port (1024–65535), uses a fixed `PATH` and reads no environment variable. `flush` removes every tagged rule, deleting by rule spec (never by line number), and is a teardown hook; `list` shows exactly those rules; without iptables every non-`--print` command fails, so the guard's `sudo -n … list` catches it.
76. **`scripts/stack.sh`** is shared by Tier 1a and Tier 2: fixed ports from `STACK_PORT_BASE` (17000, below the race suite's 20000–32000 and the ephemeral range; refused if any is busy), one non-Cluster `redis-server` (Cluster arrives in 6c), production timing, devtoken serving the JWKS, `-kick-addr-file` in place of pod DNS, Noise `key_id` 1, gateways listening on 127.0.0.1. `stack.env` is the contract loadgen (6a2) reads. Redis, devtoken and Logic share the Logic + Redis CPU set. `stack_up` fails with Redis's log if it does not answer within 10s. `stack_down` sends SIGTERM and SIGKILLs whatever is left after 10s.
77. **`make test-scripts`** runs `lib_test.sh` and `stack_test.sh` on any machine (python3, curl, redis-server). It is a Tier 1a check for changes under `scripts/`, not part of `make test`. The `foundation` scenario (Task 5) is the first Tier 2 run of this plan.
78. **`stack_down` stops in dependency order** (decided 2026-10-09 by Claude at the user's request, after Tier 2 run 11 observation 1): gateways first (SIGTERM together, up to `STACK_DOWN_WAIT` 10s, then SIGKILL), then Logic, devtoken and Redis, each up to 5s. Logic stays up while the gateways drain, so a drain no longer loses its lease renewals, and teardown stays bounded. A drain that a scenario measures (6b) uses a separate `stack_stop NAME WAIT` with the full drain bound (85s `-drain-timeout` + margin), added in 6b.
79. **Each gateway's `GOMAXPROCS` is its CPU share** (decided 2026-10-09 by Claude at the user's request, after run 11 observation 2): Go sizes `GOMAXPROCS` from the affinity mask, so 4 gateways pinned to the shared 12-CPU set each ran 12 Ps and 12 gnet loops (48 on 12 CPUs). `stack.sh` sets `GOMAXPROCS = max(1, gateway CPUs / STACK_GATEWAYS)` per gateway (3 on the host; nproc/N unpinned), overridable with `STACK_GATEWAY_PROCS`; `-loops` and handshake concurrency follow their `GOMAXPROCS` defaults. This mirrors production, where Go sizes `GOMAXPROCS` from the pod's cgroup CPU limit. Each gateway keeps the whole shared set rather than its own CPUs, because 12 CPUs (6 cores × 2 SMT) do not split into 4 along core boundaries. Consequence: handshake concurrency is 1 per gateway, the production default for a 3-CPU pod, which 6b's storm measures.

---

## File Structure

| File | Responsibility |
|---|---|
| `scripts/tier2/lib.sh` | guard (`t2_init`, verdicts), checks and `result.json` (`t2_check`, `t2_finish`), report, exit hooks; run recording (`t2_record_start`/`_end`, `t2_mem_budget`, `t2_nofile`); host-setup check (`t2_require_host_setup`) |
| `scripts/tier2/lib_test.sh` | tests for `lib.sh`, `host-setup.sh` (test mode) and `goim-netdrop --print` |
| `scripts/tier2/race.sh`, `store.sh` (modify) | moved onto `lib.sh`; the same checks |
| `scripts/tier2/sysctl.conf`, `modules.conf` | what `host-setup.sh` installs and the guard checks |
| `scripts/tier2/host-setup.sh` | one-time root setup (#68), idempotent, test mode `--root DIR --user NAME` |
| `scripts/tier2/goim-netdrop` | the root-owned DROP helper for scenario (b2) |
| `scripts/stack.sh` | build, keys, start, wait Ready, snapshot, pin check, teardown of a local stack |
| `scripts/stack_test.sh` | a real 2-gateway stack on the dev machine |
| `scripts/tier2/foundation.sh` | Tier 2 scenario: setup, pinned 4-gateway stack, recording, artifacts, netdrop round trip |
| `Makefile` (modify) | `test-scripts`; the `test-tier2` comment |

---

### Task 1: `lib.sh`: guard, checks, `result.json`, report

**Files:**
- Create: `scripts/tier2/lib.sh`
- Create: `scripts/tier2/lib_test.sh`
- Modify: `Makefile`

**Interfaces:**
- Consumes: nothing.
- Produces (sourced bash functions; Tasks 2–5 use them): `t2_init SCENARIO` (sets `T2_SCENARIO`, `T2_OUT`, `T2_REPORT`, `T2_CORES`, `T2_MEM_GB`, `T2_T0`; exits 2 on refusal), `t2_traps`, `t2_refuse MSG` (exit 2; the report shows MSG), `t2_need TOOL...`, `t2_check NAME true|false [MEMBERS]`, `t2_bool CMD...`, `t2_json_str S`, `t2_field KEY JSON`, `t2_duration NAME SECONDS`, `t2_not_covered TEXT...`, `t2_on_exit CMD`, `t2_section TITLE`, `t2_finish` (exits 0/1), the optional hook `t2_report_extra`; variables `T2_CPUS_GATEWAY`, `T2_CPUS_LOGIC`, `T2_CPUS_LOADGEN`, `T2_MIN_CORES`, `T2_MIN_MEM_GB`; pure verdicts `t2_guard_verdict TIER CORES MEM_GB`, `t2_kind_verdict CLUSTERS CONTAINERS`.

- [ ] **Step 1: Write the failing test**

Create `scripts/tier2/lib_test.sh` (and `chmod +x` it):

```bash
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

[ "$fails" -eq 0 ] && echo "lib_test: all passed" || { echo "lib_test: $fails failed"; exit 1; }
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `scripts/tier2/lib_test.sh`
Expected: exits non-zero; `lib.sh` does not exist (`No such file or directory`).

- [ ] **Step 3: Write `lib.sh`**

Create `scripts/tier2/lib.sh`:

```bash
# shellcheck shell=bash
# lib.sh: what every Tier 2 scenario shares (P5bI-5, Plan 6a1). Source it,
# then call t2_init first:
#
#   . scripts/tier2/lib.sh
#   t2_init race                  # guard, output dir, exit trap
#   ... run, then record checks:
#   t2_check tests "$(t2_bool [ "$rc" -eq 0 ])" "\"failed\": $(t2_json_str "$failed")"
#   t2_finish                     # writes result.json, exits 0 (pass) or 1
#
# Exit codes: 0 every check passed, 1 a check failed or the run stopped
# early, 2 the host was refused (spec: Automation contract).
#
# Output: tier2-results/<scenario>-<timestamp>/ holds result.json and the
# artifacts; tier2-results/<scenario>-<timestamp>.report.txt is the ONE file
# to send back. result.json (schema 1, choice #73):
#   {"schema": 1, "scenario", "tier": 2, "commit", "dirty", "host": {...},
#    "pass", "duration_s": {"total", ...}, "checks": {name: {"pass", ...}},
#    <fields added with t2_field>, "not_covered": [...]}
# A run passes only if it recorded at least one check and every check passed.

# CPU sets by core type on the Tier 2 host (choice #71); scripts/stack.sh
# pins with them. Overridable for another host.
: "${T2_CPUS_GATEWAY:=4-9,16-21}"
: "${T2_CPUS_LOGIC:=10,11,22,23}"
: "${T2_CPUS_LOADGEN:=0-3,12-15}"

T2_MIN_CORES=12
T2_MIN_MEM_GB=24

# t2_guard_verdict TIER CORES MEM_GB: prints why a host is refused and
# returns 1, or returns 0. Pure, so it is tested on any machine.
t2_guard_verdict() {
  local tier=$1 cores=$2 mem=$3
  if [ "$tier" != 2 ]; then
    echo "refused: Tier 2 scenarios need GOIM_TEST_TIER=2 set by the caller, e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race"
    return 1
  fi
  local v
  for v in "$cores" "$mem"; do
    case "$v" in
      ''|*[!0-9]*) echo "refused: cannot determine cores/RAM (cores='$cores', mem_gb='$mem')"; return 1 ;;
    esac
  done
  if [ "$cores" -lt "$T2_MIN_CORES" ] || [ "$mem" -lt "$T2_MIN_MEM_GB" ]; then
    echo "refused: Tier 2 needs >= $T2_MIN_CORES cores and >= ${T2_MIN_MEM_GB}GB RAM (have $cores cores, ${mem}GB)"
    return 1
  fi
}

# t2_kind_verdict CLUSTERS CONTAINERS: refuses while a kind cluster or its
# node containers exist (choice #67): Tier 3 leftovers distort Tier 2 numbers.
t2_kind_verdict() {
  if [ -n "$1$2" ]; then
    echo "refused: a kind cluster is up (clusters: ${1:-none}; containers: ${2:-none}); delete it first: kind delete clusters --all"
    return 1
  fi
}

# t2_json_str S: S as a JSON string literal.
t2_json_str() {
  local s=$1
  s=${s//\\/\\\\}
  s=${s//\"/\\\"}
  s=${s//$'\n'/\\n}
  s=${s//$'\r'/\\r}
  s=${s//$'\t'/\\t}
  s=$(printf '%s' "$s" | tr -d '\000-\010\013\014\016-\037')
  printf '"%s"' "$s"
}

# t2_bool CMD...: prints true if CMD succeeds, else false.
t2_bool() { if "$@"; then echo true; else echo false; fi; }

# t2_refuse MSG: refuses the run (exit 2). After t2_init the report shows
# MSG, so a refusal by a later check is visible in the one file sent back.
t2_refuse() { T2_REFUSED=$1; echo "$1" >&2; exit 2; }

# t2_need TOOL...: refuses (exit 2) when a tool is missing.
t2_need() {
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null || t2_refuse "refused: $tool not installed"
  done
}

T2_CHECKS=()
T2_FIELDS=()
T2_DURATIONS=()
T2_NOT_COVERED=()
T2_EXIT_HOOKS=()

# t2_check NAME PASS [MEMBERS]: records a check; PASS is true or false,
# MEMBERS more JSON object members, e.g. "\"failed\": \"x\", \"n\": 3".
t2_check() {
  case "$2" in true|false) ;; *) echo "t2_check $1: pass must be true or false, got '$2'" >&2; exit 1 ;; esac
  T2_CHECKS+=("$(t2_json_str "$1"): {\"pass\": $2${3:+, $3}}")
}
# t2_field KEY JSON: a top-level member of result.json.
t2_field() { T2_FIELDS+=("$(t2_json_str "$1"): $2"); }
# t2_duration NAME SECONDS: a member of duration_s.
t2_duration() { T2_DURATIONS+=("$(t2_json_str "$1"): $2"); }
# t2_not_covered TEXT...: what this scenario does not verify.
t2_not_covered() { local s; for s in "$@"; do T2_NOT_COVERED+=("$(t2_json_str "$s")"); done; }
# t2_on_exit CMD: runs CMD (eval'd) at exit, newest first, before the report.
t2_on_exit() { T2_EXIT_HOOKS+=("$1"); }

t2_join() { local IFS=,; echo "$*"; }

# t2_result_json PASS: the result document.
t2_result_json() {
  cat <<JSON
{
  "schema": 1,
  "scenario": $(t2_json_str "$T2_SCENARIO"),
  "tier": 2,
  "commit": $(t2_json_str "$(git rev-parse HEAD 2>/dev/null)"),
  "dirty": $(git status --porcelain 2>/dev/null | wc -l),
  "host": {"cores": $T2_CORES, "mem_gb": $T2_MEM_GB, "kernel": $(t2_json_str "$(uname -r)")},
  "pass": $1,
  "duration_s": {"total": $((SECONDS - T2_T0))${T2_DURATIONS[0]+, $(t2_join "${T2_DURATIONS[@]}")}},
  "checks": {$(t2_join "${T2_CHECKS[@]}")}${T2_FIELDS[0]+,
  $(t2_join "${T2_FIELDS[@]}")},
  "not_covered": [$(t2_join "${T2_NOT_COVERED[@]}")]
}
JSON
}

# t2_finish: writes result.json and exits 0 if every check passed, else 1.
t2_finish() {
  local pass=true c
  [ "${#T2_CHECKS[@]}" -gt 0 ] || pass=false
  for c in "${T2_CHECKS[@]}"; do
    case "$c" in *'{"pass": false'*) pass=false ;; esac
  done
  t2_result_json "$pass" >"$T2_OUT/result.json"
  cat "$T2_OUT/result.json"
  $pass && exit 0 || exit 1
}

t2_section() { printf '\n===== %s =====\n' "$1"; }

# t2_write_report: environment, result.json, then the scenario's own
# sections (define t2_report_extra in the scenario script).
t2_write_report() {
  {
    echo "goim Tier 2 report: scenario $T2_SCENARIO"
    t2_section "environment"
    echo "date:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit:  $(git rev-parse HEAD) ($(git rev-parse --abbrev-ref HEAD))"
    echo "dirty:   $(git status --porcelain | wc -l) changed files"
    echo "host:    $T2_CORES cores, ${T2_MEM_GB}GB RAM, $(uname -sr)"
    echo "go:      $(go version)"
    if [ -n "${T2_REFUSED:-}" ]; then
      t2_section "refused"
      echo "$T2_REFUSED"
    fi
    t2_section "result.json"
    cat "$T2_OUT/result.json" 2>/dev/null || echo "(not written: the run stopped early)"
    if declare -F t2_report_extra >/dev/null; then t2_report_extra; fi
  } >"$T2_REPORT" 2>&1
}

t2_exit() {
  local rc=$? i
  trap - EXIT
  for ((i = ${#T2_EXIT_HOOKS[@]} - 1; i >= 0; i--)); do
    eval "${T2_EXIT_HOOKS[i]}" >/dev/null 2>&1
  done
  t2_write_report
  echo "report: $T2_REPORT"
  exit "$rc"
}

# t2_traps: the exit hooks and the report run on any exit, including
# Ctrl-C and SIGTERM, so no process or DROP rule outlives the run.
t2_traps() {
  trap t2_exit EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
}

# t2_kind_state: prints "CLUSTERS|CONTAINERS" as seen by kind and docker.
t2_kind_state() {
  local clusters="" containers=""
  if command -v kind >/dev/null; then clusters=$(kind get clusters 2>/dev/null | paste -sd' ' -); fi
  if command -v docker >/dev/null; then
    containers=$(docker ps -a --filter label=io.x-k8s.kind.cluster --format '{{.Names}}' 2>/dev/null | paste -sd' ' -)
  fi
  echo "$clusters|$containers"
}

# t2_init SCENARIO: the host guard, then the output directory and the exit
# trap. Refusals exit 2 before anything is written.
t2_init() {
  T2_SCENARIO=$1
  cd "$(git rev-parse --show-toplevel)" || exit 2
  T2_CORES=$(getconf _NPROCESSORS_ONLN 2>/dev/null)
  T2_MEM_GB=$(awk '/^MemTotal:/ {printf "%d", $2 / 1024 / 1024}' /proc/meminfo 2>/dev/null)
  local why kind
  why=$(t2_guard_verdict "${GOIM_TEST_TIER:-}" "$T2_CORES" "$T2_MEM_GB") || { echo "$why" >&2; exit 2; }
  t2_need go gcc make git
  kind=$(t2_kind_state)
  why=$(t2_kind_verdict "${kind%%|*}" "${kind#*|}") || { echo "$why" >&2; exit 2; }
  export GOIM_TEST_TIER CGO_ENABLED=1
  T2_T0=$SECONDS
  T2_OUT="$PWD/tier2-results/$T2_SCENARIO-$(date +%Y%m%d-%H%M%S)"
  T2_REPORT="$T2_OUT.report.txt"
  mkdir -p "$T2_OUT"
  t2_traps
}
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `scripts/tier2/lib_test.sh`
Expected: every line `ok`, last line `lib_test: all passed`. On a host with 12 or more cores the line `init: this small host is refused` is skipped.

Check that the tests can fail: in `t2_guard_verdict`, change `[ "$mem" -lt "$T2_MIN_MEM_GB" ]` to `false`: expect `FAIL guard: 23GB is refused`. Make `t2_finish` ignore failed checks (replace its `case "$c" in ... esac` line with `:`): expect `FAIL finish: a failed check exits 1`. Revert both.

- [ ] **Step 5: Add `make test-scripts`**

In `Makefile`, add `test-scripts` to `.PHONY`:

```make
.PHONY: test test-race-suite test-scripts redis-server test-redis-local proto redis-up redis-down test-redis test-tier2
```

Add this target after `test-race-suite`:

```make
# The Tier 2 and load-run scripts (Plan 6a1), on any machine: lib.sh's guard,
# result.json, recording and host-setup checks, and a real stack.sh stack.
# Needs python3, curl and redis-server (make redis-server).
test-scripts:
	scripts/tier2/lib_test.sh
```

Replace the comment above `test-tier2:` with:

```make
# Tier 2 scenario entry point (spec: automation contract). The caller sets
# GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
# scenarios: store, race, foundation, probe (read-only, no guard).
```

Run: `make test-scripts`
Expected: `lib_test: all passed`.

- [ ] **Step 6: Commit**

```bash
git add scripts/tier2/lib.sh scripts/tier2/lib_test.sh Makefile
git commit -m "test(tier2): lib.sh with the host guard, result.json and the report (P5bI-5)"
```

---

### Task 2: `race.sh` and `store.sh` on `lib.sh`

**Files:**
- Modify: `scripts/tier2/race.sh` (whole file below)
- Modify: `scripts/tier2/store.sh` (whole file below)

**Interfaces:**
- Consumes: Task 1's `t2_init`, `t2_need`, `t2_on_exit`, `t2_report_extra`, `t2_section`, `t2_duration`, `t2_check`, `t2_bool`, `t2_json_str`, `t2_field`, `t2_not_covered`, `t2_finish`, `T2_OUT`, `T2_T0`.
- Produces: the same four checks each as before (`race`: tests, all_ran, mutation, redis; `store`: tests, no_skips, mutation, eviction), in schema 1. Runs now need the caller's `GOIM_TEST_TIER=2`.

The checks, the mutants and the log handling are unchanged; what moves to `lib.sh` is the guard, the output paths, the teardown, the report frame and the JSON.

- [ ] **Step 1: Replace `scripts/tier2/race.sh`**

```bash
#!/usr/bin/env bash
# Tier 2: run the race integration suite (Plan 5b) on the Tier 2 host. The
# binaries run as local processes (choice #47), with a redis-server primary
# and replica per scenario, the pinned 7.4.11 that make redis-server builds
# (choice #62): the same suite as the dev machine, on a host with CPU to spare.
#
# Usage (on the Tier 2 host, from the repo root):
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race
#   The host guard (scripts/tier2/lib.sh) refuses smaller hosts.
#   Results: tier2-results/race-<timestamp>/ (result.json, tests.log, the
#   process logs of every scenario), plus ONE file to send back if anything
#   fails: tier2-results/race-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. tests:    every scenario passes (GOIM_RACE_SUITE=1, binaries built with -race)
#   2. all_ran:  every scenario ran (passed, not skipped): `go test -list` count = PASS count
#   3. mutation: with fence (a) removed (a closed connection's Logout is sent
#                before its Login reply), TestRace5CloseWhileLoginInFlight fails
#   4. redis:    the suite ran on the pinned redis-server 7.4.11
set -uo pipefail

. "$(dirname "$0")/lib.sh"
t2_init race
t2_need curl
export GOIM_RACE_SUITE=1
export GOIM_REDIS_SERVER="$PWD/.cache/redis/bin/redis-server"
redis_version=7.4.11
out=$T2_OUT
mkdir -p "$out/logs"
export GOIM_RACE_LOGS="$out/logs"
mutdir=""
t2_on_exit '[ -n "$mutdir" ] && git worktree remove --force "$mutdir"'

t2_report_extra() {
  t2_section "tools"
  echo "gcc:     $(gcc --version | head -1)"
  echo "redis:   $("$GOIM_REDIS_SERVER" --version 2>&1)"
  echo "ports:   suite 20000-32000; ephemeral $(cat /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null)"
  t2_section "redis-build.log"
  tail -n 50 "$out/redis-build.log" 2>/dev/null
  t2_section "tests.log (RUN/PASS lines removed; failing scenarios include their process logs)"
  if [ -f "$out/tests.log" ]; then
    grep -vE '^[[:space:]]*(=== (RUN|PAUSE|CONT)|--- PASS)' "$out/tests.log" | head -n 4000
  fi
  t2_section "mutation.log"
  tail -n 200 "$out/mutation.log" 2>/dev/null
}

if ! make redis-server >"$out/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $out/redis-build.log" >&2
  exit 1
fi

t2_duration setup $((SECONDS - T2_T0))

# 1 + 2: the suite. Each scenario starts its own Redis primary and replica.
t=$SECONDS
listed=$(GOIM_RACE_SUITE= go test -list '.*' ./internal/racetest/ | grep -c '^Test') # no suite: lists without building the binaries
go test -count=1 -v -timeout 45m ./internal/racetest/ >"$out/tests.log" 2>&1
tests_rc=$?
passed=$(grep -cE '^--- PASS' "$out/tests.log")
failed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)
tests_s=$((SECONDS - t))

# 3: mutation check in a throwaway worktree. The mutant sends a closed
# connection's Logout as soon as it closes while its Login is in flight
# (fence (a) removed): the Logout lands before the write and a ghost stays.
t=$SECONDS
mutdir=$(mktemp -d)
if ! git worktree add --detach "$mutdir" HEAD >"$out/mutation.log" 2>&1; then
  rmdir "$mutdir" 2>/dev/null
  mutdir=""
  mutation="error: git worktree add failed, see mutation.log"
elif ! sed -i.orig 's/if c.outcome == loginOK || c.fenceB { \/\/ fence (a) or (b)/if c.outcome == loginOK || c.outcome == loginPending || c.fenceB { \/\/ MUTANT: no fence (a)/' "$mutdir/internal/gateway/gateway.go" ||
  ! grep -q 'MUTANT: no fence (a)' "$mutdir/internal/gateway/gateway.go"; then
  mutation="error: could not apply the mutation"
elif ! (cd "$mutdir" && go vet ./internal/gateway/) >>"$out/mutation.log" 2>&1; then
  mutation="error: the mutant does not compile"
elif (cd "$mutdir" && GOIM_RACE_LOGS= go test -count=1 -v -run '^TestRace5CloseWhileLoginInFlight$' ./internal/racetest/) >>"$out/mutation.log" 2>&1; then
  mutation="fail: race 5 still passes without fence (a)"
elif grep -q -- '--- FAIL: TestRace5CloseWhileLoginInFlight' "$out/mutation.log" && grep -q 'no ghost\|fenced Logout' "$out/mutation.log"; then
  mutation="pass"
else
  mutation="error: the test failed for another reason, see mutation.log"
fi
mutation_s=$((SECONDS - t))

# 4: the Redis the suite reported it ran (TestMain prints it).
ran_on=$(grep -m1 '^racetest: .*: Redis ' "$out/tests.log" | sed 's/.*: Redis //')

t2_check tests "$(t2_bool [ "$tests_rc" -eq 0 ])" "\"failed\": $(t2_json_str "$failed"), \"duration_s\": $tests_s"
t2_check all_ran "$(t2_bool [ "$passed" -eq "$listed" ])" "\"listed\": $listed, \"passed\": $passed"
t2_check mutation "$(t2_bool [ "$mutation" = pass ])" "\"detail\": $(t2_json_str "$mutation"), \"duration_s\": $mutation_s"
t2_check redis "$(t2_bool [ "$ran_on" = "$redis_version" ])" "\"want\": \"$redis_version\", \"ran_on\": $(t2_json_str "$ran_on")"
t2_not_covered "Redis Cluster (one primary + one replica here)" "a real network between the processes (all on 127.0.0.1)"
t2_finish
```

- [ ] **Step 2: Replace `scripts/tier2/store.sh`**

```bash
#!/usr/bin/env bash
# Tier 2: run Plan 1's store tests against a real Redis primary + replica.
#
# Usage (on the Tier 2 host, from the repo root):
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=store
#   The host guard (scripts/tier2/lib.sh) refuses smaller hosts.
#   Results: tier2-results/store-<timestamp>/ (result.json and the raw logs), plus
#   ONE file to send back if anything fails: tier2-results/store-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. tests:    `make test-redis` passes (every test on real Redis 7.4, race detector on)
#   2. no_skips: no test was skipped as "TODO: Tier 2 integration"
#   3. mutation: with Revoke's WAIT moved to a pooled connection,
#                TestRevokeFailsWhenReplicaDoesNotAck fails (the test catches the bug)
#   4. eviction: the test Redis reports maxmemory-policy noeviction
set -uo pipefail

. "$(dirname "$0")/lib.sh"
t2_init store
t2_need docker
export GOIM_REDIS_PORT="${GOIM_REDIS_PORT:-16379}" GOIM_REDIS_REPLICA_PORT="${GOIM_REDIS_REPLICA_PORT:-16380}"
export GOIM_TEST_REDIS="127.0.0.1:$GOIM_REDIS_PORT" GOIM_TEST_REDIS_REPLICA="127.0.0.1:$GOIM_REDIS_REPLICA_PORT"
out=$T2_OUT
mutdir=""
t2_on_exit 'make redis-down'
# Exit hooks run before the report, newest first: save the Redis logs before redis-down.
t2_on_exit 'docker compose -f deploy/compose/redis-test.yml logs --no-color --tail 100 >"$out/redis.log" 2>&1'
t2_on_exit '[ -n "$mutdir" ] && git worktree remove --force "$mutdir"'

# The report: the log lines that matter (passing-test noise removed).
t2_report_extra() {
  t2_section "tools"
  echo "gcc:     $(gcc --version | head -1)"
  echo "docker:  $(docker --version); $(docker compose version 2>&1 | head -1)"
  echo "ports:   primary $GOIM_REDIS_PORT, replica $GOIM_REDIS_REPLICA_PORT"
  t2_section "redis-up.log"
  cat "$out/redis-up.log" 2>/dev/null
  t2_section "tests.log (RUN/PASS lines removed)"
  if [ -f "$out/tests.log" ]; then
    grep -vE '^[[:space:]]*(=== (RUN|PAUSE|CONT)|--- PASS)' "$out/tests.log" | head -n 3000
  fi
  t2_section "mutation.log"
  tail -n 200 "$out/mutation.log" 2>/dev/null
  t2_section "redis logs (last 100 lines each)"
  cat "$out/redis.log" 2>/dev/null
}

make redis-down >/dev/null 2>&1   # idempotent: start from a clean Redis

# The host ports must be free; name whatever holds them.
for port in "$GOIM_REDIS_PORT" "$GOIM_REDIS_REPLICA_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    {
      echo "port $port is already in use. Pick free ports, e.g.:"
      echo "  GOIM_REDIS_PORT=26379 GOIM_REDIS_REPLICA_PORT=26380 make test-tier2 SCENARIO=store"
      echo "holder:"
      command -v lsof >/dev/null && lsof -nP -iTCP:"$port" -sTCP:LISTEN
      docker ps --filter "publish=$port" --format '  container {{.Names}} ({{.Image}}) {{.Ports}}'
    } | tee "$out/redis-up.log" >&2
    exit 1
  fi
done
if ! make redis-up >"$out/redis-up.log" 2>&1; then
  echo "redis-up failed, see $out/redis-up.log" >&2
  exit 1
fi

t2_duration setup $((SECONDS - T2_T0))

# 1 + 2: full suite on real Redis.
t=$SECONDS
go test -race -count=1 -v ./... >"$out/tests.log" 2>&1
tests_rc=$?
skips=$(grep -c 'TODO: Tier 2 integration' "$out/tests.log")
failed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)
tests_s=$((SECONDS - t))

# 3: mutation check in a throwaway worktree, so the checkout is never modified.
# The mutant sends WAIT through node (the pooled *redis.Client) instead of the
# Conn that wrote the marker. It counts as caught only if it compiles and the
# test fails its own assertion; a build error or any other failure is an error.
t=$SECONDS
mutdir=$(mktemp -d)
git worktree add --detach "$mutdir" HEAD >/dev/null 2>&1
sed -i.orig 's/conn\.Wait(ctx, 1, s\.cfg\.WaitTimeout)/node.Wait(ctx, 1, s.cfg.WaitTimeout)/' "$mutdir/internal/store/revoke.go"
if ! grep -q 'node.Wait(ctx, 1' "$mutdir/internal/store/revoke.go"; then
  mutation="error: could not apply the mutation"
elif ! (cd "$mutdir" && go vet ./internal/store/) >"$out/mutation.log" 2>&1; then
  mutation="error: the mutant does not compile"
elif (cd "$mutdir" && go test -race -count=1 -v -run '^TestRevokeFailsWhenReplicaDoesNotAck$' ./internal/store/) >>"$out/mutation.log" 2>&1; then
  mutation="fail: test still passes with WAIT on a pooled connection"
elif grep -q -- '--- FAIL: TestRevokeFailsWhenReplicaDoesNotAck' "$out/mutation.log" &&
     grep -q 'want ErrNotReplicated' "$out/mutation.log"; then
  mutation="pass"
else
  mutation="error: the test failed for another reason, see mutation.log"
fi
mutation_s=$((SECONDS - t))

# 4: eviction policy (Execution Notes risk 12).
policy=$(docker compose -f deploy/compose/redis-test.yml exec -T redis-primary \
  redis-cli -p 6379 config get maxmemory-policy | tail -1 | tr -d '\r')

t2_field redis "$(t2_json_str "$(docker compose -f deploy/compose/redis-test.yml exec -T redis-primary redis-server --version | tr -d '\r')")"
t2_check tests "$(t2_bool [ "$tests_rc" -eq 0 ])" "\"failed\": $(t2_json_str "$failed"), \"duration_s\": $tests_s"
t2_check no_skips "$(t2_bool [ "$skips" -eq 0 ])" "\"tier2_skips\": $skips"
t2_check mutation "$(t2_bool [ "$mutation" = pass ])" "\"detail\": $(t2_json_str "$mutation"), \"duration_s\": $mutation_s"
t2_check eviction "$(t2_bool [ "$policy" = noeviction ])" "\"maxmemory_policy\": $(t2_json_str "$policy")"
t2_not_covered "Redis Cluster: MasterForKey, MOVED after failover, multi-node NOSCRIPT, CRC16 key slots" \
  "lost replies after a write (needs a fault-injecting proxy)"
t2_finish
```

- [ ] **Step 3: Check them**

Run: `bash -n scripts/tier2/race.sh && bash -n scripts/tier2/store.sh && GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race; echo "exit=$?"`
Expected on the dev machine: `refused: Tier 2 needs >= 12 cores and >= 24GB RAM (have 2 cores, 1GB)`, `exit=2`, and no new directory under `tier2-results/`. Without `GOIM_TEST_TIER=2`: the `GOIM_TEST_TIER=2 set by the caller` message, exit 2.

Then run `race.sh` end to end with the guard lowered, in a throwaway worktree (the mutation check needs the change committed there, and the checkout must stay untouched):

```bash
w=$(mktemp -d) && git worktree add --detach "$w" HEAD && cp scripts/tier2/*.sh "$w/scripts/tier2/" && ln -s "$PWD/.cache" "$w/.cache"
sed -i 's/^T2_MIN_CORES=12/T2_MIN_CORES=1/; s/^T2_MIN_MEM_GB=24/T2_MIN_MEM_GB=1/' "$w/scripts/tier2/lib.sh"
(cd "$w" && git add -A scripts && git -c user.name=t -c user.email=t@t commit -qm lowered && GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race; echo "exit=$?")
```

Expected (~6 minutes on the dev machine): `result.json` printed with `"pass": true`, all four checks true (`"listed": 58, "passed": 58`, `"detail": "pass"`, `"ran_on": "7.4.11"`), `exit=0`, and `python3 -m json.tool "$w"/tier2-results/race-*/result.json` succeeds. The report has the sections environment, result.json, tools, redis-build.log, tests.log, mutation.log. Then `git worktree remove --force "$w"`. `store.sh` needs Docker, so it is first run on the Tier 2 host (Task 5, Step 7).

- [ ] **Step 4: Commit**

```bash
git add scripts/tier2/race.sh scripts/tier2/store.sh
git commit -m "test(tier2): race.sh and store.sh on lib.sh; GOIM_TEST_TIER comes from the caller"
```

---

### Task 3: Run recording: background load, swap, CPU clocks (#70, #71)

**Files:**
- Modify: `scripts/tier2/lib.sh` (append)
- Modify: `scripts/tier2/lib_test.sh` (insert)

**Interfaces:**
- Consumes: Task 1's `t2_field`, `t2_check`, `t2_bool`, `t2_on_exit`, `t2_join`, `T2_OUT`, `T2_CPUS_*`; the test's `fake_run`.
- Produces: `t2_mem_budget GB` (exit 2 below it), `t2_nofile N` (exit 2 if the hard limit is lower), `t2_record_start`, `t2_record_end` (adds the `background` member and the `no_swap_in` check), `t2_cpulist_expand LIST`, `t2_proc_stats`, `t2_pgid PID`, `t2_cpu_delta BEFORE AFTER OURS HZ`, `t2_mem_verdict WANT AVAIL`, `t2_top`. Files in `T2_OUT`: `background-before.txt`, `background-after.txt`, `clocks.tsv`.

- [ ] **Step 1: Write the failing tests**

In `scripts/tier2/lib_test.sh`, insert this block just before the last line (`[ "$fails" -eq 0 ] && echo "lib_test: all passed" ...`):

```bash
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
# bash tell them apart from whatever else runs.
ln -s "$(command -v bash)" "$tmp/t2outside"
ln -s "$(command -v bash)" "$tmp/t2ours"
rc=$(fake_run '
  t2_record_start
  setsid "$tmp/t2outside" -c "end=\$((\${EPOCHREALTIME/./} + 1200000)); while ((\${EPOCHREALTIME/./} < end)); do :; done; sleep 30; true" & other=$!
  "$tmp/t2ours" -c "end=\$((\${EPOCHREALTIME/./} + 1200000)); while ((\${EPOCHREALTIME/./} < end)); do :; done"
  sleep 1.5
  t2_record_end
  kill $other
  t2_finish')
expect "record: result.json is JSON" "$(valid_json "$tmp/run/out/result.json")" ""
r=$(python3 -c '
import json,sys
d = json.load(open(sys.argv[1])); b = d["background"]
top = {t["comm"]: t["cpu_s"] for t in b["top_outside"]}
print(top.get("t2outside", 0) >= 0.5, "t2ours" not in top, b["outside_user_cpu_s"] >= 0.5, "no_swap_in" in d["checks"], b["window_s"] >= 2)' "$tmp/run/out/result.json")
expect "record: the other group counts, ours does not" "$r" "True True True True True"
[ -s "$tmp/run/out/background-before.txt" ] && ok "record: background-before.txt" || bad "record: background-before.txt missing"
```

- [ ] **Step 2: Run them to make sure they fail**

Run: `scripts/tier2/lib_test.sh`
Expected: non-zero exit; `t2_cpulist_expand: command not found` and the `cpulist`, `mem`, `cpu delta`, `procs` and `record` lines FAIL.

- [ ] **Step 3: Append the recording functions to `lib.sh`**

Append to `scripts/tier2/lib.sh`:

```bash
# ---- Run recording (choices #70, #71): what else ran, swap, CPU clocks ----
#
# The Tier 2 host is also a desktop. A load scenario calls t2_mem_budget and
# t2_nofile before its setup, then t2_record_start / t2_record_end around its
# measured window. result.json gets a "background" member and a no_swap_in
# check: a window with any page swapped in fails (#70).
# "Ours" is this run's process group: make, the script and every process it
# starts (none of them calls setpgid). Kernel threads are reported apart,
# since they do our network work. CPU used by a process that started and
# exited inside the window is not seen.

# t2_mem_verdict WANT_GB AVAIL_GB: pure.
t2_mem_verdict() {
  if [ "$2" -lt "$1" ]; then
    echo "refused: this scenario needs ${1}GB available, the host has ${2}GB; close the desktop apps (top users in background-before.txt)"
    return 1
  fi
}

# t2_mem_budget GB: refuses (exit 2) when MemAvailable is below GB.
t2_mem_budget() {
  local avail why
  avail=$(awk '/^MemAvailable:/ {printf "%d", $2 / 1024 / 1024}' /proc/meminfo)
  t2_field mem_budget "{\"want_gb\": $1, \"available_gb\": $avail}"
  why=$(t2_mem_verdict "$1" "$avail") || { t2_top >"$T2_OUT/background-before.txt"; t2_refuse "$why"; }
}

# t2_nofile N: raises the soft nofile limit to N for this run and its
# children (the hard limit is 1,048,576 on the host, #68), or refuses.
t2_nofile() {
  local hard
  hard=$(ulimit -Hn)
  if [ "$hard" != unlimited ] && [ "$hard" -lt "$1" ]; then
    t2_refuse "refused: nofile hard limit $hard < $1"
  fi
  ulimit -Sn "$1"
}

# t2_top: the largest memory and CPU users.
t2_top() {
  echo "== by memory"
  ps -eo pid,pgid,rss,pcpu,etimes,comm --sort=-rss | head -n 11
  echo "== by CPU (lifetime average)"
  ps -eo pid,pgid,rss,pcpu,etimes,comm --sort=-pcpu | head -n 11
}

# t2_cpulist_expand LIST: "4-6,9" -> "4 5 6 9".
t2_cpulist_expand() {
  local part out=() i
  IFS=, read -ra parts <<<"$1"
  for part in "${parts[@]}"; do
    if [[ $part == *-* ]]; then
      for ((i = ${part%-*}; i <= ${part#*-}; i++)); do out+=("$i"); done
    else
      out+=("$part")
    fi
  done
  echo "${out[*]}"
}

# t2_proc_stats: one line per process, "pid pgrp ppid starttime ticks comm"
# (ticks = utime + stime), parsed after the last ')' so a comm with spaces
# or parentheses cannot shift the fields.
t2_proc_stats() {
  cat /proc/[0-9]*/stat 2>/dev/null | awk '{
    n = length($0); while (n > 0 && substr($0, n, 1) != ")") n--
    o = index($0, "(")
    comm = substr($0, o + 1, n - o - 1); gsub(/[^A-Za-z0-9._:\/-]/, "_", comm)
    split(substr($0, n + 2), f, " ")
    print $1, f[3], f[2], f[20], f[12] + f[13], comm
  }'
}

# t2_pgid PID: PID's process group.
t2_pgid() { t2_proc_stats | awk -v p="$1" '$1 == p { print $2 }'; }

# t2_cpu_delta BEFORE AFTER OURS HZ: from two t2_proc_stats files, prints
# "outside_user_s kernel_threads_s top" where top is a JSON array of the
# ten biggest outside users. Kernel threads: pid 2 and its children.
t2_cpu_delta() {
  awk -v ours="$3" -v hz="$4" '
    NR == FNR { before[$1 " " $4] = $5; next }
    $2 == ours { next }
    {
      d = $5 - before[$1 " " $4]
      if (d <= 0) next
      if ($1 == 2 || $3 == 2) { kern += d; next }
      user += d; by[$6] += d
    }
    END {
      top = ""
      for (k = 0; k < 10; k++) {
        best = ""; bv = 0
        for (c in by) if (by[c] > bv) { bv = by[c]; best = c }
        if (best == "") break
        top = top (top == "" ? "" : ", ") sprintf("{\"comm\": \"%s\", \"cpu_s\": %.1f}", best, bv / hz)
        delete by[best]
      }
      printf "%.1f %.1f [%s]\n", user / hz, kern / hz, top
    }' "$1" "$2"
}

# t2_busy_ticks: host-wide busy CPU ticks (all CPUs; idle and iowait excluded).
t2_busy_ticks() { awk '/^cpu / { print $2 + $3 + $4 + $7 + $8 + $9; exit }' /proc/stat; }
# t2_swap_pages: "pswpin pswpout" since boot.
t2_swap_pages() { awk '/^pswpin / { i = $2 } /^pswpout / { o = $2 } END { print i + 0, o + 0 }' /proc/vmstat; }

# t2_clock_sampler FILE: appends "unix_s cpu khz" for every CPU every 5s.
t2_clock_sampler() {
  local f
  f=(/sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_cur_freq)
  [ -r "${f[0]}" ] || return 0
  while :; do
    awk -v t="$(date +%s)" '{ split(FILENAME, a, "/"); print t, substr(a[6], 4), $1 }' "${f[@]}" >>"$1"
    sleep 5
  done
}

# t2_clock_summary FILE: {"gateway": {"min_mhz", "mean_mhz"}, ...} over the
# CPU sets of #71, or null when the host reports no clocks.
t2_clock_summary() {
  [ -s "$1" ] || { echo null; return; }
  local name list out=()
  for name in gateway logic loadgen; do
    case $name in
      gateway) list=$T2_CPUS_GATEWAY ;; logic) list=$T2_CPUS_LOGIC ;; loadgen) list=$T2_CPUS_LOADGEN ;;
    esac
    out+=("\"$name\": $(awk -v cpus=" $(t2_cpulist_expand "$list") " '
      index(cpus, " " $2 " ") { n++; s += $3; if (min == "" || $3 < min) min = $3 }
      END { if (n) printf "{\"min_mhz\": %d, \"mean_mhz\": %d}", min / 1000, s / n / 1000; else printf "null" }' "$1")")
  done
  echo "{$(t2_join "${out[@]}")}"
}

# t2_record_start: start of the measured window.
t2_record_start() {
  T2_PGID=$(t2_pgid $$)
  t2_top >"$T2_OUT/background-before.txt"
  t2_proc_stats >"$T2_OUT/.procs-before"
  T2_BUSY0=$(t2_busy_ticks)
  T2_SWAP0=$(t2_swap_pages)
  T2_REC_T0=$SECONDS
  t2_clock_sampler "$T2_OUT/clocks.tsv" &
  T2_SAMPLER=$!
  t2_on_exit 'kill $T2_SAMPLER'
}

# t2_record_end: end of the window: the background member and no_swap_in.
t2_record_end() {
  local hz busy swap1 delta in out
  kill "$T2_SAMPLER" 2>/dev/null
  wait "$T2_SAMPLER" 2>/dev/null
  hz=$(getconf CLK_TCK)
  t2_proc_stats >"$T2_OUT/.procs-after"
  t2_top >"$T2_OUT/background-after.txt"
  busy=$(( $(t2_busy_ticks) - T2_BUSY0 ))
  swap1=$(t2_swap_pages)
  in=$(( ${swap1% *} - ${T2_SWAP0% *} ))
  out=$(( ${swap1#* } - ${T2_SWAP0#* } ))
  read -r user kern top <<<"$(t2_cpu_delta "$T2_OUT/.procs-before" "$T2_OUT/.procs-after" "$T2_PGID" "$hz")"
  t2_field background "{\"window_s\": $((SECONDS - T2_REC_T0)), \"host_busy_cpu_s\": $(awk -v b="$busy" -v h="$hz" 'BEGIN { printf "%.1f", b / h }'), \"outside_user_cpu_s\": $user, \"kernel_threads_cpu_s\": $kern, \"top_outside\": $top, \"swap_in_pages\": $in, \"swap_out_pages\": $out, \"cpu_clocks\": $(t2_clock_summary "$T2_OUT/clocks.tsv")}"
  t2_check no_swap_in "$(t2_bool [ "$in" -eq 0 ])" "\"swap_in_pages\": $in"
}
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `for i in 1 2 3; do scripts/tier2/lib_test.sh | tail -1; done`
Expected: `lib_test: all passed` three times. The `record` test spins 1.2s in each of two named copies of bash; it holds while other processes load the machine (it was run next to the race suite).

Check that the tests can fail: in `t2_cpu_delta`, change `$2 == ours { next }` to `0 { next }`: expect `FAIL cpu delta` and `FAIL record: the other group counts, ours does not`. Change `if ($1 == 2 || $3 == 2)` to `if (0)`: expect `FAIL cpu delta`. Revert both.

- [ ] **Step 5: Commit**

```bash
git add scripts/tier2/lib.sh scripts/tier2/lib_test.sh
git commit -m "test(tier2): record background CPU and memory, swap-ins and CPU clocks (#70, #71)"
```

---

### Task 4: One-time host setup and `goim-netdrop` (#68)

**Files:**
- Create: `scripts/tier2/sysctl.conf`, `scripts/tier2/modules.conf`
- Create: `scripts/tier2/host-setup.sh`, `scripts/tier2/goim-netdrop` (both `chmod +x`)
- Modify: `scripts/tier2/lib.sh` (append), `scripts/tier2/lib_test.sh` (insert)

**Interfaces:**
- Consumes: Task 1's test helpers (`expect`, `contains`, `tmp`).
- Produces: `t2_require_host_setup` (exit 2 with the missing items and `run once: sudo scripts/tier2/host-setup.sh`); `t2_host_setup_missing DIR PROC_SYS SYS_MODULE NETDROP`; on the host, `/usr/local/sbin/goim-netdrop add|del PORT | flush | list` via `sudo -n` (Plan 6b's b2 uses `add`/`del`; every load scenario registers `flush` as an exit hook).

- [ ] **Step 1: Write the failing tests**

In `scripts/tier2/lib_test.sh`, insert this block just before the last line:

```bash
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

# goim-netdrop prints its rules and refuses anything else.
expect "netdrop: add" "$(scripts/tier2/goim-netdrop --print add 21100)" \
"iptables -w -I INPUT -i lo -p tcp --dport 21100 -m comment --comment goim-netdrop -j DROP
iptables -w -I INPUT -i lo -p tcp --sport 21100 -m comment --comment goim-netdrop -j DROP"
contains "netdrop: del" "$(scripts/tier2/goim-netdrop --print del 21100)" "iptables -w -D INPUT -i lo -p tcp --dport 21100"
for bad_args in "add 80" "add 70000" "add 21100x" "add" "add 1 2" "drop 21100" "--print flush"; do
  # shellcheck disable=SC2086
  scripts/tier2/goim-netdrop $bad_args >/dev/null 2>&1; expect "netdrop: refuses '$bad_args'" "$?" 2
done
```

- [ ] **Step 2: Run them to make sure they fail**

Run: `scripts/tier2/lib_test.sh`
Expected: non-zero exit; the `setup`, `setup check` and `netdrop` lines FAIL (the scripts and `t2_host_setup_missing` do not exist).

- [ ] **Step 3: Write the setup files**

Create `scripts/tier2/sysctl.conf`:

```ini
# goim Tier 2 load tests (choice #68). host-setup.sh installs this file as
# /etc/sysctl.d/90-goim-tier2.conf; lib.sh's t2_require_host_setup checks
# that each running value is at least the one here.
# Conntrack: 600k connections on loopback are 600k entries, and a storm
# adds as many again in TIME_WAIT (120s); the gateway's self-check wants
# 2 x its cap (R5S-O7).
net.netfilter.nf_conntrack_max = 4194304
net.netfilter.nf_conntrack_buckets = 1048576
# Accept queues for 20k handshakes/s storms.
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
# TCP buffer memory in pages (4, 6, 8 GB): both ends of 600k sockets live on this host.
net.ipv4.tcp_mem = 1048576 1572864 2097152
# kind (Tier 3 TARGET=local, choice #67).
fs.inotify.max_user_instances = 8192
fs.inotify.max_user_watches = 1048576
```

Create `scripts/tier2/modules.conf`:

```text
# goim Tier 2 and Tier 3 local (choice #68): installed as
# /etc/modules-load.d/goim-tier2.conf. nf_conntrack must be loaded for its
# sysctls; ip_vs and its schedulers are kube-proxy's IPVS mode in kind (#67).
nf_conntrack
ip_vs
ip_vs_rr
ip_vs_wrr
ip_vs_sh
ip_vs_lc
```

- [ ] **Step 4: Write `goim-netdrop`**

Create `scripts/tier2/goim-netdrop` and `chmod +x` it:

```bash
#!/bin/bash
# goim-netdrop: the one root action Tier 2 runs need (choice #68): silently
# drop a gateway's loopback traffic for scenario (b2), silent node loss.
# host-setup.sh installs it root-owned in /usr/local/sbin, and a sudoers
# line lets the Tier 2 user run it, and only it, without a password.
#
#   sudo -n /usr/local/sbin/goim-netdrop add PORT   drop TCP to and from PORT on lo
#   sudo -n /usr/local/sbin/goim-netdrop del PORT   remove that port's rules
#   sudo -n /usr/local/sbin/goim-netdrop flush      remove every goim-netdrop rule
#   sudo -n /usr/local/sbin/goim-netdrop list       show them
#   goim-netdrop --print add|del PORT               print the iptables commands, run nothing
#
# It runs only iptables, from a fixed path, with a fixed rule shape: DROP in
# INPUT on lo, by destination port (client to gateway) and by source port
# (gateway to client), tagged with the comment goim-netdrop. PORT must be
# 1024-65535. It reads no environment variable.
set -eu
PATH=/usr/sbin:/usr/bin:/sbin:/bin
IPT=/usr/sbin/iptables
TAG=goim-netdrop

usage() { echo "usage: goim-netdrop [--print] add|del PORT | flush | list" >&2; exit 2; }

print=0
if [ "${1:-}" = --print ]; then print=1; shift; fi
cmd=${1:-}
port=${2:-}

if [ $print = 0 ] && [ ! -x $IPT ]; then
  echo "goim-netdrop: $IPT not found (apt install iptables)" >&2
  exit 1
fi

# rule DIR: the match for one direction.
rule() { echo "-i lo -p tcp --$1 $port -m comment --comment $TAG -j DROP"; }

case $cmd in
  add|del)
    [ $# -eq 2 ] || usage
    case $port in ''|*[!0-9]*) usage ;; esac
    { [ "$port" -ge 1024 ] && [ "$port" -le 65535 ]; } || usage
    if [ $print = 1 ]; then
      for dir in dport sport; do
        if [ "$cmd" = add ]; then echo "iptables -w -I INPUT $(rule $dir)"; else echo "iptables -w -D INPUT $(rule $dir)"; fi
      done
      exit 0
    fi
    for dir in dport sport; do
      # shellcheck disable=SC2046
      if [ "$cmd" = add ]; then
        $IPT -w -C INPUT $(rule $dir) 2>/dev/null || $IPT -w -I INPUT $(rule $dir)
      else
        while $IPT -w -C INPUT $(rule $dir) 2>/dev/null; do $IPT -w -D INPUT $(rule $dir); done
      fi
    done
    ;;
  flush)
    [ $# -eq 1 ] && [ $print = 0 ] || usage
    # Delete by rule spec, never by line number: a rule another tool inserts
    # meanwhile cannot shift what is deleted.
    $IPT -w -S INPUT | grep -- "--comment $TAG -j DROP\$" |
      while read -r -a r; do r[0]=-D; $IPT -w "${r[@]}"; done
    ;;
  list)
    [ $# -eq 1 ] && [ $print = 0 ] || usage
    $IPT -w -S INPUT | grep -- "--comment $TAG -j DROP\$" || true
    ;;
  *) usage ;;
esac
```

- [ ] **Step 5: Write `host-setup.sh`**

Create `scripts/tier2/host-setup.sh` and `chmod +x` it:

```bash
#!/usr/bin/env bash
# One-time root setup of the Tier 2 host (choice #68). Run it by hand, once,
# and again whenever sysctl.conf, modules.conf or goim-netdrop change (the
# Tier 2 guard says so):
#
#   sudo scripts/tier2/host-setup.sh
#
# It is idempotent and installs:
#   /etc/sysctl.d/90-goim-tier2.conf       from scripts/tier2/sysctl.conf, applied now
#   /etc/modules-load.d/goim-tier2.conf    from scripts/tier2/modules.conf, loaded now
#   /usr/local/sbin/goim-netdrop           root-owned, 0755 (the b2 DROP rule only)
#   /etc/sudoers.d/goim-netdrop            $SUDO_USER may run goim-netdrop, and only
#                                          it, without a password (checked with visudo)
# Everything else the runs need (nofile up to the hard limit, loopback
# source addresses, CPU pinning) needs no root.
#
# Test mode, no root, nothing applied: host-setup.sh --root DIR --user NAME
# writes the same files under DIR.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
root="" user=${SUDO_USER:-}
while [ $# -gt 0 ]; do
  case $1 in
    --root) root=$2; shift 2 ;;
    --user) user=$2; shift 2 ;;
    *) echo "usage: sudo $0   (test mode: $0 --root DIR --user NAME)" >&2; exit 2 ;;
  esac
done
if [ -z "$root" ] && [ "$(id -u)" != 0 ]; then
  echo "host-setup: run it as root: sudo $0" >&2
  exit 2
fi
case $user in
  ''|root) echo "host-setup: run it through sudo from the Tier 2 user's account (SUDO_USER is '$user')" >&2; exit 2 ;;
  *[!A-Za-z0-9._-]*) echo "host-setup: unexpected user name '$user'" >&2; exit 2 ;;
esac

# put SRC DEST MODE: installs SRC as DEST unless it is already identical.
put() {
  local dest=$root$2
  mkdir -p "$(dirname "$dest")"
  if cmp -s "$1" "$dest"; then
    echo "unchanged: $2"
  else
    install -m "$3" "$1" "$dest"
    [ -n "$root" ] || chown root:root "$dest"
    echo "installed: $2"
  fi
}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

put "$here/sysctl.conf" /etc/sysctl.d/90-goim-tier2.conf 0644
put "$here/modules.conf" /etc/modules-load.d/goim-tier2.conf 0644
put "$here/goim-netdrop" /usr/local/sbin/goim-netdrop 0755
echo "$user ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop" >"$tmp/sudoers"
if [ -z "$root" ]; then
  visudo -cqf "$tmp/sudoers" || { echo "host-setup: visudo rejected the sudoers line" >&2; exit 1; }
fi
put "$tmp/sudoers" /etc/sudoers.d/goim-netdrop 0440

if [ -n "$root" ]; then
  echo "test mode: nothing applied"
  exit 0
fi
grep -vE '^[[:space:]]*(#|$)' "$here/modules.conf" | while read -r m; do modprobe "$m"; done
sysctl -q -p /etc/sysctl.d/90-goim-tier2.conf
command -v iptables >/dev/null || echo "host-setup: warning: iptables is not installed; goim-netdrop needs it (apt install iptables)" >&2
echo "host-setup: done. Check: sudo -n /usr/local/sbin/goim-netdrop list"
```

- [ ] **Step 6: Append the check to `lib.sh`**

Append to `scripts/tier2/lib.sh`:

```bash
# ---- Host setup (choice #68) ----

# t2_host_setup_missing DIR PROC_SYS SYS_MODULE NETDROP: prints one line per
# item host-setup.sh would fix: a sysctl below sysctl.conf (every field of a
# multi-value one), a module of modules.conf not loaded, the installed
# goim-netdrop missing or differing from DIR's. Tests pass fake paths.
t2_host_setup_missing() {
  local dir=$1 proc=$2 mods=$3 netdrop=$4 key want have m
  while IFS=$'\t' read -r key want; do
    have=$(tr -s '\t ' '  ' <"$proc/${key//.//}" 2>/dev/null)
    awk -v w="$want" -v h="$have" 'BEGIN {
      n = split(w, a, " "); if (split(h, b, " ") != n) exit 1
      for (i = 1; i <= n; i++) if (b[i] + 0 < a[i] + 0) exit 1 }' ||
      echo "sysctl $key is ${have:-absent}, want >= $want"
  done < <(awk -F= '!/^[[:space:]]*(#|$)/ { gsub(/^[ \t]+|[ \t]+$/, "", $1); gsub(/^[ \t]+|[ \t]+$/, "", $2); print $1 "\t" $2 }' "$dir/sysctl.conf")
  while read -r m; do
    [ -d "$mods/$m" ] || echo "module $m is not loaded"
  done < <(grep -vE '^[[:space:]]*(#|$)' "$dir/modules.conf")
  cmp -s "$dir/goim-netdrop" "$netdrop" || echo "$netdrop is missing or differs from scripts/tier2/goim-netdrop"
}

# t2_require_host_setup: load scenarios call it after t2_init; refuses
# (exit 2) until host-setup.sh has run with this checkout's files.
t2_require_host_setup() {
  local missing
  missing=$(t2_host_setup_missing scripts/tier2 /proc/sys /sys/module /usr/local/sbin/goim-netdrop)
  if [ -z "$missing" ] && ! sudo -n /usr/local/sbin/goim-netdrop list >/dev/null 2>&1; then
    missing="sudo -n /usr/local/sbin/goim-netdrop fails (no sudoers line?)"
  fi
  if [ -n "$missing" ]; then
    t2_refuse "$(printf 'refused: the host is not set up for load tests:\n%s\nrun once: sudo scripts/tier2/host-setup.sh' "$missing")"
  fi
}
```

- [ ] **Step 7: Run the tests and make sure they pass**

Run: `make test-scripts`
Expected: `lib_test: all passed`. (`setup: no user refused` is skipped when the tests run under sudo.)

Check that the tests can fail: in `t2_host_setup_missing`, change `if (b[i] + 0 < a[i] + 0) exit 1` to `if (0) exit 1`: expect `FAIL setup check: low conntrack`. In `goim-netdrop`, change `[ "$port" -ge 1024 ]` to `true`: expect `FAIL netdrop: refuses 'add 80'`. Revert both.

- [ ] **Step 8: Commit**

```bash
git add scripts/tier2/sysctl.conf scripts/tier2/modules.conf scripts/tier2/host-setup.sh scripts/tier2/goim-netdrop scripts/tier2/lib.sh scripts/tier2/lib_test.sh
git commit -m "feat(tier2): one-time host setup and the goim-netdrop helper (#68)"
```

---

### Task 5: `stack.sh`, and the `foundation` scenario

**Files:**
- Create: `scripts/stack.sh`, `scripts/stack_test.sh` (`chmod +x` the test)
- Create: `scripts/tier2/foundation.sh` (`chmod +x`)
- Modify: `Makefile` (`test-scripts`)

**Interfaces:**
- Consumes: `t2_cpulist_expand` (Task 3; `stack_pinned` needs `lib.sh` sourced); for `foundation.sh` everything from Tasks 1, 3 and 4; the binaries' flags as they are (`gateway noise-keygen` prints `key_id 1 public <base64>`; `devtoken keygen|jwks|serve`; Logic `-redis-cluster=false`, `-kick-addr-file`; gateway `-listen tcp://…`, `/readyz` on each admin port).
- Produces: `stack_build`, `stack_up`, `stack_snapshot NAME`, `stack_pinned NAME CPUS`, `stack_down`, `stack_port_free PORT`, `STACK_PIDS[name]` (names `redis`, `devtoken`, `logic`, `gw-0`…), `STACK_ORDER`; settings `STACK_DIR`, `STACK_GATEWAYS`, `STACK_PORT_BASE`, `STACK_PIN`, `STACK_REDIS_SERVER`, `STACK_GATEWAY_ARGS`, `STACK_LOGIC_ARGS`; `$STACK_DIR/stack.env` with `GATEWAY_ADDRS`, `NOISE_KEY_ID`, `NOISE_PUBLIC`, `JWT_KEY`, `REDIS_ADDR`, `LOGIC_ADMIN`, `ADMIN_ADDRS` (Plan 6a2's loadgen and probe read it); the Tier 2 scenario `foundation`.

- [ ] **Step 1: Write the failing test**

Create `scripts/stack_test.sh` and `chmod +x` it:

```bash
#!/usr/bin/env bash
# Tests for scripts/stack.sh on any machine (make test-scripts): a real stack
# of two gateways comes up Ready, pinned when asked, writes stack.env and a
# metrics snapshot, and stack_down leaves no process and no port behind.
# Needs redis-server (make redis-server) and curl.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/tier2/lib.sh
. scripts/stack.sh
STACK_DIR=$(mktemp -d)
trap 'stack_down; rm -rf "$STACK_DIR"' EXIT
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }

STACK_GATEWAYS=2
STACK_PORT_BASE=${STACK_PORT_BASE:-17500}
STACK_PIN=1
T2_CPUS_GATEWAY=$(($(getconf _NPROCESSORS_ONLN) - 1)) # the last CPU
T2_CPUS_LOGIC=0
STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000 -loops 1)

stack_build || { echo "build failed"; exit 1; }
if stack_up; then ok "up: every process Ready"; else bad "up"; exit 1; fi

. "$STACK_DIR/stack.env"
expect "env: two gateway addresses" "$GATEWAY_ADDRS" "127.0.0.1:$((STACK_PORT_BASE + 100)),127.0.0.1:$((STACK_PORT_BASE + 110))"
expect "env: Noise public key is 32 bytes" "$(printf '%s' "$NOISE_PUBLIC" | base64 -d 2>/dev/null | wc -c)" 32
[ -s "$JWT_KEY" ] && ok "env: token key" || bad "env: token key missing"
for name in gw-0 gw-1; do
  stack_pinned "$name" "$T2_CPUS_GATEWAY" && ok "pin: $name on CPU $T2_CPUS_GATEWAY" || bad "pin: $name"
done
for name in redis devtoken logic; do
  stack_pinned "$name" "$T2_CPUS_LOGIC" && ok "pin: $name on CPU 0" || bad "pin: $name"
done
stack_pinned gw-0 "$T2_CPUS_LOGIC" && bad "pin: a wrong set must not match" || ok "pin: a wrong set does not match"

stack_snapshot idle
for f in logic gw-0 gw-1; do
  grep -q '^# TYPE' "$STACK_DIR/artifacts/idle/$f.metrics" && ok "snapshot: $f metrics" || bad "snapshot: $f metrics"
done
grep -q '^redis_version:' "$STACK_DIR/artifacts/idle/redis.info" && ok "snapshot: redis INFO" || bad "snapshot: redis INFO"

# A second stack on the same ports is refused before it starts anything.
(STACK_DIR=$(mktemp -d); STACK_ORDER=(); stack_up 2>"$STACK_DIR/err"; echo $? >"$STACK_DIR/rc"; cat "$STACK_DIR/err" "$STACK_DIR/rc") >"$STACK_DIR/second" 2>&1
grep -q "is in use" "$STACK_DIR/second" && ok "up: busy ports are refused" || bad "up: busy ports: $(cat "$STACK_DIR/second")"

pids=("${STACK_PIDS[@]}")
stack_down
left=0
for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && left=$((left + 1)); done
expect "down: no process left" "$left" 0
stack_port_free "$STACK_PORT_BASE" && stack_port_free $((STACK_PORT_BASE + 100)) && ok "down: ports free" || bad "down: ports still in use"

[ "$fails" -eq 0 ] && echo "stack_test: all passed" || { echo "stack_test: $fails failed"; exit 1; }
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `STACK_REDIS_SERVER=$(command -v redis-server || echo .cache/redis/bin/redis-server) scripts/stack_test.sh`
Expected: non-zero exit; `scripts/stack.sh: No such file or directory`.

- [ ] **Step 3: Write `stack.sh`**

Create `scripts/stack.sh`:

```bash
# shellcheck shell=bash
# stack.sh: a goim stack as local processes for load runs (Plan 6a1, P5bI-1):
# one redis-server, devtoken serving the JWKS, Logic and N gateways, built
# without -race and run at production timing. Tier 2 scenarios pin it by
# core type (choice #71); the Tier 1a load probe (Plan 6a2) runs it unpinned.
# Source it, set STACK_DIR, then:
#
#   stack_build                 # binaries into $STACK_DIR/bin
#   stack_up                    # keys, processes, waits until all are Ready
#   stack_snapshot NAME         # /metrics of each process + redis INFO -> $STACK_DIR/artifacts/NAME/
#   stack_down                  # SIGTERM, then SIGKILL after 10s; call it from the exit trap
#
# Settings (environment, read by stack_up):
#   STACK_DIR            work dir (required): bin/, keys/, logs/, artifacts/, stack.env
#   STACK_GATEWAYS       gateways, ids gw-0.. (default 1)
#   STACK_PORT_BASE      redis +0, devtoken +1, Logic gRPC +2, lease +3, admin +4;
#                        gateway i: listen +100+10i, Kick +101+10i, admin +102+10i (default 17000:
#                        below the race suite's 20000-32000 and the ephemeral range)
#   STACK_PIN            1: taskset gateways to T2_CPUS_GATEWAY, Logic and Redis
#                        to T2_CPUS_LOGIC (lib.sh sets both); 0: no pinning (default)
#   STACK_REDIS_SERVER   default: redis-server on PATH, else .cache/redis/bin/redis-server
#   STACK_GATEWAY_ARGS, STACK_LOGIC_ARGS   arrays of extra flags
#
# stack.env (for loadgen, Plan 6a2) holds GATEWAY_ADDRS (comma-separated),
# NOISE_KEY_ID, NOISE_PUBLIC, JWT_KEY (the devtoken private key file), and
# the admin addresses. Every process is started from this shell, so it
# stays in the caller's process group (lib.sh counts that group as ours).

declare -A STACK_PIDS=()
STACK_ORDER=()

stack_build() {
  mkdir -p "$STACK_DIR/bin"
  go build -o "$STACK_DIR/bin/" ./cmd/gateway ./cmd/logic ./cmd/devtoken
}

# stack_start NAME CPUS CMD...: starts CMD in the background, logging to
# logs/NAME.log, pinned to CPUS when STACK_PIN=1.
stack_start() {
  local name=$1 cpus=$2
  shift 2
  if [ "${STACK_PIN:-0}" = 1 ]; then set -- taskset -c "$cpus" "$@"; fi
  "$@" >"$STACK_DIR/logs/$name.log" 2>&1 &
  STACK_PIDS[$name]=$!
  STACK_ORDER+=("$name")
}

# stack_wait NAME URL: waits up to 30s for URL to answer 200.
stack_wait() {
  local i
  for i in $(seq 300); do
    if ! kill -0 "${STACK_PIDS[$1]}" 2>/dev/null; then
      echo "stack: $1 exited; last log lines:" >&2
      tail -n 20 "$STACK_DIR/logs/$1.log" >&2
      return 1
    fi
    curl -sf -o /dev/null "$2" && return 0
    sleep 0.1
  done
  echo "stack: $1 not ready at $2 within 30s; last log lines:" >&2
  tail -n 20 "$STACK_DIR/logs/$1.log" >&2
  return 1
}

stack_port_free() { ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

stack_up() {
  : "${STACK_DIR:?set STACK_DIR}"
  local n=${STACK_GATEWAYS:-1} base=${STACK_PORT_BASE:-17000} bin=$STACK_DIR/bin i p
  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
  local gw_cpus=${T2_CPUS_GATEWAY:-} logic_cpus=${T2_CPUS_LOGIC:-}
  if [ "${STACK_PIN:-0}" = 1 ] && { [ -z "$gw_cpus" ] || [ -z "$logic_cpus" ]; }; then
    echo "stack: STACK_PIN=1 needs T2_CPUS_GATEWAY and T2_CPUS_LOGIC" >&2
    return 1
  fi
  [ -x "$redis" ] || { echo "stack: $redis not found (make redis-server)" >&2; return 1; }
  for p in $(seq "$base" $((base + 4))) $(for ((i = 0; i < n; i++)); do echo $((base + 100 + 10 * i)) $((base + 101 + 10 * i)) $((base + 102 + 10 * i)); done); do
    stack_port_free "$p" || { echo "stack: port $p is in use; pick another STACK_PORT_BASE" >&2; return 1; }
  done
  mkdir -p "$STACK_DIR/keys/noise" "$STACK_DIR/logs" "$STACK_DIR/artifacts" "$STACK_DIR/redis"

  # Keys: the Noise static key (key_id 1) and the token signing key.
  local pub
  rm -f "$STACK_DIR/keys/noise/1" "$STACK_DIR/keys/jwt.jwk"
  pub=$("$bin/gateway" noise-keygen -key-id 1 -out "$STACK_DIR/keys/noise/1") || return 1
  pub=${pub##* public }
  "$bin/devtoken" keygen -kid k1 -out "$STACK_DIR/keys/jwt.jwk" || return 1
  "$bin/devtoken" jwks -out "$STACK_DIR/keys/jwks.json" "$STACK_DIR/keys/jwt.jwk" || return 1

  local redis_addr=127.0.0.1:$base devtoken=127.0.0.1:$((base + 1))
  local grpc=127.0.0.1:$((base + 2)) lease=127.0.0.1:$((base + 3)) admin=127.0.0.1:$((base + 4))
  local kick="" addrs="" admins="$admin"
  for ((i = 0; i < n; i++)); do
    kick+="${kick:+,}\"gw-$i\": \"127.0.0.1:$((base + 101 + 10 * i))\""
  done
  echo "{$kick}" >"$STACK_DIR/kick.json"

  stack_start redis "$logic_cpus" "$redis" --port "$base" --bind 127.0.0.1 --dir "$STACK_DIR/redis" \
    --save "" --appendonly no --maxmemory-policy noeviction
  local pong=""
  for i in $(seq 100); do
    "$(dirname "$redis")/redis-cli" -p "$base" ping >/dev/null 2>&1 && { pong=1; break; }
    sleep 0.1
  done
  if [ -z "$pong" ]; then
    echo "stack: redis not answering on port $base within 10s; last log lines:" >&2
    tail -n 20 "$STACK_DIR/logs/redis.log" >&2
    return 1
  fi
  stack_start devtoken "$logic_cpus" "$bin/devtoken" serve -addr "$devtoken" -jwks "$STACK_DIR/keys/jwks.json"
  stack_wait devtoken "http://$devtoken/" || return 1
  stack_start logic "$logic_cpus" "$bin/logic" -redis "$redis_addr" -redis-cluster=false \
    -grpc-addr "$grpc" -lease-addr "$lease" -admin-addr "$admin" \
    -jwks-url "http://$devtoken/" -jwks-fallback "$STACK_DIR/keys/jwks.json" \
    -kick-addr-file "$STACK_DIR/kick.json" "${STACK_LOGIC_ARGS[@]}"
  stack_wait logic "http://$admin/readyz" || return 1
  for ((i = 0; i < n; i++)); do
    local listen=127.0.0.1:$((base + 100 + 10 * i)) gadmin=127.0.0.1:$((base + 102 + 10 * i))
    stack_start "gw-$i" "$gw_cpus" "$bin/gateway" -id "gw-$i" -listen "tcp://$listen" \
      -noise-keys "$STACK_DIR/keys/noise" -logic "$grpc" -lease "$lease" \
      -kick-addr "127.0.0.1:$((base + 101 + 10 * i))" -admin-addr "$gadmin" "${STACK_GATEWAY_ARGS[@]}"
    addrs+="${addrs:+,}$listen"
    admins+=",$gadmin"
  done
  for ((i = 0; i < n; i++)); do
    stack_wait "gw-$i" "http://127.0.0.1:$((base + 102 + 10 * i))/readyz" || return 1
  done
  cat >"$STACK_DIR/stack.env" <<ENV
GATEWAY_ADDRS=$addrs
NOISE_KEY_ID=1
NOISE_PUBLIC=$pub
JWT_KEY=$STACK_DIR/keys/jwt.jwk
REDIS_ADDR=$redis_addr
LOGIC_ADMIN=$admin
ADMIN_ADDRS=$admins
ENV
}

# stack_snapshot NAME: every process's /metrics and Redis INFO, for result
# artifacts (spec: Automation contract, a metrics snapshot).
stack_snapshot() {
  local dir=$STACK_DIR/artifacts/$1 base=${STACK_PORT_BASE:-17000} i
  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
  mkdir -p "$dir"
  curl -sf "http://127.0.0.1:$((base + 4))/metrics" >"$dir/logic.metrics"
  for ((i = 0; i < ${STACK_GATEWAYS:-1}; i++)); do
    curl -sf "http://127.0.0.1:$((base + 102 + 10 * i))/metrics" >"$dir/gw-$i.metrics"
  done
  "$(dirname "$redis")/redis-cli" -p "$base" info all >"$dir/redis.info" 2>&1
}

# stack_pinned NAME CPUS: the process's affinity is exactly CPUS.
stack_pinned() {
  local have
  have=$(taskset -pc "${STACK_PIDS[$1]}" 2>/dev/null) || return 1
  [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$2")" ]
}

# stack_down: SIGTERM to all (gateways drain), SIGKILL whatever is left after 10s.
stack_down() {
  local name pid i
  for ((i = ${#STACK_ORDER[@]} - 1; i >= 0; i--)); do
    kill -TERM "${STACK_PIDS[${STACK_ORDER[i]}]}" 2>/dev/null
  done
  for i in $(seq 100); do
    pid=""
    for name in "${STACK_ORDER[@]}"; do
      kill -0 "${STACK_PIDS[$name]}" 2>/dev/null && pid=1
    done
    [ -z "$pid" ] && break
    sleep 0.1
  done
  for name in "${STACK_ORDER[@]}"; do
    kill -KILL "${STACK_PIDS[$name]}" 2>/dev/null
    wait "${STACK_PIDS[$name]}" 2>/dev/null
  done
  STACK_ORDER=()
  STACK_PIDS=()
}
```

- [ ] **Step 4: Run the test and make sure it passes**

Add the second line to the `test-scripts` target in `Makefile`, so it reads:

```make
test-scripts:
	scripts/tier2/lib_test.sh
	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
```

Run: `make test-scripts`
Expected: `lib_test: all passed`, then `stack_test: all passed` (about 10s on the dev machine: two gateways pinned to the last CPU, Redis, devtoken and Logic to CPU 0). Afterwards `pgrep -f "$PWD/.*bin/(gateway|logic|devtoken)"` finds nothing.

Check that the test can fail: make `stack_pinned` always succeed (replace its last line with `true`): expect `FAIL pin: a wrong set must not match`. Revert.

- [ ] **Step 5: Write the `foundation` scenario**

Create `scripts/tier2/foundation.sh` and `chmod +x` it:

```bash
#!/usr/bin/env bash
# Tier 2: the load-test foundation on the Tier 2 host (Plan 6a1), before any
# load: the host setup is in place, a stack of 4 gateways comes up pinned by
# core type (#71), the run is recorded (#70, #71), artifacts are written, and
# goim-netdrop really silences a gateway port and restores it (#68, for b2).
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=foundation
#   Results: tier2-results/foundation-<timestamp>/ (result.json, stack/ with
#   logs and artifacts), plus ONE file to send back:
#   tier2-results/foundation-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. stack:      redis, devtoken, Logic and gw-0..gw-3 become Ready
#   2. pinning:    gateways on T2_CPUS_GATEWAY, the rest on T2_CPUS_LOGIC
#   3. artifacts:  a metrics snapshot of every process and Redis INFO
#   4. netdrop:    with goim-netdrop add, a connect to gw-0 times out; after
#                  del it connects again, and no goim-netdrop rule is left
#   5. no_swap_in: no page swapped in during the 30s idle window
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
t2_init foundation
t2_need curl taskset sudo
t2_require_host_setup
t2_mem_budget 4
t2_nofile 1048576

export STACK_DIR=$T2_OUT/stack STACK_GATEWAYS=4 STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'sudo -n /usr/local/sbin/goim-netdrop flush'

t2_report_extra() {
  t2_section "host setup"
  for k in net.netfilter.nf_conntrack_max net.core.somaxconn net.ipv4.tcp_mem; do
    echo "$k = $(cat "/proc/sys/${k//.//}" 2>/dev/null)"
  done
  echo "nofile: $(ulimit -Sn) / $(ulimit -Hn)"
  t2_section "background-before.txt"
  cat "$T2_OUT/background-before.txt" 2>/dev/null
  local f
  for f in "$STACK_DIR"/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "$(basename "$f") (last 30 lines)"
    tail -n 30 "$f"
  done
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

# 1: the stack.
stack_up 2>"$T2_OUT/stack-up.err"
up=$?
t2_check stack "$(t2_bool [ $up -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"error\": $(t2_json_str "$(cat "$T2_OUT/stack-up.err")")"
[ $up -eq 0 ] || t2_finish

# 2: pinning.
wrong=""
for name in "${STACK_ORDER[@]}"; do
  case $name in gw-*) want=$T2_CPUS_GATEWAY ;; *) want=$T2_CPUS_LOGIC ;; esac
  stack_pinned "$name" "$want" || wrong+="${wrong:+ }$name"
done
t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": \"$T2_CPUS_GATEWAY\", \"logic_cpus\": \"$T2_CPUS_LOGIC\", \"wrong\": $(t2_json_str "$wrong")"

# 5 (window): 30s idle, recorded.
t2_record_start
sleep 30 & wait $! # a SIGTERM runs the trap now, not after the sleep
t2_record_end

# 3: artifacts.
stack_snapshot idle
missing=""
for f in logic gw-0 gw-1 gw-2 gw-3; do
  grep -q '^# TYPE' "$STACK_DIR/artifacts/idle/$f.metrics" 2>/dev/null || missing+="${missing:+ }$f"
done
grep -q '^redis_version:' "$STACK_DIR/artifacts/idle/redis.info" 2>/dev/null || missing+="${missing:+ }redis"
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "$missing")"

# 4: netdrop on gw-0's client port.
port=$((STACK_PORT_BASE + 100))
connects() { timeout 3 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; }
before=$(t2_bool connects)
sudo -n /usr/local/sbin/goim-netdrop add "$port"
dropped=$(t2_bool connects)
sudo -n /usr/local/sbin/goim-netdrop del "$port"
after=$(t2_bool connects)
left=$(sudo -n /usr/local/sbin/goim-netdrop list | wc -l)
t2_check netdrop "$(t2_bool [ "$before$dropped$after$left" = truefalsetrue0 ])" \
  "\"connects_before\": $before, \"connects_while_dropped\": $dropped, \"connects_after\": $after, \"rules_left\": $left"

t2_not_covered "load: loadgen is Plan 6a2" "Redis Cluster (Plan 6c)"
t2_finish
```

- [ ] **Step 6: Check it on the dev machine**

Run: `bash -n scripts/tier2/foundation.sh && GOIM_TEST_TIER=2 make test-tier2 SCENARIO=foundation; echo "exit=$?"`
Expected: refused by the guard, `exit=2`.

Optional dry run with the guard lowered and the root parts faked (the prototype did this): in a throwaway copy, replace `t2_require_host_setup` with `:`, `t2_mem_budget 4` with `t2_mem_budget 0`, drop `sudo` from `t2_need`, point the `goim-netdrop` calls at a script that only exits 0, and add `STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000 -loops 1)`; run with `T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=0 GOIM_TEST_TIER=2`. Expected: `stack`, `pinning` and `artifacts` pass; `netdrop` fails with `"connects_while_dropped": true` (the fake drops nothing); `no_swap_in` depends on the 2GB machine (the prototype saw 50 pages swapped in, which is the check working); `background.cpu_clocks` is `null` (the VM has no cpufreq); exit 1; no stack process is left.

- [ ] **Step 7: Commit**

```bash
git add scripts/stack.sh scripts/stack_test.sh scripts/tier2/foundation.sh Makefile
git commit -m "feat(tier2): stack.sh, a pinned local stack, and the foundation scenario"
```

- [ ] **Step 8: Tier 2 run 11 (the user, on the Tier 2 host)**

```bash
git pull && make redis-server
sudo scripts/tier2/host-setup.sh            # once; prints installed/unchanged per file
GOIM_TEST_TIER=2 make test-tier2 SCENARIO=foundation
GOIM_TEST_TIER=2 make test-tier2 SCENARIO=store
GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race
```

Expected: each exits 0. `foundation`'s `result.json` has every check true, `background.cpu_clocks` with three CPU sets, and `top_outside` naming the desktop processes. Close the desktop apps first if `no_swap_in` or `mem_budget` complains. Send back the three `.report.txt` files; record the results in *Execution Notes*.

---

### Task 6: Interface review fixes (P6a1I-1..4, P6a1I-6) and run 11 decisions (#78, #79)

Added 2026-10-09 after Tier 2 run 11 and the interface review (*Execution Notes → Interface review*). Written at requirement level, not as prototyped code: implement each step test first (the new test fails before the change, as in the deferred-minors pass), in a fresh session, inline (superpowers:executing-plans), on `feat/plan6a-foundation`. Each step is its own commit. Plan 6a2 is written only after this task, against the result.

**Files:** `scripts/stack.sh`, `scripts/stack_test.sh`, `scripts/tier2/lib.sh`, `scripts/tier2/lib_test.sh`, `scripts/tier2/foundation.sh`; comments in `store.sh`/`race.sh` only if they name what changed.

- [ ] **Step 1: `stack_down` stops in dependency order (#78).** Gateways first: SIGTERM to every `gw-*` at once, wait up to `STACK_DOWN_WAIT` (default 10s) for them, SIGKILL what is left. Then Logic, devtoken, Redis, in that order, each SIGTERMed and waited up to 5s, then SIGKILLed. Total stays bounded (≤ 25s). While the gateways stop, Logic is still up, so a gateway's drain no longer logs `lease renewal failed` (run 11 observation 1). Tests in `stack_test.sh`: after `stack_down`, gw-0's log has no `lease renewal failed`; the existing "no process left" and "ports free" tests still pass. A measured drain (6b) gets its own `stack_stop NAME WAIT`, which is not added here.
- [ ] **Step 2: each gateway runs with its CPU share as `GOMAXPROCS` (#79).** `stack_up` starts each gateway with `GOMAXPROCS=$STACK_GATEWAY_PROCS`. When that is unset it defaults to `max(1, C / STACK_GATEWAYS)`, where C is the number of CPUs in `T2_CPUS_GATEWAY` when `STACK_PIN=1` and `getconf _NPROCESSORS_ONLN` otherwise. `-loops` and the handshake concurrency follow (both default from `GOMAXPROCS`); an explicit `-loops` in `STACK_GATEWAY_ARGS` still wins. `stack.env` gets `GATEWAY_PROCS`. Test: a stack with `STACK_GATEWAY_PROCS` unset logs the expected `loops` value in each gateway's startup line (`"loops"` attribute), computed from the test's own CPU set; with `STACK_GATEWAY_PROCS=1` it logs `loops=1`. (`stack_test.sh` passes `-loops 1` today; drop it so the default is exercised.) Logic keeps its affinity-derived `GOMAXPROCS` (4 on the host).
- [ ] **Step 3: process health (P6a1I-2).** `stack_down` records each process's exit status in `$STACK_DIR/exit-status` (`NAME STATUS`, `killed` when it needed SIGKILL). A clean stop is status 0 for gateways, Logic and Redis, and 0 or 143 for devtoken (it has no SIGTERM handler; check this on the dev machine and fix the rule to what the binaries do). New `stack_health`: prints one line per problem, nothing when healthy: a process not alive (`kill -0`) while the stack is up, an unclean or killed stop once `exit-status` exists, and any log line starting with `panic:` or `fatal error:`. Scenarios call `stack_down` themselves before `t2_finish` (the exit hook is then a no-op) and record `t2_check processes` from `stack_health` (`"problems": "<lines>"`). `foundation.sh` does so. Tests: a healthy stack has no problems; a gateway killed with SIGKILL during the run is reported (dead, then `killed`); a log with `panic:` is reported.
- [ ] **Step 4: `result.json` is always written after `t2_init` (P6a1I-1).** If `t2_exit` finds no `result.json`, it writes one with `"pass": false` and `"stopped": {"exit": RC}`, or `"refused": "<reason>"` when `T2_REFUSED` is set, after the exit hooks and before the report, so the report shows it. The exit code is unchanged. Refusals before `t2_init` creates the output directory still write nothing (existing test). Tests in `lib_test.sh`: a fake run that exits 1 before `t2_finish` leaves a valid `result.json` with `pass` false and `stopped.exit` 1; a `t2_refuse` after `t2_init` leaves `refused` with the reason and exits 2; SIGTERM leaves `stopped.exit` 143.
- [ ] **Step 5: named recording windows, duplicate keys fail (P6a1I-3).** `t2_record_start [NAME]` and `t2_record_end [NAME]`: without NAME the names stay `background`, `no_swap_in`, `background-before.txt`, `clocks.tsv`; with NAME they are `background_NAME`, `no_swap_in_NAME`, `background-before-NAME.txt`, `clocks-NAME.tsv`. Windows do not overlap (`t2_record_start` while one is open fails). `t2_check`, `t2_field` and `t2_duration` exit 1 with a message when a name was already used (a programming error, not a host problem). Tests: two named windows give two `background_*` members and two checks in valid JSON; a duplicate `t2_check` name fails.
- [ ] **Step 6: `stack.env` names the gateways (P6a1I-4).** Keep `GATEWAY_ADDRS`, `NOISE_KEY_ID`, `NOISE_PUBLIC`, `JWT_KEY`, `REDIS_ADDR`, `LOGIC_ADMIN`; add `GATEWAY_IDS` (`gw-0,gw-1,…`, the same order as `GATEWAY_ADDRS`), `GATEWAY_ADMINS` (gateway admin addresses only, same order), `GATEWAY_KICK_ADDRS`, and `GATEWAY_PROCS` (Step 2); drop `ADMIN_ADDRS`. Update the header comment. Plan 6a2 adds whatever token fields loadgen needs (issuer, kid) when it is written. Tests in `stack_test.sh` for the new keys.
- [ ] **Step 7: the report lists changed files (P6a1I-6).** The report's environment section prints `git status --porcelain` (first 20 lines, then `… N more`) under the `dirty:` count. Test: a fake run in a scratch git repo with an untracked file names it in the report.
- [ ] **Step 8: verify.** `make test-scripts` passes; `bash -n` on every script; race.sh on `lib.sh` with the guard lowered passes 4/4 locally; `foundation.sh` dry run as in Task 5 Step 6 (now with a `processes` check and a `result.json` even where it stops early). Record in *Execution Notes*; Tier 2 run 12 (`foundation`, `store`, `race`) then confirms on the host: `foundation` 6/6 checks, gateway logs show `loops=3`, and no `lease renewal failed` in any gateway log.

---

## After this plan

- **Plan 6a2** (its own session): `internal/loadgen` and `cmd/loadgen`, reading `stack.env`, binding sources 127.0.0.2… (#69), started under `taskset -c $T2_CPUS_LOADGEN`; pprof on the gateway and Logic admin ports, so `stack_snapshot` can also save profiles; the Tier 1a probe on `stack.sh` unpinned; a Tier 2 smoke load built on `foundation.sh`'s steps.
- **Plan 6b, from the 6a1 review:** (1) b2 must call `goim-netdrop add` for the gateway's client port **and** its Kick port (Kicks to the lost pod must time out, spec (b2)); the gateway's own connections to Logic and the lease use ephemeral source ports and are not dropped, so the "lost" gateway keeps renewing its lease. Either also `SIGSTOP` the gateway (no root needed; the kernel stays silent because of the DROP, the process goes silent because it is stopped), or list it in b2's `not_covered`. (2) Bash runs a trap only after the foreground command ends, so long steps (a loadgen run) are started as `cmd & wait $!`; background commands ignore SIGINT in a non-interactive script, so each one gets a kill hook with `t2_on_exit`. (3) `cpu_clocks` means something only for a busy CPU set (idle CPUs clock down), so throttling is judged on the gateway set during load.
- **Plan 6b, from Task 6:** stack gateways drain over `STACK_DRAIN_WINDOW` (default 1s, so teardown ends cleanly); a scenario that measures a drain sets `STACK_DRAIN_WINDOW=60s` (production) and stops the gateway with its own `stack_stop NAME WAIT`. The setting is stack-wide: with 60s every other gateway also outlives `STACK_DOWN_WAIT` at teardown and is reported `killed`, so 6b needs a per-gateway drain window (or stops the others with their own wait). Under load, stop loadgen before `stack_down`, so the 1s teardown drain has few connections left. Recording windows take a name (`t2_record_start load` → `background_load`, `no_swap_in_load`); every scenario calls `stack_down` before `t2_finish` and records `t2_check processes` from `stack_health`.
- **Plan 6b** uses `t2_require_host_setup`, `t2_mem_budget`, `t2_nofile 1048576`, `t2_record_start`/`_end` around each measured window, `stack_up` with `STACK_PIN=1`, `goim-netdrop add|del` for (b2), and `stack_pinned` as a check in every scenario. Logic slowed by CPU (P5bI-2) can be a narrower `T2_CPUS_LOGIC` for Logic alone, or `cpu.max`.

## Execution Notes

(Filled in during execution.)

- **Prototype (2026-10-09):** every block above was written and run in a scratch worktree of `1f4c3ce`, then copied into this plan by a render script. `lib_test.sh` passed 6 runs in a row (one flake found and fixed on the way: the `record` busy loops used `SECONDS`, which can end after 0.1s; they now use `EPOCHREALTIME`); `stack_test.sh` passed. `race.sh` on `lib.sh`, with the guard lowered, passed all four checks on the dev machine (58/58 scenarios, 293s; mutation caught; Redis 7.4.11; valid JSON). `foundation.sh` was dry-run as in Task 5 Step 6. Mutants caught: our group counted as outside; kernel threads counted as user; `t2_finish` ignoring failed checks; every sysctl read as high enough; `goim-netdrop` accepting port 80; `stack_pinned` always true. Not caught, by design: removing the INT/TERM traps (bash runs the EXIT trap on those signals anyway; see Review Focus 1). The guard's first draft let an empty core count through (`"$cores$mem"` was checked as one string); the test caught it.
- **Plan review (2026-10-09, Claude, at the user's request):** #73, #74, #77 accepted as proposed; #75 and #76 amended. Fixes applied to the plan text: (R1, Important) `store.sh` lost its Redis logs: exit hooks run before the report, so `make redis-down` removed the containers before `t2_report_extra` read their logs; a newer hook now saves them to `redis.log` first. (R2) A refusal after `t2_init` (`t2_need`, `t2_mem_budget`, `t2_nofile`, `t2_require_host_setup`) now goes through `t2_refuse`, so the report, the one file sent back, says why (new test `refuse: …`). (R3, #75) `goim-netdrop flush` deleted by line number as root; it now deletes by rule spec from `iptables -S`, and `list` matches the exact tag (a `goim-netdrop-other` rule was matched before); the helper fails without iptables, so the guard's `list` call no longer passes on a host without it. (R4, #76) `stack_up` went on silently when Redis never answered; it now fails with Redis's log. (R5, #76) `STACK_PORT_BASE` 21000 → 17000 (`stack_test` 23000 → 17500): the race suite probes free ports in 20000–32000, so a stack in that range could take a port between its probe and its listen. (R6) `foundation.sh`'s idle window is `sleep 30 & wait $!`, so SIGTERM is handled at once. Re-verified on the dev machine: the plan's blocks re-extracted into a scratch worktree, `lib_test.sh` and `stack_test.sh` pass, `bash -n` on every script; `goim-netdrop flush`/`list` run against a fake iptables (only the two tagged DROP rules deleted, a lookalike tag kept), and without iptables `list` exits 1; the mutant "report omits the refusal" fails `refuse: the report says why`. Notes for 6b are under *After this plan*. Execution: inline (superpowers:executing-plans) in a fresh session, on `feat/plan6a-foundation`.
- **Not verified on the dev machine** (no sudo, no Docker, no cpufreq): `host-setup.sh` outside test mode, `goim-netdrop` against iptables, `t2_require_host_setup` on real values, `store.sh`, CPU clock sampling. Tier 2 run 11 covers them.
- **Execution (2026-10-09, inline, superpowers:executing-plans):** commits c3c4bcb (Task 1), be5502e (2), 61f075b (3), 5529a1d (4), 1e5e736 (5); plan code verbatim except one ruling. Every listed mutant caught. `race.sh` on `lib.sh` with the guard lowered: 4/4 checks, 58/58, 322s. `foundation.sh` dry run as in Task 5 Step 6: as expected (netdrop fails on the fake, `cpu_clocks` null, no process left). **Ruling (Task 3):** the `record` test ran `t2ours` in the foreground, so it exited inside the window and was never seen; `"t2ours" not in top` held even with our group counted as outside (the mutant passed that line). `t2ours` now runs in the background through the window and is killed after (wait 2.7s); the mutant now fails both `cpu delta` and `record`.
- **Final review (2026-10-09, fresh Opus):** no Critical. Fixed in d9580f7, each with a test that failed first: (I1) `goim-netdrop` accepted `01167`, which iptables reads as octal 631: a leading 0 is refused. (I2) A DROP rule left by a SIGKILLed run made the next `stack_port_free` hang ~2 min and then fail `netdrop` confusingly: `t2_require_host_setup` refuses while `goim-netdrop list` prints rules (`t2_netdrop_leftover`, says to run `flush`), and `stack_port_free` gives up after 2s, counting a hang as busy. (I3, ruling) GNU make exits 2 on any failing recipe, so `make test-tier2` cannot carry 1 vs 2: the recipe now prints `test-tier2: <scenario> exited N`, and the script itself is the entry point that carries 0/1/2. (M1, re-graded Important, Review Focus 5) `t2_json_str` let invalid UTF-8 through (log tails of a crashing process) and `result.json` failed to parse: it is dropped with `iconv -c`. Minors deferred: the busy-port refusal says "pick another STACK_PORT_BASE" instead of naming the holder, and lands as a failed `stack` check, not a refusal; the `kill $T2_SAMPLER` exit hook runs after the sampler was reaped (pid reuse); a second Ctrl-C inside `t2_exit` skips the remaining hooks and the report; `goim-netdrop list` exits 0 when iptables exists but fails; `foundation.sh` puts `T2_CPUS_*` into JSON unescaped. Next: Tier 2 run 11 (Task 5 Step 8).
- **Deferred minors fixed (2026-10-09, at the user's request)**, each with a test that failed first: busy stack ports name their holder via `ss` and say to stop it (`stack_busy_ports`; stack_test `up: names the holder`, `busy: every port of the stack`), and `foundation.sh` refuses (exit 2) on busy ports instead of failing `stack` (dry run: exit 1 before, 2 after, report names the holder); `t2_record_end` clears `T2_SAMPLER` once reaped and the hook is guarded (`record: no sampler pid is left …`); `t2_exit` ignores INT/TERM so a second signal cannot cut the teardown short (`exit: a signal during teardown …`); `goim-netdrop list`/`flush` fail when `iptables -S` fails (`netdrop: list/flush fails when iptables fails`, run as non-root); `foundation.sh` passes `T2_CPUS_*` through `t2_json_str` (no test of its own; dry run JSON valid).
- **Tier 2 run 11 (2026-10-09, on 7347083, 24 cores, 26GB, kernel 7.0):** all three scenarios passed. `foundation` 5/5 in 35s: 4 gateways Ready and pinned to `4-9,16-21`, the rest to `10,11,22,23`; artifacts complete; `goim-netdrop` really silenced gw-0's port (`connects_while_dropped: false`) and left no rule; no swap-in; 23GB available against the 4GB budget. `host-setup.sh` ran, and `t2_require_host_setup` passed on real values (conntrack 4,194,304, somaxconn 65,535, `tcp_mem` 4/6/8GB, nofile 1,048,576). Background over the 30s idle window: 1.0 CPU-s outside our group (mongod, a host redis-server, containerd/dockerd, gnome-shell, sunshine), 0.1 CPU-s kernel threads. `cpu_clocks` was sampled for all three sets; idle means are 1.5GHz (gateway), 1.5GHz (logic) and 2.0GHz (loadgen), so as noted for 6b, clocks only mean something under load. `store` 4/4 in 35s (`store.sh` on `lib.sh`; the Redis logs are in the report, so R1 is confirmed fixed). `race` 4/4, 58/58, 271s, mutation caught. Observations for later plans: (1) `stack_down` sends SIGTERM to every process at once, so Logic stops before the gateways finish draining and each gateway logs one `lease renewal failed` while draining. That is harmless here; 6b should stop the gateways first if a drain is measured. (2) Each gateway starts 12 gnet loops (half the host's CPUs), so 48 loops share the 12 gateway CPUs; 6a2/6b should set `-loops` deliberately. (3) Redis warns that `vm.overcommit_memory` is 0. The stack runs without persistence or replicas, so it does not matter for 6a/6b, but 6c's Cluster replicas do a BGSAVE for full sync: add it to `sysctl.conf` there. (4) `dirty: 1` on the host: one changed file in the host checkout, not identified.
- **Interface review and run 11 decisions (2026-10-09, Claude, at the user's request):** run 11 observations 1 and 2 decided as #78 (`stack_down` order) and #79 (gateway `GOMAXPROCS` = CPU share); observation 3 (`vm.overcommit_memory`) stays with 6c; observation 4 (`dirty: 1`): `tier2-results/` and `.cache/` are git-ignored, so the changed file is not ours, and the report cannot name it (P6a1I-6). Interface items, reviewed against 6a2 (loadgen, smoke), 6b and 6c, all accepted by the user as proposed: **P6a1I-1** (Important) `result.json` is missing when a run stops early (build failure, Ctrl-C, `set -u`, a late refusal), against the spec's *Automation contract*: `t2_exit` writes one with `pass` false and `stopped`/`refused`. **P6a1I-2** (Important) nothing checks process health: a gateway that panics in a load window or exits unclean on SIGTERM goes unseen: `stack_down` records exit statuses, `stack_health` reports dead, killed, unclean and panicking processes, and every stack scenario records a `processes` check. **P6a1I-3** one recording window per run with fixed names, and duplicate keys write invalid JSON: named windows, duplicate `t2_check`/`t2_field`/`t2_duration` names fail. **P6a1I-4** `stack.env` lacks gateway ids and mixes Logic and gateway admin addresses in `ADMIN_ADDRS`: add `GATEWAY_IDS`, `GATEWAY_ADMINS`, `GATEWAY_KICK_ADDRS`, `GATEWAY_PROCS`, drop `ADMIN_ADDRS`. **P6a1I-5** (note, no change) `stack_up` always starts one non-Cluster Redis; 6c adds an external Redis option (`STACK_REDIS_ADDR`, cluster mode). **P6a1I-6** (Minor) the report prints the changed files, not only their count. Implemented as Task 6 (requirement level, test first), in a fresh session, before Plan 6a2 is written.
- **Task 6 execution (2026-10-09, inline, superpowers:executing-plans):** commits b73d6c3 (Step 1, #78), 8f0cee1 (2, #79), 20ea73a (3, P6a1I-2), d10394f (4, P6a1I-1), 4b6116b (5, P6a1I-3), 97f9df9 (6, P6a1I-4), 12dd203 (7, P6a1I-6); every new test failed first. Measured on the dev machine: on SIGTERM gateways, Logic and Redis exit 0, devtoken 143 (no handler), as the plan assumed. **Ruling (Step 3):** the production drain (`Drain.Window` 60s, spent in full even with no connections) can never finish inside `stack_down`'s 10s, so every teardown SIGKILLed the gateways and would report them `killed`; stack gateways now start with `-drain-window ${STACK_DRAIN_WINDOW:-1s}` (before `STACK_GATEWAY_ARGS`, which can override it) and stop with status 0 in about a second, through Drain, EpochEnded and Stop. Cost: a measured drain must set 60s (6b note above). **Ruling (Step 3):** with the 1s drain, the reverse-order mutant (Logic stopped first) no longer produced `lease renewal failed`, so the #78 test also refuses `EpochEnded failed` in gw-0/gw-1's logs; the mutant fails it. **Ruling (Step 2):** `stack_test.sh` pins its gateways to every CPU (was the last one) and adds a 1-gateway override stack: with one pinned CPU the CPU share equals what Go derives from affinity, so the test could not fail on the dev machine. Beyond the brief (Step 5): `t2_field`/`t2_duration` also refuse names `t2_result_json` writes itself (`pass`, `total`, …), and ending a window that is not open stops the run. Step 8: `make test-scripts` passes; `bash -n` clean on every script; the guard refuses `foundation` here (exit 2); `race.sh` with the guard lowered 4/4, 58/58, 324s; `foundation.sh` dry run (root parts faked, `T2_CPUS_GATEWAY=1`): `stack`, `pinning`, `artifacts`, `processes` pass, `netdrop` fails on the fake, `no_swap_in` saw 5 pages on this 1GB VM, clocks `null`, exit 1, every exit status clean, `loops=1` (1 CPU / 4 gateways), no `lease renewal failed`, no process left; the same dry run SIGTERMed in its idle window wrote `result.json` with `"stopped": {"exit": 143}` and the checks so far, and stopped the stack cleanly. Next: Tier 2 run 12 (`foundation` 6/6, `loops=3`, no `lease renewal failed`), then Plan 6a2.
- **Task 6 final review (2026-10-09, fresh Opus, 6275daf..355021c):** no Critical; the three rulings judged sound. Fixed in b9d1b6e, with a test that failed first: (I1) a SIGTERM or Ctrl-C while `t2_finish` wrote `result.json` (inside `git status`) left valid JSON with `"pass": true` plus a trailing `report:` line, so the file did not parse and the run exited 143; `t2_finish` now writes aside and renames, and `t2_exit` prints on the stdout saved by `t2_traps`. Deferred minors: (M1) a process that exits right at the stop deadline can be recorded `killed`; (M2) a SIGTERM inside the scenario's own `stack_down` re-runs it from the exit hook (duplicate `exit-status` lines); (M3) a terminal Ctrl-C reaches every stack process at once (Go re-enables SIGINT), so an interrupted run's logs show lease/EpochEnded failures; (M4) a scenario that ends without `t2_finish` exits 0 with `"pass": false`; (M5) a refused second `stack_up` in the same `STACK_DIR` keeps the old `exit-status`, and stale `gw-N` logs are still scanned for `panic:`; (M6) `STACK_PIN=1` with `stack.sh` sourced without `lib.sh` silently gives `GOMAXPROCS` 1.
