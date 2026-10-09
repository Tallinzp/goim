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
