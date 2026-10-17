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
#    "pass", ["stopped": {"exit"} | "refused": reason,] "duration_s": {"total", ...},
#    "checks": {name: {"pass", ...}}, <fields added with t2_field>, "not_covered": [...]}
# A run passes only if it recorded at least one check and every check passed.
# After t2_init a result.json is written however the run ends: a run that
# stops before t2_finish gets "stopped" (its exit code) or "refused"; one
# that ends without t2_finish exits 1, never 0.

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
  # Control bytes and invalid UTF-8 (log tails of a crashing process) are dropped.
  s=$(printf '%s' "$s" | tr -d '\000-\010\013\014\016-\037' | iconv -c -f utf-8 -t utf-8 2>/dev/null)
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
# Process groups besides the run's own that count as ours (stack.sh starts
# each stack process in a group of its own and adds it here).
T2_OURS_PGIDS=()
# Names already used in result.json, as "check:NAME", "field:KEY" and
# "duration:NAME"; the members t2_result_json writes itself are taken.
declare -A T2_USED=([duration:total]=1)
for k in schema scenario tier commit dirty host pass stopped refused duration_s checks not_covered; do
  T2_USED[field:$k]=1
done

# t2_claim KIND NAME: stops the run (exit 1) when NAME was already used: a
# duplicate key would make result.json ambiguous (P6a1I-3).
t2_claim() {
  if [ -n "${T2_USED[$1:$2]:-}" ]; then
    echo "lib.sh: $1 name '$2' already used in result.json (a bug in the scenario)" >&2
    exit 1
  fi
  T2_USED[$1:$2]=1
}

# t2_check NAME PASS [MEMBERS]: records a check; PASS is true or false,
# MEMBERS more JSON object members, e.g. "\"failed\": \"x\", \"n\": 3".
t2_check() {
  case "$2" in true|false) ;; *) echo "t2_check $1: pass must be true or false, got '$2'" >&2; exit 1 ;; esac
  t2_claim check "$1"
  T2_CHECKS+=("$(t2_json_str "$1"): {\"pass\": $2${3:+, $3}}")
}
# t2_field KEY JSON: a top-level member of result.json.
t2_field() { t2_claim field "$1"; T2_FIELDS+=("$(t2_json_str "$1"): $2"); }
# t2_duration NAME SECONDS: a member of duration_s.
t2_duration() { t2_claim duration "$1"; T2_DURATIONS+=("$(t2_json_str "$1"): $2"); }
# t2_not_covered TEXT...: what this scenario does not verify.
t2_not_covered() { local s; for s in "$@"; do T2_NOT_COVERED+=("$(t2_json_str "$s")"); done; }
# t2_on_exit CMD: runs CMD (eval'd) at exit, newest first, before the report.
t2_on_exit() { T2_EXIT_HOOKS+=("$1"); }

t2_join() { local IFS=,; echo "$*"; }

# t2_result_json PASS [MEMBER]: the result document; MEMBER (a run that
# stopped early: "stopped" or "refused") follows "pass".
t2_result_json() {
  cat <<JSON
{
  "schema": 1,
  "scenario": $(t2_json_str "$T2_SCENARIO"),
  "tier": 2,
  "commit": $(t2_json_str "$(git rev-parse HEAD 2>/dev/null)"),
  "dirty": $(git status --porcelain 2>/dev/null | wc -l),
  "host": {"cores": $T2_CORES, "mem_gb": $T2_MEM_GB, "kernel": $(t2_json_str "$(uname -r)")},
  "pass": $1,${2:+
  $2,}
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
  # Written aside and renamed: a signal mid-write leaves no result.json, so
  # t2_exit writes the "stopped" one instead of keeping a half file.
  t2_result_json "$pass" >"$T2_OUT/result.json.tmp" && mv "$T2_OUT/result.json.tmp" "$T2_OUT/result.json"
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
    git status --porcelain | awk 'NR <= 20 { print "  " $0 } END { if (NR > 20) print "  … " NR - 20 " more" }'
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
  trap '' INT TERM # a second Ctrl-C must not cut the teardown short
  for ((i = ${#T2_EXIT_HOOKS[@]} - 1; i >= 0; i--)); do
    eval "${T2_EXIT_HOOKS[i]}" >/dev/null 2>&1
  done
  # A run that ends before t2_finish still leaves a result.json (P6a1I-1).
  rm -f "$T2_OUT/result.json.tmp"
  if [ ! -f "$T2_OUT/result.json" ]; then
    if [ -n "${T2_REFUSED:-}" ]; then
      t2_result_json false "\"refused\": $(t2_json_str "$T2_REFUSED")"
    elif [ "$rc" -eq 0 ]; then
      # Ended without t2_finish (a bug in the scenario): not a pass.
      rc=1
      t2_result_json false "\"stopped\": {\"exit\": 1, \"detail\": \"ended without t2_finish\"}"
    else
      t2_result_json false "\"stopped\": {\"exit\": $rc}"
    fi >"$T2_OUT/result.json"
  fi
  t2_write_report
  # To the stdout saved by t2_traps: a signal can run this trap while a
  # function's stdout is redirected to a file (t2_finish's result.json).
  echo "report: $T2_REPORT" >&"$T2_STDOUT"
  exit "$rc"
}

# t2_traps: the exit hooks and the report run on any exit, including
# Ctrl-C and SIGTERM, so no process or DROP rule outlives the run.
t2_traps() {
  exec {T2_STDOUT}>&1
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

# ---- Run recording (choices #70, #71): what else ran, swap, CPU clocks ----
#
# The Tier 2 host is also a desktop. A load scenario calls t2_mem_budget and
# t2_nofile before its setup, then t2_record_start / t2_record_end around its
# measured window. result.json gets a "background" member and a no_swap_in
# check: a window with any page swapped in fails (#70).
# "Ours" is this run's process group (make, the script and what it starts)
# plus the groups in T2_OURS_PGIDS (stack.sh's processes). Kernel threads are reported apart,
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

# t2_cpu_delta BEFORE AFTER OURS HZ: from two t2_proc_stats files (OURS: the
# process groups that are ours, space-separated), prints
# "outside_user_s kernel_threads_s top" where top is a JSON array of the
# ten biggest outside users. Kernel threads: pid 2 and its children.
t2_cpu_delta() {
  awk -v ours="$3" -v hz="$4" '
    NR == FNR { before[$1 " " $4] = $5; next }
    index(" " ours " ", " " $2 " ") { next }
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
# t2_cpustat_snap FILE [STAT SOFTNET]: per-CPU ticks from /proc/stat and
# softnet drops and budget squeezes from /proc/net/softnet_stat (hex; the
# CPU is its last column when present, else its row) into FILE, for
# t2_cpusets_json. Plain awk: the Tier 2 host's mawk has no strtonum.
t2_cpustat_snap() {
  awk 'function hex(s,   i, n) { n = 0; s = tolower(s)
         for (i = 1; i <= length(s); i++) n = n * 16 + index("0123456789abcdef", substr(s, i, 1)) - 1
         return n }
       FNR == NR { if ($1 ~ /^cpu[0-9]+$/) print $1, $2 + $3, $4, $5 + $6, $7 + $8, $9; next }
       { print "softnet" (NF >= 13 ? hex($13) : FNR - 1), hex($2), hex($3) }' \
    "${2:-/proc/stat}" "${3:-/proc/net/softnet_stat}" >"$1" 2>/dev/null
}

# t2_cpusets_json SNAP0 SNAP1: for the gateway, Logic and loadgen CPU sets
# (T2_CPUS_*), the share of the set's CPU time in user, system, softirq
# (with irq) and idle (with iowait) between the snapshots, and the softnet
# drops and squeezes on its CPUs; null if a snapshot is missing. Averaged
# CPU per process can look idle while its CPUs are busy in softirq (run 15).
t2_cpusets_json() {
  [ -s "$1" ] && [ -s "$2" ] || { echo null; return; }
  awk -v sets="gateway=$(t2_cpulist_expand "$T2_CPUS_GATEWAY" | tr ' ' ,) logic=$(t2_cpulist_expand "$T2_CPUS_LOGIC" | tr ' ' ,) loadgen=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | tr ' ' ,)" '
    FNR == NR { a[$1] = $0; next }
    { b[$1] = $0 }
    END {
      n = split(sets, s, " "); out = ""
      for (k = 1; k <= n; k++) {
        split(s[k], kv, "="); m = split(kv[2], cpus, ",")
        u = sy = si = id = dr = sq = 0
        for (c = 1; c <= m; c++) {
          split(a["cpu" cpus[c]], x, " "); split(b["cpu" cpus[c]], y, " ")
          u += y[2] - x[2]; sy += y[3] - x[3]; id += y[4] - x[4]; si += y[5] - x[5] + y[6] - x[6]
          split(a["softnet" cpus[c]], x, " "); split(b["softnet" cpus[c]], y, " ")
          dr += y[2] - x[2]; sq += y[3] - x[3]
        }
        t = u + sy + si + id; if (t <= 0) t = 1
        out = out (k > 1 ? ", " : "") sprintf("\"%s\": {\"cpus\": %d, \"user_pct\": %.1f, \"sys_pct\": %.1f, \"softirq_pct\": %.1f, \"idle_pct\": %.1f, \"softnet_dropped\": %d, \"softnet_squeezed\": %d}",
          kv[1], m, 100 * u / t, 100 * sy / t, 100 * si / t, 100 * id / t, dr, sq)
      }
      print "{" out "}"
    }' "$1" "$2"
}

# t2_tcpstat_snap FILE [NETSTAT SNMP]: the Tcp and TcpExt counters of
# /proc/net/snmp and /proc/net/netstat ("Prefix: names" then "Prefix:
# values") as "name value" lines into FILE, for t2_tcpstat_json.
t2_tcpstat_snap() {
  awk '$1 == "Tcp:" || $1 == "TcpExt:" {
         if (hdr[$1] == "") { hdr[$1] = $0; next }
         n = split(hdr[$1], h, " "); split($0, v, " "); for (i = 2; i <= n; i++) print h[i], v[i]
         hdr[$1] = "" }' "${2:-/proc/net/netstat}" "${3:-/proc/net/snmp}" >"$1" 2>/dev/null
}

# t2_tcpstat_json SNAP0 SNAP1: {NAME: delta} for T2_TCPSTAT_NAMES (a counter
# the kernel lacks is null); null if a snapshot is missing. The defaults
# tell a connect that fails (AttemptFails), a SYN resent (TCPSynRetrans), a
# full accept queue (ListenOverflows/Drops), a TIME_WAIT port reused with an
# old timestamp (PAWSActive) and resets.
T2_TCPSTAT_NAMES="ActiveOpens PassiveOpens AttemptFails EstabResets RetransSegs OutRsts InErrs TCPSynRetrans TCPTimeouts ListenOverflows ListenDrops TCPReqQFullDrop TCPReqQFullDoCookies PAWSActive PAWSEstab TW TWRecycled TCPTimeWaitOverflow TCPChallengeACK TCPAbortOnTimeout TCPAbortOnData TCPAbortOnClose TCPAbortOnMemory TCPMemoryPressures"
t2_tcpstat_json() {
  [ -s "$1" ] && [ -s "$2" ] || { echo null; return; }
  awk -v names="$T2_TCPSTAT_NAMES" 'FNR == NR { a[$1] = $2; next } { b[$1] = $2 }
    END { n = split(names, k, " "); out = ""
          for (i = 1; i <= n; i++) out = out (i > 1 ? ", " : "") "\"" k[i] "\": " ((k[i] in a) && (k[i] in b) ? sprintf("%.0f", b[k[i]] - a[k[i]]) : "null")
          print "{" out "}" }' "$1" "$2"
}

# t2_lease_floor SECONDS: the fewest lease renewals a gateway must make in a
# window of SECONDS (whole). A renewal counts when its call ends, at most 5s
# (the call timeout) after its 10s tick, so (SECONDS - 6) // 10 of them (1s
# for SECONDS' resolution), and never fewer than SECONDS // 10 - 1
# (P6b2b-slowI-5; shared with floods, Plan 6b3b).
t2_lease_floor() {
  local s=$1
  echo $((s / 10 - 1 > (s - 6) / 10 ? s / 10 - 1 : (s - 6) / 10))
}

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

# t2_record_start [NAME]: start of a measured window. Without NAME the run
# records one window: background, no_swap_in, background-before.txt,
# clocks.tsv; with NAME: background_NAME, no_swap_in_NAME,
# background-before-NAME.txt, clocks-NAME.tsv (P6a1I-3). Windows do not
# overlap: starting one while another is open stops the run (exit 1).
t2_record_start() {
  if [ -n "${T2_REC_OPEN:-}" ]; then
    echo "lib.sh: t2_record_start ${1:-}: window '${T2_REC_NAME:-(unnamed)}' is already open (a bug in the scenario)" >&2
    exit 1
  fi
  T2_REC_OPEN=1 T2_REC_NAME=${1:-}
  local f=${1:+-$1}
  T2_PGID=$(t2_pgid $$)
  t2_top >"$T2_OUT/background-before$f.txt"
  t2_proc_stats >"$T2_OUT/.procs-before"
  T2_BUSY0=$(t2_busy_ticks)
  T2_SWAP0=$(t2_swap_pages)
  T2_REC_T0=$SECONDS
  t2_clock_sampler "$T2_OUT/clocks$f.tsv" &
  T2_SAMPLER=$!
  t2_on_exit '[ -n "$T2_SAMPLER" ] && kill "$T2_SAMPLER"'
}

# t2_record_end [NAME]: end of the window: the background member and
# no_swap_in, named as t2_record_start says.
t2_record_end() {
  local hz busy swap1 delta in out m=${1:+_$1} f=${1:+-$1}
  if [ -z "${T2_REC_OPEN:-}" ] || [ "$T2_REC_NAME" != "${1:-}" ]; then
    echo "lib.sh: t2_record_end ${1:-}: no such window open (a bug in the scenario)" >&2
    exit 1
  fi
  T2_REC_OPEN=""
  kill "$T2_SAMPLER" 2>/dev/null
  wait "$T2_SAMPLER" 2>/dev/null
  T2_SAMPLER="" # reaped: its pid may be reused before the exit hook runs
  hz=$(getconf CLK_TCK)
  t2_proc_stats >"$T2_OUT/.procs-after"
  t2_top >"$T2_OUT/background-after$f.txt"
  busy=$(( $(t2_busy_ticks) - T2_BUSY0 ))
  swap1=$(t2_swap_pages)
  in=$(( ${swap1% *} - ${T2_SWAP0% *} ))
  out=$(( ${swap1#* } - ${T2_SWAP0#* } ))
  read -r user kern top <<<"$(t2_cpu_delta "$T2_OUT/.procs-before" "$T2_OUT/.procs-after" "$T2_PGID ${T2_OURS_PGIDS[*]}" "$hz")"
  t2_field "background$m" "{\"window_s\": $((SECONDS - T2_REC_T0)), \"host_busy_cpu_s\": $(awk -v b="$busy" -v h="$hz" 'BEGIN { printf "%.1f", b / h }'), \"outside_user_cpu_s\": $user, \"kernel_threads_cpu_s\": $kern, \"top_outside\": $top, \"swap_in_pages\": $in, \"swap_out_pages\": $out, \"cpu_clocks\": $(t2_clock_summary "$T2_OUT/clocks$f.tsv")}"
  t2_check "no_swap_in$m" "$(t2_bool [ "$in" -eq 0 ])" "\"swap_in_pages\": $in"
}

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

# t2_netdrop_leftover RULES: refuses (returns 1) when goim-netdrop rules are
# left, e.g. by a SIGKILLed run: they would silently drop the new stack's ports.
t2_netdrop_leftover() {
  if [ -n "$1" ]; then
    printf 'refused: goim-netdrop rules left by an earlier run:\n%s\nremove them: sudo -n /usr/local/sbin/goim-netdrop flush\n' "$1"
    return 1
  fi
}

# t2_require_host_setup: load scenarios call it after t2_init; refuses
# (exit 2) until host-setup.sh has run with this checkout's files, and while
# an earlier run's DROP rules are left.
t2_require_host_setup() {
  local missing rules why
  missing=$(t2_host_setup_missing scripts/tier2 /proc/sys /sys/module /usr/local/sbin/goim-netdrop)
  if [ -z "$missing" ] && ! rules=$(sudo -n /usr/local/sbin/goim-netdrop list 2>/dev/null); then
    missing="sudo -n /usr/local/sbin/goim-netdrop fails (no sudoers line?)"
  fi
  if [ -n "$missing" ]; then
    t2_refuse "$(printf 'refused: the host is not set up for load tests:\n%s\nrun once: sudo scripts/tier2/host-setup.sh' "$missing")"
  fi
  why=$(t2_netdrop_leftover "$rules") || t2_refuse "$why"
}
