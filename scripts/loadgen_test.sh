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

# A control API that accepts but never answers reads as null within
# LG_CURL_TIMEOUT, so a wedged loadgen cannot hang a run (6b1 minor M7).
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
python3 -c 'import socket, sys, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(8); time.sleep(30)' "$port" &
mute=$!
sleep 0.5
start=$SECONDS
got=$(STACK_PORT_BASE=$((port - 5)) LG_CURL_TIMEOUT=1 timeout 10 bash -c '. scripts/loadgen.sh; lg_get /stats "d[\"online\"]"')
expect "get: a silent control API is null" "$got" null
expect "get: within the timeout" "$((SECONDS - start < 5))" 1
kill $mute 2>/dev/null; wait $mute 2>/dev/null

# The waits keep wall-clock deadlines (#103): a control API that takes 1s
# per reply (a /slo over 600k clients took ~0.1s in run 22, which made a
# 600s wait last 739s) must not stretch a 2s wait to the count of its polls.
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
python3 -c 'import http.server, sys, time
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(1)
        b = b"{\"back\": 0, \"population\": 1, \"ramp_done\": false, \"online\": 0}"
        self.send_response(200); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()' "$port" &
slow=$!
sleep 0.5
start=$SECONDS
STACK_PORT_BASE=$((port - 5)) bash -c '. scripts/loadgen.sh; lg_wait_back death 2'
expect "wait_back: not back is a failure" "$?" 1
expect "wait_back: a 2s wait ends by its deadline" "$((SECONDS - start <= 3))" 1
start=$SECONDS
STACK_PORT_BASE=$((port - 5)) bash -c '. scripts/loadgen.sh; LG_PID=$1; lg_wait_online 1 2' _ $$ 2>"$dir/online.err"
expect "wait_online: not online is a failure" "$?" 1
expect "wait_online: a 2s wait ends by its deadline" "$((SECONDS - start <= 5))" 1 # + one read for the message
expect "wait_online: gave up on the deadline, not on loadgen" "$(grep -c "online and failed after" "$dir/online.err")" 1
kill $slow 2>/dev/null; wait $slow 2>/dev/null

# Instances (P6a2bI-1), with a fake loadgen that serves /stats on its
# -control port, and at SIGTERM writes its -out and exits $FAKE_EXIT.
(
  STACK_DIR=$dir/stack
  mkdir -p "$STACK_DIR/bin"
  cat >"$STACK_DIR/bin/loadgen" <<'PY'
#!/usr/bin/env python3
import http.server, json, os, signal, sys
a = sys.argv[1:]
host, port = a[a.index("-control") + 1].rsplit(":", 1)
out = a[a.index("-out") + 1]
doc = json.dumps({"out": os.path.basename(out), "mode": a[a.index("-mode") + 1]}).encode()
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header("Content-Length", str(len(doc))); self.end_headers(); self.wfile.write(doc)
    def log_message(self, *x): pass
def term(*_):
    open(out, "wb").write(doc)
    os._exit(int(os.environ.get("FAKE_EXIT", "0")))
signal.signal(signal.SIGTERM, term)
http.server.HTTPServer((host, int(port)), H).serve_forever()
PY
  chmod +x "$STACK_DIR/bin/loadgen"
  STACK_PORT_BASE=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  T2_OURS_PGIDS=()
  lg_start a -mode light || echo "FAIL inst: main did not start"
  main_pid=$LG_PID
  LG_ENV=(FAKE_EXIT=3)
  STACK_PIN=1 T2_CPUS_LOADGEN=0 LG_CPUS=1 lg_in flood lg_start b -mode garbage || echo "FAIL inst: flood did not start"
  expect "inst: main's control port is base + 5" "$(lg_control)" "127.0.0.1:$((STACK_PORT_BASE + 5))"
  expect "inst: flood's control port is base + 6" "$(lg_in flood lg_control)" "127.0.0.1:$((STACK_PORT_BASE + 6))"
  expect "inst: main answers as main" "$(lg_get /stats 'd["out"]')" '"loadgen-a.json"'
  expect "inst: flood answers as flood" "$(lg_in flood lg_get /stats 'd["mode"]')" '"garbage"'
  expect "inst: LG_PID is main's again after lg_in" "$LG_PID" "$main_pid"
  expect "inst: LG_CPUS pins the flood" "$(taskset -pc "${LG_PIDS[flood]}" | awk '{print $NF}')" 1
  out=$(lg_start c -mode light 2>&1)
  expect "inst: a second start of a running instance is refused" "$? $out" "1 loadgen: instance main already runs (a)"
  out=$(lg_in probe lg_start a -mode light 2>&1)
  expect "inst: another instance's running name is refused" "$? $out" "1 loadgen: name a is instance main's"
  lg_in x3 true && lg_in x4 true # main, flood, x3, x4 (probe was named in a subshell only)
  out=$(lg_in x5 true 2>&1)
  expect "inst: a 5th instance is refused" "$? $out" "1 loadgen: instance x5: already 4 instances"
  lg_wait 'd["mode"] == "light"' 2
  expect "wait: a true expression" "$?" 0
  start=$SECONDS
  lg_wait 'd["mode"] == "sdk"' 1
  expect "wait: a false one fails by its deadline" "$? $((SECONDS - start <= 2))" "1 1"
  flood_pid=${LG_PIDS[flood]}
  lg_in flood lg_stop
  expect "inst: the flood stopped" "$(kill -0 "$flood_pid" 2>/dev/null && echo running || echo stopped) ${LG_STATUSES[flood]}" "stopped 3"
  expect "inst: LG_STATUS is main's after lg_in" "[$LG_STATUS]" "[]"
  start=$SECONDS
  lg_in flood lg_wait 'd["mode"] == "garbage"' 5 2>"$dir/wait.err"
  expect "wait: a stopped instance fails at once" "$? $((SECONDS - start <= 1)) $(grep -c 'loadgen exited' "$dir/wait.err")" "1 1 1"
  expect "inst: main still runs" "$(kill -0 "$LG_PID" 2>/dev/null && echo running)" running
  expect "inst: the flood's result" "$(lg_in flood lg_json 'd["out"]')" '"loadgen-b.json"'
  lg_in flood lg_start b2 -mode slow-auth || echo "FAIL inst: a stopped instance did not start again"
  LG_ENV=()
  expect "inst: a stopped instance runs again under a new name" "$(lg_in flood lg_get /stats 'd["mode"]')" '"slow-auth"'
  lg_stop_all
  expect "inst: lg_stop_all stopped main" "$(kill -0 "$main_pid" 2>/dev/null && echo running || echo stopped) ${LG_STATUSES[main]} [$LG_PID]" "stopped 0 []"
  expect "inst: LG_STATUS is main's after lg_stop_all" "$LG_STATUS ${LG_STATUSES[flood]}" "0 3"
  expect "inst: the restarted instance's result" "$(lg_in flood lg_json 'd["out"]')" '"loadgen-b2.json"'
  expect "inst: main's result" "$(lg_json 'd["out"]')" '"loadgen-a.json"'
) | tee "$dir/inst.out"
fails=$((fails + $(grep -c '^FAIL' "$dir/inst.out")))

[ "$fails" -eq 0 ] && echo "loadgen_test: all passed" || { echo "loadgen_test: $fails failed"; exit 1; }
