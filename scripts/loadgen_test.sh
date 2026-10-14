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

[ "$fails" -eq 0 ] && echo "loadgen_test: all passed" || { echo "loadgen_test: $fails failed"; exit 1; }
