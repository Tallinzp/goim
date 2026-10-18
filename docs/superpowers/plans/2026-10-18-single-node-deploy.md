# Single-Node Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `make deploy` starts a whole goim stack (Redis, devtoken, Logic, one gateway) on one Ubuntu or macOS machine, so a real client app on a laptop in the LAN can connect to it. `make deploy-down` stops it gracefully.

**Architecture:** One new script, `scripts/deploy.sh`, sourceable (its functions are tested) and runnable (`deploy.sh up|down|status|logs|token`). It does not reuse `stack.sh`, which is built for load runs: that file makes new keys at every start, listens on 127.0.0.1 only, needs bash 4 and Linux tools, and keeps its processes as children of one shell. `deploy.sh` keeps keys and data in `$GOIM_HOME`, starts detached processes with pid files, and runs only the commands both OSes have. It starts the same binaries with the same flags as `stack_up`. Makefile targets call it.

**Tech Stack:** bash 3.2-compatible shell, `go build`, `curl`, the existing `scripts/redis-build.sh` (made portable), shellcheck for the lint step.

**Spec:** none was written for this small change. The decisions below were approved in the conversation on 2026-10-18 and are copied here. Where this plan changes a call that was shown to the user, it says so.

## Global Constraints

- Target OS: Ubuntu and macOS. The script must run under macOS's bash 3.2. It must not use `declare -A`, `mapfile`, `${v,,}`, `|&`, `&>>`, `setsid`, `taskset`, `timeout`, `readlink -f`, `sha256sum`, `ss`, or GNU-only `sed`, `date`, `stat` flags. A lint test enforces this list.
- No Docker, no systemd, no TLS, no real account service, no multi-node. devtoken signs the tokens.
- Redis, devtoken, Logic and every admin and Kick port listen on 127.0.0.1. Only the gateway client port listens on `GOIM_LISTEN_HOST` (default `0.0.0.0`).
- Ports, with `B` = `GOIM_PORT_BASE` (default 18000): Redis `B`, devtoken `B+1`, Logic gRPC `B+2`, lease `B+3`, Logic admin `B+4`, gateway client `B+100`, gateway Kick `B+101`, gateway admin `B+102`. The gateway id is `gw-0`.
- Data directory `GOIM_HOME` (default `$HOME/.goim`): `bin/`, `keys/` (`noise/1`, `noise-public`, `jwt.jwk`, `jwks.json`), `logs/NAME.log`, `run/` (`NAME.pid`, `NAME.status`), `redis/`, `kick.json`, `node.env`. The path may contain spaces.
- Keys are made once and kept. A second `make deploy` never changes them.
- The gateway refuses to be Ready unless `RLIMIT_NOFILE` ≥ `-max-conns` × 1.10 and, when conntrack is loaded, `nf_conntrack_max` ≥ 2 × `-max-conns` (`internal/gateway/limits.go`). The default cap of 250,000 would fail on every laptop (Ubuntu soft limit 1024 or 1,048,576; macOS 256). So the script starts the gateway with `-max-conns ${GOIM_MAX_CONNS:-5000} -pending-cap 2500 -logout-queue ${GOIM_MAX_CONNS:-5000}` and first raises the soft file limit with `ulimit -n` to `min(hard limit, 10240)` (10240 is macOS's per-process maximum). It does not pass `-conntrack-notrack`, which would claim something untrue. If the raised limit is still below 1.10 × the cap, `up` stops with a message that names both numbers.
- Redis runs with `--appendonly yes --save ""` and `--maxmemory-policy noeviction`.
- Token defaults: lifetime `GOIM_TOKEN_TTL=24h`, `appid` = `im`, platform `mobile`.
- Stop order: gateway, Logic, devtoken, Redis. The gateway gets up to `GOIM_DOWN_WAIT` seconds (default 60) to drain; the other three get 10 seconds each. A process that does not stop in time is reported, not killed. `FORCE=1` sends SIGKILL to it.
- Exit status counts as clean when it is 0. For devtoken, 143 is clean too (it has no SIGTERM handler; `stack_health` does the same).
- Commit messages: STE style (imperative subject near 60 characters, short-sentence body), and end with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`.

**Calls made in this plan that differ from what the user saw:**
- The gateway drain defaults take up to 85 seconds (`-drain-window` 60s, `-drain-flush` 10s, `-drain-timeout` 85s), which is longer than the 60-second wait that was shown. A single node has no use for a 60-second spread. The script starts the gateway with `-drain-window ${GOIM_DRAIN_WINDOW:-5s}` (default 5s). The drain then ends in about 20 seconds with the other drain defaults.
- The `make` variable for the token user is `USER_ID`, not `USER`. Make takes `USER` from the environment (the login name), so `USER=alice` would work but a missing value would silently use the login name.

## Review Focus

- A stale pid file whose pid belongs to an unrelated process. `down` and `status` must not signal that process.
- A busy port found at start (for example `B+100`). `up` must fail before it starts anything, and leave no process and no pid file.
- `down` when nothing runs, and `up` when the node already runs. Both exit 0 and change nothing.
- `GOIM_HOME` with a space in the path.
- `token` before the first `up`, or without `USER_ID`. A clear error, exit 2, no stack trace.

---

### Task 1: Process lifecycle helpers and portability lint

**Files:**
- Create: `scripts/deploy.sh` (helpers only in this task)
- Create: `scripts/deploy_test.sh`

**Interfaces:**
- Produces, in `scripts/deploy.sh`. All functions read the globals set by `deploy_config`:
  - `deploy_config` — sets `D_HOME D_BASE D_HOST D_ADV D_REDIS D_WAIT D_TTL D_APPID D_DRAIN` from `GOIM_HOME GOIM_PORT_BASE GOIM_LISTEN_HOST GOIM_ADVERTISE_HOST GOIM_REDIS_SERVER GOIM_DOWN_WAIT GOIM_TOKEN_TTL GOIM_APPID GOIM_DRAIN_WINDOW`. `D_ADV` defaults to `D_HOST` when that is not `0.0.0.0`, else `$(deploy_lan_ip)`. `D_REDIS` defaults to `redis-server` on PATH, else `.cache/redis/bin/redis-server` in the repo.
  - `deploy_lan_ip` — prints the machine's LAN IPv4. Linux: the `src` of `ip route get 1.1.1.1`. macOS: `ipconfig getifaddr` on the interface from `route -n get default`. If none is found, it prints `127.0.0.1` and writes a warning to stderr.
  - `deploy_preflight` — returns 1 and prints one install hint per missing tool: `go`, a C compiler (`cc`), `curl`, and the Redis server (hint: `apt install redis-server`, `brew install redis`, or `make redis-server`).
  - `deploy_port_free PORT` — true when a connect to `127.0.0.1:PORT` is refused (bash `/dev/tcp`).
  - `deploy_spawn NAME CMD...` — starts CMD detached (survives the caller and a closed terminal), with stdin from `/dev/null` and stdout and stderr of CMD and of the supervising subshell both sent to `$D_HOME/logs/NAME.log` (so `out=$(deploy_up)` and `make deploy | tee` do not hang on an inherited pipe), writes `$D_HOME/run/NAME.pid` as `PID COMM` (COMM = basename of CMD), and removes any old `NAME.status`, and returns only after the pid file exists (up to 5 seconds; else return 1). A small supervising subshell waits for CMD and writes its exit status to `NAME.status`, then removes the pid file.
  - `deploy_alive NAME` — true only when the pid file exists, the pid is alive, and `ps -p PID -o comm=` has the same basename as the stored COMM (compared on the first 15 characters). A pid file that fails this check is stale: `deploy_alive` removes it and prints `NAME: stale pid file removed` to stderr.
  - `deploy_stop NAME WAIT` — returns 0 when NAME is not running, or stopped with a clean status (0; for `devtoken` also 143). Otherwise it sends SIGTERM and waits up to WAIT seconds for `NAME.status`. It prints `NAME: stopped` or `NAME: exited with status N` (returns 1) or `NAME: still running after Ns` (returns 1, process left running). With `FORCE=1` it then sends SIGKILL and returns 1.
  - `deploy_ready NAME URL SECONDS` — polls `curl -sf URL` until it answers; returns 1 and prints the last 20 log lines if NAME died or the time ends.

- [ ] **Step 1: Write the failing tests in `scripts/deploy_test.sh`.** Follow the style of `scripts/stack_test.sh` (`ok`, `bad`, `expect`, a `fails` counter, exit 1 on any failure). Source `scripts/deploy.sh`. Set `GOIM_HOME` to a `mktemp -d` path that contains a space. Tests for this task, using `sleep 300` and a shell snippet that exits 7 as stand-in services:
  - `config: defaults` — with no `GOIM_*` set, `D_BASE=18000`, `D_HOST=0.0.0.0`, `D_WAIT=60`, `D_TTL=24h`, `D_APPID=im`, `D_DRAIN=5s`.
  - `spawn: returns at once, no inherited pipe` — `out=$(deploy_spawn sleeper sleep 300)` returns in under 2 seconds, and `deploy_alive sleeper` is true right after it.
  - `spawn: pid file and log` — after `deploy_spawn sleeper sleep 300`, `deploy_alive sleeper` is true, `run/sleeper.pid` has two fields, and output the command writes is in `logs/sleeper.log`.
  - `stop: clean stop` — `deploy_stop sleeper 5` returns 1 (sleep dies from SIGTERM, status 143, not clean), prints `exited with status 143`, and leaves no pid file; a stand-in that traps TERM and exits 0 returns 0.
  - `stop: not running is a no-op` — returns 0 and prints nothing.
  - `stop: timeout leaves the process` — a stand-in that ignores TERM: `deploy_stop` with WAIT 1 returns 1, prints `still running after 1s`, the process is still alive; with `FORCE=1` it is gone.
  - `alive: stale pid file` — write a pid file with the pid of an unrelated live process (`$$`) and COMM `sleep`: `deploy_alive` is false, the file is gone, and the unrelated process still runs.
  - `port: free and busy` — `deploy_port_free` is true for an unused port and false while a stand-in listener holds it (use `python3 -c` with a socket, or `redis-server` if python is missing).
  - `lint: portability` — `grep -nE` over `scripts/deploy.sh` for each pattern in Global Constraints' list must find nothing; `bash -n scripts/deploy.sh` passes; `shellcheck -s bash scripts/deploy.sh` passes when shellcheck is installed (print `skip` otherwise).
  - `lan ip: fallback` — with `PATH` set to an empty temp dir plus the shell builtins, `deploy_lan_ip` prints `127.0.0.1` and writes a warning.

- [ ] **Step 2: Run `scripts/deploy_test.sh` and see it fail** (the script does not exist). Expected: `No such file` for `scripts/deploy.sh`.

- [ ] **Step 3: Write the helpers in `scripts/deploy.sh`.** Start with `set -uo pipefail` only inside the `main` guard (the file is sourced by tests, so it must not change the caller's shell options at load time). Put `if [ "${BASH_SOURCE[0]}" = "$0" ]; then deploy_main "$@"; fi` at the end. Comments: short, in the style of `scripts/stack.sh`.

- [ ] **Step 4: Run `scripts/deploy_test.sh` and see every test pass.**

- [ ] **Step 5: Commit.**

```bash
git add scripts/deploy.sh scripts/deploy_test.sh
git commit -m "feat: add process helpers for the single-node deploy"
```

---

### Task 2: The `up`, `down`, `status`, `logs` and `token` commands

**Files:**
- Modify: `scripts/deploy.sh`
- Modify: `scripts/deploy_test.sh`

**Interfaces:**
- Consumes: every helper from Task 1.
- Produces:
  - `deploy_up` — in this order: `deploy_config`; `deploy_preflight`; if the node already runs (all four `deploy_alive`), print the node summary and return 0; refuse with a message if only some run (name them; tell the user to run `make deploy-down` first); check that ports `B..B+4` and `B+100..B+102` are free (list every busy port, then return 1); `go build -o $D_HOME/bin/ ./cmd/gateway ./cmd/logic ./cmd/devtoken`; make keys if `keys/noise/1` is missing (`gateway noise-keygen -key-id 1 -out …`, public half after the text ` public ` goes to `keys/noise-public`; `devtoken keygen -kid k1 -out keys/jwt.jwk`; `devtoken jwks -out keys/jwks.json keys/jwt.jwk`); write `kick.json` as `{"gw-0": "127.0.0.1:B+101"}`; start Redis, wait for `redis-cli ping` (the `redis-cli` next to the server binary); start devtoken (wait on `http://127.0.0.1:B+1/`), Logic (wait on `http://127.0.0.1:B+4/readyz`), the gateway (wait on `http://127.0.0.1:B+102/readyz`); write `node.env`; print the summary. Flags are those of `stack_up` in `scripts/stack.sh`, with `-drain-window "$D_DRAIN"` and the connection-cap flags and `ulimit -n` raise from Global Constraints on the gateway, `-listen "tcp://$D_HOST:$((B+100))"`, and Redis `--bind 127.0.0.1 --dir $D_HOME/redis --appendonly yes --save ""  --maxmemory-policy noeviction`. A `trap` on INT, TERM and on failure calls `deploy_down` for what was started, then returns 1. The summary has, one per line: `gateway   ADV:B+100`, `noise key id  1`, `noise public  HEX`, `token       make deploy-token USER_ID=<uid>`, `logs        D_HOME/logs`.
  - `deploy_down` — `deploy_config`; stop gateway (`$D_WAIT`), then logic, devtoken, redis (10 each) with `deploy_stop`; return 1 if any returned 1, after trying all four. If none was running, print `goim: no node is running` and return 0.
  - `deploy_status` — first the same summary `deploy_up` prints (read from `node.env`; this is how you see the Noise public key again), then one line per process: `NAME running pid N` or `NAME stopped`, plus `ready` or `not ready` for Logic and the gateway from their `/readyz`. Exit 0 when all four run and are ready, else 1.
  - `deploy_logs [NAME]` — `tail -n 50 -F` on one log, or on all four.
  - `deploy_token UID` — uses `devtoken issue -key $D_HOME/keys/jwt.jwk -sub UID -appid $D_APPID -platform ${GOIM_PLATFORM:-mobile} -ttl $D_TTL`. With no UID, or when `keys/jwt.jwk` or `bin/devtoken` is missing, it prints a one-line error (`token: UID is required` / `token: run make deploy first`) and returns 2.
  - `deploy_main` — dispatches `up down status logs token`; anything else prints usage and returns 2.

- [ ] **Step 1: Write the failing end-to-end tests in `scripts/deploy_test.sh`.** Use `GOIM_PORT_BASE=18700`, `GOIM_LISTEN_HOST=127.0.0.1`, `GOIM_REDIS_SERVER` as the caller gives it (the Makefile passes it; unset means `redis-server` on PATH), `GOIM_DOWN_WAIT=40`, and a `trap` that runs `deploy_down` and removes the temp home. Cases, in this order:
  - `up: busy port refuses` — hold port `18800` with a listener, run `deploy_up`: returns 1, names the port, `run/` has no pid file, no `redis-server` listens on `18700`. Then free the port.
  - `up: low file limit` — run `deploy_up` in a subshell after `ulimit -S -n 256`: it returns 0 and the gateway's `/readyz` answers (the script raised the limit). Stop the node after this case, then the next case starts it again.
  - `up: node comes up` — `deploy_up` returns 0; `deploy_status` returns 0; the output holds `noise public` followed by 64 hex characters; a TCP connect to `127.0.0.1:18800` succeeds; `curl -sf http://127.0.0.1:18704/readyz` and `…:18802/readyz` answer.
  - `up: second run is a no-op` — keys' `cksum` and the four pids are the same before and after a second `deploy_up`, which returns 0 and prints the summary.
  - `token: valid` — `deploy_token alice` prints one line with two dots. Decode its payload (python3 `base64` on the middle part) and check `sub` is `alice`, `appid` is `im`, and `exp - iat` is 86,400,000 ms.
  - `token: errors` — no UID returns 2; with `GOIM_HOME` pointing at an empty directory returns 2 with `run make deploy first`.
  - `down: graceful` — `deploy_down` returns 0; no pid file remains; `run/gateway.status` is `0` (the four processes are named `redis`, `devtoken`, `logic` and `gateway` in pid, status and log files); `run/redis.status` is `0`; `run/devtoken.status` is `0` or `143`; no listener on `18700..18704` or `18800..18802`.
  - `down: nothing running` — a second `deploy_down` returns 0 and prints `no node is running`.
  - `up: after down keeps keys` — `deploy_up` again: the Noise public key is the same as in the first run. Stop it again.
  - `up: rollback on failure` — start with `GOIM_REDIS_SERVER` pointing at a script that exits 1 at once: `deploy_up` returns 1, no pid file remains, no listener remains.
  - `up: Ctrl-C rolls back` — a background job of a non-interactive shell ignores SIGINT, and an ignored signal cannot be trapped. So start `scripts/deploy.sh up` with job control on (`set -m` around the launch, then `set +m`), send SIGINT as soon as `run/redis.pid` appears, wait for it: no pid file and no listener remain. Also run the same with SIGTERM.
  - `lint` — the Task 1 lint cases run again on the longer file.

- [ ] **Step 2: Run `scripts/deploy_test.sh` and see the new cases fail** (`deploy_up: command not found`).

- [ ] **Step 3: Implement the five functions and `deploy_main` in `scripts/deploy.sh`** as specified in Interfaces. Read the Gateway's `-drain-timeout` default (85s in `cmd/gateway/main.go`) before you pick the wait: the gateway wait `D_WAIT` must be longer than the drain you start it with. If `-drain-window 5s` fails the gateway's config validation, raise the default to the smallest value that passes and record it in the Global Constraints.

- [ ] **Step 4: Run `scripts/deploy_test.sh` and see every case pass.** Expected last line: `0 failures` in the style of `stack_test.sh`. If `redis-server` is not installed, run `make redis-server` first.

- [ ] **Step 5: Commit.**

```bash
git add scripts/deploy.sh scripts/deploy_test.sh
git commit -m "feat: add up, down, status, logs and token to deploy.sh"
```

---

### Task 3: Makefile targets and a portable `redis-build.sh`

**Files:**
- Modify: `Makefile` (new targets; `test-scripts` and `.PHONY`)
- Modify: `scripts/redis-build.sh` (checksum step)

**Interfaces:**
- Consumes: `scripts/deploy.sh up|down|status|logs|token` from Task 2.
- Produces: targets `deploy`, `deploy-down`, `deploy-status`, `deploy-logs`, `deploy-token`.

- [ ] **Step 1: Write the failing check.** Run `make -n deploy-token USER_ID=alice`. Expected: `No rule to make target 'deploy-token'`.

- [ ] **Step 2: Add the targets to `Makefile`.** Put them in their own block with a comment in the style of the file. `deploy` runs `GOIM_REDIS_SERVER=$(REDIS_SERVER)` only when that file exists (so a Redis on PATH or an installed one still wins in the script); `deploy-down` passes `FORCE=$(FORCE)`; `deploy-token` passes `$(USER_ID)` and fails with `USER_ID=<uid> is required` when it is empty; `deploy-logs` passes `$(NAME)`. Add `scripts/deploy_test.sh` to the end of `test-scripts` (with `GOIM_REDIS_SERVER=$(REDIS_SERVER)`), and the new names to `.PHONY`.

- [ ] **Step 3: Make `scripts/redis-build.sh` portable.** Replace the `sha256sum -c` line with a check that uses `sha256sum` when present and `shasum -a 256` otherwise. The `getconf _NPROCESSORS_ONLN` call works on both OSes and stays. No other change.

- [ ] **Step 4: Verify.**
  - `make -n deploy-token USER_ID=alice` prints a `deploy.sh token alice` line; `make deploy-token` (no `USER_ID`) fails with the message.
  - `make test-scripts` passes (all existing scripts and the new one).
  - `make deploy && make deploy-status && make deploy-token USER_ID=alice && make deploy-down` run by hand on the dev machine under the default home in a temporary `GOIM_HOME`; the whole cycle exits 0.
  - `shellcheck -s bash scripts/redis-build.sh` passes if shellcheck exists.

- [ ] **Step 5: Commit.**

```bash
git add Makefile scripts/redis-build.sh
git commit -m "feat: add deploy targets to the Makefile"
```

---

### Task 4: README sections and the final check

**Files:**
- Modify: `README.md` (English section after "Running the tests", and the Chinese section under `## 中文` in the same place)

**Interfaces:**
- Consumes: the targets from Task 3.

- [ ] **Step 1: Write an English section "Run a node on your machine"** (about 25 lines): requirements (Go 1.27, a C compiler, `curl`, Redis from `apt`/`brew` or `make redis-server`); the five commands; what `make deploy` prints; the settings table (`GOIM_HOME`, `GOIM_PORT_BASE`, `GOIM_LISTEN_HOST`, `GOIM_ADVERTISE_HOST`, `GOIM_DOWN_WAIT`, `GOIM_DRAIN_WINDOW`, `GOIM_TOKEN_TTL`, `GOIM_APPID`); the firewall note (Ubuntu `ufw allow B+100/tcp`; macOS asks once to accept incoming connections); the reboot note (run `make deploy` again, keys stay the same); and this line: "Tested on Ubuntu. The macOS parts are checked by reading, shellcheck and cross-compiling; they have not run on a real Mac yet." Add `scripts/deploy.sh` to the `scripts/` row of the layout table.

- [ ] **Step 2: Write the same section in Chinese** under `## 中文`, in the style of the existing Chinese text.

- [ ] **Step 3: Final checks.**
  - `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o /dev/null ./cmd/gateway ./cmd/logic ./cmd/devtoken` succeeds.
  - `bash -n scripts/deploy.sh scripts/redis-build.sh` and `shellcheck -s bash scripts/deploy.sh scripts/deploy_test.sh` pass.
  - `make test-scripts` passes.
  - `make test` passes (nothing in Go changed; this proves it).
  - Run the cycle once by hand with the default `GOIM_HOME`, then connect from a second machine on the LAN (`nc -vz ADV B+100`) if one is at hand; report what you could not check.

- [ ] **Step 4: Commit.**

```bash
git add README.md
git commit -m "docs: add the single-node deploy to the README"
```
