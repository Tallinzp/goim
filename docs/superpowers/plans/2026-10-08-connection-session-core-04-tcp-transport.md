# TCP Transport Implementation Plan — Plan 4 of 6

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the gateway core on the network: the 6-byte frame codec, Noise NK with the `key_id` prologue, the handshake limiter (semaphore + token bucket), the gnet v2.10.0 transport with its loop-ownership `-race` test, and `cmd/gateway`, so a client can connect over TCP, handshake, log in through Logic, ping, be kicked and log out.

**Architecture:** Two new packages. `internal/wire` is the byte format with no I/O policy: header, frame cutting, Noise NK on both sides, per-frame AEAD and the key set; the server uses the responder side, and the test client here, Plan 5's SDK and Plan 6's loadgen use the initiator side. `internal/gateway/tcp` is the gnet event handler: it implements Plan 3's `gateway.Loop` and `gateway.Link`, cuts frames in `OnTraffic`, runs the handshake inline on the loop under the limiter, and calls the core's `Open`, `HandshakeDone`, `Frame` and `Closed` on the connection's loop. A re-entry guard defers `Closed` when gnet closes a socket inside a write. `cmd/gateway` wires flags, the Noise key Secret, the core and the transport, and drains on SIGTERM.

**Tech Stack:** Go 1.27, `github.com/panjf2000/gnet/v2 v2.10.0` (exact), `github.com/flynn/noise v1.1.0`, `golang.org/x/time/rate` (already a dependency), and the Plan 2/3 stack (`grpc v1.84.0`, `protobuf v1.36.12`, `client_golang v1.24.1`). Tests run real gnet on 127.0.0.1 against a fake Logic and, in one end-to-end file, Plan 2's real Logic on miniredis.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative), sections *Client protocol* (outer frame, frame 1, later frames), *Decisions → Client encryption*, *Gateway* (gnet pin, *Transport seam → TCP transport*, *Noise key runbook*, *Identity* checked against gnet, *Admission control → Noise handshake limit*), *Storms, drain, failures* (bad frame, handshake flood), *Observability* (handshakes/s, reject reasons), *Verification → Unit tests* (frame codec, Noise, gnet loop-ownership). Roadmap, choices and invariant index: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`. The seam this plan plugs into: `docs/superpowers/plans/2026-10-08-connection-session-core-03-gateway-core.md`, *Execution Notes → For Plan 4*.

## Global Constraints

- Module path `github.com/Tallinzp/goim`. Layer rule: "The gateway never touches Redis. Only Logic touches Redis." The layer test (Task 3) covers `internal/gateway/...`, so the transport is checked too; test files may import Logic and the store.
- "gnet pinned to exactly **`github.com/panjf2000/gnet/v2 v2.10.0`** in `go.mod` … Any gnet upgrade must first pass the `-race` loop-ownership test and the loopState tests (Tier 1a). No goroutine per connection. `OnTraffic` never blocks; RPCs go through a bounded worker pool."
- Outer frame: "a fixed 6-byte header, then the payload. All fields big-endian. The gateway buffers partial frames and splits frames that arrive together." `Version (1 B) | Cmd (2 B) | Flags (1 B) | Length (2 B)`; "`Length` counts the payload only, not the header (max 65535)."
- "`Version`: an unsupported version is closed before the handshake and counted as a `bad_version` reject"; "`Flags`: every bit is reserved in v1 and must be 0; a frame with any bit set is closed as a bad frame."
- "**Frame 1**: `Cmd = HANDSHAKE`, payload is `key_id(2) || noise_msg` (the raw Noise NK message), with no `req_id` and no AEAD … The header plus `key_id` is mixed into the handshake hash as the Noise prologue … **Handshake messages carry no application payload** (LoginReq is the first frame after the handshake)."
- "**Every later frame**: payload = AEAD-seal(`req_id(4) || body`), with a nonce counter per direction. The 6-byte header is passed as AEAD associated data … `Length` includes the 16-byte tag, so the largest body is 65535 − 4 − 16 = 65515 bytes."
- Encryption: "**Noise Protocol Framework, NK pattern** (`flynn/noise`): server static key pinned in the app, with a key ID for rotation …; ChaCha20-Poly1305 per frame. No resumption tickets."
- Key set: "gateways load `{key_id → private key}` from a Kubernetes Secret (Readiness requires it) and select the key by the handshake's `key_id`. An unknown or retired `key_id` is closed before the handshake and counted as a `noise_unknown_key` reject".
- Handshake limit: "(per gateway, global; never per IP): a concurrency limit on handshake computations running at once (a semaphore sized to the cores dedicated to handshakes) plus a gateway-wide token bucket on handshakes/s … The bucket must allow at least the 20k/s storm peak with margin … When either is exhausted the gateway does not start the handshake: it closes the connection and counts a `handshake_rate` reject."
- Connection cap: "At the cap the gateway closes new connections at accept, before any crypto".
- "Bad frame / decryption failure / frame too large | Close immediately."
- Identity, checked against gnet v2.10.0: "`OnOpen` always runs on the owning loop's goroutine on both accept paths … `Conn.EventLoop()` gives the loop's identity"; "gnet's round-robin load balancer (`gnet.WithLoadBalancing(gnet.RoundRobin)`, the default, chosen explicitly)".
- The Plan 3 seam (*Execution Notes → For Plan 4*): `NumLoops` from gnet's `NumEventLoop`; in `OnOpen`, `g.Open(loop, link)` and on a reason close before any crypto; "The `Loop` passed must be the same comparable value every time for the same loop"; `HandshakeDone` after Noise completes, `Frame` for every decrypted frame, `Closed` from `OnClose` (once); `Link.Send` and `Link.Close` only on the loop; the Noise key check as a `Deps.ReadyChecks` entry; `cmd/gateway` sets `GatewayID` from the pod name, `Epoch` from `NewEpoch()`, and runs SIGTERM → `Drain` then `Stop`. No code may rely on the order of two `Execute` calls.
- Readiness: "only after the gRPC connections to Logic … are connected, the Noise static key set is loaded and the first `GatewayLease` call has succeeded."
- Drain: "`terminationGracePeriodSeconds` must be longer than the drain window"; choice #35 puts the drain at about 77s and the grace period at ≥ 90s.
- Unit tests the spec lists for this plan: "frame codec (partial frames, several frames at once, fuzzing), Noise handshake against a test client; a tampered header fails AEAD, a nonzero flag closes, an unknown version is closed before the handshake; a handshake succeeds with either of two configured key IDs, an unknown key ID is closed before the handshake and counted, and a tampered `key_id` fails the handshake through the prologue"; "conn_id generation, and the **gnet loop-ownership test under `-race`** (`OnOpen`, `OnClose`, the `Execute` callback and wheel expiry callbacks all see the same loop for a given connection)".
- Tiers: Tier 1a on the dev machine (2 cores / 2GB), `go test -race`, no Docker. Roadmap choices #1–#37 apply; this plan's are #38–#45 below.

## Review Focus

1. **A write fails inside a core call.** The peer resets while the core sends a Kick, `LoginResp` or `Pong`; gnet's `conn.write` then closes the socket synchronously and calls `OnClose` from inside `Link.Send`. Expected: the core hears `Closed` exactly once, after its current call returns, and `Link` calls in between do nothing. Pinned in Task 5 (`TestCloseInsideACoreCallIsReportedAfterIt`).
2. **A socket rejected at accept** (`at_capacity`, `pending_mem`, `draining`). gnet still fires `OnClose` for it. Expected: the core never sees `Closed` for a connection it didn't admit, so the open-socket count stays right and a later socket is admitted once a slot frees. Pinned in Task 5 (`TestAtCapacityClosesAtAccept`).
3. **A client that announces a large frame and then trickles it, or never sends it.** gnet would buffer up to 64KB per connection, 16GB at the 250k cap. Expected: a `Length` above `MaxClientFrame` closes the connection as soon as the header arrives; before the handshake only a frame of exactly the handshake size is accepted, and the `HANDSHAKE` timeout bounds a slow one. Pinned in Task 5 (`TestOversizedFrameIsClosedFromItsHeader`, `TestPreHandshakeRejectsCloseBeforeAnyReply`).
4. **More handshakes at the same instant than `HandshakeConcurrency`.** Handshakes run on the loops, which can't wait, so the extra ones are closed as `handshake_rate` and the clients retry after 0.5–1.5s. Expected: a full semaphore never spends a bucket token, and a released slot is reusable at once. At 16 loops and 8 slots, a 25k/s storm averages 2.5 handshakes at once, so rejects stay rare; Plan 6 measures it. Pinned in Task 4 (`TestLimiterSemaphore`, `TestFullSemaphoreSpendsNoToken`). The prototype hit this: under `-race`, 80 concurrent handshakes on 4 loops with 2 slots were rejected, so the ownership test sets the concurrency to the loop count.
5. **SIGTERM with clients logged in.** The signal ends `run`'s context, but the drain still needs the Logout sender, the wheel ticker and the lease connection. Expected: each client gets `Reconnect`, its fenced Logout reaches Logic, then `EpochEnded`. Pinned in Task 7 (`TestRunServesThenDrainsOnSIGTERM`). The first prototype started the core on the signal context: no Logout was sent, and the drain waited its full 6s.

## Implementation choices this plan makes (decided 2026-10-08)

The spec leaves these open. Each is restated in the task that implements it. They are recorded in the roadmap as #38–#45. **Decided 2026-10-08** at the plan review: all accepted, #42 and #44 with one amendment (the limiter reports which limit refused), #39 with its rationale corrected; see *Execution Notes → Review decisions*.

38. **Two packages.** `internal/wire` holds the header codec, `Cut`, Noise NK on both sides, `Session.Seal`/`Open`, the key set, and blocking client helpers (`ClientHandshake`, `WriteFrame`, `ReadFrame`) for the test client, Plan 5's SDK and Plan 6's loadgen. `internal/gateway/tcp` is the gnet transport. Plan 5 decides whether an app-facing SDK needs `wire` outside `internal/`.
39. **Noise suite `Noise_NK_25519_ChaChaPoly_SHA256`** on `flynn/noise v1.1.0`. SHA-256 rather than BLAKE2s: no mobile platform library ships BLAKE2s, while SHA-256 is everywhere. iOS CryptoKit has all three primitives (X25519, ChaCha20-Poly1305, SHA-256); Android's platform JCA has ChaCha20-Poly1305 from API 28 and X25519 only from API 33, so the Android SDK needs a crypto library (e.g. Tink) below API 33 either way, and SHA-256 keeps that library's footprint to the common primitives.
40. **Handshake framing.** The prologue is frame 1's 6-byte header followed by `key_id`. The server answers with one frame, `Cmd = HANDSHAKE`, whose payload is the raw 48-byte Noise message (e, ee and an empty payload's tag); its header can't be in the prologue (already fixed), so the client checks every header field exactly. Both messages carry no payload, so frame 1's `Length` is exactly 50 (`key_id` 2 + e 32 + tag 16); any other size, any other `Cmd` before the handshake, or a decrypted payload is a bad frame.
41. **Client frame limit.** After the handshake a client frame's `Length` must be in [20, `MaxClientFrame`], default **8192** (a `LoginReq` with a 4096-byte token fits), checked from the header before the payload is buffered. Server frames may use the full 65535. Sub-project 2 raises the limit for messages.
42. **Handshake limiter inline on the loop.** No hand-off to a worker pool: a handshake costs ~100µs, and a hand-off would add an `Execute` round trip and a reordering risk. Semaphore `HandshakeConcurrency` = half the loops, rounded up (8 at 16 loops), so at least half the loops are never computing a handshake. Token bucket `HandshakeRate` **25k/s** (above the 20k/s storm peak) with burst **2.5k**. A slot is taken before a token. A refused handshake is counted as `handshake_rate` (the spec's reject reason) and also in `handshakes_total{result=no_slot|no_token}`, so Plan 6 can tell a semaphore collision from an empty bucket. The checks run when the whole frame 1 has arrived, after the version, flag, size and `key_id` checks. Plan 6 re-derives the numbers from the measured NK cost (handshake flood and storm tests).
43. **Noise key Secret format.** A directory with one file per key, named by its decimal `key_id` (0–65535), holding the base64 X25519 private key. Other names are skipped (a Secret volume's `..data` entries). A bad key file or an empty set stops the gateway at startup: a broken Secret should fail loudly, not leave the pod quietly NotReady. `gateway noise-keygen -key-id N -out FILE` writes a key and prints the public key to pin in the app. The transport's `ReadyChecks` entry is "listening".
44. **Transport mechanics.** One acceptor (`WithReusePort(false)`), so `RoundRobin` applies. `TCP_NODELAY` on, gnet's default buffers (Plan 6 tunes them for memory), no TCP keepalive (the 310s heartbeat finds dead peers). The listener stays open during a drain: the core rejects `draining` at accept, and the NLB stops sending once the pod is NotReady. A re-entry guard defers `Closed` while the loop is inside a core call (Review Focus 1). The core seam gains `Gateway.Reject(c, reason)`, `Gateway.Close(c, reason)` and `Gateway.Config()`. New `closes_total` reasons: `rejected`, `bad_frame`, `decrypt`, `handshake`. New metric: `goim_gateway_handshakes_total{result=ok|failed|no_slot|no_token}` (computed, or refused by the limiter). gnet logs through `slog`.
45. **`cmd/gateway`.** Flags mirror `gateway.Config` and `tcp.Config`. `-id` defaults to `$POD_NAME`, then the host name. The client port is `:8000`, and `-loops` defaults to `GOMAXPROCS`. The `Busy` window and the `LoginResp` reconnect windows are `lo-hi` flags (unset means not sent). The core runs on its own context: SIGTERM starts `Drain` on a fresh context bounded by `-drain-timeout` (85s, below the ≥ 90s grace period), then the transport stops, then the core. Go and process collectors are registered.

## Execution Notes

- **Branch:** Plan 3 (`feat/plan3-gateway`) is not merged, so Plan 4 branches from it: `git checkout -b feat/plan4-tcp feat/plan3-gateway`. Commit this plan before Task 1, so the branch diff holds only Plan 4's code.
- **Prototype:** every code block in this plan was compiled and run before the plan was written. The whole plan was built in a scratch clone of `e7ebb31`, then replayed task by task into a second clone, one commit per task, with `go vet ./...` and the task's package tests passing under `-race` after each task. The replay's final tree matched the first clone byte for byte. The full suite (`./...`) passed under `-race` three times on the dev machine, and `./internal/gateway/tcp ./internal/wire ./cmd/gateway` six more times in a row.
- **Mutation check:** each of these breakages was made in the prototype and caught by the named test:
  - `Execute` running its function on a new goroutine (`TestLoopOwnershipUnderRace`);
  - `Closed` called inside the core call (`TestCloseInsideACoreCallIsReportedAfterIt`);
  - no prologue (`TestTamperedKeyIDFailsThroughThePrologue`);
  - no header AD on both sides (`TestTamperedHeaderFailsTheAEAD`, `TestTamperedHeaderIsClosedAsDecryptFailure`);
  - token before slot (`TestFullSemaphoreSpendsNoToken`);
  - no `MaxClientFrame` check (`TestOversizedFrameIsClosedFromItsHeader`);
  - a rejected socket reported to the core (`TestAtCapacityClosesAtAccept`, which panics);
  - `bad_version` not counted, an unknown `key_id` falling back to another key, and any `Cmd` accepted before the handshake (`TestPreHandshakeRejectsCloseBeforeAnyReply`);
  - the core started on the signal context (`TestRunServesThenDrainsOnSIGTERM`).

  Two mutations first survived, because their tests only checked that something failed: the prologue removed on one side only, and the AD removed from `Seal` only. Both tests now also check that the untampered case succeeds.
- **Execution mode:** Native (decided 2026-10-08, as Plans 1–3), on branch `feat/plan4-tcp`.
- **Review decisions (2026-10-08).** The plan was reviewed against the spec and the gnet v2.10.0 and `flynn/noise` v1.1.0 sources. #38, #40, #41, #43 and #45 are accepted as proposed. Amended:
  - **#42/#44, limiter result.** `limiter.acquire` returns `""`, `noSlot` or `noToken` instead of a bool, and `conn.handshake` counts the refusal in `handshakes_total{result}` before the `handshake_rate` reject. Why: with handshakes inline, the semaphore and the bucket overlap (the bucket already caps crypto CPU at ~2.5 cores at 25k/s × 100µs), and a semaphore reject is a collision of more than half the loops in the same 100µs, not a rate excess. The Plan 6 flood and storm tests must see which limit fires to re-derive both numbers; one `handshake_rate` counter can't show it. Applied to Tasks 4 and 5 (`TestLimiterSemaphore`, `TestLimiterBucket` and `TestFullSemaphoreSpendsNoToken` check the reason; `TestHandshakeRateRejects` checks `no_token`), then re-run in the prototype: `go vet ./...` and `go test -race ./...` pass.
  - **#39, rationale only.** Android's platform JCA has X25519 only from API 33; the suite choice stands.
  - Checked in the gnet source and found safe, no change: after a close inside `OnTraffic`, gnet's `inboundBuffer.Write(c.buffer)` is a no-op (`release` nils `c.buffer`, and `elastic.RingBuffer.Write` returns at once on an empty slice); queued writes are copied (`linkedlist.Buffer.PushBack`), so reusing `loop.out` after `Write` is safe; `loop.close` ignores a second close of the same socket.
  - **For Plan 6:** a drain takes at most ~76s while Logic answers (60s of steps, then at most `FenceAfter` 6s + `Drain.Flush` 10s). Only when Logic fails do the lease stop (≤ `LeaseTimeout` 5s) and three `EpochEnded` attempts (≤ 5s each) push it past `-drain-timeout` 85s; the timeout then cuts `EpochEnded` short and the lease TTL ends the epoch instead, which is the spec's path for a gateway that dies. `run` then allows 5s to stop the transport, so set `terminationGracePeriodSeconds` to at least 95s in the manifests (within choice #35's "≥ 90s"), so SIGKILL never lands in the stop.
- **Tests and timing.** Transport tests run gnet with 2 loops (the ownership test uses 4) on `127.0.0.1:0`. Each takes ~0.5s, mostly gnet's shutdown poll. The ownership test waits for 3s `HANDSHAKE` timeouts and takes ~5s. The tcp package takes ~11s under `-race` on the dev machine.
- **Not verified at Tier 1a, and where each is closed:**
  1. **`Execute` reordering under load.** The ownership test runs the core on real gnet loops while connections open, handshake, log in, are kicked, time out and send garbage, but the urgent queue rarely fills to `MaxPollEventsCap` at this scale. The design doesn't rely on order (Plan 3); Plan 6's load tests run at scale.
  2. **Cost and memory.** The NK cost (~100µs), memory per connection with gnet's default buffers, the handshake limiter defaults and the listen backlog (gnet uses `somaxconn`) are measured in Plan 6: load test (a), handshake flood and storm. The `somaxconn ≥ 4096` requirement is a node setting (Plan 6 manifests); the gateway does not check it, since the spec's self-check covers only `nofile` and conntrack.
  3. **Real Redis.** `TestE2EOverTCP` runs on miniredis at Tier 1a and on the real Redis at Tier 2 (`make test-redis` covers `./...`), with a unique gateway id and uid. Redis Cluster stays Plan 1's open risk.
  4. **Linux only.** `OnBoot` reads the bound port with `syscall.Getsockname` on a dup of the listener; the gateway targets Linux pods.
- **Plan 3 deferred minor fixed here:** `Stop` and `Drain` before a successful `Start` no longer panic (Task 3).
- **For Plan 5 (SDK and race suite):**
  - The SDK's byte layer is `wire.Initiate` / `Initiator.Finish` (or `wire.ClientHandshake`) and `Session.Seal` / `Open`. Handshake `Cmd` is `clientpb.Cmd_CMD_HANDSHAKE`; the server's reply is exactly `HeaderLen + HandshakeMsgLen` bytes.
  - A pre-handshake close (`at_capacity`, `handshake_rate`, `bad_version`, `noise_unknown_key`, `draining`) is a close with no bytes before the handshake reply.
  - The test timing profile sets `-handshake-timeout`/`-auth-timeout` ≥ 3s (not scaled), `-handshake-concurrency` equal to `-loops` on a 2-core host, and small `-drain-window`/`-drain-flush`.
  - `deploy/compose` needs a Noise key directory (`gateway noise-keygen`) mounted at `-noise-keys`, with its public key given to the scripted client.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/wire/frame.go` | `Header`, `ParseHeader`, `Put`, `Check`; `Peeker`; `Cut` |
| `internal/wire/noise.go` | `Suite`, `Key`, `KeySet`, `LoadKeyDir`, `ParsePrivate`, `EncodeKey`, `GenerateKey`; `Respond`, `Initiate`, `Initiator.Finish`; `Session.Seal`/`Open`; client helpers |
| `internal/gateway/gateway.go` | (modify) `Config()`, `Reject`, `Close` |
| `internal/gateway/metrics.go` | (modify) reject reasons `handshake_rate`, `bad_version`, `noise_unknown_key` |
| `internal/gateway/server.go`, `drain.go` | (modify) `Stop`/`Drain` before `Start` |
| `internal/gateway/layer_test.go` | (modify) the layer rule covers `internal/gateway/...` |
| `internal/gateway/tcp/limiter.go` | handshake semaphore + token bucket |
| `internal/gateway/tcp/transport.go` | `Config`, `Transport`, `New`, `Start`, `Stop`, `Addr`, `Ready`; gnet `OnBoot`/`OnOpen`/`OnTraffic`/`OnClose` |
| `internal/gateway/tcp/loop.go` | `loop`: `gateway.Loop` on gnet, the re-entry guard, scratch buffers |
| `internal/gateway/tcp/conn.go` | `conn`: `gateway.Link`, frame cutting, the handshake, opening frames |
| `cmd/gateway/main.go` | flags, `noise-keygen`, `run` with the SIGTERM drain |

Shared test helpers: `internal/wire/frame_test.go` (`buffer`, `frame`, `cutAll`, Task 1), `internal/wire/noise_test.go` (`key`, `handshake`, Task 2), `internal/gateway/seam_test.go` (`closes`, Task 3), `internal/gateway/tcp/harness_test.go` (fake Logic, `newServer`, `startServer`, `client`, `waitFor`, Task 5), `internal/gateway/tcp/transport_test.go` (`handshakeFrame`, `ioReadFull`, Task 5).

---

### Task 1: Frame header and cutter (`internal/wire`)

**Files:**
- Create: `internal/wire/frame.go`
- Test: `internal/wire/frame_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `const HeaderLen = 6, Version = 1, MaxLength = 65535, ReqIDLen = 4, TagLen = 16, Overhead = 20, MaxBody = 65515`; `type Header struct{ Version uint8; Cmd uint16; Flags uint8; Length uint16 }`; `ParseHeader([]byte) Header`; `(Header) Put([]byte)`; `(Header) Check() error` (wrapping `ErrVersion` or `ErrFlags`); `type Peeker interface{ Peek(n int) ([]byte, error); InboundBuffered() int }` (gnet.Conn satisfies it); `Cut(p Peeker, check func(Header) error) (Header, []byte, error)`: a nil frame and nil error mean "not complete yet". Test helpers `buffer`, `frame(cmd, n)`, `cutAll`.

- [ ] **Step 1: Write the failing test**

`internal/wire/frame_test.go`:

```go
package wire

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// buffer is a Peeker over bytes received so far, like gnet's inbound buffer.
type buffer struct{ b []byte }

func (p *buffer) Peek(n int) ([]byte, error) {
	if n > len(p.b) {
		return nil, io.ErrShortBuffer
	}
	return p.b[:n], nil
}
func (p *buffer) InboundBuffered() int { return len(p.b) }

func anyHeader(Header) error { return nil }

// frame builds a plaintext frame with an n-byte payload.
func frame(cmd uint16, n int) []byte {
	f := make([]byte, HeaderLen+n)
	Header{Version: Version, Cmd: cmd, Length: uint16(n)}.Put(f)
	for i := range n {
		f[HeaderLen+i] = byte(cmd) + byte(i)
	}
	return f
}

// cutAll feeds stream to a buffer in pieces of the given sizes and returns
// every frame cut, in order.
func cutAll(t testing.TB, stream []byte, sizes []int) [][]byte {
	var p buffer
	var got [][]byte
	for i := 0; len(stream) > 0; i++ {
		n := min(len(stream), max(1, sizes[i%len(sizes)]))
		p.b = append(p.b, stream[:n]...)
		stream = stream[n:]
		for {
			_, f, err := Cut(&p, anyHeader)
			if err != nil {
				t.Fatal(err)
			}
			if f == nil {
				break
			}
			got = append(got, bytes.Clone(f))
			p.b = p.b[len(f):]
		}
	}
	if len(p.b) != 0 {
		t.Fatalf("%d bytes left over", len(p.b))
	}
	return got
}

func TestHeaderRoundTripAndCheck(t *testing.T) {
	h := Header{Version: Version, Cmd: 0x0102, Flags: 0, Length: 0xfffe}
	var b [HeaderLen]byte
	h.Put(b[:])
	if !bytes.Equal(b[:], []byte{1, 1, 2, 0, 0xff, 0xfe}) || ParseHeader(b[:]) != h {
		t.Fatalf("% x", b)
	}
	if err := h.Check(); err != nil {
		t.Fatal(err)
	}
	if err := (Header{Version: 2}).Check(); !errors.Is(err, ErrVersion) {
		t.Fatalf("version 2: %v", err)
	}
	if err := (Header{Version: Version, Flags: 0x80}).Check(); !errors.Is(err, ErrFlags) {
		t.Fatalf("flag bit: %v", err)
	}
}

func TestCutPartialFramesAndSeveralAtOnce(t *testing.T) {
	want := [][]byte{frame(1, 0), frame(2, 5), frame(3, 300)}
	stream := bytes.Join(want, nil)
	for name, sizes := range map[string][]int{"byte by byte": {1}, "all at once": {len(stream)}, "uneven": {4, 7, 1, 290}} {
		got := cutAll(t, stream, sizes)
		if len(got) != len(want) {
			t.Fatalf("%s: %d frames, want %d", name, len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("%s: frame %d differs", name, i)
			}
		}
	}
}

func TestCutRefusesFromTheHeaderBeforeThePayload(t *testing.T) {
	tooBig := errors.New("too big")
	p := &buffer{b: frame(1, 1000)[:HeaderLen]} // only the header has arrived
	_, f, err := Cut(p, func(h Header) error {
		if h.Length > 100 {
			return tooBig
		}
		return nil
	})
	if !errors.Is(err, tooBig) || f != nil {
		t.Fatalf("frame %v, err %v", f, err)
	}
}

// FuzzCut: frames of fuzzed lengths, delivered in fuzzed pieces, come out
// whole and in order.
func FuzzCut(f *testing.F) {
	f.Add([]byte{0, 5, 200}, []byte{1, 3})
	f.Add([]byte{255, 0}, []byte{6})
	f.Fuzz(func(t *testing.T, lens, sizes []byte) {
		if len(sizes) == 0 {
			sizes = []byte{1}
		}
		var want [][]byte
		for i, n := range lens {
			want = append(want, frame(uint16(i), int(n)*7))
		}
		cuts := make([]int, len(sizes))
		for i, s := range sizes {
			cuts[i] = int(s)
		}
		got := cutAll(t, bytes.Join(want, nil), cuts)
		if len(got) != len(want) {
			t.Fatalf("%d frames, want %d", len(got), len(want))
		}
		for i := range want {
			if !bytes.Equal(got[i], want[i]) {
				t.Fatalf("frame %d differs", i)
			}
		}
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/wire`
Expected: FAIL to build, `undefined: Header` (and `Cut`, `ParseHeader`, …).

- [ ] **Step 3: Write the implementation**

`internal/wire/frame.go`:

```go
// Package wire is the client protocol's byte format (spec: Client protocol):
// the 6-byte frame header, cutting frames from a byte stream, and the Noise
// NK handshake and per-frame AEAD. The gateway's TCP transport uses the
// responder side; the test client, the SDK (Plan 5) and loadgen (Plan 6) use
// the initiator side. It knows nothing about connections or event loops.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Frame layout: Version(1) Cmd(2) Flags(1) Length(2), big-endian, then
// Length bytes of payload.
const (
	HeaderLen = 6
	Version   = 1
	MaxLength = 65535 // Length is 2 bytes and counts the payload only

	ReqIDLen = 4
	TagLen   = 16                   // ChaCha20-Poly1305
	Overhead = ReqIDLen + TagLen    // an encrypted frame's payload beyond its body
	MaxBody  = MaxLength - Overhead // 65515
)

// Header is a frame's fixed 6-byte header.
type Header struct {
	Version uint8
	Cmd     uint16
	Flags   uint8
	Length  uint16
}

var (
	ErrVersion = errors.New("wire: unsupported version")
	ErrFlags   = errors.New("wire: reserved flag set")
)

// ParseHeader reads the header from b[:HeaderLen].
func ParseHeader(b []byte) Header {
	return Header{Version: b[0], Cmd: binary.BigEndian.Uint16(b[1:3]), Flags: b[3],
		Length: binary.BigEndian.Uint16(b[4:6])}
}

// Put writes h to b[:HeaderLen].
func (h Header) Put(b []byte) {
	b[0] = h.Version
	binary.BigEndian.PutUint16(b[1:3], h.Cmd)
	b[3] = h.Flags
	binary.BigEndian.PutUint16(b[4:6], h.Length)
}

// Check rejects a version other than Version and any flag bit: every bit is
// reserved in v1 and must be 0.
func (h Header) Check() error {
	switch {
	case h.Version != Version:
		return fmt.Errorf("%w %d", ErrVersion, h.Version)
	case h.Flags != 0:
		return fmt.Errorf("%w: %#02x", ErrFlags, h.Flags)
	}
	return nil
}

// Peeker is a connection's inbound buffer, as gnet.Conn offers it: Peek(n)
// returns the next n bytes without consuming them, or an error while fewer
// are buffered.
type Peeker interface {
	Peek(n int) ([]byte, error)
	InboundBuffered() int
}

// Cut returns the next whole frame buffered in p, header included, without
// consuming it; the caller discards len(frame) bytes once done with it. A
// nil frame and nil error mean the frame is not complete yet. check vets
// the header as soon as it has arrived, before the payload is buffered, so
// a bad or oversized frame is refused without reading it.
func Cut(p Peeker, check func(Header) error) (Header, []byte, error) {
	if p.InboundBuffered() < HeaderLen {
		return Header{}, nil, nil
	}
	hb, err := p.Peek(HeaderLen)
	if err != nil {
		return Header{}, nil, err
	}
	h := ParseHeader(hb)
	if err := check(h); err != nil {
		return h, nil, err
	}
	n := HeaderLen + int(h.Length)
	if p.InboundBuffered() < n {
		return h, nil, nil
	}
	f, err := p.Peek(n)
	return h, f, err
}
```

- [ ] **Step 4: Run the tests, then fuzz briefly**

Run: `go test -race -count=1 ./internal/wire && go test -run '^$' -fuzz FuzzCut -fuzztime 10s ./internal/wire`
Expected: `ok`, then `PASS` after about 10s (the prototype ran ~130k executions with no failure). If the fuzzer finds a failure it writes `testdata/fuzz/FuzzCut/…`; fix the bug and commit that file as a regression case.

- [ ] **Step 5: Commit**

```bash
git add internal/wire
git commit -m "feat(wire): frame header and cutter"
```

---

### Task 2: Noise NK, sessions and the key set (choices #39, #40, #43)

**Files:**
- Create: `internal/wire/noise.go`
- Test: `internal/wire/noise_test.go`
- Modify: `go.mod`, `go.sum` (`github.com/flynn/noise v1.1.0`)

**Interfaces:**
- Consumes: Task 1's `Header`, `HeaderLen`, `Version`, `TagLen`, `ReqIDLen`, `Overhead`, `MaxBody`, `MaxLength`.
- Produces: `var Suite noise.CipherSuite`; `const KeyIDLen = 2, HandshakeMsgLen = 48, HandshakeLen = 50`; errors `ErrPayload`, `ErrHandshake`, `ErrFrame`; `type Key = noise.DHKey`; `GenerateKey() (Key, error)`; `ParsePrivate(string) (Key, error)`; `EncodeKey(Key) (private, public string)`; `type KeySet map[uint16]Key`; `LoadKeyDir(dir string) (KeySet, error)`; `KeyID(frame []byte) uint16`; `type Session`; `Respond(key Key, frame []byte, cmd uint16) (*Session, []byte, error)` (the reply frame); `Initiate(keyID uint16, serverPub []byte, cmd uint16) (*Initiator, []byte, error)`; `(*Initiator) Finish(frame []byte) (*Session, error)`; `(*Session) Seal(dst []byte, cmd uint16, reqID uint32, body []byte) ([]byte, error)` (appends to `dst`); `(*Session) Open(dst, frame []byte) (cmd uint16, reqID uint32, body, out []byte, err error)` (appends `req_id || body` to `dst`, returns the grown buffer as `out`); `ClientHandshake(rw io.ReadWriter, keyID uint16, serverPub []byte, cmd uint16) (*Session, error)`; `(*Session) WriteFrame(w, cmd, reqID, body) error`; `(*Session) ReadFrame(r) (cmd uint16, reqID uint32, body []byte, err error)`.

The handshake `Cmd` is a parameter, so `wire` does not import `clientpb`; callers pass `uint16(clientpb.Cmd_CMD_HANDSHAKE)`.

- [ ] **Step 1: Write the failing test**

`internal/wire/noise_test.go`:

```go
package wire

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

const cmdHandshake = 1

func key(t *testing.T) Key {
	t.Helper()
	k, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// handshake runs both sides in memory; the server picks the key by key_id.
func handshake(t *testing.T, ks KeySet, keyID uint16, pinned []byte, tamper func([]byte)) (cli, srv *Session, err error) {
	t.Helper()
	in, f1, err := Initiate(keyID, pinned, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if tamper != nil {
		tamper(f1)
	}
	k, ok := ks[KeyID(f1)]
	if !ok {
		t.Fatalf("key_id %d not in the set", KeyID(f1))
	}
	srv, f2, err := Respond(k, f1, cmdHandshake)
	if err != nil {
		return nil, nil, err
	}
	cli, err = in.Finish(f2)
	return cli, srv, err
}

func TestHandshakeFrameSizes(t *testing.T) {
	k := key(t)
	in, f1, err := Initiate(7, k.Public, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if len(f1) != HeaderLen+HandshakeLen || ParseHeader(f1) != (Header{Version: Version, Cmd: cmdHandshake, Length: HandshakeLen}) || KeyID(f1) != 7 {
		t.Fatalf("client frame % x", f1)
	}
	_, f2, err := Respond(k, f1, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if len(f2) != HeaderLen+HandshakeMsgLen || ParseHeader(f2).Length != HandshakeMsgLen {
		t.Fatalf("server frame % x", f2)
	}
	if _, err := in.Finish(f2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Respond(k, f1[:len(f1)-1], cmdHandshake); !errors.Is(err, ErrHandshake) {
		t.Fatalf("short frame: %v", err)
	}
}

func TestHandshakeWithEitherOfTwoKeyIDs(t *testing.T) {
	ks := KeySet{1: key(t), 2: key(t)}
	for id, k := range ks {
		cli, srv, err := handshake(t, ks, id, k.Public, nil)
		if err != nil {
			t.Fatalf("key_id %d: %v", id, err)
		}
		f, err := cli.Seal(nil, 2, 9, []byte("login"))
		if err != nil {
			t.Fatal(err)
		}
		cmd, reqID, body, _, err := srv.Open(nil, f)
		if err != nil || cmd != 2 || reqID != 9 || string(body) != "login" {
			t.Fatalf("key_id %d: %d %d %q %v", id, cmd, reqID, body, err)
		}
	}
}

// The prologue authenticates key_id: with one key filed under two IDs, a
// key_id changed in flight still selects the right key, so only the
// prologue can make the handshake fail.
func TestTamperedKeyIDFailsThroughThePrologue(t *testing.T) {
	k := key(t)
	ks := KeySet{1: k, 2: k}
	if _, _, err := handshake(t, ks, 1, k.Public, nil); err != nil {
		t.Fatalf("untampered: %v", err)
	}
	_, _, err := handshake(t, ks, 1, k.Public, func(f []byte) { f[HeaderLen+1] = 2 })
	if !errors.Is(err, ErrHandshake) {
		t.Fatalf("tampered key_id: %v", err)
	}
}

func TestWrongPinnedKeyFails(t *testing.T) {
	ks := KeySet{1: key(t)}
	if _, _, err := handshake(t, ks, 1, key(t).Public, nil); !errors.Is(err, ErrHandshake) {
		t.Fatalf("client pinned another key: %v", err)
	}
}

func TestTamperedHeaderFailsTheAEAD(t *testing.T) {
	k := key(t)
	for i := range HeaderLen {
		cli, srv, err := handshake(t, KeySet{1: k}, 1, k.Public, nil)
		if err != nil {
			t.Fatal(err)
		}
		f, _ := cli.Seal(nil, 4, 1, []byte("ping"))
		bad := bytes.Clone(f)
		if i == 4 || i == 5 { // Length: a frame of another size; Open checks it first
			bad[i] ^= 0x01
		} else {
			bad[i] ^= 0x80
		}
		if _, _, _, _, err := srv.Open(nil, bad); !errors.Is(err, ErrFrame) {
			t.Fatalf("header byte %d tampered: %v", i, err)
		}
		// The same frame untampered opens, so the header alone failed it.
		if _, _, _, _, err := srv.Open(nil, f); err != nil {
			t.Fatalf("header byte %d: the untampered frame fails too: %v", i, err)
		}
	}
}

func TestEachDirectionHasItsOwnNonceCounter(t *testing.T) {
	k := key(t)
	cli, srv, err := handshake(t, KeySet{1: k}, 1, k.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	f1, _ := cli.Seal(nil, 4, 1, nil)
	f2, _ := cli.Seal(nil, 4, 2, nil)
	if _, _, _, _, err := srv.Open(nil, f2); err == nil {
		t.Fatal("frame 2 opened before frame 1")
	}
	// A failed Open leaves the counter where it was.
	if _, id, _, _, err := srv.Open(nil, f1); err != nil || id != 1 {
		t.Fatal(id, err)
	}
	if _, _, _, _, err := srv.Open(nil, f1); err == nil {
		t.Fatal("replayed frame opened")
	}
	r, _ := srv.Seal(nil, 5, 1, nil) // the server's first frame: nonce 0 the other way
	if _, id, _, _, err := cli.Open(nil, r); err != nil || id != 1 {
		t.Fatal(id, err)
	}
}

func TestLargestBodyAndOversizedBody(t *testing.T) {
	k := key(t)
	cli, srv, err := handshake(t, KeySet{1: k}, 1, k.Public, nil)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte{0xab}, MaxBody)
	f, err := cli.Seal(make([]byte, 3, 8), 2, 1, body) // appends after existing bytes
	if err != nil || len(f) != 3+HeaderLen+MaxLength || ParseHeader(f[3:]).Length != MaxLength {
		t.Fatalf("len %d, err %v", len(f), err)
	}
	if _, _, got, _, err := srv.Open(nil, f[3:]); err != nil || !bytes.Equal(got, body) {
		t.Fatal(err)
	}
	if _, err := cli.Seal(nil, 2, 1, make([]byte, MaxBody+1)); !errors.Is(err, ErrFrame) {
		t.Fatalf("oversized body: %v", err)
	}
}

func TestClientHelpersOverAPipe(t *testing.T) {
	k := key(t)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	done := make(chan error, 1)
	go func() {
		f := make([]byte, HeaderLen+HandshakeLen)
		if _, err := io.ReadFull(b, f); err != nil {
			done <- err
			return
		}
		srv, reply, err := Respond(k, f, cmdHandshake)
		if err != nil {
			done <- err
			return
		}
		b.Write(reply)
		cmd, id, body, err := srv.ReadFrame(b)
		if err == nil {
			err = srv.WriteFrame(b, cmd+1, id, body)
		}
		done <- err
	}()
	cli, err := ClientHandshake(a, 3, k.Public, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.WriteFrame(a, 4, 77, []byte("x")); err != nil {
		t.Fatal(err)
	}
	cmd, id, body, err := cli.ReadFrame(a)
	if err != nil || cmd != 5 || id != 77 || string(body) != "x" {
		t.Fatal(cmd, id, body, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLoadKeyDir(t *testing.T) {
	dir := t.TempDir()
	k1, k7 := key(t), key(t)
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p1, pub1 := EncodeKey(k1)
	p7, _ := EncodeKey(k7)
	write("1", p1+"\n")
	write("7", p7)
	write("README", "not a key")
	os.Mkdir(filepath.Join(dir, "..data"), 0o700) // a Secret volume's own entries
	ks, err := LoadKeyDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ks) != 2 || !bytes.Equal(ks[1].Public, k1.Public) || !bytes.Equal(ks[7].Private, k7.Private) {
		t.Fatalf("loaded %v", ks)
	}
	if _, pub := EncodeKey(ks[1]); pub != pub1 {
		t.Fatal("public key not derived from the private key")
	}
	write("8", "!!!")
	if _, err := LoadKeyDir(dir); err == nil {
		t.Fatal("a bad key file loaded")
	}
	if _, err := LoadKeyDir(t.TempDir()); err == nil {
		t.Fatal("an empty key set loaded")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/wire`
Expected: FAIL to build, `undefined: Key` (and `KeySet`, `Initiate`, `Respond`, …).

- [ ] **Step 3: Write the implementation and add the dependency**

`internal/wire/noise.go`:

```go
package wire

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/flynn/noise"
)

// Suite is Noise_NK_25519_ChaChaPoly_SHA256 (spec: Client encryption).
// SHA-256 rather than BLAKE2s because the apps' platform crypto (iOS
// CryptoKit, Android) has it natively.
var Suite = noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashSHA256)

// Handshake sizes. NK message 1 is e, es and an empty payload's tag; message
// 2 is e, ee and an empty payload's tag. Handshake messages carry no
// application payload (R4G-O6), so both sizes are exact.
const (
	KeyIDLen        = 2
	HandshakeMsgLen = 32 + TagLen
	// HandshakeLen is the Length of the client's handshake frame:
	// key_id(2) || noise_msg.
	HandshakeLen = KeyIDLen + HandshakeMsgLen
)

var (
	ErrPayload   = errors.New("wire: handshake message carries a payload")
	ErrHandshake = errors.New("wire: bad handshake frame")
	ErrFrame     = errors.New("wire: bad frame")
)

// Key is a server static key: X25519, 32 bytes each.
type Key = noise.DHKey

// GenerateKey returns a new server static key.
func GenerateKey() (Key, error) { return Suite.GenerateKeypair(rand.Reader) }

// ParsePrivate builds a Key from a base64 (standard encoding) private key.
func ParsePrivate(s string) (Key, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return Key{}, fmt.Errorf("wire: private key: %w", err)
	}
	priv, err := ecdh.X25519().NewPrivateKey(b)
	if err != nil {
		return Key{}, fmt.Errorf("wire: private key: %w", err)
	}
	return Key{Private: priv.Bytes(), Public: priv.PublicKey().Bytes()}, nil
}

// EncodeKey returns k's private (for the Secret) and public (pinned in the
// app) halves in base64.
func EncodeKey(k Key) (private, public string) {
	return base64.StdEncoding.EncodeToString(k.Private), base64.StdEncoding.EncodeToString(k.Public)
}

// KeySet is the gateway's {key_id → static key} (spec: Noise key runbook).
type KeySet map[uint16]Key

// LoadKeyDir reads a key set from a directory holding one file per key: the
// file name is the key_id in decimal (0–65535), the content the base64
// private key. Other names are skipped, such as the ..data entries of a
// Kubernetes Secret volume. A key file that can't be read or parsed is an
// error, and so is a set with no key.
func LoadKeyDir(dir string) (KeySet, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	ks := KeySet{}
	for _, e := range ents {
		id, err := strconv.ParseUint(e.Name(), 10, 16)
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		k, err := ParsePrivate(string(b))
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", id, err)
		}
		ks[uint16(id)] = k
	}
	if len(ks) == 0 {
		return nil, fmt.Errorf("wire: no key file (named by its decimal key_id) in %s", dir)
	}
	return ks, nil
}

// KeyID returns the key_id of a client handshake frame (header included)
// whose Length has been checked to be HandshakeLen.
func KeyID(frame []byte) uint16 {
	return binary.BigEndian.Uint16(frame[HeaderLen : HeaderLen+KeyIDLen])
}

// prologue is the header and key_id of the client's handshake frame. Mixing
// it into the handshake hash authenticates both (R4G-O6).
func prologue(frame []byte) []byte { return frame[:HeaderLen+KeyIDLen] }

// Session is one connection's pair of cipher states, one per direction, each
// with its own nonce counter.
type Session struct {
	send, recv *noise.CipherState
}

// Respond runs the server side of the handshake on the client's handshake
// frame, with the static key its key_id selected. It returns the session and
// the server's handshake frame (Cmd = cmd, payload = the raw Noise message).
func Respond(key Key, frame []byte, cmd uint16) (*Session, []byte, error) {
	if len(frame) != HeaderLen+HandshakeLen {
		return nil, nil, ErrHandshake
	}
	hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: Suite, Pattern: noise.HandshakeNK,
		Prologue: prologue(frame), StaticKeypair: key})
	if err != nil {
		return nil, nil, err
	}
	payload, _, _, err := hs.ReadMessage(nil, frame[HeaderLen+KeyIDLen:])
	switch {
	case err != nil:
		return nil, nil, fmt.Errorf("%w: %v", ErrHandshake, err)
	case len(payload) != 0:
		return nil, nil, ErrPayload
	}
	out := make([]byte, HeaderLen, HeaderLen+HandshakeMsgLen)
	out, c1, c2, err := hs.WriteMessage(out, nil)
	if err != nil {
		return nil, nil, err
	}
	Header{Version: Version, Cmd: cmd, Length: HandshakeMsgLen}.Put(out)
	return &Session{send: c2, recv: c1}, out, nil
}

// Initiator is the client side of a handshake in progress.
type Initiator struct {
	hs  *noise.HandshakeState
	cmd uint16
}

// Initiate starts the client side: it returns the handshake frame to send,
// for the server key pinned under keyID.
func Initiate(keyID uint16, serverPub []byte, cmd uint16) (*Initiator, []byte, error) {
	frame := make([]byte, HeaderLen+KeyIDLen, HeaderLen+HandshakeLen)
	Header{Version: Version, Cmd: cmd, Length: HandshakeLen}.Put(frame)
	binary.BigEndian.PutUint16(frame[HeaderLen:], keyID)
	hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: Suite, Pattern: noise.HandshakeNK,
		Initiator: true, Prologue: prologue(frame), PeerStatic: serverPub})
	if err != nil {
		return nil, nil, err
	}
	frame, _, _, err = hs.WriteMessage(frame, nil)
	if err != nil {
		return nil, nil, err
	}
	return &Initiator{hs: hs, cmd: cmd}, frame, nil
}

// Finish completes the client side with the server's handshake frame. Its
// header is not authenticated, so every field is checked exactly.
func (i *Initiator) Finish(frame []byte) (*Session, error) {
	if len(frame) != HeaderLen+HandshakeMsgLen ||
		ParseHeader(frame) != (Header{Version: Version, Cmd: i.cmd, Length: HandshakeMsgLen}) {
		return nil, ErrHandshake
	}
	payload, c1, c2, err := i.hs.ReadMessage(nil, frame[HeaderLen:])
	switch {
	case err != nil:
		return nil, fmt.Errorf("%w: %v", ErrHandshake, err)
	case len(payload) != 0:
		return nil, ErrPayload
	}
	return &Session{send: c1, recv: c2}, nil
}

// Seal appends one encrypted frame to dst: the header, then
// AEAD-seal(req_id || body) with the header as associated data, so a
// tampered header fails decryption.
func (s *Session) Seal(dst []byte, cmd uint16, reqID uint32, body []byte) ([]byte, error) {
	if len(body) > MaxBody {
		return nil, fmt.Errorf("%w: body of %d bytes exceeds %d", ErrFrame, len(body), MaxBody)
	}
	start := len(dst)
	dst = append(dst, make([]byte, HeaderLen+ReqIDLen)...)
	Header{Version: Version, Cmd: cmd, Length: uint16(Overhead + len(body))}.Put(dst[start:])
	binary.BigEndian.PutUint32(dst[start+HeaderLen:], reqID)
	dst = append(dst, body...)
	plain := dst[start+HeaderLen:]
	// Encrypt in place: the ciphertext overwrites the plaintext exactly.
	out, err := s.send.Encrypt(dst[:start+HeaderLen], dst[start:start+HeaderLen], plain)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Open decrypts one frame (header included) and appends req_id || body to
// dst. It returns the header's cmd, the req_id and the body (a slice of the
// returned dst). Any failure means the connection must close.
func (s *Session) Open(dst, frame []byte) (cmd uint16, reqID uint32, body, out []byte, err error) {
	if len(frame) < HeaderLen+Overhead {
		return 0, 0, nil, dst, ErrFrame
	}
	h := ParseHeader(frame)
	if int(h.Length) != len(frame)-HeaderLen {
		return 0, 0, nil, dst, ErrFrame
	}
	start := len(dst)
	out, err = s.recv.Decrypt(dst, frame[:HeaderLen], frame[HeaderLen:])
	if err != nil {
		return 0, 0, nil, dst, fmt.Errorf("%w: %v", ErrFrame, err)
	}
	p := out[start:]
	return h.Cmd, binary.BigEndian.Uint32(p), p[ReqIDLen:], out, nil
}

// ---- blocking helpers for clients (test client, SDK, loadgen) ----

// ClientHandshake runs the client side over rw.
func ClientHandshake(rw io.ReadWriter, keyID uint16, serverPub []byte, cmd uint16) (*Session, error) {
	in, f, err := Initiate(keyID, serverPub, cmd)
	if err != nil {
		return nil, err
	}
	if _, err := rw.Write(f); err != nil {
		return nil, err
	}
	reply := make([]byte, HeaderLen+HandshakeMsgLen)
	if _, err := io.ReadFull(rw, reply); err != nil {
		return nil, err
	}
	return in.Finish(reply)
}

// WriteFrame seals and writes one frame.
func (s *Session) WriteFrame(w io.Writer, cmd uint16, reqID uint32, body []byte) error {
	f, err := s.Seal(nil, cmd, reqID, body)
	if err != nil {
		return err
	}
	_, err = w.Write(f)
	return err
}

// ReadFrame reads and opens one frame.
func (s *Session) ReadFrame(r io.Reader) (cmd uint16, reqID uint32, body []byte, err error) {
	f := make([]byte, HeaderLen, HeaderLen+MaxLength)
	if _, err := io.ReadFull(r, f); err != nil {
		return 0, 0, nil, err
	}
	h := ParseHeader(f)
	if err := h.Check(); err != nil {
		return 0, 0, nil, err
	}
	f = f[:HeaderLen+int(h.Length)]
	if _, err := io.ReadFull(r, f[HeaderLen:]); err != nil {
		return 0, 0, nil, err
	}
	cmd, reqID, body, _, err = s.Open(nil, f)
	return cmd, reqID, body, err
}
```

Seal encrypts in place: the plaintext sits right after the header in `dst`, and `Encrypt(dst[:start+HeaderLen], header, plaintext)` appends its ciphertext exactly over it, which ChaCha20-Poly1305 allows (exact overlap). The header slice used as AD lies before the write position, so it is not overwritten. If `dst` has to grow, `Seal` in the AEAD copies, and the result is still correct.

Then:

```bash
go get github.com/flynn/noise@v1.1.0
go mod tidy
```

`go.mod` gains `github.com/flynn/noise v1.1.0` in the main `require` block and `golang.org/x/crypto` as indirect.

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/wire && go test -race -count=1 ./internal/wire`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/wire go.mod go.sum
git commit -m "feat(wire): Noise NK handshake, sessions and key set"
```

---

### Task 3: Core seam for the transport; Stop and Drain before Start (choice #44)

**Files:**
- Modify: `internal/gateway/metrics.go:7-16`, `internal/gateway/gateway.go:102-105`, `internal/gateway/server.go:140-142`, `internal/gateway/drain.go:90`, `internal/gateway/layer_test.go:9-11`
- Test: `internal/gateway/seam_test.go`

**Interfaces:**
- Consumes: Plan 3's `Gateway`, `Conn`, `reject`, `closeConn`, `Metrics.Rejects`, `Metrics.Closes`, test harness `newCore`, `core.open`, `core.online`, `core.sync`, `core.tracked`, `rejects`, `fakeLink`.
- Produces: `const RejectHandshakeRate = "handshake_rate", RejectBadVersion = "bad_version", RejectNoiseUnknownKey = "noise_unknown_key"`; `(*Gateway) Config() Config`; `(*Gateway) Reject(c *Conn, reason string)` (counts the reject, closes with `closes_total{reason="rejected"}`); `(*Gateway) Close(c *Conn, reason string)`. Both are called on `c`'s loop. `Stop` without `Start` does nothing; `Drain` without `Start` sends no `EpochEnded`. Test helper `closes(h, reason)`.

- [ ] **Step 1: Write the failing test**

`internal/gateway/seam_test.go`:

```go
package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func closes(h *core, reason string) int {
	return int(testutil.ToFloat64(h.g.m.Closes.WithLabelValues(reason)))
}

func TestTransportRejectAndCloseAreCounted(t *testing.T) {
	h := newCore(t, nil)
	for _, reason := range []string{RejectBadVersion, RejectNoiseUnknownKey, RejectHandshakeRate} {
		k, _ := h.open(0)
		k.loop.do(func() { h.g.Reject(k.c, reason) })
		h.sync()
		if !k.isClosed() || rejects(h, reason) != 1 || h.tracked(k) {
			t.Fatalf("%s: closed=%v rejects=%d tracked=%v", reason, k.isClosed(), rejects(h, reason), h.tracked(k))
		}
	}
	if n := closes(h, "rejected"); n != 3 {
		t.Fatalf("closes_total{rejected} = %d, want 3", n)
	}
	k := h.online(1, "u1")
	k.loop.do(func() { h.g.Close(k.c, "decrypt") })
	h.sync()
	if !k.isClosed() || closes(h, "decrypt") != 1 || closes(h, "client") != 0 {
		t.Fatal("a transport close must be counted under its reason, not as a client close")
	}
	if h.g.Config().NumLoops != len(h.loops) {
		t.Fatal("Config does not return the gateway's configuration")
	}
}

// cmd/gateway may stop or drain a gateway whose Start failed.
func TestStopAndDrainBeforeStartDoNotPanic(t *testing.T) {
	h := newCore(t, func(c *Config) { c.Drain.Window = 10 * time.Millisecond })
	h.g.Drain(context.Background())
	h.g.Stop()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -run 'TestTransportReject|TestStopAndDrain' ./internal/gateway`
Expected: FAIL to build, `h.g.Reject undefined` (and `RejectBadVersion`, `Close`, `Config`).

- [ ] **Step 3: Write the implementation**

In `internal/gateway/metrics.go`, replace the reject reason block:

```go
// Admission reject reasons (spec: Observability). The last three are the
// TCP transport's, passed to Reject before the Noise handshake.
const (
	RejectAtCapacity       = "at_capacity"
	RejectPendingMem       = "pending_mem"
	RejectDraining         = "draining"
	RejectLoginQueue       = "login_queue"
	RejectHandshakeTimeout = "handshake_timeout"
	RejectAuthTimeout      = "auth_timeout"
	RejectHandshakeRate    = "handshake_rate"
	RejectBadVersion       = "bad_version"
	RejectNoiseUnknownKey  = "noise_unknown_key"
)
```

In `internal/gateway/gateway.go`, after `func (g *Gateway) reject(reason string) string { … }`, add:

```go
// Config returns the configuration the gateway was built with; the transport
// sizes its event loops from NumLoops.
func (g *Gateway) Config() Config { return g.cfg }

// Reject closes c before its handshake for an admission reason the transport
// detects (bad_version, noise_unknown_key, handshake_rate), counted like the
// core's own rejects. Call it on c's loop.
func (g *Gateway) Reject(c *Conn, reason string) {
	g.reject(reason)
	g.closeConn(c, "rejected")
}

// Close closes c for a transport reason, such as a bad frame or a failed
// decryption, counted in closes_total. Call it on c's loop.
func (g *Gateway) Close(c *Conn, reason string) { g.closeConn(c, reason) }
```

In `internal/gateway/server.go`, start `Stop` with the guard:

```go
// Stop ends everything. Call Drain first for a graceful exit. Stop on a
// gateway that never started does nothing.
func (g *Gateway) Stop() {
	if g.cancel == nil {
		return
	}
	g.cancel()
```

In `internal/gateway/drain.go`, start `endEpoch` with:

```go
func (g *Gateway) endEpoch(ctx context.Context) {
	if g.leaseC == nil { // never started: no epoch was registered
		return
	}
```

In `internal/gateway/layer_test.go`, make the rule cover every package under the gateway:

```go
// The layer rule: the gateway never touches Redis; only Logic does. The
// pattern covers the core and every transport under it.
func TestGatewayDoesNotImportTheStore(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "github.com/Tallinzp/goim/internal/gateway/...").CombinedOutput()
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: `ok` (~14s). Without the two guards, `TestStopAndDrainBeforeStartDoNotPanic` panics with a nil pointer dereference.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): transport seam Reject, Close and Config; Stop and Drain before Start"
```

---

### Task 4: Handshake limiter (choice #42)

**Files:**
- Create: `internal/gateway/tcp/limiter.go`
- Test: `internal/gateway/tcp/limiter_test.go`

**Interfaces:**
- Consumes: `golang.org/x/time/rate`.
- Produces (unexported, package `tcp`): `type limiter`; `newLimiter(concurrency int, perSec float64, burst int, now func() time.Time) *limiter`; `(*limiter) acquire() string` (a slot and a token, or neither; returns `""` or why it refused: `noSlot`, `noToken`); `(*limiter) release()`.

- [ ] **Step 1: Write the failing test**

`internal/gateway/tcp/limiter_test.go`:

```go
package tcp

import (
	"testing"
	"time"
)

func TestLimiterSemaphore(t *testing.T) {
	l := newLimiter(2, 1000, 1000, time.Now)
	if l.acquire() != "" || l.acquire() != "" {
		t.Fatal("two slots")
	}
	if why := l.acquire(); why != noSlot {
		t.Fatalf("a third handshake with two slots: %q, want %q", why, noSlot)
	}
	l.release()
	if l.acquire() != "" {
		t.Fatal("a released slot is not reusable")
	}
}

func TestLimiterBucket(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newLimiter(10, 10, 2, func() time.Time { return now }) // 10/s, burst 2
	for i := range 2 {
		if l.acquire() != "" {
			t.Fatalf("burst handshake %d refused", i)
		}
		l.release()
	}
	if why := l.acquire(); why != noToken {
		t.Fatalf("bucket empty: %q, want %q", why, noToken)
	}
	now = now.Add(100 * time.Millisecond) // one token at 10/s
	if l.acquire() != "" {
		t.Fatal("refill refused")
	}
	l.release()
	if l.acquire() == "" {
		t.Fatal("one token, two handshakes")
	}
}

// A full semaphore must not spend tokens: the bucket shapes handshakes that
// actually run.
func TestFullSemaphoreSpendsNoToken(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newLimiter(1, 1, 2, func() time.Time { return now })
	if l.acquire() != "" {
		t.Fatal("first")
	}
	for range 5 {
		if why := l.acquire(); why != noSlot {
			t.Fatalf("semaphore full: %q", why)
		}
	}
	l.release()
	if l.acquire() != "" {
		t.Fatal("the second token was spent by refused handshakes")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/gateway/tcp`
Expected: FAIL to build, `undefined: newLimiter`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/tcp/limiter.go`:

```go
package tcp

import (
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// limiter is the Noise handshake limit (spec: Admission control): a
// concurrency limit on handshakes computing at once plus a gateway-wide token
// bucket on handshakes/s. It is global, never per IP. Handshakes run on the
// event loops, which can't wait, so a handshake that finds either exhausted
// is not started: the connection is closed as handshake_rate.
type limiter struct {
	free   atomic.Int64 // free semaphore slots
	bucket *rate.Limiter
	now    func() time.Time
}

func newLimiter(concurrency int, perSec float64, burst int, now func() time.Time) *limiter {
	l := &limiter{bucket: rate.NewLimiter(rate.Limit(perSec), burst), now: now}
	l.free.Store(int64(concurrency))
	return l
}

// Why acquire refused a handshake: the handshakes_total result, so the
// flood and storm tests can tell which limit fired.
const (
	noSlot  = "no_slot"  // HandshakeConcurrency handshakes already computing
	noToken = "no_token" // the handshakes/s bucket is empty
)

// acquire takes a slot and a token, or neither, and returns "" or why it
// refused. The slot is taken first, so a full semaphore never spends a token.
func (l *limiter) acquire() string {
	if l.free.Add(-1) < 0 {
		l.free.Add(1)
		return noSlot
	}
	if !l.bucket.AllowN(l.now(), 1) {
		l.free.Add(1)
		return noToken
	}
	return ""
}

// release returns the slot once the handshake computation is done.
func (l *limiter) release() { l.free.Add(1) }
```

- [ ] **Step 4: Run the tests**

Run: `go test -race -count=1 ./internal/gateway/tcp`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/tcp
git commit -m "feat(tcp): handshake limiter with a semaphore and a token bucket"
```

---

### Task 5: The gnet transport (choices #40–#44)

**Files:**
- Create: `internal/gateway/tcp/transport.go`, `internal/gateway/tcp/loop.go`, `internal/gateway/tcp/conn.go`
- Test: `internal/gateway/tcp/harness_test.go`, `internal/gateway/tcp/transport_test.go`
- Modify: `go.mod`, `go.sum` (`github.com/panjf2000/gnet/v2 v2.10.0`)

**Interfaces:**
- Consumes: Task 1/2's `wire` API; Task 3's `Gateway.Reject`, `Close`, `Config`, `RejectBadVersion`, `RejectNoiseUnknownKey`, `RejectHandshakeRate`; Task 4's `limiter`; Plan 3's `gateway.New`, `Deps{Metrics, Registry, ReadyChecks}`, `Start`, `Stop`, `Addrs`, `Open`, `HandshakeDone`, `Frame`, `Closed`, `Loop`, `Link`, `Conn.ID`, `Metrics{Rejects, Closes, Conns, Tracked}`, `DefaultConfig`, `NewEpoch`, `ServerConfig`; Plan 2's `api/internalpb` (`LogicServer`, `LeaseServer`, `GatewayClient`).
- Produces: `type Config struct{ Addr string; MaxClientFrame int; HandshakeConcurrency int; HandshakeRate float64; HandshakeBurst int }`; `DefaultConfig(numLoops int) Config`; `(Config) Validate(numLoops int) error`; `New(g *gateway.Gateway, ks wire.KeySet, cfg Config, reg prometheus.Registerer) (*Transport, error)`; `(*Transport) Start() error`, `Addr() string`, `Ready() error`, `Stop(ctx) error`; gnet callbacks; unexported `loop` (implements `gateway.Loop`; `run`, `closed`) and `conn` (implements `gateway.Link`; `read`, `check`, `handshake`, `frame`, `hook`); `Transport.onLoop` test hook; close reasons `closeBadFrame`, `closeDecrypt`, `closeHandshake`. Test helpers: `fakeLogic` (`connID`, `loggedOut`), `server` (`rejects`, `closes`, `dial`, `hello`, `online`, `login`), `testConfig`, `newServer(t, mod, set...)`, `startServer(t, mod, logicAddr, leaseAddr, set...)`, `client` (`send`, `seal`, `recv`, `closedByServer`), `waitFor`, `handshakeFrame`, `ioReadFull`.

What gnet v2.10.0 does that this design depends on (read in its source):
- `eventloop.open` calls `OnOpen` on the owning loop; returning `gnet.Close` closes the socket at once, and `OnClose` still fires, so a rejected socket must carry no context (Review Focus 2).
- `conn.write` (used by `Conn.Write` on the loop) calls `loop.close` synchronously on a write error other than `EAGAIN`, so `OnClose` can run inside `Link.Send` (Review Focus 1). Bytes it can't write go into the outbound buffer (copied), and `loop.close` flushes that buffer before closing, so a Kick frame followed by `Close` is sent first.
- `Conn.Close` is asynchronous (`Trigger(LowPriority, loop.close)`); `OnClose` follows on the loop, and a second close of the same socket is ignored.
- `Peek(n)` returns `io.ErrShortBuffer` while fewer than `n` bytes are buffered; the slice stays valid until `Discard`. `InboundBuffered` counts the ring buffer plus the bytes just read.
- `EventLoop.Execute` fails with `ErrEngineInShutdown` once the engine shut down; `Engine.Stop` closes every connection on its loop (each fires `OnClose`).
- `Engine.Dup` duplicates the single listener's fd (used for the bound port).
- At most 256 event loops (`gfd.EventLoopIndexMax`); the listen backlog is `somaxconn`.

- [ ] **Step 1: Write the test harness and the failing tests**

`internal/gateway/tcp/harness_test.go`:

```go
package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// fakeLogic accepts every Login (uid = the token), Logout and Reconcile, and
// renews every lease. It records Login entries and Logouts.
type fakeLogic struct {
	pb.UnimplementedLogicServer
	pb.UnimplementedLeaseServer

	mu         sync.Mutex
	entries    []*pb.LoginEntry
	logouts    []*pb.LogoutEntry
	epochEnded int

	logicAddr, leaseAddr string
}

func startFakeLogic(t *testing.T) *fakeLogic {
	t.Helper()
	f := &fakeLogic{}
	for i, reg := range []func(*grpc.Server){
		func(s *grpc.Server) { pb.RegisterLogicServer(s, f) },
		func(s *grpc.Server) { pb.RegisterLeaseServer(s, f) },
	} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		s := grpc.NewServer()
		reg(s)
		go s.Serve(l)
		t.Cleanup(s.Stop)
		if i == 0 {
			f.logicAddr = l.Addr().String()
		} else {
			f.leaseAddr = l.Addr().String()
		}
	}
	return f
}

func (f *fakeLogic) Login(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
	f.mu.Lock()
	f.entries = append(f.entries, b.GetEntries()...)
	f.mu.Unlock()
	for i, e := range b.GetEntries() {
		if err := s.Send(&pb.LoginResult{Index: uint32(i), Code: pb.LoginCode_LOGIN_CODE_OK, Uid: e.GetToken(), TokIatMs: 1}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeLogic) Logout(_ context.Context, b *pb.LogoutBatch) (*pb.LogoutReply, error) {
	f.mu.Lock()
	f.logouts = append(f.logouts, b.GetEntries()...)
	f.mu.Unlock()
	r := &pb.LogoutReply{Done: make([]bool, len(b.GetEntries()))}
	for i := range r.Done {
		r.Done[i] = true
	}
	return r, nil
}

func (f *fakeLogic) Reconcile(_ context.Context, c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
	r := &pb.ReconcileReply{}
	for range c.GetEntries() {
		r.Results = append(r.Results, &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_WRITTEN})
	}
	return r, nil
}

func (f *fakeLogic) ReconcileEnd(context.Context, *pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error) {
	return &pb.ReconcileEndReply{}, nil
}

func (f *fakeLogic) GatewayLease(context.Context, *pb.LeaseRequest) (*pb.LeaseReply, error) {
	return &pb.LeaseReply{}, nil
}

func (f *fakeLogic) EpochEnded(context.Context, *pb.EpochEndedRequest) (*pb.EpochEndedReply, error) {
	f.mu.Lock()
	f.epochEnded++
	f.mu.Unlock()
	return &pb.EpochEndedReply{}, nil
}

// connID returns the conn_id Logic saw for uid's Login.
func (f *fakeLogic) connID(uid string) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.entries {
		if e.GetToken() == uid {
			return e.GetConnId(), true
		}
	}
	return 0, false
}

func (f *fakeLogic) loggedOut(uid string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.logouts {
		if e.GetUid() == uid {
			return true
		}
	}
	return false
}

// server is a started gateway core with this transport on 127.0.0.1.
type server struct {
	t     *testing.T
	g     *gateway.Gateway
	m     *gateway.Metrics
	tr    *Transport
	logic *fakeLogic
	keys  wire.KeySet
	ref   *pb.GatewayRef
}

func testConfig() gateway.Config {
	cfg := gateway.DefaultConfig()
	cfg.GatewayID, cfg.Epoch = "gw-0", gateway.NewEpoch()
	cfg.NumLoops = 2
	cfg.MaxConns, cfg.PendingCap, cfg.LoginQueueCap, cfg.LogoutQueue = 500, 500, 500, 500
	cfg.Limits.Notrack = true
	return cfg
}

// newServer starts a gateway with this transport against a fake Logic. mod
// changes the configuration; each set runs on the transport before it
// starts.
func newServer(t *testing.T, mod func(*gateway.Config, *Config), set ...func(*Transport)) *server {
	t.Helper()
	logic := startFakeLogic(t)
	s := startServer(t, mod, logic.logicAddr, logic.leaseAddr, set...)
	s.logic = logic
	return s
}

// startServer starts a gateway with this transport against the Logic at
// logicAddr and leaseAddr.
func startServer(t *testing.T, mod func(*gateway.Config, *Config), logicAddr, leaseAddr string, set ...func(*Transport)) *server {
	t.Helper()
	cfg := testConfig()
	tcfg := DefaultConfig(cfg.NumLoops)
	tcfg.Addr = "tcp://127.0.0.1:0"
	if mod != nil {
		mod(&cfg, &tcfg)
	}
	k1, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	k2, _ := wire.GenerateKey()
	keys := wire.KeySet{1: k1, 2: k2}
	reg := prometheus.NewRegistry()
	m := gateway.NewMetrics(reg)
	var tr *Transport
	g, err := gateway.New(cfg, gateway.Deps{Metrics: m, Registry: reg,
		ReadyChecks: []func() error{func() error { return tr.Ready() }}})
	if err != nil {
		t.Fatal(err)
	}
	if tr, err = New(g, keys, tcfg, reg); err != nil {
		t.Fatal(err)
	}
	for _, f := range set {
		f(tr)
	}
	if err := g.Start(context.Background(), gateway.ServerConfig{LogicTarget: logicAddr,
		LeaseTarget: leaseAddr, KickAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Start(); err != nil {
		g.Stop()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tr.Stop(ctx)
		g.Stop()
	})
	return &server{t: t, g: g, m: m, tr: tr, keys: keys,
		ref: &pb.GatewayRef{Gateway: cfg.GatewayID, Epoch: cfg.Epoch}}
}

func (s *server) rejects(reason string) int {
	return int(testutil.ToFloat64(s.m.Rejects.WithLabelValues(reason)))
}

func (s *server) closes(reason string) int {
	return int(testutil.ToFloat64(s.m.Closes.WithLabelValues(reason)))
}

// client is a test client on one TCP connection.
type client struct {
	t    *testing.T
	nc   net.Conn
	sess *wire.Session
}

func (s *server) dial() *client {
	s.t.Helper()
	nc, err := net.Dial("tcp", s.tr.Addr())
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(func() { nc.Close() })
	nc.SetDeadline(time.Now().Add(10 * time.Second))
	return &client{t: s.t, nc: nc}
}

// hello dials and completes the handshake with key 1.
func (s *server) hello() *client {
	s.t.Helper()
	c := s.dial()
	sess, err := wire.ClientHandshake(c.nc, 1, s.keys[1].Public, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		s.t.Fatal(err)
	}
	c.sess = sess
	return c
}

// online dials, handshakes and logs in as uid; the fake Logic takes the
// token as the uid.
func (s *server) online(uid string) *client { return s.login(uid, "d-"+uid) }

// login dials, handshakes and logs in with token from device.
func (s *server) login(token, device string) *client {
	s.t.Helper()
	c := s.hello()
	c.send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: token, Appid: "app",
		Platform: clientpb.Platform_PLATFORM_MOBILE, DeviceId: device, ConnSeq: 1})
	var resp clientpb.LoginResp
	if cmd, id := c.recv(&resp); cmd != clientpb.Cmd_CMD_LOGIN_RESP || id != 1 || resp.HeartbeatS != 150 {
		s.t.Fatalf("login reply %v %d %v", cmd, id, &resp)
	}
	return c
}

func (c *client) send(cmd clientpb.Cmd, reqID uint32, msg proto.Message) {
	c.t.Helper()
	c.nc.Write(c.seal(cmd, reqID, msg))
}

func (c *client) seal(cmd clientpb.Cmd, reqID uint32, msg proto.Message) []byte {
	c.t.Helper()
	var body []byte
	if msg != nil {
		var err error
		if body, err = proto.Marshal(msg); err != nil {
			c.t.Fatal(err)
		}
	}
	f, err := c.sess.Seal(nil, uint16(cmd), reqID, body)
	if err != nil {
		c.t.Fatal(err)
	}
	return f
}

// recv reads one frame, decoding its body into msg if not nil.
func (c *client) recv(msg proto.Message) (clientpb.Cmd, uint32) {
	c.t.Helper()
	cmd, id, body, err := c.sess.ReadFrame(c.nc)
	if err != nil {
		c.t.Fatalf("read frame: %v", err)
	}
	if msg != nil {
		if err := proto.Unmarshal(body, msg); err != nil {
			c.t.Fatal(err)
		}
	}
	return clientpb.Cmd(cmd), id
}

// closedByServer reports whether the server closed the connection with no
// more bytes sent.
func (c *client) closedByServer() bool {
	c.t.Helper()
	var b [1]byte
	n, err := c.nc.Read(b[:])
	return n == 0 && (errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || isReset(err))
}

func isReset(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && !op.Timeout()
}

// waitFor polls cond for up to 5s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
```

`internal/gateway/tcp/transport_test.go`:

```go
package tcp

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLoginPingLogoutOverTCP(t *testing.T) {
	s := newServer(t, nil)
	c := s.online("u1")
	c.send(clientpb.Cmd_CMD_PING, 5, nil)
	if cmd, id := c.recv(nil); cmd != clientpb.Cmd_CMD_PONG || id != 5 {
		t.Fatalf("got %v %d, want Pong 5", cmd, id)
	}
	c.send(clientpb.Cmd_CMD_LOGOUT, 6, nil)
	if cmd, id := c.recv(nil); cmd != clientpb.Cmd_CMD_LOGOUT_RESP || id != 6 {
		t.Fatalf("got %v %d, want LogoutResp 6", cmd, id)
	}
	if !c.closedByServer() {
		t.Fatal("not closed after LogoutResp")
	}
	waitFor(t, "fenced Logout reaches Logic", func() bool { return s.logic.loggedOut("u1") })
	if n := testutil.ToFloat64(s.tr.hs.WithLabelValues("ok")); n != 1 {
		t.Fatalf("handshakes_total{ok} = %v", n)
	}
}

func TestPartialAndCoalescedFrames(t *testing.T) {
	s := newServer(t, nil)
	// The handshake frame one byte at a time.
	c := s.dial()
	in, f1, _ := wire.Initiate(2, s.keys[2].Public, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	for _, b := range f1 {
		c.nc.Write([]byte{b})
		time.Sleep(time.Millisecond)
	}
	reply := make([]byte, wire.HeaderLen+wire.HandshakeMsgLen)
	if _, err := ioReadFull(c, reply); err != nil {
		t.Fatal(err)
	}
	sess, err := in.Finish(reply)
	if err != nil {
		t.Fatal(err)
	}
	c.sess = sess
	c.send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: "u2", Appid: "app",
		Platform: clientpb.Platform_PLATFORM_MOBILE, DeviceId: "d", ConnSeq: 1})
	if cmd, _ := c.recv(nil); cmd != clientpb.Cmd_CMD_LOGIN_RESP {
		t.Fatalf("got %v", cmd)
	}
	// Three Pings in one write, then one Ping split into pieces.
	var burst []byte
	for id := uint32(10); id < 13; id++ {
		burst = append(burst, c.seal(clientpb.Cmd_CMD_PING, id, nil)...)
	}
	c.nc.Write(burst)
	for want := uint32(10); want < 13; want++ {
		if cmd, id := c.recv(nil); cmd != clientpb.Cmd_CMD_PONG || id != want {
			t.Fatalf("got %v %d, want Pong %d", cmd, id, want)
		}
	}
	f := c.seal(clientpb.Cmd_CMD_PING, 20, nil)
	for _, piece := range [][]byte{f[:3], f[3:9], f[9:]} {
		c.nc.Write(piece)
		time.Sleep(5 * time.Millisecond)
	}
	if cmd, id := c.recv(nil); cmd != clientpb.Cmd_CMD_PONG || id != 20 {
		t.Fatalf("got %v %d, want Pong 20", cmd, id)
	}
}

func ioReadFull(c *client, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := c.nc.Read(b[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// handshakeFrame is a client handshake frame for key 1, changed by mod.
func handshakeFrame(t *testing.T, s *server, mod func([]byte)) []byte {
	t.Helper()
	_, f, err := wire.Initiate(1, s.keys[1].Public, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		t.Fatal(err)
	}
	if mod != nil {
		mod(f)
	}
	return f
}

func TestPreHandshakeRejectsCloseBeforeAnyReply(t *testing.T) {
	s := newServer(t, nil)
	for _, tc := range []struct {
		name, reject, close string
		mod                 func([]byte)
	}{
		{"version 2", gateway.RejectBadVersion, "rejected", func(f []byte) { f[0] = 2 }},
		{"unknown key_id", gateway.RejectNoiseUnknownKey, "rejected", func(f []byte) { binary.BigEndian.PutUint16(f[6:], 9) }},
		{"flag bit set", "", closeBadFrame, func(f []byte) { f[3] = 0x01 }},
		{"Login before the handshake", "", closeBadFrame, func(f []byte) { f[2] = byte(clientpb.Cmd_CMD_LOGIN) }},
		{"wrong handshake size", "", closeBadFrame, func(f []byte) { f[5]++ }},
	} {
		before := s.closes(tc.close)
		c := s.dial()
		c.nc.Write(handshakeFrame(t, s, tc.mod))
		if !c.closedByServer() {
			t.Fatalf("%s: not closed, or bytes were sent", tc.name)
		}
		waitFor(t, tc.name+" counted", func() bool { return s.closes(tc.close) == before+1 })
		if tc.reject != "" && s.rejects(tc.reject) != 1 {
			t.Fatalf("%s: %s rejects = %d", tc.name, tc.reject, s.rejects(tc.reject))
		}
	}
	// A key_id changed in flight to another known key fails the handshake.
	c := s.dial()
	c.nc.Write(handshakeFrame(t, s, func(f []byte) { binary.BigEndian.PutUint16(f[6:], 2) }))
	if !c.closedByServer() {
		t.Fatal("a handshake for the wrong key was answered")
	}
	waitFor(t, "failed handshake counted", func() bool { return testutil.ToFloat64(s.tr.hs.WithLabelValues("failed")) == 1 })
}

func TestHandshakeRateRejects(t *testing.T) {
	s := newServer(t, func(_ *gateway.Config, tc *Config) { tc.HandshakeRate, tc.HandshakeBurst = 0.001, 1 })
	s.hello() // the one token
	c := s.dial()
	c.nc.Write(handshakeFrame(t, s, nil))
	if !c.closedByServer() {
		t.Fatal("second handshake answered with an empty bucket")
	}
	waitFor(t, "handshake_rate counted", func() bool { return s.rejects(gateway.RejectHandshakeRate) == 1 })
	if n := testutil.ToFloat64(s.tr.hs.WithLabelValues(noToken)); n != 1 {
		t.Fatalf("handshakes_total{no_token} = %v, want 1", n)
	}
}

// A socket over the cap is closed at accept; its close is not reported to
// the core, so the cap's count stays right.
func TestAtCapacityClosesAtAccept(t *testing.T) {
	s := newServer(t, func(c *gateway.Config, _ *Config) { c.MaxConns = 1 })
	a := s.online("ua")
	b := s.dial()
	if !b.closedByServer() || s.rejects(gateway.RejectAtCapacity) != 1 {
		t.Fatalf("second socket: at_capacity rejects = %d", s.rejects(gateway.RejectAtCapacity))
	}
	a.send(clientpb.Cmd_CMD_PING, 2, nil)
	if cmd, _ := a.recv(nil); cmd != clientpb.Cmd_CMD_PONG {
		t.Fatal("first connection broken by the reject")
	}
	a.nc.Close()
	waitFor(t, "a slot after the close", func() bool {
		c := s.dial()
		c.nc.Write(handshakeFrame(t, s, nil))
		reply := make([]byte, wire.HeaderLen+wire.HandshakeMsgLen)
		_, err := ioReadFull(c, reply)
		return err == nil
	})
}

func TestOversizedFrameIsClosedFromItsHeader(t *testing.T) {
	s := newServer(t, nil)
	c := s.online("u3")
	var h [wire.HeaderLen]byte
	wire.Header{Version: wire.Version, Cmd: uint16(clientpb.Cmd_CMD_PING), Length: 8193}.Put(h[:])
	c.nc.Write(h[:]) // the payload never comes
	if !c.closedByServer() {
		t.Fatal("not closed on an oversized header")
	}
	waitFor(t, "bad_frame counted", func() bool { return s.closes(closeBadFrame) == 1 })
}

func TestTamperedHeaderIsClosedAsDecryptFailure(t *testing.T) {
	s := newServer(t, nil)
	c := s.online("u4")
	f := c.seal(clientpb.Cmd_CMD_PING, 3, nil)
	f[2] = byte(clientpb.Cmd_CMD_LOGOUT) // Cmd changed in flight
	c.nc.Write(f)
	if !c.closedByServer() {
		t.Fatal("tampered frame accepted")
	}
	waitFor(t, "decrypt counted", func() bool { return s.closes(closeDecrypt) == 1 })
}

// A Kick arrives by gRPC and reaches the connection through Execute.
func TestKickReachesTheClient(t *testing.T) {
	s := newServer(t, nil)
	c := s.online("u5")
	id, ok := s.logic.connID("u5")
	if !ok {
		t.Fatal("no conn_id")
	}
	kickAddr, _ := s.g.Addrs()
	cc, err := grpc.NewClient(kickAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	if _, err := pb.NewGatewayClient(cc).Kick(context.Background(), &pb.KickRequest{Gw: s.ref, ConnId: id,
		Reason: pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}); err != nil {
		t.Fatal(err)
	}
	var k clientpb.Kick
	if cmd, _ := c.recv(&k); cmd != clientpb.Cmd_CMD_KICK || k.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("got %v %v", cmd, &k)
	}
	if !c.closedByServer() {
		t.Fatal("not closed after the Kick")
	}
}

// gnet closes a socket inside Write when the write fails; the core must hear
// of it only after its current call returns.
func TestCloseInsideACoreCallIsReportedAfterIt(t *testing.T) {
	s := newServer(t, nil)
	l := &loop{g: s.g}
	tc := &conn{t: s.tr, l: l}
	var reason string
	l.run(func() { tc.c, reason = s.g.Open(l, tc) })
	if reason != "" {
		t.Fatal(reason)
	}
	handshaking := func() float64 { return testutil.ToFloat64(s.m.Conns.WithLabelValues("handshake")) }
	l.run(func() {
		l.closed(tc) // as if a Send inside this call failed
		if handshaking() != 1 {
			t.Error("Closed reached the core inside its call")
		}
		tc.Send(clientpb.Cmd_CMD_PONG, 1, nil) // a no-op now
		tc.Close()
	})
	if handshaking() != 0 {
		t.Fatal("Closed not reported after the call")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/gateway/tcp`
Expected: FAIL to build, `undefined: Transport` (and `Config`, `New`, `conn`, `loop`, …).

- [ ] **Step 3: Write the implementation and add gnet**

`internal/gateway/tcp/transport.go`:

```go
// Package tcp is the gateway's TCP transport (spec: Transport seam): gnet
// event loops, the 6-byte frame codec and Noise NK. It turns sockets into the
// core's events (Open, HandshakeDone, Frame, Closed) on the connection's
// loop, and implements gateway.Loop and gateway.Link on top of gnet.
package tcp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/panjf2000/gnet/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// maxLoops is gnet v2.10.0's limit on event loops (gfd.EventLoopIndexMax).
const maxLoops = 256

// Config is the transport's own configuration. The number of event loops is
// the core's NumLoops.
type Config struct {
	Addr string // gnet listen address, e.g. "tcp://:8000"
	// MaxClientFrame is the largest Length a client frame may have after the
	// handshake, checked from the header before the payload is buffered. A
	// LoginReq with a 4096-byte token fits in 8192 (roadmap choice #16).
	MaxClientFrame int
	// The handshake limit: handshakes computing at once, and a token bucket
	// on handshakes/s (spec: Admission control).
	HandshakeConcurrency int
	HandshakeRate        float64
	HandshakeBurst       int
}

// DefaultConfig returns the defaults for a gateway with numLoops loops.
// Plan 6 re-derives the handshake limit from the measured NK cost.
func DefaultConfig(numLoops int) Config {
	return Config{Addr: "tcp://:8000", MaxClientFrame: 8192,
		HandshakeConcurrency: max(1, (numLoops+1)/2), HandshakeRate: 25_000, HandshakeBurst: 2_500}
}

// Validate refuses a configuration the transport can't run with.
func (c Config) Validate(numLoops int) error {
	var errs []error
	if numLoops > maxLoops {
		errs = append(errs, fmt.Errorf("NumLoops %d exceeds gnet's %d event loops", numLoops, maxLoops))
	}
	if c.MaxClientFrame < wire.HandshakeLen || c.MaxClientFrame > wire.MaxLength {
		errs = append(errs, fmt.Errorf("MaxClientFrame %d must be in [%d, %d]", c.MaxClientFrame, wire.HandshakeLen, wire.MaxLength))
	}
	if c.HandshakeConcurrency < 1 || c.HandshakeRate <= 0 || c.HandshakeBurst < 1 {
		errs = append(errs, errors.New("HandshakeConcurrency, HandshakeRate and HandshakeBurst must be positive"))
	}
	return errors.Join(errs...)
}

// Transport is the gnet event handler for one gateway.
type Transport struct {
	gnet.BuiltinEventEngine

	g     *gateway.Gateway
	cfg   Config
	loops int
	keys  wire.KeySet
	lim   *limiter
	hs    *prometheus.CounterVec // handshakes_total{result=ok|failed|no_slot|no_token}
	log   *slog.Logger

	byLoop sync.Map // gnet.EventLoop → *loop
	eng    gnet.Engine
	addr   string
	booted chan struct{}
	up     atomic.Bool
	done   chan error // gnet.Run returned

	// onLoop, if set, is called on the loop at every transport event and
	// Link call (the loop-ownership test).
	onLoop func(c *conn, site string)
}

// New builds the transport for g, with the Noise key set ks.
func New(g *gateway.Gateway, ks wire.KeySet, cfg Config, reg prometheus.Registerer) (*Transport, error) {
	n := g.Config().NumLoops
	if err := cfg.Validate(n); err != nil {
		return nil, err
	}
	if len(ks) == 0 {
		return nil, errors.New("tcp: empty Noise key set")
	}
	hs := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "goim_gateway", Name: "handshakes_total",
		Help: "Noise handshakes by result: computed (ok, failed) or refused by the limiter (no_slot, no_token)."}, []string{"result"})
	reg.MustRegister(hs)
	return &Transport{g: g, cfg: cfg, loops: n, keys: ks, hs: hs,
		lim:    newLimiter(cfg.HandshakeConcurrency, cfg.HandshakeRate, cfg.HandshakeBurst, time.Now),
		log:    slog.Default().With("transport", "tcp"),
		booted: make(chan struct{}), done: make(chan error, 1)}, nil
}

// Start runs gnet and returns once it listens. The listener stays open while
// the gateway drains: the core rejects new connections as draining, and the
// NLB stops sending them once the pod is NotReady.
func (t *Transport) Start() error {
	go func() {
		t.done <- gnet.Run(t, t.cfg.Addr,
			gnet.WithMulticore(true),
			gnet.WithNumEventLoop(t.loops),
			gnet.WithLoadBalancing(gnet.RoundRobin), // spec: Pre-allocation, chosen explicitly
			gnet.WithReusePort(false),               // one acceptor, so RoundRobin applies
			gnet.WithTCPNoDelay(gnet.TCPNoDelay),
			gnet.WithLogger(gnetLogger{t.log}))
	}()
	select {
	case <-t.booted:
		return nil
	case err := <-t.done:
		return fmt.Errorf("tcp: %w", err)
	}
}

// Addr is the bound listen address, once started.
func (t *Transport) Addr() string { return t.addr }

// Ready is the transport's readiness condition: listening, with a key set.
func (t *Transport) Ready() error {
	if !t.up.Load() {
		return errors.New("TCP listener not started")
	}
	return nil
}

// Stop shuts gnet down; every socket still open is closed and reported to
// the core through Closed.
func (t *Transport) Stop(ctx context.Context) error {
	if !t.up.Load() {
		return nil
	}
	if err := t.eng.Stop(ctx); err != nil {
		return err
	}
	select {
	case <-t.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// OnBoot records the engine and the bound address.
func (t *Transport) OnBoot(eng gnet.Engine) gnet.Action {
	t.eng = eng
	if fd, err := eng.Dup(); err == nil {
		if sa, err := syscall.Getsockname(fd); err == nil {
			t.addr = sockaddr(sa)
		}
		syscall.Close(fd)
	}
	t.up.Store(true)
	close(t.booted)
	return gnet.None
}

func sockaddr(sa syscall.Sockaddr) string {
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		return (&net.TCPAddr{IP: a.Addr[:], Port: a.Port}).String()
	case *syscall.SockaddrInet6:
		return (&net.TCPAddr{IP: a.Addr[:], Port: a.Port}).String()
	}
	return ""
}

// loopOf returns the transport's state for gnet loop el, created the first
// time el is seen (on el's goroutine).
func (t *Transport) loopOf(el gnet.EventLoop) *loop {
	if l, ok := t.byLoop.Load(el); ok {
		return l.(*loop)
	}
	l := &loop{el: el, g: t.g}
	t.byLoop.Store(el, l)
	return l
}

// OnOpen admits the socket in the core, before any crypto (spec: Admission
// control). A rejected socket is closed at once; its OnClose finds no conn.
func (t *Transport) OnOpen(gc gnet.Conn) ([]byte, gnet.Action) {
	l := t.loopOf(gc.EventLoop())
	c := &conn{t: t, l: l, gc: gc}
	var reason string
	l.run(func() { c.c, reason = t.g.Open(l, c) })
	if reason != "" {
		return nil, gnet.Close
	}
	gc.SetContext(c)
	c.hook("open")
	return nil, gnet.None
}

// OnTraffic cuts and handles every whole frame buffered.
func (t *Transport) OnTraffic(gc gnet.Conn) gnet.Action {
	c, ok := gc.Context().(*conn)
	if !ok {
		return gnet.Close
	}
	c.hook("traffic")
	c.l.run(c.read)
	return gnet.None
}

// OnClose reports the close to the core, once per admitted connection.
func (t *Transport) OnClose(gc gnet.Conn, _ error) gnet.Action {
	if c, ok := gc.Context().(*conn); ok {
		c.hook("close")
		c.l.closed(c)
	}
	return gnet.None
}

// gnetLogger sends gnet's own log lines to slog.
type gnetLogger struct{ l *slog.Logger }

func (g gnetLogger) Debugf(f string, a ...any) { g.l.Debug(fmt.Sprintf(f, a...)) }
func (g gnetLogger) Infof(f string, a ...any)  { g.l.Info(fmt.Sprintf(f, a...)) }
func (g gnetLogger) Warnf(f string, a ...any)  { g.l.Warn(fmt.Sprintf(f, a...)) }
func (g gnetLogger) Errorf(f string, a ...any) { g.l.Error(fmt.Sprintf(f, a...)) }
func (g gnetLogger) Fatalf(f string, a ...any) {
	g.l.Error(fmt.Sprintf(f, a...))
	panic(fmt.Sprintf(f, a...))
}
```

`internal/gateway/tcp/loop.go`:

```go
package tcp

import (
	"context"

	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/panjf2000/gnet/v2"
)

// loop is the transport's share of one gnet event loop: the gateway.Loop the
// core binds to, and scratch buffers for sealing and opening frames. Every
// field is touched only on the loop's goroutine.
//
// It also guards against re-entry. gnet closes a socket synchronously when a
// write fails (conn.write calls loop.close, which calls OnClose), so a
// Link.Send from inside the core could otherwise call Gateway.Closed while
// the core is still in the middle of a call for that connection. run counts
// how deep the loop is inside the core; a close inside it is queued and
// reported once the outermost call returns.
type loop struct {
	el     gnet.EventLoop
	g      *gateway.Gateway
	depth  int
	queued []*conn // closed by gnet inside a core call, not reported yet
	in     []byte  // opened frames: req_id || body
	out    []byte  // sealed frames
}

// Execute runs fn on the loop (gateway.Loop). gnet does not keep the order
// of two Execute calls; the core never relies on it.
func (l *loop) Execute(fn func()) error {
	return l.el.Execute(context.Background(), gnet.RunnableFunc(func(context.Context) error {
		l.run(fn)
		return nil
	}))
}

// run calls into the core, then reports the closes queued meanwhile.
func (l *loop) run(fn func()) {
	l.depth++
	fn()
	l.depth--
	if l.depth > 0 {
		return
	}
	for len(l.queued) > 0 {
		batch := l.queued
		l.queued = nil
		l.depth++
		for _, c := range batch {
			l.g.Closed(c.c)
		}
		l.depth--
	}
}

// closed is gnet's OnClose for c: from now on c's Link is a no-op, and the
// core hears of the close once it is not inside a call.
func (l *loop) closed(c *conn) {
	c.gc = nil
	l.queued = append(l.queued, c)
	if l.depth == 0 {
		l.run(func() {})
	}
}
```

`internal/gateway/tcp/conn.go`:

```go
package tcp

import (
	"errors"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/panjf2000/gnet/v2"
)

// Close reasons this transport adds to closes_total.
const (
	closeBadFrame  = "bad_frame" // header, size or command not allowed here
	closeDecrypt   = "decrypt"   // AEAD failure: tampered, replayed or garbage
	closeHandshake = "handshake" // the Noise handshake failed
)

var errBadFrame = errors.New("tcp: bad frame")

// conn is one admitted socket: the core's Link, and the frame and Noise
// state. Touched only on its loop.
type conn struct {
	t       *Transport
	l       *loop
	gc      gnet.Conn // nil once gnet closed the socket
	c       *gateway.Conn
	sess    *wire.Session // nil until the handshake completes
	closing bool          // Close requested
}

func (c *conn) hook(site string) {
	if c.t.onLoop != nil {
		c.t.onLoop(c, site)
	}
}

// Send seals one frame and writes it (gateway.Link).
func (c *conn) Send(cmd clientpb.Cmd, reqID uint32, body []byte) {
	if c.gc == nil || c.sess == nil {
		return
	}
	c.hook("send")
	f, err := c.sess.Seal(c.l.out[:0], uint16(cmd), reqID, body)
	if err != nil {
		c.t.log.Error("seal", "cmd", cmd, "err", err)
		return
	}
	c.l.out = f[:0]
	c.gc.Write(f) // gnet copies what the socket doesn't take at once
}

// Close closes the socket (gateway.Link); OnClose follows.
func (c *conn) Close() {
	if c.gc == nil || c.closing {
		return
	}
	c.closing = true
	c.hook("closereq")
	c.gc.Close()
}

// read handles every whole frame buffered, in order, until the buffer runs
// out or the connection is closing.
func (c *conn) read() {
	for c.gc != nil && !c.closing {
		_, f, err := wire.Cut(c.gc, c.check)
		if err != nil {
			if errors.Is(err, wire.ErrVersion) && c.sess == nil {
				c.t.g.Reject(c.c, gateway.RejectBadVersion)
			} else {
				c.t.g.Close(c.c, closeBadFrame)
			}
			return
		}
		if f == nil {
			return
		}
		if c.sess == nil {
			c.handshake(f)
		} else {
			c.frame(f)
		}
		if c.gc == nil {
			return
		}
		c.gc.Discard(len(f))
	}
}

// check vets a header before its payload is buffered: before the handshake
// only the handshake frame, of its exact size; after it, frames of at least
// the AEAD overhead and at most MaxClientFrame.
func (c *conn) check(h wire.Header) error {
	if err := h.Check(); err != nil {
		return err
	}
	if c.sess == nil {
		if h.Cmd != uint16(clientpb.Cmd_CMD_HANDSHAKE) || h.Length != wire.HandshakeLen {
			return errBadFrame
		}
		return nil
	}
	if int(h.Length) < wire.Overhead || int(h.Length) > c.t.cfg.MaxClientFrame {
		return errBadFrame
	}
	return nil
}

// handshake answers the client's handshake frame (spec: Noise key runbook,
// Admission control): an unknown key_id or an exhausted limiter closes the
// connection before any crypto.
func (c *conn) handshake(f []byte) {
	key, ok := c.t.keys[wire.KeyID(f)]
	if !ok {
		c.t.g.Reject(c.c, gateway.RejectNoiseUnknownKey)
		return
	}
	if why := c.t.lim.acquire(); why != "" {
		c.t.hs.WithLabelValues(why).Inc()
		c.t.g.Reject(c.c, gateway.RejectHandshakeRate)
		return
	}
	sess, reply, err := wire.Respond(key, f, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	c.t.lim.release()
	if err != nil {
		c.t.hs.WithLabelValues("failed").Inc()
		c.t.g.Close(c.c, closeHandshake)
		return
	}
	c.t.hs.WithLabelValues("ok").Inc()
	c.gc.Write(reply)
	if c.gc == nil {
		return
	}
	c.sess = sess
	c.t.g.HandshakeDone(c.c)
}

// frame opens one frame and hands it to the core.
func (c *conn) frame(f []byte) {
	cmd, reqID, body, out, err := c.sess.Open(c.l.in[:0], f)
	if err != nil {
		c.t.g.Close(c.c, closeDecrypt)
		return
	}
	c.t.g.Frame(c.c, clientpb.Cmd(cmd), reqID, body) // the core copies what it keeps
	c.l.in = out[:0]
}
```

Then:

```bash
go get github.com/panjf2000/gnet/v2@v2.10.0
go mod tidy
```

`go.mod` gains `github.com/panjf2000/gnet/v2 v2.10.0` in the main `require` block, and gnet's own dependencies as indirect (`panjf2000/ants/v2`, `valyala/bytebufferpool`, `go.uber.org/zap`, `go.uber.org/multierr`, `golang.org/x/sync`, `gopkg.in/natefinch/lumberjack.v2`). Check that the line reads exactly `v2.10.0`.

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway/... && go test -race -count=1 ./internal/gateway/tcp ./internal/gateway`
Expected: `ok` for both (tcp ~6s; the gateway package's layer test now lists `internal/gateway/tcp` too and finds no Redis dependency).

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/tcp go.mod go.sum
git commit -m "feat(tcp): gnet transport with the frame codec, Noise NK and the re-entry guard"
```

---

### Task 6: The gnet loop-ownership test and the gnet pin (race 13, R4G-O7)

**Files:**
- Test: `internal/gateway/tcp/ownership_test.go`

**Interfaces:**
- Consumes: Task 5's `newServer(t, mod, set...)`, `Transport.onLoop`, the hook sites `open`, `traffic`, `send`, `closereq`, `close`, `conn.c.ID()`, `server.online`, `server.dial`, `handshakeFrame`, `client.closedByServer`, `fakeLogic.connID`, `server.ref`, `gateway.Metrics.Tracked`.
- Produces: `TestLoopOwnershipUnderRace`, `TestGnetIsPinned`; helpers `goid()`, `waitForLong`.

This task adds tests only: the behaviour exists since Task 5. The test pins it, and Step 3 shows that it catches a violation.

- [ ] **Step 1: Write the tests**

`internal/gateway/tcp/ownership_test.go`:

```go
package tcp

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// goid returns the calling goroutine's id, read from its stack header.
func goid() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	id, _ := strconv.ParseUint(string(b[:bytes.IndexByte(b, ' ')]), 10, 64)
	return id
}

// The gnet loop-ownership test (spec: Verification, race 13): for every
// connection, OnOpen, OnTraffic, OnClose, the Execute callbacks (Kick, wheel
// ticks) and the wheel's expiry callbacks run on one goroutine, and each
// worker_id belongs to exactly one loop goroutine. Run under -race, with
// connections opening, handshaking, logging in, being kicked, timing out and
// sending garbage at once.
func TestLoopOwnershipUnderRace(t *testing.T) {
	type event struct {
		site string
		g    uint64
	}
	var mu sync.Mutex
	seen := map[*conn][]event{}
	s := newServer(t, func(c *gateway.Config, tc *Config) {
		c.NumLoops = 4
		c.HandshakeTimeout = 3 * time.Second // idle sockets close from the wheel
		tc.HandshakeConcurrency = 4          // every loop may handshake at once
	}, func(tr *Transport) {
		tr.onLoop = func(c *conn, site string) {
			g := goid()
			mu.Lock()
			seen[c] = append(seen[c], event{site, g})
			mu.Unlock()
		}
	})

	kickAddr, _ := s.g.Addrs()
	cc, err := grpc.NewClient(kickAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	kick := pb.NewGatewayClient(cc)

	const each = 20
	var wg sync.WaitGroup
	for i := range each {
		wg.Go(func() { // log in, ping, hang up
			c := s.online(fmt.Sprintf("full-%d", i))
			for id := uint32(2); id < 5; id++ {
				c.send(clientpb.Cmd_CMD_PING, id, nil)
				c.recv(nil)
			}
			c.nc.Close()
		})
		wg.Go(func() { // log in, then a Kick through Execute
			uid := fmt.Sprintf("kick-%d", i)
			c := s.online(uid)
			id, _ := s.logic.connID(uid)
			if _, err := kick.Kick(context.Background(), &pb.KickRequest{Gw: s.ref, ConnId: id,
				Reason: pb.KickReason_KICK_REASON_REPLACED}); err != nil {
				t.Error(err)
			}
			c.recv(nil)
			c.closedByServer()
		})
		wg.Go(func() { s.dial() })                                                             // idle: the HANDSHAKE timeout
		wg.Go(func() { s.dial().nc.Write(handshakeFrame(t, s, func(f []byte) { f[0] = 9 })) }) // garbage
	}
	wg.Wait()
	waitForLong(t, "every connection closed", 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		closed := 0
		for _, evs := range seen {
			for _, e := range evs {
				if e.site == "close" {
					closed++
				}
			}
		}
		return closed == 4*each
	})

	mu.Lock()
	defer mu.Unlock()
	loopOf := map[uint64]uint64{} // worker_id → goroutine
	owner := map[uint64]uint64{}  // goroutine → worker_id
	sites := map[string]int{}
	for c, evs := range seen {
		w := c.c.ID() >> 48
		for _, e := range evs {
			sites[e.site]++
			if e.g != evs[0].g {
				t.Fatalf("conn %x: %s on goroutine %d, opened on %d", c.c.ID(), e.site, e.g, evs[0].g)
			}
		}
		if g, ok := loopOf[w]; ok && g != evs[0].g {
			t.Fatalf("worker_id %d on goroutines %d and %d", w, g, evs[0].g)
		}
		if o, ok := owner[evs[0].g]; ok && o != w {
			t.Fatalf("goroutine %d owns worker_ids %d and %d", evs[0].g, o, w)
		}
		loopOf[w], owner[evs[0].g] = evs[0].g, w
	}
	if len(loopOf) != 4 {
		t.Fatalf("connections spread over %d loops, want 4 (round robin)", len(loopOf))
	}
	for _, site := range []string{"open", "traffic", "send", "closereq", "close"} {
		if sites[site] == 0 {
			t.Fatalf("no %q event: the test does not exercise it", site)
		}
	}
	waitForLong(t, "nothing tracked", 10*time.Second, func() bool { return testutil.ToFloat64(s.m.Tracked) == 0 })
}

func waitForLong(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// gnet is pinned to exactly v2.10.0 (R4G-O7). Upgrading it means re-running
// this package's loop-ownership test and the core's loopState tests, then
// changing this pin.
func TestGnetIsPinned(t *testing.T) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("no build info")
	}
	for _, d := range bi.Deps {
		if d.Path == "github.com/panjf2000/gnet/v2" {
			if d.Version != "v2.10.0" || d.Replace != nil {
				t.Fatalf("gnet %s (replace %v); the design was checked against v2.10.0", d.Version, d.Replace)
			}
			return
		}
	}
	t.Fatal("gnet not in the build")
}
```

- [ ] **Step 2: Run them**

Run: `go test -race -count=1 -run 'TestLoopOwnership|TestGnetIsPinned' -v ./internal/gateway/tcp`
Expected: both PASS (the ownership test takes ~5s: it waits for the 3s `HANDSHAKE` timeouts).

- [ ] **Step 3: Show that the test catches a loop violation, then revert**

In `internal/gateway/tcp/loop.go`, change `l.run(fn)` inside `Execute` to `go l.run(fn)` and run the same command.
Expected: `TestLoopOwnershipUnderRace` FAILS (a Kick's `send` or a wheel's `closereq` runs on another goroutine, and `-race` reports the table access). Revert the change and run again: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/gateway/tcp/ownership_test.go
git commit -m "test(tcp): gnet loop-ownership test under -race; gnet pinned at v2.10.0"
```

---

### Task 7: `cmd/gateway` (choices #43, #45)

**Files:**
- Create: `cmd/gateway/main.go`
- Test: `cmd/gateway/main_test.go`

**Interfaces:**
- Consumes: `gateway.DefaultConfig`, `DefaultServerConfig`, `NewEpoch`, `New`, `Deps`, `NewMetrics`, `Window`, `Start`, `Addrs`, `Drain`, `Stop`; `tcp.DefaultConfig`, `Config.Validate`, `New`, `Start`, `Addr`, `Ready`, `Stop`; `wire.GenerateKey`, `EncodeKey`, `LoadKeyDir`, `ClientHandshake`, `Session.WriteFrame`/`ReadFrame` (test).
- Produces: the `gateway` binary: `gateway [flags]` serves, `gateway noise-keygen -key-id N -out FILE` writes a key. Functions `keygen(args, out) error`, `parseFlags(args) (gateway.Config, gateway.ServerConfig, tcp.Config, options, error)`, `run(ctx, args, started func(addr string)) error`; flag type `window` (`lo-hi`).

- [ ] **Step 1: Write the failing test**

`cmd/gateway/main_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestDefaultFlagsAreValid(t *testing.T) {
	cfg, sc, tc, o, err := parseFlags([]string{"-id", "gw-0"})
	if err != nil {
		t.Fatal(err)
	}
	n := runtime.GOMAXPROCS(0)
	if cfg.NumLoops != n || tc.HandshakeConcurrency != max(1, (n+1)/2) || cfg.Epoch == 0 {
		t.Fatalf("loops %d, handshake concurrency %d, epoch %x", cfg.NumLoops, tc.HandshakeConcurrency, cfg.Epoch)
	}
	if tc.Addr != "tcp://:8000" || sc.KickAddr != ":9100" || o.keyDir != "/etc/goim/noise" || o.drainTimeout != 85*time.Second {
		t.Fatalf("defaults: %+v %+v %+v", tc, sc, o)
	}
	cfg, _, tc, _, err = parseFlags([]string{"-id", "gw-1", "-loops", "16", "-storm-wait", "5s-60s"})
	if err != nil {
		t.Fatal(err)
	}
	if tc.HandshakeConcurrency != 8 || cfg.Windows.StormWait.Lo != 5*time.Second || cfg.Windows.StormWait.Hi != time.Minute {
		t.Fatalf("concurrency %d, storm wait %+v", tc.HandshakeConcurrency, cfg.Windows.StormWait)
	}
}

func TestRefusesBadConfig(t *testing.T) {
	for _, args := range [][]string{
		{"-handshake-timeout", "2s"}, // spec: 3–30s
		{"-auth-timeout", "31s"},
		{"-loops", "300"}, // gnet's limit is 256
		{"-busy-retry", "3s-1s"},
		{"-max-client-frame", "10"},
		{"-id", "Not_A_Label"},
	} {
		if _, _, _, _, err := parseFlags(append([]string{"-id", "gw-0"}, args...)); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}

func TestNoiseKeygen(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	if err := keygen([]string{"-key-id", "3", "-out", filepath.Join(dir, "3")}, &out); err != nil {
		t.Fatal(err)
	}
	ks, err := wire.LoadKeyDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, pub := wire.EncodeKey(ks[3])
	if got := strings.TrimSpace(out.String()); got != "key_id 3 public "+pub {
		t.Fatalf("printed %q, want the public key %s", got, pub)
	}
	if fi, _ := os.Stat(filepath.Join(dir, "3")); fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %v", fi.Mode())
	}
	if err := keygen([]string{"-key-id", "70000", "-out", filepath.Join(dir, "x")}, &out); err == nil {
		t.Fatal("key_id above 65535 accepted")
	}
}

// fakeLogic accepts every Login (uid = token) and Logout, renews every
// lease, and counts Logouts and EpochEnded.
type fakeLogic struct {
	pb.UnimplementedLogicServer
	pb.UnimplementedLeaseServer
	logouts       atomic.Int32
	ended         atomic.Int32
	endedLoggedIn atomic.Int32 // EpochEnded calls made before any Logout
}

func (f *fakeLogic) Login(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
	for i, e := range b.GetEntries() {
		s.Send(&pb.LoginResult{Index: uint32(i), Code: pb.LoginCode_LOGIN_CODE_OK, Uid: e.GetToken(), TokIatMs: 1})
	}
	return nil
}

func (f *fakeLogic) Logout(_ context.Context, b *pb.LogoutBatch) (*pb.LogoutReply, error) {
	f.logouts.Add(int32(len(b.GetEntries())))
	r := &pb.LogoutReply{Done: make([]bool, len(b.GetEntries()))}
	for i := range r.Done {
		r.Done[i] = true
	}
	return r, nil
}

func (f *fakeLogic) GatewayLease(context.Context, *pb.LeaseRequest) (*pb.LeaseReply, error) {
	return &pb.LeaseReply{}, nil
}

func (f *fakeLogic) EpochEnded(context.Context, *pb.EpochEndedRequest) (*pb.EpochEndedReply, error) {
	f.ended.Add(1)
	if f.logouts.Load() == 0 {
		f.endedLoggedIn.Add(1)
	}
	return &pb.EpochEndedReply{}, nil
}

// The whole binary: serve, log a client in over TCP + Noise, then SIGTERM
// (ctx done): the client gets Reconnect, the socket closes, its fenced
// Logout reaches Logic before EpochEnded, and run returns.
func TestRunServesThenDrainsOnSIGTERM(t *testing.T) {
	logic := &fakeLogic{}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	pb.RegisterLogicServer(s, logic)
	pb.RegisterLeaseServer(s, logic)
	go s.Serve(l)
	defer s.Stop()

	keys := t.TempDir()
	var out bytes.Buffer
	if err := keygen([]string{"-key-id", "1", "-out", filepath.Join(keys, "1")}, &out); err != nil {
		t.Fatal(err)
	}
	pub, _ := base64.StdEncoding.DecodeString(strings.Fields(out.String())[3])

	ctx, cancel := context.WithCancel(context.Background())
	addr := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-id", "gw-0", "-listen", "tcp://127.0.0.1:0", "-noise-keys", keys,
			"-logic", l.Addr().String(), "-lease", l.Addr().String(), "-kick-addr", "127.0.0.1:0",
			"-admin-addr", "127.0.0.1:0", "-loops", "2", "-max-conns", "100", "-pending-cap", "100",
			"-logout-queue", "100", "-conntrack-notrack", "-drain-window", "200ms", "-drain-flush", "0s"},
			func(a string) { addr <- a })
	}()
	var gw string
	select {
	case gw = <-addr:
	case err := <-done:
		t.Fatal(err)
	}

	nc, err := net.Dial("tcp", gw)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(10 * time.Second))
	sess, err := wire.ClientHandshake(nc, 1, pub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := proto.Marshal(&clientpb.LoginReq{Token: "u1", Appid: "app", Platform: clientpb.Platform_PLATFORM_MOBILE,
		DeviceId: "d1", ConnSeq: 1})
	sess.WriteFrame(nc, uint16(clientpb.Cmd_CMD_LOGIN), 1, body)
	if cmd, _, _, err := sess.ReadFrame(nc); err != nil || clientpb.Cmd(cmd) != clientpb.Cmd_CMD_LOGIN_RESP {
		t.Fatalf("login: %v %v", cmd, err)
	}

	cancel() // SIGTERM
	cmd, _, body, err := sess.ReadFrame(nc)
	var rc clientpb.Reconnect
	if err != nil || clientpb.Cmd(cmd) != clientpb.Cmd_CMD_RECONNECT || proto.Unmarshal(body, &rc) != nil || rc.JitterMs != 5000 {
		t.Fatalf("drain: %v %v %v", cmd, &rc, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return after the drain")
	}
	if logic.ended.Load() != 1 || logic.logouts.Load() != 1 || logic.endedLoggedIn.Load() != 0 {
		t.Fatalf("EpochEnded %d times (%d before any Logout), Logouts %d; want 1, 0, 1",
			logic.ended.Load(), logic.endedLoggedIn.Load(), logic.logouts.Load())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/gateway`
Expected: FAIL to build, `undefined: parseFlags` (and `keygen`, `run`).

- [ ] **Step 3: Write the implementation**

`cmd/gateway/main.go`:

```go
// Command gateway runs one gateway pod (spec: Gateway): the TCP transport
// with Noise NK on gnet, the gateway core, and its Kick and admin servers.
//
//	gateway [flags]                                serve
//	gateway noise-keygen -key-id 1 -out keys/1     write a Noise static key
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/gateway/tcp"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "noise-keygen" {
		if err := keygen(os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "gateway:", err)
			os.Exit(2)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx, os.Args[1:], nil); err != nil {
		slog.Error("gateway: exit", "err", err)
		os.Exit(1)
	}
}

// keygen writes a new Noise static key's private half to -out (for the
// Secret, file name = key_id) and prints the public half to pin in the app
// (spec: Noise key runbook).
func keygen(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("noise-keygen", flag.ContinueOnError)
	id := fs.Uint("key-id", 0, "key_id (0–65535); the Secret's file name")
	path := fs.String("out", "", "private key file to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *path == "" || *id > 65535 {
		return errors.New("noise-keygen: -out is required and -key-id must be in 0–65535")
	}
	k, err := wire.GenerateKey()
	if err != nil {
		return err
	}
	priv, pub := wire.EncodeKey(k)
	if err := os.WriteFile(*path, []byte(priv+"\n"), 0o600); err != nil {
		return err
	}
	fmt.Fprintf(out, "key_id %d public %s\n", *id, pub)
	return nil
}

// options are the flags that are not part of a Config.
type options struct {
	keyDir       string
	drainTimeout time.Duration
}

// window is a flag holding a uniform wait "lo-hi", e.g. "1s-3s".
type window struct{ w *gateway.Window }

func (f window) String() string {
	if f.w == nil || *f.w == (gateway.Window{}) {
		return ""
	}
	return f.w.Lo.String() + "-" + f.w.Hi.String()
}

func (f window) Set(s string) error {
	lo, hi, ok := strings.Cut(s, "-")
	if !ok {
		return fmt.Errorf("want lo-hi, e.g. 1s-3s")
	}
	var err error
	if f.w.Lo, err = time.ParseDuration(lo); err != nil {
		return err
	}
	f.w.Hi, err = time.ParseDuration(hi)
	return err
}

// parseFlags returns the validated configuration. The gateway refuses to
// start on a configuration out of range or breaking a required ratio (spec:
// Timeouts, Test execution model).
func parseFlags(args []string) (gateway.Config, gateway.ServerConfig, tcp.Config, options, error) {
	cfg, sc := gateway.DefaultConfig(), gateway.DefaultServerConfig()
	cfg.NumLoops = runtime.GOMAXPROCS(0)
	cfg.GatewayID = os.Getenv("POD_NAME")
	if cfg.GatewayID == "" {
		cfg.GatewayID, _ = os.Hostname()
	}
	tc := tcp.DefaultConfig(cfg.NumLoops)
	o := options{keyDir: "/etc/goim/noise", drainTimeout: 85 * time.Second}

	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.StringVar(&cfg.GatewayID, "id", cfg.GatewayID, "gateway id: the pod name (default $POD_NAME, then the host name)")
	fs.StringVar(&tc.Addr, "listen", tc.Addr, "client TCP listen address (gnet)")
	fs.StringVar(&o.keyDir, "noise-keys", o.keyDir, "Noise key Secret directory: one file per key_id")
	fs.DurationVar(&o.drainTimeout, "drain-timeout", o.drainTimeout, "SIGTERM drain limit; keep below terminationGracePeriodSeconds")

	fs.StringVar(&sc.LogicTarget, "logic", sc.LogicTarget, "Logic gRPC target (Login/Logout/Reconcile)")
	fs.StringVar(&sc.LeaseTarget, "lease", sc.LeaseTarget, "Logic Lease gRPC target (dedicated channel)")
	fs.StringVar(&sc.KickAddr, "kick-addr", sc.KickAddr, "Kick gRPC listen address")
	fs.StringVar(&sc.AdminAddr, "admin-addr", sc.AdminAddr, "/metrics, /healthz, /readyz listen address")

	fs.IntVar(&cfg.NumLoops, "loops", cfg.NumLoops, "gnet event loops (default GOMAXPROCS)")
	fs.IntVar(&cfg.MaxConns, "max-conns", cfg.MaxConns, "connection cap: open sockets in any state")
	fs.IntVar(&cfg.PendingCap, "pending-cap", cfg.PendingCap, "HANDSHAKE + AUTH_PENDING sockets")
	fs.IntVar(&cfg.LoginQueueCap, "login-queue-cap", cfg.LoginQueueCap, "Logins waiting for or inside a Login RPC")
	fs.IntVar(&cfg.LogoutQueue, "logout-queue", cfg.LogoutQueue, "Logouts queued or waiting for a retry")
	fs.DurationVar(&cfg.HandshakeTimeout, "handshake-timeout", cfg.HandshakeTimeout, "accept → handshake done (3s–30s)")
	fs.DurationVar(&cfg.AuthTimeout, "auth-timeout", cfg.AuthTimeout, "handshake done → LoginReq (3s–30s)")
	fs.DurationVar(&cfg.Heartbeat, "heartbeat", cfg.Heartbeat, "heartbeat interval sent in LoginResp")
	fs.DurationVar(&cfg.LoginDeadline, "login-deadline", cfg.LoginDeadline, "login_deadline, as Logic's (not scaled by the test profile)")
	fs.DurationVar(&cfg.LoginTimeout, "login-timeout", cfg.LoginTimeout, "per-entry Login timeout from recv_ms")
	fs.DurationVar(&cfg.LeaseInterval, "lease-interval", cfg.LeaseInterval, "GatewayLease period")
	fs.DurationVar(&cfg.LeaseTimeout, "lease-timeout", cfg.LeaseTimeout, "GatewayLease call timeout")
	fs.DurationVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "Logic's lease TTL (the trigger rule)")
	fs.DurationVar(&cfg.RetryFor, "retry-for", cfg.RetryFor, "Logout and ReconcileEnd retry budget")
	fs.DurationVar(&cfg.Reconcile.Cycle, "reconcile-cycle", cfg.Reconcile.Cycle, "periodic Reconcile cycle")
	fs.DurationVar(&cfg.Reconcile.Jitter, "reconcile-jitter", cfg.Reconcile.Jitter, "periodic Reconcile jitter")
	fs.IntVar(&cfg.Reconcile.TriggeredRate, "triggered-rate", cfg.Reconcile.TriggeredRate, "triggered pass entries/s")
	fs.IntVar(&cfg.Reconcile.DeleteRate, "delete-rate", cfg.Reconcile.DeleteRate, "Logic's delete-phase rate, uids/s")
	fs.DurationVar(&cfg.Drain.Window, "drain-window", cfg.Drain.Window, "Reconnect frames spread over this window")
	fs.DurationVar(&cfg.Drain.Jitter, "drain-jitter", cfg.Drain.Jitter, "Reconnect{jitter_ms}")
	fs.DurationVar(&cfg.Drain.Flush, "drain-flush", cfg.Drain.Flush, "after the last close, wait for Logouts")
	fs.BoolVar(&cfg.Limits.Notrack, "conntrack-notrack", cfg.Limits.Notrack, "conntrack is disabled for the gateway port")
	fs.Var(window{&cfg.BusyRetry}, "busy-retry", "Busy{retry_after} window, lo-hi")
	fs.Var(window{&cfg.Windows.StormWait}, "storm-wait", "LoginResp storm wait window, lo-hi (unset: not sent)")
	fs.Var(window{&cfg.Windows.LocalTrigger}, "local-trigger", "LoginResp local trigger window, lo-hi")
	fs.Var(window{&cfg.Windows.PreHandshakeRetry}, "pre-handshake-retry", "LoginResp pre-handshake retry window, lo-hi")
	fs.Var(window{&cfg.Windows.Retryable}, "retryable", "LoginResp retryable reject window, lo-hi")

	fs.IntVar(&tc.MaxClientFrame, "max-client-frame", tc.MaxClientFrame, "largest client frame Length after the handshake")
	concurrency := fs.Int("handshake-concurrency", 0, "handshakes computing at once (default: half the loops)")
	fs.Float64Var(&tc.HandshakeRate, "handshake-rate", tc.HandshakeRate, "handshakes/s, gateway-wide")
	fs.IntVar(&tc.HandshakeBurst, "handshake-burst", tc.HandshakeBurst, "handshake token bucket burst")
	if err := fs.Parse(args); err != nil {
		return cfg, sc, tc, o, err
	}
	tc.HandshakeConcurrency = tcp.DefaultConfig(cfg.NumLoops).HandshakeConcurrency
	if *concurrency != 0 {
		tc.HandshakeConcurrency = *concurrency
	}
	cfg.Epoch = gateway.NewEpoch()
	if err := errors.Join(cfg.Validate(), tc.Validate(cfg.NumLoops)); err != nil {
		return cfg, sc, tc, o, fmt.Errorf("config: %w", err)
	}
	return cfg, sc, tc, o, nil
}

// run serves until ctx is done, then drains. started, if set, gets the
// client listen address once the gateway is serving (tests).
func run(ctx context.Context, args []string, started func(addr string)) error {
	cfg, sc, tc, o, err := parseFlags(args)
	if err != nil {
		return err
	}
	keys, err := wire.LoadKeyDir(o.keyDir)
	if err != nil {
		return fmt.Errorf("noise keys: %w", err)
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	var tr *tcp.Transport // set before Start; Ready reads it only after
	g, err := gateway.New(cfg, gateway.Deps{Metrics: gateway.NewMetrics(reg), Registry: reg,
		ReadyChecks: []func() error{func() error { return tr.Ready() }}})
	if err != nil {
		return err
	}
	if tr, err = tcp.New(g, keys, tc, reg); err != nil {
		return err
	}
	// The gateway runs on its own context: ctx ending is the SIGTERM that
	// starts the drain, and the drain still needs the Logout sender, the
	// wheel ticker and the lease connection.
	if err := g.Start(context.Background(), sc); err != nil {
		return err
	}
	if err := tr.Start(); err != nil {
		g.Stop()
		return err
	}
	kick, admin := g.Addrs()
	slog.Info("gateway: serving", "id", cfg.GatewayID, "epoch", fmt.Sprintf("%016x", cfg.Epoch),
		"listen", tr.Addr(), "kick", kick, "admin", admin, "loops", cfg.NumLoops, "keys", len(keys))
	if started != nil {
		started(tr.Addr())
	}
	<-ctx.Done()

	// SIGTERM: drain on a fresh context (ctx is already done), then stop.
	slog.Info("gateway: draining")
	dctx, cancel := context.WithTimeout(context.Background(), o.drainTimeout)
	defer cancel()
	g.Drain(dctx)
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := tr.Stop(sctx); err != nil {
		slog.Warn("gateway: transport stop", "err", err)
	}
	g.Stop()
	return nil
}
```

- [ ] **Step 4: Run the tests and build**

Run: `go vet ./cmd/gateway && go test -race -count=1 ./cmd/gateway && go build -o /dev/null ./cmd/gateway`
Expected: `ok` (`TestRunServesThenDrainsOnSIGTERM` takes under 1s) and a clean build. If `g.Start` is given `ctx` instead of `context.Background()`, the drain test fails after ~6s with `EpochEnded 1 times (1 before any Logout), Logouts 0` (Review Focus 5).

- [ ] **Step 5: Commit**

```bash
git add cmd/gateway
git commit -m "feat(cmd/gateway): flags, noise-keygen, SIGTERM drain"
```

---

### Task 8: End to end over TCP and Noise against Logic

**Files:**
- Test: `internal/gateway/tcp/e2e_test.go`

**Interfaces:**
- Consumes: Task 5's `startServer`, `server.login`, `client.recv`/`send`/`closedByServer`, `waitFor`; Plan 2's `logic.DefaultConfig`, `DefaultServerConfig`, `NewServer`, `Deps{Store, Keys, Dial, Registry, Metrics}`, `NewMetrics`, `Server.Start`/`Stop`/`Addrs`; `auth.GenerateKey`, `Sign`, `Verify`, `Claims`, `KeySet`; Plan 1's `store.New`, `session.Decode`, `testredis.New`, `UID`, `Gateway`.
- Produces: `TestE2EOverTCP`; helper `staticKeys`.

A test-only task: it pins the composition of everything so far, the first full vertical slice (client → TCP + Noise → core → Logic → Redis, and Logic's Kick back to the gateway). It passes on its first run.

- [ ] **Step 1: Write the test**

`internal/gateway/tcp/e2e_test.go`:

```go
package tcp

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// staticKeys is a JWT key set that is always loaded.
type staticKeys struct{ k auth.PrivateKey }

func (s staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, auth.KeySet{s.k.Kid: s.k.Key.Public().(ed25519.PublicKey)}, time.Now().UnixMilli(), 30_000)
}
func (staticKeys) Ready() bool         { return true }
func (staticKeys) Run(context.Context) {}

// The first vertical slice end to end: a client over TCP + Noise, this
// transport, the gateway core and Plan 2's Logic on miniredis (the real
// Redis at Tier 2). Only test files import Logic and the store.
func TestE2EOverTCP(t *testing.T) {
	rdb := testredis.New(t)
	key, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	lcfg := logic.DefaultConfig()
	lcfg.SweepInterval = time.Hour // never sweep a shared Tier 2 Redis
	var kickAddr string            // set once the gateway listens
	reg := prometheus.NewRegistry()
	lsc := logic.DefaultServerConfig()
	lsc.GRPCAddr, lsc.LeaseAddr, lsc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	srv, err := logic.NewServer(lcfg, lsc, logic.Deps{Store: store.New(rdb, lcfg.Store), Keys: staticKeys{key},
		Dial: func(string) (*grpc.ClientConn, error) {
			return grpc.NewClient(kickAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}, Registry: reg, Metrics: logic.NewMetrics(reg)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); srv.Stop() })
	grpcAddr, leaseAddr, _ := srv.Addrs()

	s := startServer(t, func(c *gateway.Config, _ *Config) { c.GatewayID = testredis.Gateway() }, grpcAddr, leaseAddr)
	kickAddr, _ = s.g.Addrs()

	uid := testredis.UID()
	token := func() string {
		now := time.Now().UnixMilli()
		tok, err := auth.Sign(key, auth.Claims{UID: uid, AppID: "app", Platform: "mobile", IatMs: now - 1000, ExpMs: now + 3_600_000})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	field := func() (session.Entry, bool) {
		v, err := rdb.HGet(context.Background(), "sess:{"+uid+"}", "app:mobile").Result()
		if err == redis.Nil {
			return session.Entry{}, false
		}
		if err != nil {
			t.Fatal(err)
		}
		en, err := session.Decode(v)
		if err != nil {
			t.Fatal(err)
		}
		return en, true
	}

	a := s.login(token(), "phone-a")
	b := s.login(token(), "phone-b") // the same slot from another device
	var k clientpb.Kick
	if cmd, _ := a.recv(&k); cmd != clientpb.Cmd_CMD_KICK || k.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("A got %v %v, want Kick KICKED_BY_OTHER_DEVICE through Logic's Kick RPC", cmd, &k)
	}
	if !a.closedByServer() {
		t.Fatal("A not closed after its Kick")
	}
	if en, ok := field(); !ok || en.DeviceID != "phone-b" || en.Gateway != s.ref.Gateway || en.Epoch != s.ref.Epoch {
		t.Fatalf("stored %+v, %v; want B's session on this gateway epoch", en, ok)
	}
	b.send(clientpb.Cmd_CMD_LOGOUT, 2, nil)
	if cmd, _ := b.recv(nil); cmd != clientpb.Cmd_CMD_LOGOUT_RESP {
		t.Fatalf("B got %v, want LogoutResp", cmd)
	}
	waitFor(t, "field deleted by the fenced Logout", func() bool { _, ok := field(); return !ok })
}
```

- [ ] **Step 2: Run it, then the whole suite**

Run: `go test -race -count=1 -run TestE2EOverTCP -v ./internal/gateway/tcp && go vet ./... && go test -race -count=1 ./...`
Expected: PASS, then `ok` for every package (the layer test still passes: only test files import Logic and the store).

- [ ] **Step 3: Commit**

```bash
git add internal/gateway/tcp/e2e_test.go
git commit -m "test(tcp): end to end over TCP and Noise against Logic"
```

---

## After this plan

A client can reach Logic through a real gateway binary: TCP, Noise NK, frames, login, ping, kick, logout, drain. Plan 5 builds the client SDK on `internal/wire`, the scripted test client, fault injection and the Tier 1b race suite (*Execution Notes → For Plan 5*).

The decisions on #38–#45 are recorded in the roadmap (2026-10-08), with this plan's test names in the invariant index.

The open risks are listed in *Execution Notes*.
