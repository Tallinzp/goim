# Plan 6b3b4: Compute Each X25519 Once Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Halve the Noise handshake's crypto on the gateway and on every client, with the same bytes on the wire. Then rerun `floods` (run 30i) to measure the gain, and the full `gwloss` (run 30j), whose b_stress storm was bound by loadgen's handshakes. This is #130, decided after the gateway profile under #129. The review (O1–O5, all accepted) added the heap benchmark, the storm's profiles and run 30j.

**Architecture:** One package, `internal/wire`, changes. flynn/noise v1.1.0's `DH25519` computes each DH, and each key generation, with x/crypto's `curve25519.X25519`. That function builds an `*ecdh.PrivateKey` from the private bytes each time. In Go 1.27, `ecdh.NewPrivateKey` computes the public key at once, a full scalar multiplication nobody reads. So every DH costs two multiplications and every key generation two. The fix:
- **A cipher suite per handshake** (`suite`, new file `x25519.go`) whose DH function keeps the `*ecdh.PrivateKey` of the static key and of the one ephemeral it generates. `DH(priv, pub)` uses the kept key whose bytes are `priv` and parses `priv` only if none matches. `GenerateKeypair` reads 32 bytes from the handshake's rng, as `DH25519` does, and parses them once. Name, DH length and bytes are `DH25519`'s.
- **`wire.Key` keeps its parsed key.** `Key` stops being an alias of `noise.DHKey`. It becomes a struct with the same `Private` and `Public` fields plus an unexported `*ecdh.PrivateKey`, set by `GenerateKey` and `ParsePrivate` (so by `LoadKeyDir`). The gateway parses its static keys once, at start.
- `Respond` and `Initiate` build a `suite` per call. Their signatures don't change, so the gateway (`internal/gateway/tcp`), the SDK, loadgen and `clientconn` change no code. An NK handshake costs 3 multiplications on each side (e, es, ee) instead of 6. A pending `Initiator` keeps ~400 bytes more until `Finish`, which loadgen's precomputed handshakes feel (Review Focus 6).
- `floods.sh`'s storm phase takes CPU profiles of the gateway and loadgen (Task 2), so run 30i shows X25519's share on both sides of the handshake. The garbage phase already profiles every gateway.

**Tech Stack:** Go 1.27.1, `crypto/ecdh`, `github.com/flynn/noise` v1.1.0 (unchanged), `math/rand/v2` (ChaCha8, tests only).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Client encryption*: Noise_NK_25519_ChaChaPoly_SHA256, the server key pinned in the app. Nothing about it changes. The apps' platform crypto (iOS CryptoKit, Android) speaks the same bytes, so this is a server- and Go-client-side change only.
- *Admission control* (~l.199): the Noise handshake limit, a concurrency limit (`-handshake-concurrency`, by default half the event loops: 3 slots per gateway in `floods`' garbage and slow phases, 6 on the storm's one gateway) and a 25,000/s bucket. The spec estimates an NK handshake at ~100µs. Plan 6b3b's review (O2) measured a slot hold of ~300–500µs. That was these 6 multiplications, and it is why the slots refuse ~27% of handshakes at Tier 2's rates (#111).

Inputs:
- Plan 6b3b3's *Execution Notes*, the last section (*Gateway CPU profile: every X25519 is done twice*): the profile (`x25519ScalarMult` 67.7% of gw-0's CPU in run 30c's garbage phase), the cause traced in the sources, and the dev-machine benchmark (one DH 135µs through `curve25519.X25519`, 68µs on a kept key).
- The roadmap's #130 (this plan), and #129's runs 30c–30h (the floods' numbers to compare with).

**Decisions:**
- #130 (the user, 2026-10-17, with AskUserQuestion): a DH function for flynn/noise in `internal/wire` that keeps `*ecdh.PrivateKey` objects, on the server and the clients alike, with the same bytes on the wire. The usual workflow, then a floods rerun (storm, and slow at 7,500/s). The rejected alternatives were fixing it inline, folding it into 6b3c, and recording it only. A fixed-base key generation (edwards25519 `ScalarBaseMult`, cheaper than a variable-base multiplication) is a possible later step, not this plan.
- No new choice is needed. Task 1's implementation choices below are within #130.
- The review (2026-10-17, `objections/connection-session-core-06b3b4-x25519-once.md` + `-dispositions.md`): O1–O5 accepted. O1: the Initiator's memory is recorded (Review Focus 6, `BenchmarkInitiatorHeap`), with no design change. O2: a full `gwloss` rerun (run 30j) in Task 3, and the results measured before #130 are named. O3: the gateways' and slow-auth's cores are read apart. O4: the gateway's garbage profile and the storm's gateway and loadgen profiles are required readings (Task 2 adds the storm's). O5: run 30e, the slow phase's baseline, was slow-only.

**Branch:** `feat/plan6b3`. Execute from its head, this plan's own commit or later.

**Size:** 3 tasks. Task 1 adds about 90 lines of code and 255 of tests and benchmarks in `internal/wire`, and changes about 35 lines of `noise.go`. Task 2 adds 10 lines to `floods.sh` (the storm's profiles). Task 3 is the user's two Tier 2 runs and the record. The prototype `proto-6b3b4` (`566e77d`, from `c4fc979`; the review's dispositions in `4230441`) ran every step. Its files are replayed here byte for byte: `bench_test.go`, `x25519.go` and `x25519_test.go` in full, and `noise.go` and `floods.sh` as `git apply` patches. On the dev machine (AMD EPYC 7742, 2 cores):
- `make test` (race) and `go vet ./...` green, and `make test-load-probe` all passed. `make test-scripts` all passed.
- Five mutations, each caught by a test (Task 1 Step 6).
- `floods` dry run: 30 checks, only a dev-machine swap check failing. The storm's two profiles are written, and in both, `ecdh.(*x25519Curve).NewPrivateKey` is called only from `wire.(*suite).GenerateKeypair`.
- Benchmarks, before and after (`-benchtime 3000x`, mean of 3–5 runs, µs per op unless stated):

  | | before | after |
  |---|---|---|
  | `Respond` (the gateway's work per handshake) | 437 | 218 |
  | `Initiate` (the client's first message) | 276 | 142 |
  | `Finish` (the client's second half) | 141 | 76 |
  | `InitiatorHeap` (live heap one pending handshake keeps, bytes) | 616 | 1,016 |

## Global Constraints

- `make test` (`go test -race -count=1 ./...`) and `go vet ./...` stay green on the dev machine (2 cores, ~1GB, no Docker, no root). `make test-load-probe` passes (it needs the local Redis: `redis-server` on `PATH`, else the pinned one `make test-redis-local` builds).
- **The same bytes on the wire.** The handshake frames, the handshake hash and the session keys are exactly those of `noise.DH25519` given the same randomness. The cipher suite name stays `25519_ChaChaPoly_SHA256`. No protocol, frame size or spec change.
- **No exported signature changes** except `wire.Key`, from an alias to a struct with the same two exported fields. `wire.Suite`, which nothing outside `noise.go` uses, is removed. No Go change outside `internal/wire`. `go.mod` is unchanged: flynn/noise stays at v1.1.0, and `crypto/ecdh` is in the standard library.
- Outside `internal/wire`, only `scripts/tier2/floods.sh` changes: the storm phase's profiles (Task 2, review O4). No other script, scenario check or setting changes.
- No change to the gateway's flags, limits, slot count or bucket. If run 30i shows the limits should change, that is a finding for the user (#111, #116), not something this plan tunes.

## Review Focus

1. **The gateway's event loops share one `Key`.** `KeySet` is read by every gnet event loop at once, so the kept `*ecdh.PrivateKey` is used concurrently. `crypto/ecdh` keys are immutable and safe for concurrent use. Each `suite` is per handshake, never shared. `TestConcurrentRespondsOnOneKey` runs 4 goroutines × 10 handshakes on one `Key` under `-race`.
2. **Old clients and the apps.** Apps already in the field, and any Go client built before this change, still run `DH25519`. A byte-level difference would break every one of them. `TestHandshakeBytesMatchDH25519` seeds both suites with the same ChaCha8 stream. It checks both handshake frames byte for byte, and that frames sealed across old and new sides open.
3. **An attacker's ephemeral.** A garbage handshake can carry any 32 bytes as `e`, a low-order point included. `curve25519.X25519` refuses an all-zero result, and `ecdh.(*PrivateKey).ECDH` does too. `TestDHRefusesWhatX25519Refuses` checks `suite.DH` directly (the all-zero and a 31-byte public key) beside `DH25519`, then a whole `Respond` with an all-zero ephemeral. At the handshake level, a wrong DH result would still fail at the tag. The direct check is what pins the refusal (the prototype's handshake-only test let the mutation through).
4. **A `Key` built from its bytes.** A `wire.Key{Private: …, Public: …}` literal has no parsed key. It must still work, at the old cost of a parse per DH. Today no caller outside tests builds one: the gateway's keys all come through `LoadKeyDir` → `ParsePrivate`. `TestAKeyFromItsBytesAloneStillWorks` checks it.
5. **The key generation's randomness.** Since Go 1.26, `ecdh.X25519().GenerateKey(rng)` ignores `rng` and always uses the system's randomness. `suite.GenerateKeypair` must read its 32 bytes from the rng flynn/noise passes (`Config.Random`, nil meaning `crypto/rand`), or the seeded equivalence test can't hold and `Config.Random` would silently mean nothing. The mutation in Step 6 that swaps in `GenerateKey(rng)` fails both the bytes test and the parse count. `wire.GenerateKey`, the static key's generator, may use `ecdh`'s `GenerateKey`, since it always wants `crypto/rand`.
6. **Memory per pending handshake** (review O1). A pending `Initiator` holds flynn/noise's handshake state, which now holds its `suite`: the parsed ephemeral (`*ecdh.PrivateKey`, its `*PublicKey`, their slices) and the suite's fields. `BenchmarkInitiatorHeap` measures 616 → 1,016 bytes of live heap per Initiator. The suite must live until `Finish`, whose `ee` DH uses the kept ephemeral. A phone holds one at a time. Loadgen holds one per SDK client: the precomputer (#106, `internal/loadgen/precompute.go`, `prepared.in`) prepares each logged-in client's next handshake, and #109 keeps an unanswered one (`held`). At gwloss b_stress's 600,000 clients that is about +240MB of live heap, ~1.5% of its 16GiB `GOMEMLIMIT`, in exchange for half of loadgen's handshake CPU, which bound that storm. Run 30j reads `loadgen_heap` (Task 3).

## Implementation choices this plan makes

1. **A suite per handshake, not a global cache.** flynn/noise passes DH only the private key's bytes, so the kept keys must be findable by bytes. A per-handshake `suite` holds at most two keys (the static one and its own ephemeral), so a lookup is two 32-byte compares. A package-level map from private bytes to keys would hold every ephemeral until evicted. `suite` is its own `noise.CipherSuite` (it embeds `noise.CipherChaChaPoly` and `noise.HashSHA256`, and `Name` returns a package-level slice). `noise.NewCipherSuite` would build the name string again on every handshake.
2. **`subtle.ConstantTimeCompare` for the lookup.** The bytes compared are the handshake's own private keys, not attacker input, so timing leaks nothing either way. The constant-time compare costs nothing next to a multiplication, and a reader of crypto code doesn't have to make that argument.
3. **`newPrivateKey` is a package variable** (`ecdh.X25519().NewPrivateKey`), so `TestAHandshakeParsesOnlyItsEphemeral` can count parses. A parse is the one multiplication this change removes or keeps. The test pins 1 parse in `Initiate` (its ephemeral), 1 in `Respond` (its ephemeral) and 0 in `Finish`. Each side's DHs are then 2 `ECDH` calls, so 3 multiplications per side.
4. **`respond` and `initiate` take the suite and the rng.** `Respond(key, frame, cmd)` is `respond(newSuite(key.priv), key, frame, cmd, nil)`, and `Initiate` likewise with `newSuite(nil)`. The tests run the same code with `noise.DH25519`'s suite and a seeded rng, so the equivalence test compares the two DH functions and nothing else.
5. **Benchmarks first.** `bench_test.go` uses only the exported API, so it runs unchanged before and after. Step 1 commits it alone, and its numbers before the change go into the *Execution Notes*.

## File Structure

- `internal/wire/bench_test.go` (new): `BenchmarkRespond`, `BenchmarkInitiate`, `BenchmarkFinish`, `BenchmarkInitiatorHeap` (Task 1).
- `internal/wire/x25519.go` (new): `suite`, `newSuite`, `newPrivateKey`, `suiteName` (Task 1).
- `internal/wire/x25519_test.go` (new): the equivalence, parse-count, fallback, refusal and concurrency tests (Task 1).
- `internal/wire/noise.go`: `Key`, `newKey`, `GenerateKey`, `ParsePrivate`, `Respond`/`respond`, `Initiate`/`initiate`; `Suite` removed (Task 1).
- `scripts/tier2/floods.sh`: the storm phase profiles gw-0 and loadgen, and `artifacts` checks both profiles (Task 2).
- This plan's *Execution Notes*, the roadmap, Plan 6b3b3's *Execution Notes* (one line), and the README (only if the user asks) (Task 3).

---

### Task 1: `internal/wire`: each X25519 once

**Files:**
- Create: `internal/wire/bench_test.go`, `internal/wire/x25519.go`, `internal/wire/x25519_test.go`
- Modify: `internal/wire/noise.go` (the `Suite` var, `Key`, `GenerateKey`, `ParsePrivate`, `Respond`, `Initiate`)

**Interfaces:**
- Consumes: flynn/noise's `noise.CipherSuite` (= `DHFunc` + `CipherFunc` + `HashFunc` + `Name() []byte`), `noise.Config{CipherSuite, Pattern, Initiator, Random, Prologue, StaticKeypair, PeerStatic}`, `noise.DHKey{Private, Public []byte}`.
- Produces (all in package `wire`):
  - `type Key struct { Private, Public []byte; priv *ecdh.PrivateKey }`, and `func newKey(priv *ecdh.PrivateKey) Key`.
  - `func newSuite(k *ecdh.PrivateKey) *suite`. `*suite` implements `noise.CipherSuite`.
  - `var newPrivateKey = ecdh.X25519().NewPrivateKey`, and `var suiteName = []byte("25519_ChaChaPoly_SHA256")`.
  - `func respond(cs noise.CipherSuite, key Key, frame []byte, cmd uint16, rng io.Reader) (*Session, []byte, error)`.
  - `func initiate(cs noise.CipherSuite, keyID uint16, serverPub []byte, cmd uint16, rng io.Reader) (*Initiator, []byte, error)`.
  - Unchanged: `GenerateKey() (Key, error)`, `ParsePrivate(string) (Key, error)`, `LoadKeyDir`, `EncodeKey`, `Respond`, `Initiate`, `Finish`, `ClientHandshake*`.

- [ ] **Step 1: The benchmarks, and the numbers before**

Create `internal/wire/bench_test.go`:

```go
package wire

import (
	"runtime"
	"testing"
)

// The handshake's cost on each side (#130): BenchmarkRespond is the
// gateway's work per legitimate handshake, BenchmarkInitiate and
// BenchmarkFinish the client's.

func BenchmarkRespond(b *testing.B) {
	k := benchKey(b)
	frames := make([][]byte, b.N)
	for i := range frames {
		_, f, err := Initiate(1, k.Public, cmdHandshake)
		if err != nil {
			b.Fatal(err)
		}
		frames[i] = f
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := Respond(k, frames[i], cmdHandshake); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInitiate(b *testing.B) {
	k := benchKey(b)
	for i := 0; i < b.N; i++ {
		if _, _, err := Initiate(1, k.Public, cmdHandshake); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFinish(b *testing.B) {
	k := benchKey(b)
	ins := make([]*Initiator, b.N)
	replies := make([][]byte, b.N)
	for i := range ins {
		in, f, err := Initiate(1, k.Public, cmdHandshake)
		if err != nil {
			b.Fatal(err)
		}
		if _, replies[i], err = Respond(k, f, cmdHandshake); err != nil {
			b.Fatal(err)
		}
		ins[i] = in
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ins[i].Finish(replies[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInitiatorHeap reports the live heap one pending handshake keeps
// until Finish (#130 review O1): loadgen's precompute holds one Initiator
// per SDK client, 600k in gwloss's b_stress.
func BenchmarkInitiatorHeap(b *testing.B) {
	k := benchKey(b)
	ins := make([]*Initiator, b.N)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range ins {
		in, _, err := Initiate(1, k.Public, cmdHandshake)
		if err != nil {
			b.Fatal(err)
		}
		ins[i] = in
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "heap-B/op")
	runtime.KeepAlive(ins)
}

func benchKey(b *testing.B) Key {
	b.Helper()
	k, err := GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	return k
}
```

Run: `go test ./internal/wire -run '^$' -bench . -benchtime 3000x -count 5`
Expected: on the dev machine, about `Respond` 420–450µs, `Initiate` 270–280µs, `Finish` 135–145µs, and `InitiatorHeap` 616 `heap-B/op` (its ns/op is `Initiate`'s again). Write the means down for the *Execution Notes*. Then commit:

```bash
git add internal/wire/bench_test.go
git commit -m "test(wire): handshake benchmarks, both sides (#130)"
```

- [ ] **Step 2: The failing tests**

Create `internal/wire/x25519_test.go`:

```go
package wire

import (
	"bytes"
	"crypto/ecdh"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/flynn/noise"
)

// dh25519 is flynn/noise's own suite, the one suite replaces (#130).
var dh25519 = noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashSHA256)

func seeded(seed byte) *rand.ChaCha8 { return rand.NewChaCha8([32]byte{seed}) }

// countParses counts newPrivateKey's calls until the test ends.
func countParses(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	orig := newPrivateKey
	newPrivateKey = func(b []byte) (*ecdh.PrivateKey, error) {
		n.Add(1)
		return orig(b)
	}
	t.Cleanup(func() { newPrivateKey = orig })
	return &n
}

func TestSuiteNameIsDH25519s(t *testing.T) {
	if got, want := newSuite(nil).Name(), dh25519.Name(); !bytes.Equal(got, want) {
		t.Fatalf("name %q, want %q", got, want)
	}
}

// The same randomness gives the same bytes on the wire as DH25519, and each
// side completes a handshake with the other's old implementation.
func TestHandshakeBytesMatchDH25519(t *testing.T) {
	k := key(t)
	_, f1, err := initiate(newSuite(nil), 3, k.Public, cmdHandshake, seeded(1))
	if err != nil {
		t.Fatal(err)
	}
	refIn, ref1, err := initiate(dh25519, 3, k.Public, cmdHandshake, seeded(1))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f1, ref1) {
		t.Fatal("the client's handshake frame differs from DH25519's")
	}
	srv, f2, err := respond(newSuite(k.priv), k, f1, cmdHandshake, seeded(2))
	if err != nil {
		t.Fatal(err)
	}
	refSrv, ref2, err := respond(dh25519, k, f1, cmdHandshake, seeded(2))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f2, ref2) {
		t.Fatal("the server's handshake frame differs from DH25519's")
	}
	cli, err := refIn.Finish(f2) // an old client finishes against the new server
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := cli.Seal(nil, 9, 1, []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]*Session{"new server": srv, "old server": refSrv} {
		if _, _, body, _, err := s.Open(nil, sealed); err != nil || string(body) != "hi" {
			t.Fatalf("%s: open %q %v", name, body, err)
		}
	}
	if sealed, err = srv.Seal(nil, 9, 2, []byte("ho")); err != nil {
		t.Fatal(err)
	}
	if _, _, body, _, err := cli.Open(nil, sealed); err != nil || string(body) != "ho" {
		t.Fatalf("old client: open %q %v", body, err)
	}
}

// A handshake parses only the ephemeral it generates: the static key was
// parsed once, and each DH reuses a kept key. So the server does 3 scalar
// multiplications (e, es, ee) and the client 3 (e, es, ee), not 6.
func TestAHandshakeParsesOnlyItsEphemeral(t *testing.T) {
	k := key(t)
	n := countParses(t)
	in, f1, err := Initiate(1, k.Public, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.Swap(0); got != 1 {
		t.Fatalf("Initiate parsed %d keys, want 1", got)
	}
	_, f2, err := Respond(k, f1, cmdHandshake)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.Swap(0); got != 1 {
		t.Fatalf("Respond parsed %d keys, want 1", got)
	}
	if _, err := in.Finish(f2); err != nil {
		t.Fatal(err)
	}
	if got := n.Load(); got != 0 {
		t.Fatalf("Finish parsed %d keys, want 0", got)
	}
}

// A Key built from its bytes alone still answers, parsing the static key per
// handshake.
func TestAKeyFromItsBytesAloneStillWorks(t *testing.T) {
	k := key(t)
	ks := KeySet{1: {Private: k.Private, Public: k.Public}}
	if _, _, err := handshake(t, ks, 1, k.Public, nil); err != nil {
		t.Fatal(err)
	}
}

// DH fails where curve25519.X25519 does: on an all-zero result (a
// low-order public key such as all zeros) and on a public key that isn't 32
// bytes. A handshake with such an ephemeral fails.
func TestDHRefusesWhatX25519Refuses(t *testing.T) {
	k := key(t)
	s := newSuite(k.priv)
	for _, pub := range [][]byte{make([]byte, 32), make([]byte, 31)} {
		if _, err := s.DH(k.Private, pub); err == nil {
			t.Fatalf("DH with a %d-byte public key %x: no error", len(pub), pub)
		}
		if _, err := dh25519.DH(k.Private, pub); err == nil {
			t.Fatalf("DH25519 accepted %x", pub)
		}
	}
	f := make([]byte, HeaderLen+HandshakeLen) // ephemeral all zeros
	Header{Version: Version, Cmd: cmdHandshake, Length: HandshakeLen}.Put(f)
	if _, _, err := Respond(k, f, cmdHandshake); !errors.Is(err, ErrHandshake) {
		t.Fatalf("err %v, want ErrHandshake", err)
	}
}

// The gateway's event loops answer on one Key at once (run with -race).
func TestConcurrentRespondsOnOneKey(t *testing.T) {
	k := key(t)
	ks := KeySet{1: k}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 10 {
				if _, _, err := handshake(t, ks, 1, k.Public, nil); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}
```

- [ ] **Step 3: Run them to see them fail**

Run: `go test ./internal/wire`
Expected: build failure, `undefined: newPrivateKey`, `undefined: newSuite`, `undefined: initiate`, `undefined: respond`, and `k.priv undefined`.

- [ ] **Step 4: The suite**

Create `internal/wire/x25519.go`:

```go
package wire

import (
	"crypto/ecdh"
	"crypto/rand"
	"crypto/subtle"
	"io"

	"github.com/flynn/noise"
)

// newPrivateKey is ecdh's X25519 NewPrivateKey, which computes the public key
// at once: one scalar multiplication. A variable so tests can count calls.
var newPrivateKey = ecdh.X25519().NewPrivateKey

// suiteName is Noise_NK_25519_ChaChaPoly_SHA256's cipher suite name, the
// one flynn/noise's own DH25519 suite has.
var suiteName = []byte("25519_ChaChaPoly_SHA256")

// suite is the cipher suite of one handshake (spec: Client encryption).
// SHA-256 rather than BLAKE2s because the apps' platform crypto (iOS
// CryptoKit, Android) has it natively.
//
// Its DH function is flynn/noise's DH25519 in bytes, but it keeps the
// *ecdh.PrivateKey of the static key and of the ephemeral it generates, so
// each DH is one scalar multiplication. DH25519 calls x/crypto's
// curve25519.X25519, which builds an *ecdh.PrivateKey from the bytes each
// time and so computes a public key nobody reads: two multiplications per DH
// and two per key generation (#130).
type suite struct {
	noise.CipherFunc
	noise.HashFunc
	keys [2]*ecdh.PrivateKey // the static key, then the ephemeral; nil if absent
	raw  [2][]byte           // their private bytes, as flynn/noise passes them back
}

// newSuite returns the suite for one handshake with the static key k (nil
// for none, the client's side of NK). A suite serves one handshake: it
// remembers the one ephemeral it generates.
func newSuite(k *ecdh.PrivateKey) *suite {
	s := &suite{CipherFunc: noise.CipherChaChaPoly, HashFunc: noise.HashSHA256}
	if k != nil {
		s.keys[0], s.raw[0] = k, k.Bytes()
	}
	return s
}

func (s *suite) Name() []byte   { return suiteName }
func (s *suite) DHLen() int     { return 32 }
func (s *suite) DHName() string { return "25519" }

// GenerateKeypair reads the private key from rng as DH25519 does, so a given
// rng gives the same key pair, and keeps it for DH.
func (s *suite) GenerateKeypair(rng io.Reader) (noise.DHKey, error) {
	if rng == nil {
		rng = rand.Reader
	}
	b := make([]byte, 32)
	if _, err := io.ReadFull(rng, b); err != nil {
		return noise.DHKey{}, err
	}
	k, err := newPrivateKey(b)
	if err != nil {
		return noise.DHKey{}, err
	}
	s.keys[1], s.raw[1] = k, b
	return noise.DHKey{Private: b, Public: k.PublicKey().Bytes()}, nil
}

// DH uses the kept key whose bytes are priv, else parses priv. Like
// curve25519.X25519 it fails on a public key that isn't 32 bytes and on an
// all-zero result (a low-order point).
func (s *suite) DH(priv, pub []byte) ([]byte, error) {
	var k *ecdh.PrivateKey
	for i, r := range s.raw {
		if s.keys[i] != nil && subtle.ConstantTimeCompare(r, priv) == 1 {
			k = s.keys[i]
			break
		}
	}
	if k == nil {
		var err error
		if k, err = newPrivateKey(priv); err != nil {
			return nil, err
		}
	}
	p, err := ecdh.X25519().NewPublicKey(pub)
	if err != nil {
		return nil, err
	}
	return k.ECDH(p)
}
```

- [ ] **Step 5: `noise.go` uses it**

Apply from the repository root with `git apply` (the patch below, saved to a file in the session scratchpad):

```diff
diff --git a/internal/wire/noise.go b/internal/wire/noise.go
index dd3c342..dc33fad 100644
--- a/internal/wire/noise.go
+++ b/internal/wire/noise.go
@@ -17,11 +17,6 @@ import (
 	"github.com/flynn/noise"
 )
 
-// Suite is Noise_NK_25519_ChaChaPoly_SHA256 (spec: Client encryption).
-// SHA-256 rather than BLAKE2s because the apps' platform crypto (iOS
-// CryptoKit, Android) has it natively.
-var Suite = noise.NewCipherSuite(noise.DH25519, noise.CipherChaChaPoly, noise.HashSHA256)
-
 // Handshake sizes. NK message 1 is e, es and an empty payload's tag; message
 // 2 is e, ee and an empty payload's tag. Handshake messages carry no
 // application payload (R4G-O6), so both sizes are exact.
@@ -43,11 +38,26 @@ var (
 	ErrClosedBeforeReply = errors.New("wire: closed before the handshake reply")
 )
 
-// Key is a server static key: X25519, 32 bytes each.
-type Key = noise.DHKey
+// Key is a server static key: X25519, 32 bytes each. It keeps the parsed
+// private key, so a handshake doesn't parse it again (#130); a Key built
+// from its bytes alone still works, parsed on each use.
+type Key struct {
+	Private, Public []byte
+	priv            *ecdh.PrivateKey
+}
+
+func newKey(priv *ecdh.PrivateKey) Key {
+	return Key{Private: priv.Bytes(), Public: priv.PublicKey().Bytes(), priv: priv}
+}
 
 // GenerateKey returns a new server static key.
-func GenerateKey() (Key, error) { return Suite.GenerateKeypair(rand.Reader) }
+func GenerateKey() (Key, error) {
+	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
+	if err != nil {
+		return Key{}, err
+	}
+	return newKey(priv), nil
+}
 
 // ParsePrivate builds a Key from a base64 (standard encoding) private key.
 func ParsePrivate(s string) (Key, error) {
@@ -59,7 +69,7 @@ func ParsePrivate(s string) (Key, error) {
 	if err != nil {
 		return Key{}, fmt.Errorf("wire: private key: %w", err)
 	}
-	return Key{Private: priv.Bytes(), Public: priv.PublicKey().Bytes()}, nil
+	return newKey(priv), nil
 }
 
 // EncodeKey returns k's private (for the Secret) and public (pinned in the
@@ -127,11 +137,17 @@ type Session struct {
 // frame, with the static key its key_id selected. It returns the session and
 // the server's handshake frame (Cmd = cmd, payload = the raw Noise message).
 func Respond(key Key, frame []byte, cmd uint16) (*Session, []byte, error) {
+	return respond(newSuite(key.priv), key, frame, cmd, nil)
+}
+
+// respond is Respond with cipher suite cs and the ephemeral key read from rng
+// (nil: crypto/rand).
+func respond(cs noise.CipherSuite, key Key, frame []byte, cmd uint16, rng io.Reader) (*Session, []byte, error) {
 	if len(frame) != HeaderLen+HandshakeLen {
 		return nil, nil, ErrHandshake
 	}
-	hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: Suite, Pattern: noise.HandshakeNK,
-		Prologue: prologue(frame), StaticKeypair: key})
+	hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: cs, Pattern: noise.HandshakeNK,
+		Random: rng, Prologue: prologue(frame), StaticKeypair: noise.DHKey{Private: key.Private, Public: key.Public}})
 	if err != nil {
 		return nil, nil, err
 	}
@@ -160,11 +176,17 @@ type Initiator struct {
 // Initiate starts the client side: it returns the handshake frame to send,
 // for the server key pinned under keyID.
 func Initiate(keyID uint16, serverPub []byte, cmd uint16) (*Initiator, []byte, error) {
+	return initiate(newSuite(nil), keyID, serverPub, cmd, nil)
+}
+
+// initiate is Initiate with cipher suite cs and the ephemeral key read from
+// rng (nil: crypto/rand).
+func initiate(cs noise.CipherSuite, keyID uint16, serverPub []byte, cmd uint16, rng io.Reader) (*Initiator, []byte, error) {
 	frame := make([]byte, HeaderLen+KeyIDLen, HeaderLen+HandshakeLen)
 	Header{Version: Version, Cmd: cmd, Length: HandshakeLen}.Put(frame)
 	binary.BigEndian.PutUint16(frame[HeaderLen:], keyID)
-	hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: Suite, Pattern: noise.HandshakeNK,
-		Initiator: true, Prologue: prologue(frame), PeerStatic: serverPub})
+	hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: cs, Pattern: noise.HandshakeNK,
+		Initiator: true, Random: rng, Prologue: prologue(frame), PeerStatic: serverPub})
 	if err != nil {
 		return nil, nil, err
 	}
```

Run: `gofmt -l internal/wire && go vet ./... && go test -race -count=1 ./internal/wire`
Expected: no gofmt output, vet clean, `ok`.

- [ ] **Step 6: Mutations**

Make each change to `x25519.go` in turn, run `go test -count=1 ./internal/wire`, check the named test fails, and restore the file (keep a copy in the scratchpad and copy it back; don't use `git checkout` on a tree with uncommitted work):

| Mutation | Fails |
|---|---|
| `if false && s.keys[i] != nil && subtle…` (no lookup) | `TestAHandshakeParsesOnlyItsEphemeral` |
| `GenerateKeypair` with `ecdh.X25519().GenerateKey(rng)` and `b = k.Bytes()` in place of reading `rng` and `newPrivateKey(b)` | `TestHandshakeBytesMatchDH25519`, `TestAHandshakeParsesOnlyItsEphemeral` |
| `Name` returns `[]byte("25519_ChaChaPoly_BLAKE2s")` | `TestSuiteNameIsDH25519s`, `TestHandshakeBytesMatchDH25519` |
| delete `s.keys[0], s.raw[0] = k, k.Bytes()` in `newSuite` | `TestAHandshakeParsesOnlyItsEphemeral` |
| `DH` returns 32 zero bytes, no error, when `ECDH` fails | `TestDHRefusesWhatX25519Refuses` |

Then `go test -count=1 ./internal/wire` passes again.

- [ ] **Step 7: The benchmarks after, the whole suite, the load probe**

Run: `go test ./internal/wire -run '^$' -bench . -benchtime 3000x -count 5`
Expected: each about half of Step 1's numbers: on the dev machine `Respond` ~218µs, `Initiate` ~142µs, `Finish` ~76µs. `InitiatorHeap` rises to ~1,016 `heap-B/op` (Review Focus 6).

Run: `make test && go vet ./...`
Expected: every package `ok`.

Run: `make test-load-probe`
Expected: last line `load-probe: all passed`. It runs real gateways, Logic, light and SDK clients through the new handshake on both sides.

- [ ] **Step 8: Commit**

```bash
git add internal/wire/noise.go internal/wire/x25519.go internal/wire/x25519_test.go
git commit -m "feat(wire): compute each X25519 once: a DH25519 that keeps its *ecdh.PrivateKeys (#130)"
```

---

### Task 2: `floods`: profile the storm

The garbage phase already profiles every gateway and the flood (`stack_profile garbage 30`, `lg_profile garbage 30`, checked by `artifacts`), so run 30i's gateway profile needs no change. The storm phase has none, and the storm is where loadgen's handshakes are the bound (`storm_rate`'s `loadgen-bound`). This task adds a profile of gw-0 and of loadgen over 8s of the storm's ramp (review O4). The ramp is short (200,000 clients at ~16,000/s, ~12.5s on the host; 2s in the dry run), so the window is 8s from 1s in, not the garbage phase's 30s from 10s in. The phase waits for the profiles before it ends, as the garbage phase does.

**Files:**
- Modify: `scripts/tier2/floods.sh` (the header's `artifacts` line, `phase_storm`)

**Interfaces:**
- Consumes: `stack_profile NAME SECONDS` (`scripts/stack.sh`: every stack process to `artifacts/NAME/<proc>.cpu.pb.gz`), `lg_profile NAME SECONDS` (`scripts/loadgen.sh`: the main instance to `artifacts/NAME/loadgen.cpu.pb.gz`), the global `prof` that `t2_on_exit` kills (`floods.sh`), `missing` (the `artifacts` check).
- Produces: `artifacts/storm/gw-0.cpu.pb.gz`, `artifacts/storm/loadgen.cpu.pb.gz` (and `logic.cpu.pb.gz`) in the storm phase's directory; `artifacts` fails naming `storm:storm/<f>.cpu.pb.gz` if either of the first two is missing or not gzip.

- [ ] **Step 1: The patch**

Apply from the repository root with `git apply` (the patch below, saved to a file in the session scratchpad):

```diff
diff --git a/scripts/tier2/floods.sh b/scripts/tier2/floods.sh
index 7261053..591a0df 100755
--- a/scripts/tier2/floods.sh
+++ b/scripts/tier2/floods.sh
@@ -173,7 +173,8 @@
 #   <phase>_processes     nothing died, panicked or stopped uncleanly
 #   artifacts             metrics before and after each phase's window, the
 #                         slow phase's series (slow-series.jsonl), the
-#                         garbage flood's CPU profiles, loadgen's results
+#                         storm's and the garbage flood's CPU profiles,
+#                         loadgen's results
 # Each phase also records its CPU sets' shares and softnet drops over its
 # window (cpu_sets: run 30's garbage SYN loss had none snapshotted), and
 # conntrack's own drops (conntrack_stat: insert_failed, drop, early_drop,
@@ -658,12 +659,18 @@ phase_storm() {
   stack_usage_start loadgen="$LG_PID"
   hs_sampler gw-0 "$STACK_DIR/artifacts/handshakes-gw-0.tsv" "$LG_PID" &
   smp=$!
+  # The gateway's and loadgen's CPU over 8s of the ramp, from 1s in: X25519's
+  # share on both sides of the handshake (#130).
+  (sleep 1; stack_profile storm 8 & lg_profile storm 8 & wait) >/dev/null 2>&1 &
+  prof=$!
   online=$(t2_bool lg_wait "$(settled "$n")" $((n / rate * 3 + 60)))
   kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
   usage=$(stack_usage_json)
   t2_cpustat_snap "$STACK_DIR/cpustat-1"
   ct_stat_snap "$STACK_DIR/ctstat-1"
   s1=$(gws_counters)
+  wait "$prof"
+  prof=""
   t2_record_end storm
   stack_snapshot after
   d=$(gws_delta "$s0" "$s1")
@@ -683,6 +690,9 @@ phase_storm() {
     "\"gw-0\": $(echo "$d" | lg_py - '{k: d["gw-0"][k] for k in ("hs_ok", "hs_no_token", "hs_no_slot", "rejects_handshake_rate", "logins_busy", "rejects_pending_mem", "rejects_at_capacity")}'), \"no_slot_share\": $(echo "$d" | lg_py - 'round(d["gw-0"]["hs_no_slot"] / (d["gw-0"]["hs_ok"] + d["gw-0"]["hs_no_slot"]), 4)'), \"failed\": $(lg_get /stats 'd["failed"]')"
   t2_field storm "{\"clients\": $n, \"rate\": $rate, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"handshakes\": $hs, \"gateways\": $d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
   down storm "$n"
+  for f in gw-0 loadgen; do
+    gzip -t "$STACK_DIR/artifacts/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" storm:storm/$f.cpu.pb.gz"
+  done
 }
 
 # garbage: #121 (1), garbage handshakes beside a Pinging population.
```

Run: `bash -n scripts/tier2/floods.sh && make test-scripts`
Expected: no syntax error; `lib_test`, `floods_test`, `loadgen_test` and `stack_test` print `all passed`.

- [ ] **Step 2: Dry run on the dev machine**

The throwaway copy and settings of Plan 6b3b3's Task 1 Step 3 (the guard lowered, the root parts removed, the scale cut, `FLOODS_RATE=500`), in the session scratchpad (`$S`):

```bash
D=$S/dry; mkdir -p "$D"; cp scripts/tier2/floods.sh "$D/floods.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget [0-9]*$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3 ss$|t2_need curl taskset python3 ss|' "$D/floods.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/floods.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/floods.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/floods.sh"
rm -rf tier2-results/floods-*
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 \
  FLOODS_STORM_CLIENTS=2000 FLOODS_STORM_RATE=1000 FLOODS_CLIENTS=2000 FLOODS_RATE=500 \
  FLOODS_GARBAGE_RATE=2000 FLOODS_GARBAGE_INFLIGHT=500 FLOODS_GARBAGE_SECONDS=45 FLOODS_GARBAGE_BUCKET=500 \
  FLOODS_SLOW_HANDSHAKE=500 FLOODS_SLOW_AUTH=500 FLOODS_SLOW_RATE=500 FLOODS_SLOW_HOLD=35 \
  bash "$D/floods.sh" >"$D/out.txt" 2>&1; echo "exit $?"
```

The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype `4230441`, ~2 min): `result.json` has **30** checks. All pass except `no_swap_in_*` checks failing from the dev machine's own swapping (the prototype: only `no_swap_in_garbage`). `artifacts` passes. `storm/artifacts/storm/` holds `gw-0.cpu.pb.gz`, `loadgen.cpu.pb.gz` and `logic.cpu.pb.gz`. Then read them as Task 3 will:

```bash
R=$(ls -d tier2-results/floods-*/ | head -1)
for f in gw-0 loadgen; do go tool pprof -top -nodecount=200 "$R/storm/artifacts/storm/$f.cpu.pb.gz" | grep -E 'Duration|x25519ScalarMult|NewPrivateKey|suite\)\.DH|GenerateKeypair'; done
go tool pprof -peek 'NewPrivateKey$' "$R/storm/artifacts/storm/gw-0.cpu.pb.gz"
```

Expected: `Duration: 8s` in both. `crypto/ecdh.x25519ScalarMult` is the largest cumulative crypto entry (the prototype: 54% of gw-0's samples, 43% of loadgen's). `-peek` shows `wire.(*suite).GenerateKeypair` as `NewPrivateKey`'s only caller: no parse under `wire.(*suite).DH`.

Afterwards, remove `tier2-results/floods-*` and `$D`, and check that no process is left (`pgrep -af "scratchpad/dry" | grep -v pgrep || echo none`).

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/floods.sh
git commit -m "feat(tier2): floods: profile gw-0 and loadgen over the storm's ramp (#130)"
```

---

### Task 3: Tier 2 runs 30i (`floods`) and 30j (`gwloss`), and the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (plan 6 row, #130), Plan 6b3b3's *Execution Notes* (one line pointing here). The README only if the user asks.

- [ ] **Step 1: Tier 2 run 30i, `floods` (the user, on the host, ~12 min)**

After `git fetch && git switch feat/plan6b3 && git pull`, with the desktop apps closed:
1. If the host rebooted since run 30h, rerun `sudo scripts/tier2/host-setup.sh` first.
2. `sudo dmesg -C`.
3. Run `GOIM_TEST_TIER=2 FLOODS_RATE=7500 make test-tier2 SCENARIO=floods`. That is all three phases, with the slow phase's light clients at 7,500/s and slow-auth on the gateways' set (the default), run 30e's settings. Run 30e ran the slow phase alone (`FLOODS_PHASES=slow`, #129). Here it follows storm and garbage on the same stack, which the phase boundaries separate (P6a2bI-3, review O5).
4. Send back the `.report.txt`, `dmesg | grep -i conntrack`, and from the run's directory (`R=tier2-results/floods-<timestamp>`) the profiles' readings (review O4):

```bash
for f in storm/artifacts/storm/gw-0 storm/artifacts/storm/loadgen garbage/artifacts/garbage/gw-0; do echo "== $f"; go tool pprof -top -nodecount=200 "$R/$f.cpu.pb.gz" | grep -E 'Duration|Total samples|x25519ScalarMult|NewPrivateKey|suite\)\.DH|GenerateKeypair|feMul'; done
go tool pprof -peek 'NewPrivateKey$' "$R/garbage/artifacts/garbage/gw-0.cpu.pb.gz"
```

There are 30 checks. The comparisons, phase by phase:
- **Storm** (against run 30c): `storm_rate`'s `no_slot_share` (0.27) should fall, since a slot is held for half as long. `new_per_s` (15,962, loadgen-bound) may rise, since loadgen's own handshakes cost half. The verdict may stay `loadgen-bound`: loadgen still runs 200k light clients on 8 CPUs.
- **Garbage** (against run 30c): a garbage handshake costs 1 multiplication, not 2. The slots bound the flood at ~21,600 DH/s per gateway in run 30c. They can now pass about twice as many, so `garbage_limited`'s `bound_by` may move from the slots to the bucket (25,000/s). The 400,000 Pinging clients' checks should pass as before.
- **Slow at 7,500/s** (against run 30e, which failed only on verdict `gateway-set-saturated`): the gateway set's busy share falls for two reasons, the gateways' `Respond` and slow-auth's `Initiate`/`Finish`, since slow-auth completes a real handshake per connection on the same CPUs (review O3). The gateways' own cores are the measure of #130's gain there. The concurrency limit's refused share (51% in 30e) should fall. A `valid` verdict with `slow_legit` and `slow_heartbeats` passing is the expected result. `loadgen-saturated` is still a valid record: slow-auth's handshakes got cheaper, but light clients' retries may not drop enough. The order in `first_s` rules on it as before.

Any outcome goes to the user before anything changes.

What to read, for the *Execution Notes*:
- `storm_rate` (`no_slot_share`, `new_per_s`, verdict), and gw-0's and loadgen's cores in the storm window (`storm.usage`), against run 30c.
- `garbage_limited` (`bound_by` per gateway, DH/s), the gateways' cores in the garbage window (`garbage.usage`), and the heartbeat checks, against run 30c.
- `slow.verdict`, `slow.window.ramp`/`.hold` busy per set, and **apart**: the gateways' cores (`slow.usage["gw-0"]`, `["gw-1"]`) and slow-auth's (`slow.usage["flood2"]`; `flood` is slow-handshake), over the light ramp and the hold, against run 30e (review O3). Then `slow_legit` (online, gave up, `hs_no_slot`, login p99), `slow_heartbeats`, `gw_runq_ms_per_s`, against run 30e (the table in Plan 6b3b3's *Execution Notes*), with the caveat that run 30e ran the slow phase alone (review O5).
- The profiles (required): `x25519ScalarMult`'s share of gw-0 in the garbage phase against run 30c's 67.7%, and of gw-0 and loadgen in the storm; `-peek` showing `wire.(*suite).GenerateKeypair` as `NewPrivateKey`'s only caller (none under `wire.(*suite).DH`). A `NewPrivateKey` under `DH` means the deployed binary isn't parsing once: stop and report.

- [ ] **Step 2: Tier 2 run 30j, the full `gwloss` (the user, on the host, ~32 min, up to ~50)**

After run 30i, same host preparation (`sudo dmesg -C`, desktop apps closed), run `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss`: the default phases `b1 b2 b_stress` and settings (b_stress's loadgen `GOMEMLIMIT` 16GiB). Send back the `.report.txt`, and from the run's directory (`R=tier2-results/gwloss-<timestamp>`) b_stress's storm profiles:

```bash
for f in b_stress/artifacts/storm/loadgen b_stress/artifacts/storm/gw-1; do echo "== $f"; go tool pprof -top -nodecount=200 "$R/$f.cpu.pb.gz" | grep -E 'Duration|Total samples|x25519ScalarMult|NewPrivateKey|suite\)\.DH|GenerateKeypair'; done
```

This is review O2. b_stress's storm was bound by loadgen's handshakes (X25519 43–47% of loadgen, 12–13.6 of 14 cores), and the README's 600k SIGKILL row (179s) is labelled an upper bound for that reason. #110's loadgen-bound ruling and the handshake-concurrency-4 trial (594s, why the default slot count stays) were measured at the old cost too. The last full pass, 29/29 at `8981580` and the sampler rerun `271c022`, predates b2. b2 was last run alone (run 28, 19/19).

What to read, for the *Execution Notes*:
- b1: back times and `b1_slo`, against runs 21 and 28's b1.
- b2: its checks against run 28.
- b_stress: `back_s` (179–188s before), `storm_first30` and `loadgen_bound` (loadgen's cores in the storm's first 30s), `b_stress_storm_real` (#110: does it still fail as loadgen-bound?), the survivors' refused handshakes (~3.9M before), `loadgen_heap` at the ramp and at the kill against the last 16GiB runs (Review Focus 6: about +240MB expected), and the profiles: `x25519ScalarMult`'s share of loadgen against 43–47%.

Any outcome goes to the user before anything changes. Whether the handshake-concurrency trial is repeated at the new cost is the user's call after this run (#111, #116), not part of this plan.

- [ ] **Step 3: Record**

- This plan's *Execution Notes*: Task 1's benchmark numbers (Step 1 and Step 7, `InitiatorHeap` included), run 30i's and run 30j's numbers above, each with the commit hash and `dirty` from `result.json`.
- Roadmap: #130 gets one line with the benchmarks and runs 30i and 30j. The plan 6 row says "6b3b4 executed, runs 30i, 30j …; next 6b3c (fresh session)", or the follow-up the runs call for.
- Plan 6b3b3's *Execution Notes*: one line under the profile section, "Fixed by Plan 6b3b4; run 30i: see there".
- The README's floods row quotes runs 30c–30h, and its 600k SIGKILL row the gwloss runs before 30j, all measured before this change. Whether runs 30i and 30j replace or join those numbers is the user's call. Ask, and change the rows (both languages) only if they say so.

```bash
git add docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-17-connection-session-core-06b3b3-slow-phase.md docs/superpowers/plans/2026-10-17-connection-session-core-06b3b4-x25519-once.md
git commit -m "docs: Plan 6b3b4 benchmarks and Tier 2 runs 30i, 30j recorded (#130)"
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **6b3c** (`POST /reset`, `POST /network-change`, scenario `reconnect`, run 31), with its inputs P6b3bI-2/-3 and P6b3b3I-4.
- **Results measured before #130** (review O2): runs 30–30h (floods), the gwloss runs before 30j (b_stress's 179–188s and #110's loadgen-bound ruling), and the handshake-concurrency-4 trial (`d218483`, 594s), on which the default slot count rests. Runs 30i and 30j replace the first two where the user says so. Whether to repeat the concurrency trial is the user's call after run 30j (input to #111 and #116).
- **#111 (parked):** run 30i's `no_slot` shares are its input at the new slot hold. The spec's ~100µs NK estimate (*Admission control*) is now nearer the measured hold, and the measured number belongs with #97, #101 and #125 for the next spec revision.
- **Fixed-base key generation** (#130's possible later step): `GenerateKeypair`'s one remaining basepoint multiplication could use edwards25519's precomputed-table `ScalarBaseMult`, several times cheaper. Only if run 30i's profiles (Task 3) still show key generation high.

## Execution Notes
