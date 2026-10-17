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

// BenchmarkSessionHeap reports the live heap one server Session keeps for its
// connection's life (final review I1).
func BenchmarkSessionHeap(b *testing.B) {
	k := benchKey(b)
	frames := make([][]byte, b.N)
	for i := range frames {
		_, f, err := Initiate(1, k.Public, cmdHandshake)
		if err != nil {
			b.Fatal(err)
		}
		frames[i] = f
	}
	ss := make([]*Session, b.N)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range ss {
		s, _, err := Respond(k, frames[i], cmdHandshake)
		if err != nil {
			b.Fatal(err)
		}
		ss[i] = s
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "heap-B/op")
	runtime.KeepAlive(ss)
}
