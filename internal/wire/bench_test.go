package wire

import "testing"

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

func benchKey(b *testing.B) Key {
	b.Helper()
	k, err := GenerateKey()
	if err != nil {
		b.Fatal(err)
	}
	return k
}
