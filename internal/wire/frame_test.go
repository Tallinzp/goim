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
