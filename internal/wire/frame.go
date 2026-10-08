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
