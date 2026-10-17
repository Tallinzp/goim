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
	"syscall"

	"github.com/flynn/noise"
)

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
	// ErrClosedBeforeReply: the server closed or reset the connection before
	// any byte of its handshake reply, which the SDK treats as a
	// pre-handshake close (spec: Client SDK rules).
	ErrClosedBeforeReply = errors.New("wire: closed before the handshake reply")
)

// Key is a server static key: X25519, 32 bytes each. It keeps the parsed
// private key, so a handshake doesn't parse it again (#130); a Key built
// from its bytes alone still works, parsed on each use.
type Key struct {
	Private, Public []byte
	priv            *ecdh.PrivateKey
}

func newKey(priv *ecdh.PrivateKey) Key {
	return Key{Private: priv.Bytes(), Public: priv.PublicKey().Bytes(), priv: priv}
}

// GenerateKey returns a new server static key.
func GenerateKey() (Key, error) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return Key{}, err
	}
	return newKey(priv), nil
}

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
	return newKey(priv), nil
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
		if _, dup := ks[uint16(id)]; dup {
			return nil, fmt.Errorf("wire: two key files for key_id %d in %s", id, dir)
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
// with its own nonce counter. One goroutine may Seal or WriteFrame while
// another Opens or ReadFrames; neither side is safe for two goroutines.
type Session struct {
	send, recv *noise.CipherState
}

// Respond runs the server side of the handshake on the client's handshake
// frame, with the static key its key_id selected. It returns the session and
// the server's handshake frame (Cmd = cmd, payload = the raw Noise message).
func Respond(key Key, frame []byte, cmd uint16) (*Session, []byte, error) {
	return respond(newSuite(key.priv), key, frame, cmd, nil)
}

// respond is Respond with cipher suite cs and the ephemeral key read from rng
// (nil: crypto/rand).
func respond(cs noise.CipherSuite, key Key, frame []byte, cmd uint16, rng io.Reader) (*Session, []byte, error) {
	if len(frame) != HeaderLen+HandshakeLen {
		return nil, nil, ErrHandshake
	}
	hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: cs, Pattern: noise.HandshakeNK,
		Random: rng, Prologue: prologue(frame), StaticKeypair: noise.DHKey{Private: key.Private, Public: key.Public}})
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
	forget(cs)
	if err != nil {
		return nil, nil, err
	}
	Header{Version: Version, Cmd: cmd, Length: HandshakeMsgLen}.Put(out)
	return &Session{send: c2, recv: c1}, out, nil
}

// Initiator is the client side of a handshake in progress.
type Initiator struct {
	hs  *noise.HandshakeState
	cs  noise.CipherSuite
	cmd uint16
}

// Initiate starts the client side: it returns the handshake frame to send,
// for the server key pinned under keyID.
func Initiate(keyID uint16, serverPub []byte, cmd uint16) (*Initiator, []byte, error) {
	return initiate(newSuite(nil), keyID, serverPub, cmd, nil)
}

// initiate is Initiate with cipher suite cs and the ephemeral key read from
// rng (nil: crypto/rand).
func initiate(cs noise.CipherSuite, keyID uint16, serverPub []byte, cmd uint16, rng io.Reader) (*Initiator, []byte, error) {
	frame := make([]byte, HeaderLen+KeyIDLen, HeaderLen+HandshakeLen)
	Header{Version: Version, Cmd: cmd, Length: HandshakeLen}.Put(frame)
	binary.BigEndian.PutUint16(frame[HeaderLen:], keyID)
	hs, err := noise.NewHandshakeState(noise.Config{CipherSuite: cs, Pattern: noise.HandshakeNK,
		Initiator: true, Random: rng, Prologue: prologue(frame), PeerStatic: serverPub})
	if err != nil {
		return nil, nil, err
	}
	frame, _, _, err = hs.WriteMessage(frame, nil)
	if err != nil {
		return nil, nil, err
	}
	return &Initiator{hs: hs, cs: cs, cmd: cmd}, frame, nil
}

// Finish completes the client side with the server's handshake frame. Its
// header is not authenticated, so every field is checked exactly.
func (i *Initiator) Finish(frame []byte) (*Session, error) {
	if len(frame) != HeaderLen+HandshakeMsgLen ||
		ParseHeader(frame) != (Header{Version: Version, Cmd: i.cmd, Length: HandshakeMsgLen}) {
		return nil, ErrHandshake
	}
	payload, c1, c2, err := i.hs.ReadMessage(nil, frame[HeaderLen:])
	forget(i.cs)
	switch {
	case err != nil:
		return nil, fmt.Errorf("%w: %v", ErrHandshake, err)
	case len(payload) != 0:
		return nil, ErrPayload
	}
	return &Session{send: c1, recv: c2}, nil
}

// forget drops a suite's kept keys after the handshake's last DH: the
// Session keeps the suite (final review I1).
func forget(cs noise.CipherSuite) {
	if s, ok := cs.(*suite); ok {
		s.forget()
	}
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
	return ClientHandshakeWith(rw, in, f)
}

// ClientHandshakeWith runs the client side over rw from Initiate's result,
// which may have been made ahead of time (loadgen's precomputed first
// message, #106). An Initiator serves one handshake.
func ClientHandshakeWith(rw io.ReadWriter, in *Initiator, frame []byte) (*Session, error) {
	if _, err := rw.Write(frame); err != nil {
		return nil, err
	}
	reply := make([]byte, HeaderLen+HandshakeMsgLen)
	if n, err := io.ReadFull(rw, reply); err != nil {
		if n == 0 && (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
			return nil, fmt.Errorf("%w: %w", ErrClosedBeforeReply, err)
		}
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
	var hb [HeaderLen]byte
	if _, err := io.ReadFull(r, hb[:]); err != nil {
		return 0, 0, nil, err
	}
	h := ParseHeader(hb[:])
	if err := h.Check(); err != nil {
		return 0, 0, nil, err
	}
	f := make([]byte, HeaderLen+int(h.Length)) // this frame's size, not the largest
	copy(f, hb[:])
	if _, err := io.ReadFull(r, f[HeaderLen:]); err != nil {
		return 0, 0, nil, err
	}
	cmd, reqID, body, _, err = s.Open(nil, f)
	return cmd, reqID, body, err
}
