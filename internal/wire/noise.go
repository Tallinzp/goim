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
