package sdk

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// State is what the SDK persists: device_id and conn_seq together, so
// clearing one clears the other (R1-O5), and the windows the server set
// (R5S-O6).
type State struct {
	DeviceID string        `json:"device_id"`
	ConnSeq  uint64        `json:"conn_seq"`
	Windows  ServerWindows `json:"windows"`
}

// Store persists State. Save must be durable when it returns: conn_seq is
// saved before every connection attempt.
type Store interface {
	// Load returns the zero State for a store never saved, and an error
	// matching ErrCorrupt for a record that can't be parsed. Any other error
	// means the record can't be read yet (an iOS Keychain before the first
	// unlock): the client keeps it and tries again.
	Load() (State, error)
	Save(State) error
}

// ErrCorrupt is a stored record that can't be parsed: the client starts as a
// fresh install (Review Focus 1).
var ErrCorrupt = errors.New("sdk: stored state is corrupt")

// Locker is a Store that can be locked: New takes the lock, so at most one
// Client uses a record, and so at most one connection per device_id is in
// use (spec: device_id). Close releases it.
type Locker interface {
	Lock() (unlock func(), err error)
}

// ErrStoreInUse is a store another Client holds.
var ErrStoreInUse = errors.New("sdk: store in use by another client")

// NewDeviceID returns a random device_id (16 bytes, hex).
func NewDeviceID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// FileStore keeps State in one JSON file, the place an app keeps device_id
// (on iOS a native SDK would use the Keychain for both, and its own lock
// across the app and its extensions).
type FileStore struct{ Path string }

// Load reads the record. A file that can't be parsed is moved aside to
// Path + ".corrupt", kept for diagnosis, and reported as ErrCorrupt.
func (f FileStore) Load() (State, error) {
	b, err := os.ReadFile(f.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, err
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		if rerr := os.Rename(f.Path, f.Path+".corrupt"); rerr != nil {
			return State{}, rerr // not moved aside: keep it, and try again
		}
		return State{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return st, nil
}

// Lock takes an exclusive flock on Path + ".lock", held until unlock.
func (f FileStore) Lock() (func(), error) {
	l, err := os.OpenFile(f.Path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(l.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		l.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrStoreInUse
		}
		return nil, err
	}
	return func() { l.Close() }, nil
}

// Save writes a temporary file, syncs it and renames it over the old one,
// so a crash leaves either the old State or the new one.
func (f FileStore) Save(st State) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := f.Path + ".tmp"
	w, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		w.Close()
		return err
	}
	if err := w.Sync(); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.Path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(f.Path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// MemStore keeps State in memory (tests, loadgen). Err, if set, fails every Save.
type MemStore struct {
	mu  sync.Mutex
	st  State
	Err error
}

func (m *MemStore) Load() (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st, nil
}

func (m *MemStore) Save(st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.st = st
	return nil
}
