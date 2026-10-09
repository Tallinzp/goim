package sdk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStoreRoundTrip(t *testing.T) {
	f := FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
	if st, err := f.Load(); err != nil || st != (State{}) {
		t.Fatalf("Load of a fresh install = %+v, %v; want the zero State", st, err)
	}
	want := State{DeviceID: NewDeviceID(), ConnSeq: 42,
		Windows: ServerWindows{Storm: Window{0, 30 * s}, Retryable: Window{s, 5 * s}}}
	if err := f.Save(want); err != nil {
		t.Fatal(err)
	}
	if got, err := f.Load(); err != nil || got != want {
		t.Fatalf("Load = %+v, %v; want %+v", got, err, want)
	}
	if _, err := os.Stat(f.Path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind: %v", err)
	}
}

// A record that can't be parsed is ErrCorrupt and moved aside; the next Load
// sees a fresh install.
func TestFileStoreMovesACorruptFileAside(t *testing.T) {
	f := FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
	os.WriteFile(f.Path, []byte("{not json"), 0o600)
	if _, err := f.Load(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Load of a corrupt file = %v, want ErrCorrupt", err)
	}
	if b, err := os.ReadFile(f.Path + ".corrupt"); err != nil || string(b) != "{not json" {
		t.Fatalf("corrupt file not kept: %q, %v", b, err)
	}
	if st, err := f.Load(); err != nil || st != (State{}) {
		t.Fatalf("Load after the corrupt file = %+v, %v; want a fresh install", st, err)
	}
}

// A file that can't be read is not corrupt: it is kept as it is.
func TestFileStoreUnreadableIsNotCorrupt(t *testing.T) {
	f := FileStore{Path: t.TempDir()} // a directory: read fails
	if _, err := f.Load(); err == nil || errors.Is(err, ErrCorrupt) {
		t.Fatalf("Load = %v, want an error that is not ErrCorrupt", err)
	}
}

func TestFileStoreLockIsExclusive(t *testing.T) {
	f := FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
	unlock, err := f.Lock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Lock(); !errors.Is(err, ErrStoreInUse) {
		t.Fatalf("second Lock = %v, want ErrStoreInUse", err)
	}
	unlock()
	unlock, err = f.Lock()
	if err != nil {
		t.Fatalf("Lock after unlock: %v", err)
	}
	unlock()
}

func TestNewDeviceIDIsRandom(t *testing.T) {
	a, b := NewDeviceID(), NewDeviceID()
	if len(a) != 32 || a == b {
		t.Fatalf("device ids %q, %q", a, b)
	}
}
