package session

import (
	"strings"
	"testing"
)

func validEntry() Entry {
	return Entry{
		Gateway:  "gw-17",
		Epoch:    1<<63 + 5, // above 2^53: must survive as a string
		ConnID:   3<<48 | 42,
		DeviceID: "dev-a",
		ConnSeq:  7,
		LoginMs:  1_760_000_000_123,
		TokIatMs: 1_759_999_000_000,
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	e := validEntry()
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	got, err := Decode(e.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if got != e {
		t.Fatalf("round trip: got %+v want %+v", got, e)
	}
	want := "gw-17|9223372036854775813|844424930132010|dev-a|7|1760000000123|1759999000000"
	if e.Encode() != want {
		t.Fatalf("Encode = %q, want %q", e.Encode(), want)
	}
	if !strings.HasPrefix(e.Encode(), e.OwnerPrefix()) || !strings.HasPrefix(e.Encode(), EpochPrefix(e.Gateway, e.Epoch)) {
		t.Fatal("prefixes must match the encoded value")
	}
}

func TestValidateRejectsUnencodable(t *testing.T) {
	cases := map[string]func(*Entry){
		"device_id with pipe": func(e *Entry) { e.DeviceID = "a|b" },
		"empty device_id":     func(e *Entry) { e.DeviceID = "" },
		"long device_id":      func(e *Entry) { e.DeviceID = strings.Repeat("x", 129) },
		"gateway with pipe":   func(e *Entry) { e.Gateway = "gw|1" },
		"gateway with colon":  func(e *Entry) { e.Gateway = "gw:1" },
		"conn_seq above 2^53": func(e *Entry) { e.ConnSeq = MaxConnSeq + 1 },
		"zero login_ms":       func(e *Entry) { e.LoginMs = 0 },
	}
	for name, mut := range cases {
		e := validEntry()
		mut(&e)
		if err := e.Validate(); err == nil {
			t.Errorf("%s: Validate accepted %+v", name, e)
		}
	}
}

func TestSlotFieldAndUID(t *testing.T) {
	f, err := SlotField("im", Mobile)
	if err != nil || f != "im:mobile" {
		t.Fatalf("SlotField = %q, %v", f, err)
	}
	if ValidField(f) != nil {
		t.Fatal("ValidField rejected a SlotField result")
	}
	for _, bad := range []string{"*", "im", "im:tv", "i|m:mobile", ":mobile"} {
		if ValidField(bad) == nil {
			t.Errorf("ValidField(%q) accepted", bad)
		}
	}
	if _, err := SlotField("im", "tv"); err == nil {
		t.Fatal("unknown platform accepted")
	}
	for _, bad := range []string{"", "a{b", "a}b", "a b", strings.Repeat("u", 65)} {
		if ValidUID(bad) == nil {
			t.Errorf("ValidUID(%q) accepted", bad)
		}
	}
	if ValidUID("10086") != nil {
		t.Fatal("ValidUID rejected a numeric uid")
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "a|b", "gw|x|1|d|1|1|1", "gw|1|1|d|1|1|1|extra"} {
		if _, err := Decode(bad); err == nil {
			t.Errorf("Decode(%q) accepted", bad)
		}
	}
}

func TestChunkWins(t *testing.T) {
	const dl = 5000
	base := validEntry()
	with := func(dev string, seq uint64, login int64) Entry {
		e := base
		e.DeviceID, e.ConnSeq, e.LoginMs = dev, seq, login
		return e
	}
	cases := []struct {
		name          string
		chunk, stored Entry
		want          bool
	}{
		{"same device, higher seq, inside window", with("d", 8, 1000), with("d", 7, 900), true},
		{"same device, lower seq, inside window", with("d", 6, 1000), with("d", 7, 900), false},
		{"same device, equal seq: stored wins", with("d", 7, 1000), with("d", 7, 900), false},
		{"same device, window edge is inside", with("d", 6, 6000), with("d", 7, 1000), false},
		{"regressed counter, later beyond window, wins", with("d", 1, 6001), with("d", 7, 1000), true},
		{"higher seq but earlier beyond window loses", with("d", 9, 1000), with("d", 7, 6001), false},
		{"other device, later wins", with("x", 1, 1001), with("d", 7, 1000), true},
		{"other device, earlier loses", with("x", 9, 999), with("d", 7, 1000), false},
		{"other device, equal login_ms: stored wins", with("x", 9, 1000), with("d", 7, 1000), false},
	}
	for _, c := range cases {
		if got := ChunkWins(c.chunk, c.stored, dl); got != c.want {
			t.Errorf("%s: ChunkWins = %v, want %v", c.name, got, c.want)
		}
		if c.want && ChunkWins(c.stored, c.chunk, dl) {
			t.Errorf("%s: both entries win against each other", c.name)
		}
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(validEntry().Encode())
	f.Add("gw|1|2|d|3|4|5")
	f.Fuzz(func(t *testing.T, s string) {
		e, err := Decode(s)
		if err != nil || e.Validate() != nil {
			return
		}
		if got, err := Decode(e.Encode()); err != nil || got != e {
			t.Fatalf("re-decode of %q: %+v, %v", s, got, err)
		}
	})
}
