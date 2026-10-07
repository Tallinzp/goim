// Package session defines the slot ownership record stored in sess:{uid}
// and the ordering rule shared by the login Lua and Reconcile rule 3.
package session

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Platform is the device class part of a slot (userid + appid + platform).
type Platform string

const (
	Mobile Platform = "mobile" // Android and iOS share one slot
	Pad    Platform = "pad"
	Web    Platform = "web"
	PC     Platform = "pc"
)

// MaxConnSeq is the largest conn_seq the Lua scripts compare exactly:
// Lua numbers are doubles, so integers above 2^53 lose precision.
const MaxConnSeq = 1<<53 - 1

const maxDeviceID = 128

var (
	uidRe   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	appidRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	// Pod names are DNS-1123 labels; ':' and '|' can never appear.
	gatewayRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,61}[a-z0-9])?$`)
)

// ValidUID reports whether uid is safe to embed in a hash-tagged key.
func ValidUID(uid string) error {
	if !uidRe.MatchString(uid) {
		return fmt.Errorf("session: invalid uid %q", uid)
	}
	return nil
}

// ValidGateway reports whether gw is a valid gateway_id (pod name).
func ValidGateway(gw string) error {
	if !gatewayRe.MatchString(gw) {
		return fmt.Errorf("session: invalid gateway id %q", gw)
	}
	return nil
}

// SlotField returns the sess:{uid} field name "appid:platform".
func SlotField(appid string, p Platform) (string, error) {
	if !appidRe.MatchString(appid) {
		return "", fmt.Errorf("session: invalid appid %q", appid)
	}
	switch p {
	case Mobile, Pad, Web, PC:
	default:
		return "", fmt.Errorf("session: invalid platform %q", p)
	}
	return appid + ":" + string(p), nil
}

// ValidField reports whether f has the form SlotField produces.
func ValidField(f string) error {
	appid, p, ok := strings.Cut(f, ":")
	if !ok {
		return fmt.Errorf("session: invalid slot field %q", f)
	}
	_, err := SlotField(appid, Platform(p))
	return err
}

// Entry is one slot's owner: gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat.
type Entry struct {
	Gateway  string
	Epoch    uint64
	ConnID   uint64
	DeviceID string
	ConnSeq  uint64
	LoginMs  int64 // the gateway's recv_ms for the Login (R1-O4)
	TokIatMs int64 // the token's iat, in milliseconds
}

// Validate rejects values the '|'-separated encoding or the Lua number
// comparisons cannot represent.
func (e Entry) Validate() error {
	if err := ValidGateway(e.Gateway); err != nil {
		return err
	}
	if e.DeviceID == "" || len(e.DeviceID) > maxDeviceID || strings.Contains(e.DeviceID, "|") {
		return fmt.Errorf("session: invalid device_id %q", e.DeviceID)
	}
	if e.ConnSeq > MaxConnSeq {
		return fmt.Errorf("session: conn_seq %d above %d", e.ConnSeq, uint64(MaxConnSeq))
	}
	if e.LoginMs <= 0 || e.TokIatMs < 0 {
		return errors.New("session: login_ms must be positive and tok_iat non-negative")
	}
	return nil
}

// Encode returns the stored field value. Call Validate first.
func (e Entry) Encode() string {
	return e.OwnerPrefix() + e.DeviceID + "|" +
		strconv.FormatUint(e.ConnSeq, 10) + "|" +
		strconv.FormatInt(e.LoginMs, 10) + "|" +
		strconv.FormatInt(e.TokIatMs, 10)
}

// OwnerPrefix is "gw|epoch|conn_id|": a stored value starting with it
// belongs to exactly this connection (conn_ids are never reused).
func (e Entry) OwnerPrefix() string {
	return EpochPrefix(e.Gateway, e.Epoch) + strconv.FormatUint(e.ConnID, 10) + "|"
}

// EpochPrefix is "gw|epoch|": every value written for that gateway epoch starts with it.
func EpochPrefix(gw string, epoch uint64) string {
	return gw + "|" + strconv.FormatUint(epoch, 10) + "|"
}

// Decode parses a stored field value.
func Decode(s string) (Entry, error) {
	p := strings.Split(s, "|")
	if len(p) != 7 {
		return Entry{}, fmt.Errorf("session: malformed entry %q", s)
	}
	var e Entry
	var err error
	e.Gateway, e.DeviceID = p[0], p[3]
	if e.Epoch, err = strconv.ParseUint(p[1], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: epoch: %w", s, err)
	}
	if e.ConnID, err = strconv.ParseUint(p[2], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: conn_id: %w", s, err)
	}
	if e.ConnSeq, err = strconv.ParseUint(p[4], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: conn_seq: %w", s, err)
	}
	if e.LoginMs, err = strconv.ParseInt(p[5], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: login_ms: %w", s, err)
	}
	if e.TokIatMs, err = strconv.ParseInt(p[6], 10, 64); err != nil {
		return Entry{}, fmt.Errorf("session: entry %q: tok_iat: %w", s, err)
	}
	return e, nil
}

// ChunkWins reports whether a Reconcile chunk entry beats the stored entry
// under the login table's ordering (Reconcile rule 3, R2-O4, R3-O4):
//   - same device_id, login_ms within deadlineMs of each other: higher conn_seq wins;
//   - same device_id otherwise, or different device_id: later login_ms wins;
//   - ties go to the stored entry, so two live sessions never close each other.
func ChunkWins(chunk, stored Entry, deadlineMs int64) bool {
	if chunk.DeviceID == stored.DeviceID {
		d := chunk.LoginMs - stored.LoginMs
		if d >= -deadlineMs && d <= deadlineMs {
			return chunk.ConnSeq > stored.ConnSeq
		}
	}
	return chunk.LoginMs > stored.LoginMs
}
