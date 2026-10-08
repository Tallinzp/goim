package clientpb_test

import (
	"strings"
	"testing"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

// The gateway converts Platform and KickReason between the two protocols by
// number, so both enums must keep the same numbers and names.
func TestEnumsMatchTheInternalProtocol(t *testing.T) {
	for n, name := range pb.Platform_name {
		if got := clientpb.Platform_name[n]; got != name {
			t.Errorf("Platform %d: client %q, internal %q", n, got, name)
		}
	}
	for n, name := range pb.KickReason_name {
		if got := clientpb.KickReason_name[n]; got != name {
			t.Errorf("KickReason %d: client %q, internal %q", n, got, name)
		}
	}
	if len(pb.Platform_name) != len(clientpb.Platform_name) || len(pb.KickReason_name) != len(clientpb.KickReason_name) {
		t.Error("the enums have different sizes")
	}
}

// Every rejection code Logic sends has a client ErrorCode of the same name.
func TestEveryLoginRejectionHasAClientErrorCode(t *testing.T) {
	for _, name := range pb.LoginCode_name {
		code := strings.TrimPrefix(name, "LOGIN_CODE_")
		switch code {
		case "UNSPECIFIED", "OK", "OUTCOME_UNKNOWN":
			continue
		}
		if _, ok := clientpb.ErrorCode_value["ERROR_CODE_"+code]; !ok {
			t.Errorf("no client ErrorCode for %s", name)
		}
	}
}
