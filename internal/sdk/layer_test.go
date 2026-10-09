package sdk

import (
	"os/exec"
	"strings"
	"testing"
)

// The SDK is the client side only: it depends on the client protocol and
// nothing of the server's (gateway, Logic, store, gRPC, Redis, gnet), so
// it stays the reference a mobile SDK ports. Test files may import the
// server to run end to end.
func TestSDKDependsOnlyOnTheClientSide(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "github.com/Tallinzp/goim/internal/sdk").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	allowed := map[string]bool{
		"github.com/Tallinzp/goim/api/clientpb":        true,
		"github.com/Tallinzp/goim/internal/wire":       true,
		"github.com/Tallinzp/goim/internal/clientconn": true,
		"github.com/Tallinzp/goim/internal/sdk":        true,
	}
	for _, dep := range strings.Fields(string(out)) {
		if strings.HasPrefix(dep, "github.com/Tallinzp/goim/") && !allowed[dep] ||
			strings.HasPrefix(dep, "google.golang.org/grpc") || strings.HasPrefix(dep, "github.com/redis/") ||
			strings.HasPrefix(dep, "github.com/panjf2000/") || strings.HasPrefix(dep, "github.com/prometheus/") {
			t.Fatalf("internal/sdk depends on %s", dep)
		}
	}
}
