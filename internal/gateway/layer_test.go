package gateway

import (
	"os/exec"
	"strings"
	"testing"
)

// The layer rule: the gateway never touches Redis; only Logic does.
func TestGatewayDoesNotImportTheStore(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "github.com/Tallinzp/goim/internal/gateway").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == "github.com/Tallinzp/goim/internal/store" || strings.HasPrefix(dep, "github.com/redis/") {
			t.Fatalf("internal/gateway depends on %s", dep)
		}
	}
}
