package gateway

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// limitSource reads the two limits the gateway can see (spec: Limits
// self-check, R5S-O7). Tests replace it.
type limitSource struct {
	nofile   func() (uint64, error)
	readFile func(string) ([]byte, error)
}

var hostLimits = limitSource{
	nofile: func() (uint64, error) {
		var r syscall.Rlimit
		err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r)
		return r.Cur, err
	},
	readFile: os.ReadFile,
}

// checkLimits returns why the gateway must stay NotReady, or nil:
// RLIMIT_NOFILE ≥ cap × (1 + headroom), and, unless conntrack is disabled for
// the gateway port, nf_conntrack_max ≥ 2 × cap. An absent conntrack file means
// conntrack isn't loaded, which passes.
func checkLimits(cfg Config, src limitSource, m *Metrics) error {
	var errs []error
	needFiles := uint64(math.Ceil(float64(cfg.MaxConns) * (1 + cfg.Limits.Headroom)))
	switch n, err := src.nofile(); {
	case err != nil:
		errs = append(errs, fmt.Errorf("read RLIMIT_NOFILE: %w", err))
	default:
		m.Limits.WithLabelValues("nofile").Set(float64(n))
		if n < needFiles {
			errs = append(errs, fmt.Errorf("RLIMIT_NOFILE %d is below %d (cap %d + %.0f%% headroom)",
				n, needFiles, cfg.MaxConns, cfg.Limits.Headroom*100))
		}
	}
	if !cfg.Limits.Notrack {
		b, err := src.readFile(cfg.Limits.ConntrackPath)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			errs = append(errs, fmt.Errorf("read %s: %w", cfg.Limits.ConntrackPath, err))
		default:
			n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse %s: %w", cfg.Limits.ConntrackPath, err))
				break
			}
			m.Limits.WithLabelValues("nf_conntrack_max").Set(float64(n))
			if need := 2 * uint64(cfg.MaxConns); n < need {
				errs = append(errs, fmt.Errorf("nf_conntrack_max %d is below %d (2 × cap), or set conntrack: notrack", n, need))
			}
		}
	}
	return errors.Join(errs...)
}
