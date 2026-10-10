// Command loadgen puts load on a goim stack (Plan 6a2). It reads
// scripts/stack.sh's stack.env, starts -clients at -rate through a
// least-connections balancer from 127.0.0.2… (choices #69, #81), holds them,
// and serves the control API on -control (GET /stats, POST /kill, POST /mark,
// GET /slo; choice #84) until SIGTERM, Ctrl-C or -duration. It then writes
// the result, the stats at that moment and every mark's SLO against
// -within, as JSON to -out, and only then stops every client: stopping 200k
// SDK clients can outlast the caller's wait, and the result must survive a
// SIGKILL during the teardown.
//
//	loadgen -env $STACK_DIR/stack.env -mode sdk -clients 2000 -rate 500 \
//	    -control 127.0.0.1:17005 -out loadgen.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/internal/loadgen"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(2)
	}
}

func run(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg loadgen.Config
	envPath := fs.String("env", "", "stack.env written by scripts/stack.sh (required)")
	fs.StringVar(&cfg.Mode, "mode", "light", "light (clientconn + shared pinger) or sdk (one SDK client each)")
	fs.IntVar(&cfg.Clients, "clients", 1000, "clients; client i logs in as <uid-prefix>-<i>")
	fs.Float64Var(&cfg.Rate, "rate", 1000, "clients started per second")
	fs.StringVar(&cfg.UIDPrefix, "uid-prefix", "lg", "uid prefix, distinct per run")
	fs.IntVar(&cfg.Sources, "sources", 0, "source addresses 127.0.0.2…; 0: one per 20,000 connections per gateway")
	fs.DurationVar(&cfg.PingEvery, "ping-every", 150*time.Second, "light: Ping interval (the gateway's -heartbeat)")
	fs.DurationVar(&cfg.Timeout, "timeout", 10*time.Second, "light: dial + handshake + Login per attempt")
	control := fs.String("control", "", "control API listen address (none if empty)")
	duration := fs.Duration("duration", 0, "stop after this long (0: at SIGTERM)")
	within := fs.Duration("within", 30*time.Second, "SLO bound for the result's marks")
	out := fs.String("out", "", "result file (default: stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *envPath == "" {
		return errors.New("-env is required")
	}
	env, err := loadgen.LoadEnv(*envPath)
	if err != nil {
		return err
	}
	r, err := loadgen.New(cfg, env)
	if err != nil {
		return err
	}
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}
	// The runner has its own context, cancelled only once the result is
	// written: the result is the run as it stood, before any client stopped.
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	if *control != "" {
		l, err := net.Listen("tcp", *control)
		if err != nil {
			return err
		}
		srv := &http.Server{Handler: r.Handler(), ReadHeaderTimeout: 5 * time.Second}
		go srv.Serve(l)
		defer srv.Close()
		slog.Info("loadgen: control API", "addr", l.Addr().String())
	}
	slog.Info("loadgen: starting", "mode", cfg.Mode, "clients", cfg.Clients, "rate", cfg.Rate, "gateways", len(env.GatewayAddrs))
	done := make(chan struct{})
	go func() { r.Run(runCtx); close(done) }()
	<-ctx.Done()
	err = writeResult(*out, r.Result(*within))
	stopRun()
	<-done
	return err
}

// writeResult writes res to path (stdout if empty), aside and renamed, so a
// reader never sees half a result.
func writeResult(path string, res loadgen.Result) error {
	doc, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	doc = append(doc, '\n')
	if path == "" {
		_, err = os.Stdout.Write(doc)
		return err
	}
	if err := os.WriteFile(path+".tmp", doc, 0o644); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
