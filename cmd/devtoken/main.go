// Command devtoken issues test access tokens and publishes a test JWKS. It
// stands in for the account service in tests and on the dev machine.
//
//	devtoken keygen -kid k1 -out k1.json
//	devtoken jwks -out jwks.json k1.json [k2.json ...]
//	devtoken issue -key k1.json -sub u1 -appid im -platform mobile [-ttl 1h] [-iat-ms N]
//	devtoken serve -addr :8081 -jwks jwks.json
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "devtoken:", err)
		os.Exit(2)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: devtoken keygen|jwks|issue|serve [flags]")
	}
	switch args[0] {
	case "keygen":
		return keygen(args[1:])
	case "jwks":
		return jwks(args[1:])
	case "issue":
		return issue(args[1:], out, time.Now)
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ContinueOnError)
		addr := fs.String("addr", ":8081", "listen address")
		path := fs.String("jwks", "jwks.json", "JWKS file, re-read on every request")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		log.Printf("devtoken: serving %s on %s", *path, *addr)
		return http.ListenAndServe(*addr, jwksHandler(*path))
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func keygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ContinueOnError)
	kid := fs.String("kid", "", "key id")
	out := fs.String("out", "", "private key file to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("keygen: -out is required")
	}
	k, err := auth.GenerateKey(*kid)
	if err != nil {
		return err
	}
	doc, err := auth.MarshalPrivateJWK(k)
	if err != nil {
		return err
	}
	return os.WriteFile(*out, doc, 0o600)
}

func jwks(args []string) error {
	fs := flag.NewFlagSet("jwks", flag.ContinueOnError)
	out := fs.String("out", "", "JWKS file to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" || fs.NArg() == 0 {
		return errors.New("jwks: -out and at least one key file are required")
	}
	var keys []auth.PrivateKey
	for _, p := range fs.Args() {
		k, err := readKey(p)
		if err != nil {
			return err
		}
		keys = append(keys, k)
	}
	doc, err := auth.MarshalJWKS(keys...)
	if err != nil {
		return err
	}
	// Write and rename, so a serve process re-reading the file never sees half of it.
	tmp := *out + ".tmp"
	if err := os.WriteFile(tmp, doc, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, *out)
}

func issue(args []string, out io.Writer, now func() time.Time) error {
	fs := flag.NewFlagSet("issue", flag.ContinueOnError)
	keyPath := fs.String("key", "", "private key file")
	sub := fs.String("sub", "", "uid")
	appid := fs.String("appid", "", "appid")
	platform := fs.String("platform", "mobile", "mobile | pad | web | pc")
	ttl := fs.Duration("ttl", time.Hour, "token lifetime")
	iatMs := fs.Int64("iat-ms", 0, "iat in ms (default: now)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	k, err := readKey(*keyPath)
	if err != nil {
		return err
	}
	iat := *iatMs
	if iat == 0 {
		iat = now().UnixMilli()
	}
	tok, err := auth.Sign(k, auth.Claims{UID: *sub, AppID: *appid, Platform: *platform,
		IatMs: iat, ExpMs: iat + ttl.Milliseconds()})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, tok)
	return err
}

func readKey(path string) (auth.PrivateKey, error) {
	doc, err := os.ReadFile(path)
	if err != nil {
		return auth.PrivateKey{}, err
	}
	return auth.ParsePrivateJWK(doc)
}

// jwksHandler serves the JWKS file, re-read on every request so a rotation
// test can publish a new key while Logic runs.
func jwksHandler(path string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		doc, err := os.ReadFile(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(doc)
	})
}
