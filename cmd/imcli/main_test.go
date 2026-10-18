package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
	"google.golang.org/protobuf/proto"
)

// fakeGateway accepts one client, answers the Noise handshake and runs handle.
// It returns the address, the key and a channel with handle's error.
func fakeGateway(t *testing.T, handle func(net.Conn, *wire.Session) error) (string, wire.Key, <-chan error) {
	t.Helper()
	key, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	done := make(chan error, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer nc.Close()
		_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
		frame := make([]byte, wire.HeaderLen+wire.HandshakeLen)
		if _, err := io.ReadFull(nc, frame); err != nil {
			done <- err
			return
		}
		sess, reply, err := wire.Respond(key, frame, uint16(clientpb.Cmd_CMD_HANDSHAKE))
		if err != nil {
			done <- err
			return
		}
		if _, err := nc.Write(reply); err != nil {
			done <- err
			return
		}
		done <- handle(nc, sess)
	}()
	return ln.Addr().String(), key, done
}

func mustMarshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRunLoginPingLogout(t *testing.T) {
	loginResp := mustMarshal(t, &clientpb.LoginResp{HeartbeatS: 1})
	addr, key, done := fakeGateway(t, func(nc net.Conn, sess *wire.Session) error {
		cmd, id, body, err := sess.ReadFrame(nc)
		if err != nil {
			return fmt.Errorf("read login: %w", err)
		}
		if clientpb.Cmd(cmd) != clientpb.Cmd_CMD_LOGIN {
			return fmt.Errorf("first frame is %v, want Login", clientpb.Cmd(cmd))
		}
		var req clientpb.LoginReq
		if err := proto.Unmarshal(body, &req); err != nil {
			return fmt.Errorf("decode login: %w", err)
		}
		if req.GetToken() != "tok" || req.GetAppid() != "im" ||
			req.GetPlatform() != clientpb.Platform_PLATFORM_MOBILE || req.GetDeviceId() == "" {
			return fmt.Errorf("login req %+v", &req)
		}
		if err := sess.WriteFrame(nc, uint16(clientpb.Cmd_CMD_LOGIN_RESP), id, loginResp); err != nil {
			return fmt.Errorf("write login resp: %w", err)
		}
		for {
			cmd, id, _, err := sess.ReadFrame(nc)
			if err != nil {
				return fmt.Errorf("read after login: %w", err)
			}
			switch clientpb.Cmd(cmd) {
			case clientpb.Cmd_CMD_PING:
				if err := sess.WriteFrame(nc, uint16(clientpb.Cmd_CMD_PONG), id, nil); err != nil {
					return fmt.Errorf("write pong: %w", err)
				}
			case clientpb.Cmd_CMD_LOGOUT:
				return sess.WriteFrame(nc, uint16(clientpb.Cmd_CMD_LOGOUT_RESP), id, nil)
			default:
				return fmt.Errorf("unexpected %v after login", clientpb.Cmd(cmd))
			}
		}
	})

	var out bytes.Buffer
	err := run([]string{
		"-addr", addr,
		"-key", base64.StdEncoding.EncodeToString(key.Public),
		"-token", "tok",
		"-ping-every", "50ms",
		"-duration", "400ms",
		"-device-id", "test-device",
	}, strings.NewReader(""), &out, io.Discard)
	if err != nil {
		t.Fatalf("run: %v\noutput:\n%s", err, out.String())
	}
	if err := <-done; err != nil {
		t.Fatalf("gateway: %v", err)
	}
	for _, want := range []string{"handshake ok", "login ok", "heartbeat=1s", "pong req=", "logout ok"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output misses %q:\n%s", want, out.String())
		}
	}
}

func TestRunLoginRejected(t *testing.T) {
	rejection := mustMarshal(t, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID})
	addr, key, done := fakeGateway(t, func(nc net.Conn, sess *wire.Session) error {
		cmd, id, _, err := sess.ReadFrame(nc)
		if err != nil {
			return err
		}
		if clientpb.Cmd(cmd) != clientpb.Cmd_CMD_LOGIN {
			return fmt.Errorf("first frame is %v, want Login", clientpb.Cmd(cmd))
		}
		return sess.WriteFrame(nc, uint16(clientpb.Cmd_CMD_ERROR), id, rejection)
	})

	var out bytes.Buffer
	err := run([]string{
		"-addr", addr,
		"-key", base64.StdEncoding.EncodeToString(key.Public),
		"-token", "bad",
		"-timeout", "2s",
	}, strings.NewReader(""), &out, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "TOKEN_INVALID") {
		t.Fatalf("run error = %v, want a TOKEN_INVALID rejection", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("gateway: %v", err)
	}
}
