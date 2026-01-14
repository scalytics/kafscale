// Copyright 2025, 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"testing"

	"github.com/jackc/pgproto3/v2"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
)

func TestReceiveStartupHandlesSSL(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(serverConn), serverConn)
	srv := New(configForTest(), log.New(io.Discard, "", 0))

	done := make(chan *pgproto3.StartupMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		msg, err := srv.receiveStartup(backend, serverConn)
		errCh <- err
		done <- msg
	}()

	buf, err := (&pgproto3.SSLRequest{}).Encode(nil)
	if err != nil {
		t.Fatalf("encode ssl request: %v", err)
	}
	if _, err := clientConn.Write(buf); err != nil {
		t.Fatalf("send ssl request: %v", err)
	}
	reply := make([]byte, 1)
	if _, err := clientConn.Read(reply); err != nil {
		t.Fatalf("read ssl reply: %v", err)
	}
	if reply[0] != 'N' {
		t.Fatalf("expected SSL deny, got %q", reply[0])
	}

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user": "test",
		},
	}
	buf, err = startup.Encode(nil)
	if err != nil {
		t.Fatalf("encode startup: %v", err)
	}
	if _, err := clientConn.Write(buf); err != nil {
		t.Fatalf("send startup: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("receive startup: %v", err)
	}
	msg := <-done
	if msg == nil || msg.Parameters["user"] != "test" {
		t.Fatalf("unexpected startup message: %+v", msg)
	}
}

func TestHandleConnAllowsQuery(t *testing.T) {
	srv := New(config.ProxyConfig{
		Listen:    ":0",
		Upstreams: []string{"upstream"},
		ACL: config.ProxyACLConfig{
			Allow: []string{"orders"},
		},
	}, log.New(io.Discard, "", 0))
	upstreamConn := startPipeUpstream(t)
	srv.dialer = func(ctx context.Context, addr string) (net.Conn, error) {
		return upstreamConn, nil
	}

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleConn(context.Background(), serverConn)
	}()

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	if err := sendStartupMessage(clientConn); err != nil {
		t.Fatalf("send startup: %v", err)
	}
	if err := readUntilReady(frontend); err != nil {
		t.Fatalf("startup response: %v", err)
	}

	if err := frontend.Send(&pgproto3.Query{String: "SELECT * FROM orders LAST 1h;"}); err != nil {
		t.Fatalf("send query: %v", err)
	}
	tag, err := readCommandTag(frontend)
	if err != nil {
		t.Fatalf("read tag: %v", err)
	}
	if tag == "" {
		t.Fatalf("expected command tag")
	}
	if err := readUntilReady(frontend); err != nil {
		t.Fatalf("ready after command: %v", err)
	}

	_ = frontend.Send(&pgproto3.Terminate{})
	if err := <-errCh; err != nil && err != io.EOF {
		t.Fatalf("handle conn: %v", err)
	}
}

func TestHandleConnDeniesQuery(t *testing.T) {
	srv := New(config.ProxyConfig{
		Listen:    ":0",
		Upstreams: []string{"upstream"},
		ACL: config.ProxyACLConfig{
			Deny: []string{"orders"},
		},
	}, log.New(io.Discard, "", 0))
	upstreamConn := startPipeUpstream(t)
	srv.dialer = func(ctx context.Context, addr string) (net.Conn, error) {
		return upstreamConn, nil
	}

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleConn(context.Background(), serverConn)
	}()

	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	if err := sendStartupMessage(clientConn); err != nil {
		t.Fatalf("send startup: %v", err)
	}
	if err := readUntilReady(frontend); err != nil {
		t.Fatalf("startup response: %v", err)
	}

	if err := frontend.Send(&pgproto3.Query{String: "SELECT * FROM orders LAST 1h;"}); err != nil {
		t.Fatalf("send query: %v", err)
	}
	errResp, err := readError(frontend)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if errResp == "" {
		t.Fatalf("expected error response")
	}
	if err := readUntilReady(frontend); err != nil {
		t.Fatalf("ready after error: %v", err)
	}

	_ = frontend.Send(&pgproto3.Terminate{})
	if err := <-errCh; err != nil && err != io.EOF {
		t.Fatalf("handle conn: %v", err)
	}
}

func configForTest() config.ProxyConfig {
	return config.ProxyConfig{
		Listen:    ":0",
		Upstreams: []string{"127.0.0.1:5432"},
	}
}

func startPipeUpstream(t *testing.T) net.Conn {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	go func() {
		defer serverConn.Close()
		backend := pgproto3.NewBackend(pgproto3.NewChunkReader(serverConn), serverConn)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			return
		}
		_ = backend.Send(&pgproto3.AuthenticationOk{})
		_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		for {
			msg, err := backend.Receive()
			if err != nil {
				return
			}
			switch msg.(type) {
			case *pgproto3.Query:
				_ = backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
				_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			case *pgproto3.Terminate:
				return
			default:
				_ = backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Message: "unsupported"})
				_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			}
		}
	}()
	return clientConn
}

func sendStartupMessage(conn net.Conn) error {
	startup := &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user": "test",
		},
	}
	buf, err := startup.Encode(nil)
	if err != nil {
		return err
	}
	_, err = conn.Write(buf)
	return err
}

func readUntilReady(frontend *pgproto3.Frontend) error {
	for {
		msg, err := frontend.Receive()
		if err != nil {
			return err
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			return nil
		}
	}
}

func readCommandTag(frontend *pgproto3.Frontend) (string, error) {
	for {
		msg, err := frontend.Receive()
		if err != nil {
			return "", err
		}
		switch m := msg.(type) {
		case *pgproto3.CommandComplete:
			return string(m.CommandTag), nil
		case *pgproto3.ErrorResponse:
			return "", errors.New(m.Message)
		case *pgproto3.ReadyForQuery:
			return "", nil
		}
	}
}

func readError(frontend *pgproto3.Frontend) (string, error) {
	for {
		msg, err := frontend.Receive()
		if err != nil {
			return "", err
		}
		if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
			return errMsg.Message, nil
		}
	}
}
