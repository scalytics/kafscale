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

package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/kafscale/platform/addons/processors/sql-processor/internal/config"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/decoder"
	"github.com/kafscale/platform/addons/processors/sql-processor/internal/discovery"
	kafsql "github.com/kafscale/platform/addons/processors/sql-processor/internal/sql"
)

type mockLister struct {
	segments []discovery.SegmentRef
}

func (m *mockLister) ListCompleted(ctx context.Context) ([]discovery.SegmentRef, error) {
	return m.segments, nil
}

type mockDecoder struct {
	records map[string][]decoder.Record
	keys    []string
}

func (m *mockDecoder) Decode(ctx context.Context, segmentKey, indexKey string, topic string, partition int32) ([]decoder.Record, error) {
	m.keys = append(m.keys, segmentKey)
	return m.records[segmentKey], nil
}

func newPipeBackend(t *testing.T) (*pgproto3.Backend, *pgproto3.Frontend, func()) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(serverConn), serverConn)
	frontend := pgproto3.NewFrontend(pgproto3.NewChunkReader(clientConn), clientConn)
	cleanup := func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	}
	return backend, frontend, cleanup
}

func collectRows(t *testing.T, frontend *pgproto3.Frontend) [][][]byte {
	t.Helper()
	rows := make([][][]byte, 0)
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			copied := make([][]byte, len(m.Values))
			for i, value := range m.Values {
				if value == nil {
					continue
				}
				buf := make([]byte, len(value))
				copy(buf, value)
				copied[i] = buf
			}
			rows = append(rows, copied)
		case *pgproto3.CommandComplete:
			return rows
		case *pgproto3.ErrorResponse:
			t.Fatalf("error response: %s", m.Message)
		}
	}
}

func collectCommandTag(t *testing.T, frontend *pgproto3.Frontend) string {
	t.Helper()
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch m := msg.(type) {
		case *pgproto3.CommandComplete:
			return string(m.CommandTag)
		case *pgproto3.ErrorResponse:
			t.Fatalf("error response: %s", m.Message)
		}
	}
}

func newTestServer(lister discovery.Lister, dec decoder.Decoder) *Server {
	cfg := config.Config{
		Query: config.QueryConfig{
			DefaultLimit:     1000,
			MaxUnbounded:     10000,
			RequireTimeBound: true,
		},
	}
	srv := New(cfg, nil)
	srv.lister = lister
	srv.listerInit = true
	srv.decoder = dec
	srv.decoderInit = true
	return srv
}

func TestHandleSelectTail(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now - 3000, Key: []byte("a")},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: now - 2000, Key: []byte("b")},
		{Topic: "orders", Partition: 0, Offset: 3, Timestamp: now - 1000, Key: []byte("c")},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT _offset FROM orders TAIL 2;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if string(rows[0][0]) != "2" || string(rows[1][0]) != "3" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestHandleSelectOrderBy(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 10, Timestamp: now - 5000},
		{Topic: "orders", Partition: 0, Offset: 11, Timestamp: now - 1000},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT _offset FROM orders ORDER BY _ts DESC LIMIT 1 LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 1 || string(rows[0][0]) != "11" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestHandleSetCommand(t *testing.T) {
	srv := newTestServer(&mockLister{}, &mockDecoder{})
	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.handleQuery(context.Background(), backend, "SET client_encoding = 'UTF8';")
	}()

	tag := collectCommandTag(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle query: %v", err)
	}
	if tag != "SET" {
		t.Fatalf("unexpected command tag: %q", tag)
	}
}

func TestHandleSelectJSONHelpers(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now - 1000, Value: []byte(`{"status":"ok","meta":{"id":123}}`)},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: now - 900, Value: []byte(`{"status":`)},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT json_value(_value, '$.status') AS status, json_query(_value, '$.meta') AS meta, json_exists(_value, '$.status') AS has_status FROM orders LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if string(rows[0][0]) != "ok" {
		t.Fatalf("unexpected json_value: %q", rows[0][0])
	}
	if string(rows[0][1]) != `{"id":123}` {
		t.Fatalf("unexpected json_query: %q", rows[0][1])
	}
	if string(rows[0][2]) != "true" {
		t.Fatalf("unexpected json_exists: %q", rows[0][2])
	}
	if rows[1][0] != nil || rows[1][1] != nil || rows[1][2] != nil {
		t.Fatalf("expected nulls for invalid json, got %+v", rows[1])
	}
}

func TestHandleAggregateGroupBy(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1"},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now - 1000},
		{Topic: "orders", Partition: 0, Offset: 2, Timestamp: now - 900},
		{Topic: "orders", Partition: 1, Offset: 3, Timestamp: now - 800},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
	}})

	parsed, err := kafsql.Parse("SELECT _partition, COUNT(*) AS total, SUM(_offset) AS sum_offset FROM orders GROUP BY _partition LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if string(rows[0][0]) != "0" || string(rows[0][1]) != "2" || string(rows[0][2]) != "3" {
		t.Fatalf("unexpected group 0: %+v", rows[0])
	}
	if string(rows[1][0]) != "1" || string(rows[1][1]) != "1" || string(rows[1][2]) != "3" {
		t.Fatalf("unexpected group 1: %+v", rows[1])
	}
}

func TestHandleJoinJSON(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-orders", IndexKey: "idx-orders"},
		{Topic: "payments", Partition: 0, SegmentKey: "seg-payments", IndexKey: "idx-payments"},
	}
	left := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now, Key: []byte("left-key"), Value: []byte(`{"id":"a"}`)},
	}
	right := []decoder.Record{
		{Topic: "payments", Partition: 0, Offset: 10, Timestamp: now, Key: []byte("right-key"), Value: []byte(`{"id":"a"}`)},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-orders":   left,
		"seg-payments": right,
	}})

	parsed, err := kafsql.Parse("SELECT o._key, p._value FROM orders o JOIN payments p ON json_value(o._value, '$.id') = json_value(p._value, '$.id') WITHIN 10m LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle join: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestHandleSelectPrunesByOffset(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	min0 := int64(0)
	max9 := int64(9)
	min10 := int64(10)
	max19 := int64(19)
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1", MinOffset: &min0, MaxOffset: &max9},
		{Topic: "orders", Partition: 0, SegmentKey: "seg-2", IndexKey: "idx-2", MinOffset: &min10, MaxOffset: &max19},
	}
	records1 := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 5, Timestamp: now - 1000},
	}
	records2 := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 15, Timestamp: now - 1000},
	}
	dec := &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records1,
		"seg-2": records2,
	}}
	srv := newTestServer(&mockLister{segments: segments}, dec)

	parsed, err := kafsql.Parse("SELECT _offset FROM orders WHERE _offset >= 12 LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("handle select: %v", err)
	}
	if len(rows) != 1 || string(rows[0][0]) != "15" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if len(dec.keys) != 1 || dec.keys[0] != "seg-2" {
		t.Fatalf("expected decoder to hit seg-2 only, got %v", dec.keys)
	}
}

func TestExplainSelect(t *testing.T) {
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1", SizeBytes: 1024},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{})

	parsed, err := kafsql.Parse("EXPLAIN SELECT * FROM orders LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, frontend, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleExplain(context.Background(), backend, parsed)
		errCh <- err
	}()

	rows := collectRows(t, frontend)
	if err := <-errCh; err != nil {
		t.Fatalf("explain: %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("expected plan rows")
	}
	if string(rows[0][0]) != "Query Plan" {
		t.Fatalf("unexpected plan header: %s", rows[0][0])
	}
}

func TestHandleSelectMaxSegments(t *testing.T) {
	now := time.Now().UTC().UnixMilli()
	segments := []discovery.SegmentRef{
		{Topic: "orders", Partition: 0, SegmentKey: "seg-1", IndexKey: "idx-1", SizeBytes: 1024},
		{Topic: "orders", Partition: 0, SegmentKey: "seg-2", IndexKey: "idx-2", SizeBytes: 1024},
	}
	records := []decoder.Record{
		{Topic: "orders", Partition: 0, Offset: 1, Timestamp: now},
	}
	srv := newTestServer(&mockLister{segments: segments}, &mockDecoder{records: map[string][]decoder.Record{
		"seg-1": records,
		"seg-2": records,
	}})
	srv.cfg.Query.MaxScanSegments = 1

	parsed, err := kafsql.Parse("SELECT _offset FROM orders LAST 1h;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	backend, _, cleanup := newPipeBackend(t)
	defer cleanup()
	errCh := make(chan error, 1)
	go func() {
		_, err := srv.handleSelect(context.Background(), backend, parsed)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected max segments error")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for error")
	}
}
