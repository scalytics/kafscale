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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgproto3/v2"

	"github.com/novatechflow/kafscale/addons/processors/sql-processor/internal/config"
	"github.com/novatechflow/kafscale/addons/processors/sql-processor/internal/decoder"
	"github.com/novatechflow/kafscale/addons/processors/sql-processor/internal/discovery"
	"github.com/novatechflow/kafscale/addons/processors/sql-processor/internal/metadata"
	"github.com/novatechflow/kafscale/addons/processors/sql-processor/internal/metrics"
	kafsql "github.com/novatechflow/kafscale/addons/processors/sql-processor/internal/sql"
)

type Server struct {
	listenAddr     string
	serverVersion  string
	clientEncoding string
	logger         *log.Logger

	cfg          config.Config
	resolverMu   sync.Mutex
	resolver     metadata.Resolver
	resolverInit bool
	resolverErr  error

	listerMu   sync.Mutex
	lister     discovery.Lister
	listerInit bool
	listerErr  error

	decoderMu   sync.Mutex
	decoder     decoder.Decoder
	decoderInit bool
	decoderErr  error
}

func New(cfg config.Config, logger *log.Logger) *Server {
	addr := cfg.Server.Listen
	if addr == "" {
		addr = ":5432"
	}
	if logger == nil {
		logger = log.Default()
	}
	return &Server{
		listenAddr:     addr,
		serverVersion:  cfg.Server.ServerVersion,
		clientEncoding: cfg.Server.ClientEncoding,
		logger:         logger,
		cfg:            cfg,
	}
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			errCh <- err
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleConnection(ctx, conn)
		}()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	metrics.ConnectionsTotal.Inc()
	metrics.ConnectionsActive.Inc()
	defer metrics.ConnectionsActive.Dec()

	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(conn), conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		s.logger.Printf("startup error: %v", err)
		return
	}

	_ = backend.Send(&pgproto3.AuthenticationOk{})
	_ = backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: s.clientEncoding})
	_ = backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: s.serverVersion})
	_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		switch msg.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Query:
			if err := s.handleQuery(ctx, backend, msg.(*pgproto3.Query).String); err != nil {
				_ = backend.Send(&pgproto3.ErrorResponse{
					Severity: "ERROR",
					Message:  err.Error(),
				})
			}
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		default:
			_ = backend.Send(&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Message:  "unsupported message type",
			})
			_ = backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		}
	}
}

func (s *Server) handleQuery(ctx context.Context, backend *pgproto3.Backend, query string) error {
	start := time.Now()
	metrics.ActiveQueries.Inc()
	defer metrics.ActiveQueries.Dec()

	if result, handled, err := s.handleCatalogQuery(ctx, backend, query); handled {
		status := "success"
		if err != nil {
			status = "error"
		}
		metrics.QueriesTotal.WithLabelValues("catalog", status).Inc()
		metrics.QueryDuration.WithLabelValues("catalog", status).Observe(float64(time.Since(start).Milliseconds()))
		metrics.QueryRows.WithLabelValues("catalog").Add(float64(result.rows))
		s.logger.Printf("query_complete type=catalog status=%s duration_ms=%d rows=%d segments=%d bytes=%d",
			status,
			time.Since(start).Milliseconds(),
			result.rows,
			result.segments,
			result.bytes,
		)
		return err
	}

	parsed, err := kafsql.Parse(query)
	if err != nil {
		metrics.QueriesTotal.WithLabelValues("parse", "error").Inc()
		metrics.QueryDuration.WithLabelValues("parse", "error").Observe(float64(time.Since(start).Milliseconds()))
		s.logger.Printf("query_error type=parse err=%v", err)
		return err
	}

	queryType := string(parsed.Type)
	result, execErr := s.executeQuery(ctx, backend, parsed)
	status := "success"
	if execErr != nil {
		status = "error"
	}

	metrics.QueriesTotal.WithLabelValues(queryType, status).Inc()
	metrics.QueryDuration.WithLabelValues(queryType, status).Observe(float64(time.Since(start).Milliseconds()))
	metrics.QueryRows.WithLabelValues(queryType).Add(float64(result.rows))
	metrics.QuerySegments.Add(float64(result.segments))
	metrics.QueryBytes.Add(float64(result.bytes))

	s.logger.Printf("query_complete type=%s status=%s duration_ms=%d rows=%d segments=%d bytes=%d",
		queryType,
		status,
		time.Since(start).Milliseconds(),
		result.rows,
		result.segments,
		result.bytes,
	)
	return execErr
}

func (s *Server) handleCatalogQuery(ctx context.Context, backend *pgproto3.Backend, query string) (queryResult, bool, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return queryResult{}, false, nil
	}
	lower := strings.ToLower(strings.TrimSuffix(trimmed, ";"))
	if !strings.Contains(lower, "pg_catalog") && !strings.Contains(lower, "information_schema") {
		return queryResult{}, false, nil
	}

	switch {
	case strings.Contains(lower, "information_schema.tables"):
		rows, err := s.catalogTables(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "information_schema.columns"):
		rows, err := s.catalogColumns(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_tables"):
		rows, err := s.catalogPgTables(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_namespace"):
		rows, err := s.catalogPgNamespace(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_type"):
		rows, err := s.catalogPgType(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_database"):
		rows, err := s.catalogPgDatabase(ctx, backend)
		return queryResult{rows: rows}, true, err
	case strings.Contains(lower, "pg_catalog.pg_class"):
		rows, err := s.catalogPgClass(ctx, backend)
		return queryResult{rows: rows}, true, err
	default:
		return queryResult{}, true, errors.New("unsupported catalog query")
	}
}

type queryResult struct {
	rows     int
	segments int
	bytes    int64
}

func (s *Server) executeQuery(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query) (queryResult, error) {
	switch parsed.Type {
	case kafsql.QueryShowTopics:
		rows, err := s.handleShowTopics(ctx, backend)
		return queryResult{rows: rows}, err
	case kafsql.QueryShowPartitions:
		rows, err := s.handleShowPartitions(ctx, backend, parsed.Topic)
		return queryResult{rows: rows}, err
	case kafsql.QuerySelect:
		return s.handleSelect(ctx, backend, parsed)
	default:
		return queryResult{}, errors.New("KAFSQL query execution not implemented")
	}
}

func (s *Server) handleShowTopics(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	resolver, err := s.getResolver()
	if err != nil {
		return 0, err
	}
	topics, err := resolver.Topics(ctx)
	if err != nil {
		return 0, err
	}

	fields := []pgproto3.FieldDescription{
		{Name: []byte("topic"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}

	for _, topic := range topics {
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(topic)}}); err != nil {
			return 0, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(topics))})
	return len(topics), nil
}

func (s *Server) handleShowPartitions(ctx context.Context, backend *pgproto3.Backend, topic string) (int, error) {
	if topic == "" {
		return 0, errors.New("show partitions requires a topic")
	}
	resolver, err := s.getResolver()
	if err != nil {
		return 0, err
	}
	partitions, err := resolver.Partitions(ctx, topic)
	if err != nil {
		return 0, err
	}

	fields := []pgproto3.FieldDescription{
		{Name: []byte("partition"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}

	for _, partition := range partitions {
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(itoa(int(partition)))}}); err != nil {
			return 0, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(partitions))})
	return len(partitions), nil
}

func (s *Server) catalogTables(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	tables, err := s.topicTables(ctx)
	if err != nil {
		return 0, err
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("table_catalog"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_schema"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_name"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_type"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	for _, table := range tables {
		row := [][]byte{
			[]byte("kafsql"),
			[]byte("public"),
			[]byte(table),
			[]byte("BASE TABLE"),
		}
		if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
			return 0, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(tables))})
	return len(tables), nil
}

func (s *Server) catalogColumns(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	tables, err := s.topicTables(ctx)
	if err != nil {
		return 0, err
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("table_catalog"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_schema"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("table_name"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("column_name"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("ordinal_position"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("data_type"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}

	rowCount := 0
	for _, table := range tables {
		cols := s.columnsForTopic(table)
		for i, col := range cols {
			row := [][]byte{
				[]byte("kafsql"),
				[]byte("public"),
				[]byte(table),
				[]byte(col.name),
				[]byte(itoa(i + 1)),
				[]byte(col.dataType),
			}
			if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
				return 0, err
			}
			rowCount++
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(rowCount)})
	return rowCount, nil
}

func (s *Server) catalogPgTables(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	tables, err := s.topicTables(ctx)
	if err != nil {
		return 0, err
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("schemaname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("tablename"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("tableowner"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("tablespace"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("hasindexes"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("hasrules"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("hastriggers"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("rowsecurity"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	for _, table := range tables {
		row := [][]byte{
			[]byte("public"),
			[]byte(table),
			[]byte("kafsql"),
			nil,
			[]byte("f"),
			[]byte("f"),
			[]byte("f"),
			[]byte("f"),
		}
		if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
			return 0, err
		}
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(len(tables))})
	return len(tables), nil
}

func (s *Server) catalogPgNamespace(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	fields := []pgproto3.FieldDescription{
		{Name: []byte("oid"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("nspname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1"), []byte("pg_catalog")}}); err != nil {
		return 0, err
	}
	if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("2"), []byte("public")}}); err != nil {
		return 0, err
	}
	if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("3"), []byte("information_schema")}}); err != nil {
		return 0, err
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(3)})
	return 3, nil
}

func (s *Server) catalogPgType(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	fields := []pgproto3.FieldDescription{
		{Name: []byte("oid"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("typname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	rows := [][]byte{
		[]byte("16"), []byte("bool"),
		[]byte("17"), []byte("bytea"),
		[]byte("20"), []byte("int8"),
		[]byte("23"), []byte("int4"),
		[]byte("25"), []byte("text"),
		[]byte("701"), []byte("float8"),
		[]byte("1114"), []byte("timestamp"),
		[]byte("3802"), []byte("jsonb"),
	}
	rowCount := 0
	for i := 0; i < len(rows); i += 2 {
		if err := backend.Send(&pgproto3.DataRow{Values: [][]byte{rows[i], rows[i+1]}}); err != nil {
			return 0, err
		}
		rowCount++
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(rowCount)})
	return rowCount, nil
}

func (s *Server) catalogPgDatabase(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	fields := []pgproto3.FieldDescription{
		{Name: []byte("oid"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("datname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("datistemplate"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("datallowconn"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	row := [][]byte{[]byte("1"), []byte("kafsql"), []byte("f"), []byte("t")}
	if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
		return 0, err
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(1)})
	return 1, nil
}

func (s *Server) catalogPgClass(ctx context.Context, backend *pgproto3.Backend) (int, error) {
	tables, err := s.topicTables(ctx)
	if err != nil {
		return 0, err
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("oid"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
		{Name: []byte("relname"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0},
		{Name: []byte("relkind"), DataTypeOID: 18, DataTypeSize: 1, TypeModifier: -1, Format: 0},
		{Name: []byte("relnamespace"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0},
	}
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return 0, err
	}
	rowCount := 0
	for i, table := range tables {
		row := [][]byte{
			[]byte(itoa(1000 + i)),
			[]byte(table),
			[]byte("r"),
			[]byte("2"),
		}
		if err := backend.Send(&pgproto3.DataRow{Values: row}); err != nil {
			return 0, err
		}
		rowCount++
	}
	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(rowCount)})
	return rowCount, nil
}

type catalogColumn struct {
	name     string
	dataType string
}

func (s *Server) topicTables(ctx context.Context) ([]string, error) {
	resolver, err := s.getResolver()
	if err != nil {
		return nil, err
	}
	topics, err := resolver.Topics(ctx)
	if err != nil {
		return nil, err
	}
	sort.Strings(topics)
	return topics, nil
}

func (s *Server) columnsForTopic(topic string) []catalogColumn {
	cols := []catalogColumn{
		{name: "_topic", dataType: "text"},
		{name: "_partition", dataType: "integer"},
		{name: "_offset", dataType: "bigint"},
		{name: "_ts", dataType: "timestamp without time zone"},
		{name: "_key", dataType: "bytea"},
		{name: "_value", dataType: "bytea"},
		{name: "_headers", dataType: "jsonb"},
		{name: "_segment", dataType: "text"},
	}
	schemaCols := s.schemaColumnsForTopic(topic)
	for _, col := range schemaCols {
		cols = append(cols, catalogColumn{
			name:     strings.ToLower(col.Name),
			dataType: schemaTypeName(col.Type),
		})
	}
	return cols
}

func schemaTypeName(value string) string {
	switch strings.ToLower(value) {
	case "int":
		return "integer"
	case "long":
		return "bigint"
	case "double":
		return "double precision"
	case "boolean":
		return "boolean"
	case "timestamp":
		return "timestamp without time zone"
	default:
		return "text"
	}
}

func (s *Server) getResolver() (metadata.Resolver, error) {
	s.resolverMu.Lock()
	defer s.resolverMu.Unlock()

	if s.resolverInit {
		return s.resolver, s.resolverErr
	}
	s.resolverInit = true
	s.resolver, s.resolverErr = metadata.NewResolver(s.cfg)
	return s.resolver, s.resolverErr
}

func (s *Server) getLister() (discovery.Lister, error) {
	s.listerMu.Lock()
	defer s.listerMu.Unlock()

	if s.listerInit {
		return s.lister, s.listerErr
	}
	s.listerInit = true
	s.lister, s.listerErr = discovery.New(s.cfg)
	return s.lister, s.listerErr
}

func (s *Server) getDecoder() (decoder.Decoder, error) {
	s.decoderMu.Lock()
	defer s.decoderMu.Unlock()

	if s.decoderInit {
		return s.decoder, s.decoderErr
	}
	s.decoderInit = true
	s.decoder, s.decoderErr = decoder.New(s.cfg)
	return s.decoder, s.decoderErr
}

func (s *Server) handleSelect(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query) (queryResult, error) {
	if parsed.JoinTopic != "" {
		return s.handleJoinSelect(ctx, backend, parsed)
	}
	if parsed.Topic == "" {
		return queryResult{}, errors.New("select requires a topic")
	}

	if s.cfg.Query.RequireTimeBound && !parsed.ScanFull && parsed.Last == "" && parsed.Tail == "" {
		metrics.QueryUnboundedRejected.Inc()
		return queryResult{}, errors.New("unbounded query: add LAST, TAIL, or SCAN FULL")
	}

	limit := s.cfg.Query.DefaultLimit
	if parsed.Limit != "" {
		value, err := parseLimit(parsed.Limit)
		if err != nil {
			return queryResult{}, err
		}
		limit = value
	}
	if limit <= 0 {
		limit = s.cfg.Query.DefaultLimit
	}
	if parsed.ScanFull && limit > s.cfg.Query.MaxUnbounded {
		return queryResult{}, fmt.Errorf("scan full limit exceeds max_unbounded_scan (%d)", s.cfg.Query.MaxUnbounded)
	}

	columns := parsed.Columns
	if len(columns) == 0 {
		columns = []string{"*"}
	}
	resolvedCols, schemaCols, err := s.resolveColumns(parsed.Topic, columns)
	if err != nil {
		return queryResult{}, err
	}

	fields := buildRowDescription(resolvedCols, schemaCols)
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return queryResult{}, err
	}

	lister, err := s.getLister()
	if err != nil {
		return queryResult{}, err
	}
	dec, err := s.getDecoder()
	if err != nil {
		return queryResult{}, err
	}

	segments, err := lister.ListCompleted(ctx)
	if err != nil {
		return queryResult{}, err
	}

	sent := 0
	segmentsScanned := 0
	bytesScanned := int64(0)
	for _, segment := range segments {
		if segment.Topic != parsed.Topic {
			continue
		}
		if parsed.Partition != nil && segment.Partition != *parsed.Partition {
			continue
		}
		segmentsScanned++
		records, err := dec.Decode(ctx, segment.SegmentKey, segment.IndexKey, segment.Topic, segment.Partition)
		if err != nil {
			return queryResult{}, err
		}
		for _, record := range records {
			if parsed.OffsetMin != nil && record.Offset < *parsed.OffsetMin {
				continue
			}
			if parsed.OffsetMax != nil && record.Offset > *parsed.OffsetMax {
				continue
			}
			bytesScanned += int64(len(record.Key) + len(record.Value))
			values := buildRowValues(resolvedCols, record, segment.SegmentKey, schemaCols)
			if err := backend.Send(&pgproto3.DataRow{Values: values}); err != nil {
				return queryResult{}, err
			}
			sent++
			if sent >= limit {
				_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(sent)})
				return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
			}
		}
	}

	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(sent)})
	return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
}

func (s *Server) resolveColumns(topic string, cols []string) ([]string, map[string]config.SchemaColumn, error) {
	schemaCols := s.schemaColumnMapForTopic(topic)
	schemaList := s.schemaColumnsForTopic(topic)
	if len(cols) == 1 && cols[0] == "*" {
		out := []string{"_topic", "_partition", "_offset", "_ts", "_key", "_value", "_headers", "_segment"}
		for _, col := range schemaList {
			out = append(out, strings.ToLower(col.Name))
		}
		return out, schemaCols, nil
	}
	out := make([]string, 0, len(cols))
	for _, col := range cols {
		switch col {
		case "_topic", "_partition", "_offset", "_ts", "_key", "_value", "_headers", "_segment":
			out = append(out, col)
		default:
			if _, ok := schemaCols[col]; ok {
				out = append(out, col)
				continue
			}
			return nil, nil, fmt.Errorf("unknown column %q", col)
		}
	}
	return out, schemaCols, nil
}

func buildRowDescription(cols []string, schemaCols map[string]config.SchemaColumn) []pgproto3.FieldDescription {
	fields := make([]pgproto3.FieldDescription, 0, len(cols))
	for _, col := range cols {
		switch col {
		case "_partition":
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0})
		case "_offset":
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 20, DataTypeSize: 8, TypeModifier: -1, Format: 0})
		case "_ts":
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 1114, DataTypeSize: 8, TypeModifier: -1, Format: 0})
		case "_key", "_value":
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 17, DataTypeSize: -1, TypeModifier: -1, Format: 0})
		case "_right_partition":
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0})
		case "_right_offset":
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 20, DataTypeSize: 8, TypeModifier: -1, Format: 0})
		case "_right_ts":
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 1114, DataTypeSize: 8, TypeModifier: -1, Format: 0})
		case "_right_key", "_right_value":
			fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 17, DataTypeSize: -1, TypeModifier: -1, Format: 0})
		default:
			if schema, ok := schemaCols[col]; ok {
				oid := schemaTypeOID(schema.Type)
				fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: oid, DataTypeSize: -1, TypeModifier: -1, Format: 0})
			} else {
				fields = append(fields, pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: 0})
			}
		}
	}
	return fields
}

func buildRowValues(cols []string, record decoder.Record, segmentKey string, schemaCols map[string]config.SchemaColumn) [][]byte {
	values := make([][]byte, 0, len(cols))
	for _, col := range cols {
		switch col {
		case "_topic":
			values = append(values, []byte(record.Topic))
		case "_partition":
			values = append(values, []byte(itoa(int(record.Partition))))
		case "_offset":
			values = append(values, []byte(itoa64(record.Offset)))
		case "_ts":
			values = append(values, []byte(formatTimestamp(record.Timestamp)))
		case "_key":
			values = append(values, encodeBytea(record.Key))
		case "_value":
			values = append(values, encodeBytea(record.Value))
		case "_headers":
			values = append(values, []byte(headersToJSON(record.Headers)))
		case "_segment":
			values = append(values, []byte(segmentKey))
		default:
			if schema, ok := schemaCols[col]; ok {
				values = append(values, schemaValue(record.Value, schema, record.Topic))
			} else {
				values = append(values, nil)
			}
		}
	}
	return values
}

func (s *Server) handleJoinSelect(ctx context.Context, backend *pgproto3.Backend, parsed kafsql.Query) (queryResult, error) {
	if parsed.Topic == "" || parsed.JoinTopic == "" {
		return queryResult{}, errors.New("join requires two topics")
	}
	if parsed.Tail != "" {
		return queryResult{}, errors.New("join does not support tail")
	}
	if parsed.ScanFull {
		return queryResult{}, errors.New("join does not support scan full")
	}
	if parsed.TimeWindow == "" || parsed.Last == "" {
		return queryResult{}, errors.New("join requires within <duration> and last <duration>")
	}
	if parsed.Partition != nil || parsed.OffsetMin != nil || parsed.OffsetMax != nil {
		return queryResult{}, errors.New("join does not support partition or offset filters")
	}

	within, err := parseDuration(parsed.TimeWindow)
	if err != nil {
		return queryResult{}, err
	}
	last, err := parseDuration(parsed.Last)
	if err != nil {
		return queryResult{}, err
	}
	windowStart := time.Now().Add(-last)

	limit := s.cfg.Query.DefaultLimit
	if parsed.Limit != "" {
		value, err := parseLimit(parsed.Limit)
		if err != nil {
			return queryResult{}, err
		}
		limit = value
	}
	if limit <= 0 {
		limit = s.cfg.Query.DefaultLimit
	}

	if len(parsed.Columns) > 1 || (len(parsed.Columns) == 1 && parsed.Columns[0] != "*") {
		return queryResult{}, errors.New("join only supports select * in v0.6")
	}

	cols := joinDefaultColumns()
	fields := buildRowDescription(cols, nil)
	if err := backend.Send(&pgproto3.RowDescription{Fields: fields}); err != nil {
		return queryResult{}, err
	}

	lister, err := s.getLister()
	if err != nil {
		return queryResult{}, err
	}
	dec, err := s.getDecoder()
	if err != nil {
		return queryResult{}, err
	}

	segments, err := lister.ListCompleted(ctx)
	if err != nil {
		return queryResult{}, err
	}

	leftRecords, leftBytes, err := s.loadRecords(ctx, dec, segments, parsed.Topic)
	if err != nil {
		return queryResult{}, err
	}
	rightRecords, rightBytes, err := s.loadRecords(ctx, dec, segments, parsed.JoinTopic)
	if err != nil {
		return queryResult{}, err
	}

	rightByKey := groupByKey(rightRecords)
	sent := 0
	segmentsScanned := segmentsByTopic(segments, parsed.Topic) + segmentsByTopic(segments, parsed.JoinTopic)
	bytesScanned := leftBytes + rightBytes
	for _, left := range leftRecords {
		if left.Record.Timestamp < windowStart.UnixMilli() {
			continue
		}
		key := recordKey(left.Record.Key)
		if key == "" {
			if parsed.JoinType == "left" {
				values := buildJoinRow(cols, left, joinRecord{})
				if err := backend.Send(&pgproto3.DataRow{Values: values}); err != nil {
					return queryResult{}, err
				}
				sent++
			}
			if sent >= limit {
				_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(sent)})
				return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
			}
			continue
		}

		candidates := rightByKey[key]
		matched := false
		for _, right := range candidates {
			if !withinWindow(left.Record.Timestamp, right.Record.Timestamp, within) {
				continue
			}
			values := buildJoinRow(cols, left, right)
			if err := backend.Send(&pgproto3.DataRow{Values: values}); err != nil {
				return queryResult{}, err
			}
			sent++
			matched = true
			if sent >= limit {
				_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(sent)})
				return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
			}
		}

		if !matched && parsed.JoinType == "left" {
			values := buildJoinRow(cols, left, joinRecord{})
			if err := backend.Send(&pgproto3.DataRow{Values: values}); err != nil {
				return queryResult{}, err
			}
			sent++
			if sent >= limit {
				_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(sent)})
				return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
			}
		}
	}

	_ = backend.Send(&pgproto3.CommandComplete{CommandTag: commandTag(sent)})
	return queryResult{rows: sent, segments: segmentsScanned, bytes: bytesScanned}, nil
}

type joinRecord struct {
	Record     decoder.Record
	SegmentKey string
}

func (s *Server) loadRecords(ctx context.Context, dec decoder.Decoder, segments []discovery.SegmentRef, topic string) ([]joinRecord, int64, error) {
	out := make([]joinRecord, 0)
	bytesScanned := int64(0)
	for _, segment := range segments {
		if segment.Topic != topic {
			continue
		}
		records, err := dec.Decode(ctx, segment.SegmentKey, segment.IndexKey, segment.Topic, segment.Partition)
		if err != nil {
			return nil, 0, err
		}
		for _, record := range records {
			bytesScanned += int64(len(record.Key) + len(record.Value))
			out = append(out, joinRecord{Record: record, SegmentKey: segment.SegmentKey})
		}
	}
	return out, bytesScanned, nil
}

func groupByKey(records []joinRecord) map[string][]joinRecord {
	out := make(map[string][]joinRecord)
	for _, record := range records {
		key := recordKey(record.Record.Key)
		if key == "" {
			continue
		}
		out[key] = append(out[key], record)
	}
	return out
}

func segmentsByTopic(segments []discovery.SegmentRef, topic string) int {
	count := 0
	for _, segment := range segments {
		if segment.Topic == topic {
			count++
		}
	}
	return count
}

func recordKey(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return string(key)
}

func withinWindow(left, right int64, window time.Duration) bool {
	diff := left - right
	if diff < 0 {
		diff = -diff
	}
	return time.Duration(diff)*time.Millisecond <= window
}

func joinDefaultColumns() []string {
	return []string{
		"_topic", "_partition", "_offset", "_ts", "_key", "_value", "_headers", "_segment",
		"_right_topic", "_right_partition", "_right_offset", "_right_ts", "_right_key", "_right_value", "_right_headers", "_right_segment",
	}
}

func buildJoinRow(cols []string, left joinRecord, right joinRecord) [][]byte {
	rightMissing := right.Record.Topic == ""
	values := make([][]byte, 0, len(cols))
	for _, col := range cols {
		switch col {
		case "_topic":
			values = append(values, []byte(left.Record.Topic))
		case "_partition":
			values = append(values, []byte(itoa(int(left.Record.Partition))))
		case "_offset":
			values = append(values, []byte(itoa64(left.Record.Offset)))
		case "_ts":
			values = append(values, []byte(formatTimestamp(left.Record.Timestamp)))
		case "_key":
			values = append(values, encodeBytea(left.Record.Key))
		case "_value":
			values = append(values, encodeBytea(left.Record.Value))
		case "_headers":
			values = append(values, []byte(headersToJSON(left.Record.Headers)))
		case "_segment":
			values = append(values, []byte(left.SegmentKey))
		case "_right_topic":
			if rightMissing {
				values = append(values, nil)
			} else {
				values = append(values, []byte(right.Record.Topic))
			}
		case "_right_partition":
			if rightMissing {
				values = append(values, nil)
			} else {
				values = append(values, []byte(itoa(int(right.Record.Partition))))
			}
		case "_right_offset":
			if rightMissing {
				values = append(values, nil)
			} else {
				values = append(values, []byte(itoa64(right.Record.Offset)))
			}
		case "_right_ts":
			if rightMissing || right.Record.Timestamp == 0 {
				values = append(values, nil)
			} else {
				values = append(values, []byte(formatTimestamp(right.Record.Timestamp)))
			}
		case "_right_key":
			if rightMissing {
				values = append(values, nil)
			} else {
				values = append(values, encodeBytea(right.Record.Key))
			}
		case "_right_value":
			if rightMissing {
				values = append(values, nil)
			} else {
				values = append(values, encodeBytea(right.Record.Value))
			}
		case "_right_headers":
			if rightMissing {
				values = append(values, nil)
			} else {
				values = append(values, []byte(headersToJSON(right.Record.Headers)))
			}
		case "_right_segment":
			if rightMissing {
				values = append(values, nil)
			} else {
				values = append(values, []byte(right.SegmentKey))
			}
		default:
			values = append(values, nil)
		}
	}
	return values
}

func (s *Server) schemaColumnMapForTopic(topic string) map[string]config.SchemaColumn {
	if topic == "" {
		return nil
	}
	for _, entry := range s.cfg.Metadata.Topics {
		if entry.Name != topic {
			continue
		}
		out := make(map[string]config.SchemaColumn, len(entry.Schema.Columns))
		for _, col := range entry.Schema.Columns {
			name := strings.ToLower(col.Name)
			out[name] = col
		}
		return out
	}
	return nil
}

func (s *Server) schemaColumnsForTopic(topic string) []config.SchemaColumn {
	if topic == "" {
		return nil
	}
	for _, entry := range s.cfg.Metadata.Topics {
		if entry.Name != topic {
			continue
		}
		return entry.Schema.Columns
	}
	return nil
}

func schemaTypeOID(schemaType string) uint32 {
	switch strings.ToLower(schemaType) {
	case "int":
		return 23
	case "long":
		return 20
	case "double":
		return 701
	case "boolean":
		return 16
	case "timestamp":
		return 1114
	default:
		return 25
	}
}

func schemaValue(payload []byte, schema config.SchemaColumn, topic string) []byte {
	value, ok := jsonLookup(payload, schema.Path, topic)
	if !ok {
		return nil
	}
	switch strings.ToLower(schema.Type) {
	case "string":
		if s, ok := value.(string); ok {
			return []byte(s)
		}
	case "int":
		if f, ok := value.(float64); ok {
			return []byte(strconv.FormatInt(int64(f), 10))
		}
	case "long":
		if f, ok := value.(float64); ok {
			return []byte(strconv.FormatInt(int64(f), 10))
		}
	case "double":
		if f, ok := value.(float64); ok {
			return []byte(strconv.FormatFloat(f, 'f', -1, 64))
		}
	case "boolean":
		if b, ok := value.(bool); ok {
			return []byte(strconv.FormatBool(b))
		}
	case "timestamp":
		if s, ok := value.(string); ok {
			return []byte(s)
		}
		if f, ok := value.(float64); ok {
			return []byte(formatTimestamp(int64(f)))
		}
	}
	return nil
}

func jsonLookup(payload []byte, path string, topic string) (interface{}, bool) {
	trimmed := strings.TrimSpace(path)
	trimmed = strings.TrimPrefix(trimmed, "$.")
	trimmed = strings.TrimPrefix(trimmed, "$")
	if trimmed == "" {
		return nil, false
	}
	var root interface{}
	if err := json.Unmarshal(payload, &root); err != nil {
		metrics.InvalidJSON.Inc()
		return nil, false
	}
	current := root
	for _, part := range strings.Split(trimmed, ".") {
		if part == "" {
			return nil, false
		}
		obj, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		next, ok := obj[part]
		if !ok {
			if topic != "" {
				metrics.SchemaMiss.WithLabelValues(topic).Inc()
			}
			return nil, false
		}
		current = next
	}
	return current, true
}

func formatTimestamp(ms int64) string {
	ts := time.UnixMilli(ms).UTC()
	return ts.Format("2006-01-02 15:04:05.000")
}

func encodeBytea(data []byte) []byte {
	if data == nil {
		return nil
	}
	encoded := make([]byte, 2+hex.EncodedLen(len(data)))
	encoded[0] = '\\'
	encoded[1] = 'x'
	hex.Encode(encoded[2:], data)
	return encoded
}

func headersToJSON(headers []decoder.Header) string {
	if len(headers) == 0 {
		return "{}"
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, header := range headers {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"`)
		b.WriteString(escapeJSON(header.Key))
		b.WriteString(`":"`)
		b.WriteString(escapeJSON(string(header.Value)))
		b.WriteString(`"`)
	}
	b.WriteByte('}')
	return b.String()
}

func escapeJSON(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return replacer.Replace(value)
}

func parseLimit(raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid limit")
	}
	return value, nil
}

func commandTag(rows int) []byte {
	return []byte("SELECT " + itoa(rows))
}

func parseDuration(raw string) (time.Duration, error) {
	if strings.HasSuffix(raw, "d") {
		value := strings.TrimSuffix(raw, "d")
		days, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("invalid duration")
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid duration")
	}
	return parsed, nil
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	neg := value < 0
	if neg {
		value = -value
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func itoa64(value int64) string {
	if value == 0 {
		return "0"
	}
	neg := value < 0
	if neg {
		value = -value
	}
	var buf [32]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
