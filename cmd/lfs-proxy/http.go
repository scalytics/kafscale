// Copyright 2025-2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package main

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/KafScale/platform/pkg/lfs"
	"github.com/KafScale/platform/pkg/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	headerTopic     = "X-Kafka-Topic"
	headerKey       = "X-Kafka-Key"
	headerPartition = "X-Kafka-Partition"
	headerChecksum  = "X-LFS-Checksum"

	// HTTP server timeout defaults (mitigate slowloris attacks)
	defaultHTTPReadTimeout    = 30 * time.Second
	defaultHTTPWriteTimeout   = 5 * time.Minute // Large uploads need more time
	defaultHTTPIdleTimeout    = 60 * time.Second
	defaultHTTPMaxHeaderBytes = 1 << 20 // 1MB

	// Kafka topic name constraints
	maxTopicLength = 249
)

// validTopicPattern matches valid Kafka topic names (alphanumeric, dots, underscores, hyphens)
var validTopicPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

func (p *lfsProxy) startHTTPServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/lfs/produce", p.handleHTTPProduce)
	srv := &http.Server{
		Addr:           addr,
		Handler:        mux,
		ReadTimeout:    defaultHTTPReadTimeout,
		WriteTimeout:   defaultHTTPWriteTimeout,
		IdleTimeout:    defaultHTTPIdleTimeout,
		MaxHeaderBytes: defaultHTTPMaxHeaderBytes,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		p.logger.Info("lfs proxy http listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.logger.Warn("lfs proxy http server error", "error", err)
		}
	}()
}

func (p *lfsProxy) handleHTTPProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if p.httpAPIKey != "" && !p.validateHTTPAPIKey(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	topic := strings.TrimSpace(r.Header.Get(headerTopic))
	if topic == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}
	if !isValidTopicName(topic) {
		http.Error(w, "invalid topic name", http.StatusBadRequest)
		return
	}

	var keyBytes []byte
	if keyHeader := strings.TrimSpace(r.Header.Get(headerKey)); keyHeader != "" {
		decoded, err := base64.StdEncoding.DecodeString(keyHeader)
		if err != nil {
			http.Error(w, "invalid key", http.StatusBadRequest)
			return
		}
		keyBytes = decoded
	}

	partition := int32(0)
	if partitionHeader := strings.TrimSpace(r.Header.Get(headerPartition)); partitionHeader != "" {
		parsed, err := strconv.ParseInt(partitionHeader, 10, 32)
		if err != nil {
			http.Error(w, "invalid partition", http.StatusBadRequest)
			return
		}
		partition = int32(parsed)
	}

	checksum := strings.TrimSpace(r.Header.Get(headerChecksum))
	objectKey := p.buildObjectKey(topic)

	start := time.Now()
	sha256Hex, size, err := p.s3Uploader.UploadStream(r.Context(), objectKey, r.Body, p.maxBlob)
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		p.metrics.IncS3Errors()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if checksum != "" && !strings.EqualFold(checksum, sha256Hex) {
		p.metrics.IncRequests(topic, "error", "lfs")
		http.Error(w, (&lfs.ChecksumError{Expected: checksum, Actual: sha256Hex}).Error(), http.StatusBadRequest)
		return
	}

	env := lfs.Envelope{
		Version:     1,
		Bucket:      p.s3Bucket,
		Key:         objectKey,
		Size:        size,
		SHA256:      sha256Hex,
		ContentType: r.Header.Get("Content-Type"),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		ProxyID:     p.proxyID,
	}
	encoded, err := lfs.EncodeEnvelope(env)
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	record := kmsg.Record{
		TimestampDelta64: 0,
		OffsetDelta:      0,
		Key:              keyBytes,
		Value:            encoded,
	}
	batchBytes := buildRecordBatch([]kmsg.Record{record})

	produceReq := &protocol.ProduceRequest{
		Acks:      1,
		TimeoutMs: 15000,
		Topics: []protocol.ProduceTopic{{
			Name: topic,
			Partitions: []protocol.ProducePartition{{
				Partition: partition,
				Records:   batchBytes,
			}},
		}},
	}

	correlationID := int32(atomic.AddUint32(&p.corrID, 1))
	reqHeader := &protocol.RequestHeader{APIKey: protocol.APIKeyProduce, APIVersion: 9, CorrelationID: correlationID}
	payload, err := encodeProduceRequest(reqHeader, produceReq)
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	backendConn, backendAddr, err := p.connectBackend(r.Context())
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		p.trackOrphans([]orphanInfo{{Topic: topic, Key: objectKey}})
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer backendConn.Close()

	_, err = p.forwardToBackend(r.Context(), backendConn, backendAddr, payload)
	if err != nil {
		p.metrics.IncRequests(topic, "error", "lfs")
		p.trackOrphans([]orphanInfo{{Topic: topic, Key: objectKey}})
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	p.metrics.IncRequests(topic, "ok", "lfs")
	p.metrics.AddUploadBytes(size)
	p.metrics.ObserveUploadDuration(time.Since(start).Seconds())

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(env)
}

func (p *lfsProxy) validateHTTPAPIKey(r *http.Request) bool {
	if r == nil {
		return false
	}
	key := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if key == "" {
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			key = strings.TrimSpace(auth[len("bearer "):])
		}
	}
	if key == "" {
		return false
	}
	// Use constant-time comparison to prevent timing attacks
	return subtle.ConstantTimeCompare([]byte(key), []byte(p.httpAPIKey)) == 1
}

// isValidTopicName validates a Kafka topic name.
// Topics must be 1-249 characters, containing only alphanumeric, dots, underscores, or hyphens.
func isValidTopicName(topic string) bool {
	if len(topic) == 0 || len(topic) > maxTopicLength {
		return false
	}
	return validTopicPattern.MatchString(topic)
}
