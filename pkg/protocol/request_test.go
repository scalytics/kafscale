// Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

package protocol

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestParseApiVersionsRequest(t *testing.T) {
	w := newByteWriter(16)
	w.Int16(APIKeyApiVersion)
	w.Int16(0)
	w.Int32(42)
	w.NullableString(nil)

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyApiVersion || header.CorrelationID != 42 {
		t.Fatalf("unexpected header: %#v", header)
	}
	if _, ok := req.(*ApiVersionsRequest); !ok {
		t.Fatalf("expected ApiVersionsRequest got %T", req)
	}
}

func TestParseMetadataRequest(t *testing.T) {
	w := newByteWriter(64)
	w.Int16(APIKeyMetadata)
	w.Int16(0)
	w.Int32(7)
	clientID := "client-1"
	w.NullableString(&clientID)
	w.Int32(2)
	w.String("orders")
	w.String("payments")

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	metaReq, ok := req.(*MetadataRequest)
	if !ok {
		t.Fatalf("expected MetadataRequest got %T", req)
	}
	if len(metaReq.Topics) != 2 || metaReq.Topics[0] != "orders" {
		t.Fatalf("unexpected topics: %#v", metaReq.Topics)
	}
	if header.ClientID == nil || *header.ClientID != "client-1" {
		t.Fatalf("client id mismatch: %#v", header.ClientID)
	}
}

func TestParseProduceRequest(t *testing.T) {
	w := newByteWriter(128)
	w.Int16(APIKeyProduce)
	w.Int16(9)
	w.Int32(100)
	clientID := "producer-1"
	w.NullableString(&clientID)
	w.WriteTaggedFields(0)
	w.CompactNullableString(nil)
	w.Int16(1) // acks
	w.Int32(1500)
	w.CompactArrayLen(1) // topic count
	w.CompactString("orders")
	w.CompactArrayLen(1)      // partitions
	w.Int32(0)                // partition id
	batch := []byte("record") // placeholder bytes
	w.CompactBytes(batch)
	// partition tagged fields (count=1, tag=0, len=1, val=0x7f)
	w.UVarint(1)
	w.UVarint(0)
	w.UVarint(1)
	w.write([]byte{0x7f})
	w.WriteTaggedFields(0) // topic tags
	w.WriteTaggedFields(0) // request tags
	// fmt.Printf(\"% x\\n\", w.Bytes())

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyProduce {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	produceReq, ok := req.(*ProduceRequest)
	if !ok {
		t.Fatalf("expected ProduceRequest got %T", req)
	}
	if produceReq.Acks != 1 || len(produceReq.Topics) != 1 {
		t.Fatalf("produce data mismatch: %#v", produceReq)
	}
	if string(produceReq.Topics[0].Partitions[0].Records) != "record" {
		t.Fatalf("records mismatch")
	}
}

func TestParseProduceRequestInvalidCompactArray(t *testing.T) {
	w := newByteWriter(64)
	w.Int16(APIKeyProduce)
	w.Int16(9)
	w.Int32(1)
	w.NullableString(nil)
	w.WriteTaggedFields(0)
	w.CompactNullableString(nil)
	w.Int16(1)
	w.Int32(100)
	w.UVarint(0) // compact array len => null

	if _, _, err := ParseRequest(w.Bytes()); err == nil {
		t.Fatalf("expected error for null topic array")
	}
}

func TestParseProduceRequestFranzEncoding(t *testing.T) {
	req := kmsg.NewPtrProduceRequest()
	req.Version = 9
	req.Acks = 1
	req.TimeoutMillis = 1500
	topic := kmsg.NewProduceRequestTopic()
	topic.Topic = "orders"
	part := kmsg.NewProduceRequestTopicPartition()
	part.Partition = 0
	part.Records = []byte("record batch payload")
	topic.Partitions = append(topic.Partitions, part)
	req.Topics = append(req.Topics, topic)
	body := req.AppendTo(nil)

	w := newByteWriter(len(body) + 16)
	w.Int16(APIKeyProduce)
	w.Int16(9)
	w.Int32(42)
	clientID := "kgo"
	w.NullableString(&clientID)
	w.WriteTaggedFields(0)
	w.write(body)

	header, parsed, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyProduce {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	produceReq, ok := parsed.(*ProduceRequest)
	if !ok {
		t.Fatalf("expected ProduceRequest got %T", parsed)
	}
	if len(produceReq.Topics) != 1 || len(produceReq.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected partitions: %#v", produceReq.Topics)
	}
	if produceReq.Topics[0].Partitions[0].Partition != 0 {
		t.Fatalf("expected partition 0 got %d", produceReq.Topics[0].Partitions[0].Partition)
	}
	if string(produceReq.Topics[0].Partitions[0].Records) != "record batch payload" {
		t.Fatalf("records mismatch: %q", produceReq.Topics[0].Partitions[0].Records)
	}
}

func TestParseFetchRequestV13(t *testing.T) {
	var topicID [16]byte
	for i := range topicID {
		topicID[i] = byte(i + 1)
	}
	w := newByteWriter(256)
	w.Int16(APIKeyFetch)
	w.Int16(13)
	w.Int32(9)
	clientID := "client"
	w.NullableString(&clientID)
	w.WriteTaggedFields(0)
	w.Int32(0)       // replica id
	w.Int32(500)     // max wait ms
	w.Int32(1)       // min bytes
	w.Int32(1048576) // max bytes
	w.Int8(0)        // isolation level
	w.Int32(0)       // session id
	w.Int32(0)       // session epoch
	w.CompactArrayLen(1)
	w.UUID(topicID)
	w.CompactArrayLen(1)
	w.Int32(0)  // partition
	w.Int32(-1) // current leader epoch
	w.Int64(0)  // fetch offset
	w.Int32(-1) // last fetched epoch
	w.Int64(0)  // log start offset
	w.Int32(1048576)
	w.WriteTaggedFields(0) // partition tags
	w.WriteTaggedFields(0) // topic tags
	w.CompactArrayLen(0)   // forgotten topics
	w.CompactNullableString(nil)
	w.WriteTaggedFields(0) // request tags

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyFetch || header.APIVersion != 13 {
		t.Fatalf("unexpected header: %#v", header)
	}
	fetchReq, ok := req.(*FetchRequest)
	if !ok {
		t.Fatalf("expected FetchRequest got %T", req)
	}
	if len(fetchReq.Topics) != 1 {
		t.Fatalf("expected 1 topic got %d", len(fetchReq.Topics))
	}
	if fetchReq.Topics[0].TopicID != topicID {
		t.Fatalf("unexpected topic id %v", fetchReq.Topics[0].TopicID)
	}
	if fetchReq.Topics[0].Name != "" {
		t.Fatalf("expected empty topic name got %q", fetchReq.Topics[0].Name)
	}
	if len(fetchReq.Topics[0].Partitions) != 1 {
		t.Fatalf("expected 1 partition got %d", len(fetchReq.Topics[0].Partitions))
	}
}

func TestParseMetadataRequestV12TaggedFields(t *testing.T) {
	w := newByteWriter(128)
	w.Int16(APIKeyMetadata)
	w.Int16(12)
	w.Int32(42)
	clientID := "kgo"
	w.NullableString(&clientID)
	w.WriteTaggedFields(0)
	w.CompactArrayLen(2)
	w.UUID([16]byte{})
	w.CompactNullableString(strPtr("orders-0"))
	w.WriteTaggedFields(0)
	w.UUID([16]byte{})
	w.CompactNullableString(strPtr("orders-1"))
	w.WriteTaggedFields(0)
	w.Bool(true)
	w.Bool(false)
	w.WriteTaggedFields(0)

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyMetadata || header.APIVersion != 12 {
		t.Fatalf("unexpected header: %#v", header)
	}
	metaReq, ok := req.(*MetadataRequest)
	if !ok {
		t.Fatalf("expected MetadataRequest got %T", req)
	}
	if len(metaReq.Topics) != 2 {
		t.Fatalf("expected 2 topics got %d", len(metaReq.Topics))
	}
	if !metaReq.AllowAutoTopicCreation {
		t.Fatalf("expected allow auto topic creation true")
	}
	if metaReq.IncludeClusterAuthOps || metaReq.IncludeTopicAuthOps {
		t.Fatalf("expected auth ops false")
	}
}

func TestParseMetadataRequestFranzEncoding(t *testing.T) {
	req := kmsg.NewPtrMetadataRequest()
	req.Version = 12
	req.AllowAutoTopicCreation = true
	req.IncludeTopicAuthorizedOperations = false
	req.Topics = []kmsg.MetadataRequestTopic{
		{Topic: strPtr("orders-3eb53935-0")},
	}

	formatter := kmsg.NewRequestFormatter(kmsg.FormatterClientID("kgo"))
	payload := formatter.AppendRequest(nil, req, 1)
	payload = payload[4:] // drop the length prefix to match ParseRequest input

	header, parsed, err := ParseRequest(payload)
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyMetadata || header.APIVersion != 12 {
		t.Fatalf("unexpected header: %#v", header)
	}
	metaReq, ok := parsed.(*MetadataRequest)
	if !ok {
		t.Fatalf("expected MetadataRequest got %T", parsed)
	}
	if len(metaReq.Topics) != 1 || metaReq.Topics[0] != "orders-3eb53935-0" {
		t.Fatalf("unexpected topics: %#v", metaReq.Topics)
	}
	if !metaReq.AllowAutoTopicCreation {
		t.Fatalf("expected allow auto topic creation true")
	}
	if metaReq.IncludeClusterAuthOps || metaReq.IncludeTopicAuthOps {
		t.Fatalf("expected auth ops false")
	}
}

func TestParseFindCoordinatorFlexible(t *testing.T) {
	req := kmsg.NewPtrFindCoordinatorRequest()
	req.Version = 3
	req.CoordinatorKey = "franz-e2e-consumer"
	body := req.AppendTo(nil)

	w := newByteWriter(len(body) + 16)
	w.Int16(APIKeyFindCoordinator)
	w.Int16(3)
	w.Int32(1)
	clientID := "kgo"
	w.NullableString(&clientID)
	w.WriteTaggedFields(0)
	w.write(body)

	header, parsed, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyFindCoordinator {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	findReq, ok := parsed.(*FindCoordinatorRequest)
	if !ok {
		t.Fatalf("expected FindCoordinatorRequest got %T", parsed)
	}
	if findReq.Key != "franz-e2e-consumer" {
		t.Fatalf("unexpected coordinator key %q", findReq.Key)
	}
	if findReq.KeyType != 0 {
		t.Fatalf("unexpected key type %d", findReq.KeyType)
	}
}

func TestParseOffsetCommitRequestV3(t *testing.T) {
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Version = 3
	req.Group = "group-1"
	req.Generation = 4
	req.MemberID = "member-1"
	req.RetentionTimeMillis = 60000
	topic := kmsg.NewOffsetCommitRequestTopic()
	topic.Topic = "orders"
	part := kmsg.NewOffsetCommitRequestTopicPartition()
	part.Partition = 0
	part.Offset = 100
	meta := "checkpoint"
	part.Metadata = &meta
	topic.Partitions = append(topic.Partitions, part)
	req.Topics = append(req.Topics, topic)
	body := req.AppendTo(nil)

	w := newByteWriter(len(body) + 16)
	w.Int16(APIKeyOffsetCommit)
	w.Int16(3)
	w.Int32(7)
	clientID := "kgo"
	w.NullableString(&clientID)
	w.write(body)

	header, parsed, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyOffsetCommit {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	commitReq, ok := parsed.(*OffsetCommitRequest)
	if !ok {
		t.Fatalf("expected OffsetCommitRequest got %T", parsed)
	}
	if commitReq.GroupID != "group-1" || commitReq.GenerationID != 4 {
		t.Fatalf("unexpected group data: %#v", commitReq)
	}
	if len(commitReq.Topics) != 1 || len(commitReq.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected partitions: %#v", commitReq.Topics)
	}
	if got := commitReq.Topics[0].Partitions[0]; got.Offset != 100 || got.Metadata != "checkpoint" {
		t.Fatalf("unexpected partition data: %#v", got)
	}
}

func TestParseSyncGroupFlexible(t *testing.T) {
	req := kmsg.NewPtrSyncGroupRequest()
	req.Version = 4
	req.Group = "franz-e2e-consumer"
	req.Generation = 1
	req.MemberID = "member-1"
	req.GroupAssignment = []kmsg.SyncGroupRequestGroupAssignment{
		{
			MemberID:         "member-1",
			MemberAssignment: []byte{0x00, 0x01},
		},
	}
	body := req.AppendTo(nil)

	w := newByteWriter(len(body) + 16)
	w.Int16(APIKeySyncGroup)
	w.Int16(4)
	w.Int32(9)
	clientID := "kgo"
	w.NullableString(&clientID)
	w.WriteTaggedFields(0)
	w.write(body)

	header, parsed, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeySyncGroup {
		t.Fatalf("unexpected api key %d", header.APIKey)
	}
	syncReq, ok := parsed.(*SyncGroupRequest)
	if !ok {
		t.Fatalf("expected SyncGroupRequest got %T", parsed)
	}
	if syncReq.GroupID != "franz-e2e-consumer" {
		t.Fatalf("unexpected group id %q", syncReq.GroupID)
	}
	if len(syncReq.Assignments) != 1 || syncReq.Assignments[0].MemberID != "member-1" {
		t.Fatalf("unexpected assignments %#v", syncReq.Assignments)
	}
	if len(syncReq.Assignments[0].Assignment) != 2 {
		t.Fatalf("unexpected assignment payload")
	}
}

func TestParseFetchRequest(t *testing.T) {
	w := newByteWriter(128)
	w.Int16(APIKeyFetch)
	w.Int16(11)
	w.Int32(9) // correlation
	clientID := "consumer"
	w.NullableString(&clientID)
	w.Int32(1) // replica id
	w.Int32(0) // max wait
	w.Int32(0) // min bytes
	w.Int32(1024)
	w.Int8(0)
	w.Int32(0) // session id
	w.Int32(0) // session epoch
	w.Int32(1) // topic count
	w.String("orders")
	w.Int32(1) // partition count
	w.Int32(0) // partition
	w.Int32(0) // leader epoch
	w.Int64(0) // fetch offset
	w.Int64(0) // log start offset
	w.Int32(1024)
	w.Int32(0) // forgotten topics count
	w.NullableString(nil)

	header, req, err := ParseRequest(w.Bytes())
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	if header.APIKey != APIKeyFetch {
		t.Fatalf("expected fetch api key got %d", header.APIKey)
	}
	fetchReq, ok := req.(*FetchRequest)
	if !ok {
		t.Fatalf("expected FetchRequest got %T", req)
	}
	if len(fetchReq.Topics) != 1 || len(fetchReq.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected fetch data: %#v", fetchReq.Topics)
	}
}
