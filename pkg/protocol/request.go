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
	"fmt"
)

// RequestHeader matches Kafka RequestHeader v1 (simplified without tagged fields).
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      *string
}

// Request is implemented by concrete protocol requests.
type Request interface {
	APIKey() int16
}

// ApiVersionsRequest describes the ApiVersions call.
type ApiVersionsRequest struct{}

func (ApiVersionsRequest) APIKey() int16 { return APIKeyApiVersion }

// ProduceRequest is a simplified representation of Kafka ProduceRequest v9.
type ProduceRequest struct {
	Acks            int16
	TimeoutMs       int32
	TransactionalID *string
	Topics          []ProduceTopic
}

type ProduceTopic struct {
	Name       string
	Partitions []ProducePartition
}

type ProducePartition struct {
	Partition int32
	Records   []byte
}

func (ProduceRequest) APIKey() int16 { return APIKeyProduce }

// FetchRequest represents a subset of Kafka FetchRequest v13.
type FetchRequest struct {
	ReplicaID      int32
	MaxWaitMs      int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	SessionID      int32
	SessionEpoch   int32
	Topics         []FetchTopicRequest
}

type FetchTopicRequest struct {
	Name       string
	TopicID    [16]byte
	Partitions []FetchPartitionRequest
}

type FetchPartitionRequest struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

func (FetchRequest) APIKey() int16 { return APIKeyFetch }

// MetadataRequest asks for cluster metadata. Empty Topics means "all".
type MetadataRequest struct {
	Topics                 []string
	TopicIDs               [][16]byte
	AllowAutoTopicCreation bool
	IncludeClusterAuthOps  bool
	IncludeTopicAuthOps    bool
}

func (MetadataRequest) APIKey() int16 { return APIKeyMetadata }

type CreateTopicConfig struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

type CreateTopicsRequest struct {
	Topics []CreateTopicConfig
}

func (CreateTopicsRequest) APIKey() int16 { return APIKeyCreateTopics }

type DeleteTopicsRequest struct {
	TopicNames []string
}

func (DeleteTopicsRequest) APIKey() int16 { return APIKeyDeleteTopics }

type ListOffsetsPartition struct {
	Partition     int32
	Timestamp     int64
	MaxNumOffsets int32
}

type ListOffsetsTopic struct {
	Name       string
	Partitions []ListOffsetsPartition
}

type ListOffsetsRequest struct {
	ReplicaID int32
	Topics    []ListOffsetsTopic
}

func (ListOffsetsRequest) APIKey() int16 { return APIKeyListOffsets }

// FindCoordinatorRequest targets a group coordinator lookup.
type FindCoordinatorRequest struct {
	KeyType int8
	Key     string
}

func (FindCoordinatorRequest) APIKey() int16 { return APIKeyFindCoordinator }

type JoinGroupProtocol struct {
	Name     string
	Metadata []byte
}

type JoinGroupRequest struct {
	GroupID            string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	MemberID           string
	ProtocolType       string
	Protocols          []JoinGroupProtocol
}

func (JoinGroupRequest) APIKey() int16 { return APIKeyJoinGroup }

type SyncGroupAssignment struct {
	MemberID   string
	Assignment []byte
}

type SyncGroupRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	Assignments  []SyncGroupAssignment
}

func (SyncGroupRequest) APIKey() int16 { return APIKeySyncGroup }

type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	InstanceID   *string
}

func (HeartbeatRequest) APIKey() int16 { return APIKeyHeartbeat }

type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

func (LeaveGroupRequest) APIKey() int16 { return APIKeyLeaveGroup }

type OffsetCommitPartition struct {
	Partition int32
	Offset    int64
	Metadata  string
}

type OffsetCommitTopic struct {
	Name       string
	Partitions []OffsetCommitPartition
}

type OffsetCommitRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	RetentionMs  int64
	Topics       []OffsetCommitTopic
}

func (OffsetCommitRequest) APIKey() int16 { return APIKeyOffsetCommit }

type OffsetFetchPartition struct {
	Partition int32
}

type OffsetFetchTopic struct {
	Name       string
	Partitions []OffsetFetchPartition
}

type OffsetFetchRequest struct {
	GroupID string
	Topics  []OffsetFetchTopic
}

func (OffsetFetchRequest) APIKey() int16 { return APIKeyOffsetFetch }

func isFlexibleRequest(apiKey, version int16) bool {
	switch apiKey {
	case APIKeyProduce:
		return version >= 9
	case APIKeyMetadata:
		return version >= 9
	case APIKeyFetch:
		return version >= 12
	case APIKeyFindCoordinator:
		return version >= 3
	case APIKeySyncGroup:
		return version >= 4
	case APIKeyHeartbeat:
		return version >= 4
	default:
		return false
	}
}

func compactArrayLenNonNull(r *byteReader) (int32, error) {
	n, err := r.CompactArrayLen()
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("compact array is null")
	}
	return n, nil
}

// ParseRequestHeader decodes the header portion from raw bytes.
func ParseRequestHeader(b []byte) (*RequestHeader, *byteReader, error) {
	reader := newByteReader(b)
	apiKey, err := reader.Int16()
	if err != nil {
		return nil, nil, fmt.Errorf("read api key: %w", err)
	}
	version, err := reader.Int16()
	if err != nil {
		return nil, nil, fmt.Errorf("read api version: %w", err)
	}
	correlationID, err := reader.Int32()
	if err != nil {
		return nil, nil, fmt.Errorf("read correlation id: %w", err)
	}
	clientID, err := reader.NullableString()
	if err != nil {
		return nil, nil, fmt.Errorf("read client id: %w", err)
	}
	if isFlexibleRequest(apiKey, version) {
		if err := reader.SkipTaggedFields(); err != nil {
			return nil, nil, fmt.Errorf("skip header tags: %w", err)
		}
	}
	return &RequestHeader{
		APIKey:        apiKey,
		APIVersion:    version,
		CorrelationID: correlationID,
		ClientID:      clientID,
	}, reader, nil
}

// ParseRequest decodes a request header and body from bytes.
func ParseRequest(b []byte) (*RequestHeader, Request, error) {
	header, reader, err := ParseRequestHeader(b)
	if err != nil {
		return nil, nil, err
	}
	flexible := isFlexibleRequest(header.APIKey, header.APIVersion)

	var req Request
	switch header.APIKey {
	case APIKeyApiVersion:
		req = &ApiVersionsRequest{}
	case APIKeyProduce:
		var transactionalID *string
		var err error
		if header.APIVersion >= 3 {
			if flexible {
				transactionalID, err = reader.CompactNullableString()
			} else {
				transactionalID, err = reader.NullableString()
			}
			if err != nil {
				return nil, nil, fmt.Errorf("read produce transactional id: %w", err)
			}
		}
		acks, err := reader.Int16()
		if err != nil {
			return nil, nil, fmt.Errorf("read produce acks: %w", err)
		}
		timeout, err := reader.Int32()
		if err != nil {
			return nil, nil, fmt.Errorf("read produce timeout: %w", err)
		}
		var topicCount int32
		if flexible {
			topicCount, err = compactArrayLenNonNull(reader)
		} else {
			topicCount, err = reader.Int32()
			if topicCount < 0 {
				return nil, nil, fmt.Errorf("read produce topic count: invalid %d", topicCount)
			}
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read produce topic count: %w", err)
		}
		topics := make([]ProduceTopic, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			var name string
			if flexible {
				name, err = reader.CompactString()
			} else {
				name, err = reader.String()
			}
			if err != nil {
				return nil, nil, fmt.Errorf("read produce topic name: %w", err)
			}
			var partitionCount int32
			if flexible {
				partitionCount, err = compactArrayLenNonNull(reader)
			} else {
				partitionCount, err = reader.Int32()
				if partitionCount < 0 {
					return nil, nil, fmt.Errorf("read produce partition count: invalid %d", partitionCount)
				}
			}
			if err != nil {
				return nil, nil, fmt.Errorf("read produce partition count: %w", err)
			}
			partitions := make([]ProducePartition, 0, partitionCount)
			for j := int32(0); j < partitionCount; j++ {
				index, err := reader.Int32()
				if err != nil {
					return nil, nil, fmt.Errorf("read produce partition index: %w", err)
				}
				var records []byte
				if flexible {
					records, err = reader.CompactBytes()
				} else {
					records, err = reader.Bytes()
				}
				if err != nil {
					return nil, nil, fmt.Errorf("read produce records: %w", err)
				}
				partitions = append(partitions, ProducePartition{
					Partition: index,
					Records:   records,
				})
			}
			if flexible {
				if err := reader.SkipTaggedFields(); err != nil {
					return nil, nil, fmt.Errorf("skip partition tags: %w", err)
				}
			}
			if flexible {
				if err := reader.SkipTaggedFields(); err != nil {
					return nil, nil, fmt.Errorf("skip topic tags: %w", err)
				}
			}
			topics = append(topics, ProduceTopic{Name: name, Partitions: partitions})
		}
		if flexible {
			if err := reader.SkipTaggedFields(); err != nil {
				return nil, nil, fmt.Errorf("skip produce tags: %w", err)
			}
		}
		req = &ProduceRequest{
			Acks:            acks,
			TimeoutMs:       timeout,
			TransactionalID: transactionalID,
			Topics:          topics,
		}
	case APIKeyMetadata:
		var topics []string
		var topicIDs [][16]byte
		var count int32
		var err error
		if flexible {
			count, err = reader.CompactArrayLen()
		} else {
			count, err = reader.Int32()
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read metadata topic count: %w", err)
		}
		if count >= 0 {
			topics = make([]string, 0, count)
			topicIDs = make([][16]byte, 0, count)
			for i := int32(0); i < count; i++ {
				if header.APIVersion >= 10 {
					id, err := reader.UUID()
					if err != nil {
						return nil, nil, fmt.Errorf("read metadata topic[%d] id: %w", i, err)
					}
					var namePtr *string
					if flexible {
						namePtr, err = reader.CompactNullableString()
					} else {
						namePtr, err = reader.NullableString()
					}
					if err != nil {
						return nil, nil, fmt.Errorf("read metadata topic[%d] name: %w", i, err)
					}
					if namePtr != nil {
						topics = append(topics, *namePtr)
					}
					topicIDs = append(topicIDs, id)
					if flexible {
						if err := reader.SkipTaggedFields(); err != nil {
							return nil, nil, fmt.Errorf("skip metadata topic[%d] tags: %w", i, err)
						}
					}
				} else {
					var name string
					if flexible {
						name, err = reader.CompactString()
					} else {
						name, err = reader.String()
					}
					if err != nil {
						return nil, nil, fmt.Errorf("read metadata topic[%d]: %w", i, err)
					}
					topics = append(topics, name)
					if flexible {
						if err := reader.SkipTaggedFields(); err != nil {
							return nil, nil, fmt.Errorf("skip metadata topic[%d] tags: %w", i, err)
						}
					}
				}
			}
		}
		allowAutoTopicCreation := true
		if header.APIVersion >= 4 {
			if allowAutoTopicCreation, err = reader.Bool(); err != nil {
				return nil, nil, fmt.Errorf("read metadata allow auto topic creation: %w", err)
			}
		}
		includeClusterAuthOps := false
		includeTopicAuthOps := false
		if header.APIVersion >= 8 && header.APIVersion <= 10 {
			if includeClusterAuthOps, err = reader.Bool(); err != nil {
				return nil, nil, fmt.Errorf("read metadata include cluster auth ops: %w", err)
			}
		}
		if header.APIVersion >= 8 {
			if includeTopicAuthOps, err = reader.Bool(); err != nil {
				return nil, nil, fmt.Errorf("read metadata include topic auth ops: %w", err)
			}
		}
		if flexible {
			if err := reader.SkipTaggedFields(); err != nil {
				return nil, nil, fmt.Errorf("skip metadata tags: %w", err)
			}
		}
		req = &MetadataRequest{
			Topics:                 topics,
			TopicIDs:               topicIDs,
			AllowAutoTopicCreation: allowAutoTopicCreation,
			IncludeClusterAuthOps:  includeClusterAuthOps,
			IncludeTopicAuthOps:    includeTopicAuthOps,
		}
	case APIKeyCreateTopics:
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		configs := make([]CreateTopicConfig, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partitions, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			repl, err := reader.Int16()
			if err != nil {
				return nil, nil, err
			}
			// Configs map (ignored)
			cfgCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			for j := int32(0); j < cfgCount; j++ {
				if _, err := reader.String(); err != nil {
					return nil, nil, err
				}
				if _, err := reader.String(); err != nil {
					return nil, nil, err
				}
			}
			configs = append(configs, CreateTopicConfig{Name: name, NumPartitions: partitions, ReplicationFactor: repl})
		}
		req = &CreateTopicsRequest{Topics: configs}
	case APIKeyDeleteTopics:
		count, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		names := make([]string, 0, count)
		for i := int32(0); i < count; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			names = append(names, name)
		}
		req = &DeleteTopicsRequest{TopicNames: names}
	case APIKeyListOffsets:
		replicaID, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topics := make([]ListOffsetsTopic, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			parts := make([]ListOffsetsPartition, 0, partCount)
			for j := int32(0); j < partCount; j++ {
				partition, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				timestamp, err := reader.Int64()
				if err != nil {
					return nil, nil, err
				}
				maxOffsets := int32(1)
				if header.APIVersion == 0 {
					maxOffsets, err = reader.Int32()
					if err != nil {
						return nil, nil, err
					}
				}
				parts = append(parts, ListOffsetsPartition{
					Partition:     partition,
					Timestamp:     timestamp,
					MaxNumOffsets: maxOffsets,
				})
			}
			topics = append(topics, ListOffsetsTopic{Name: name, Partitions: parts})
		}
		req = &ListOffsetsRequest{ReplicaID: replicaID, Topics: topics}
	case APIKeyFetch:
		version := header.APIVersion
		replicaID, err := reader.Int32()
		if err != nil {
			return nil, nil, fmt.Errorf("read fetch replica id: %w", err)
		}
		maxWaitMs, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		minBytes, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		var maxBytes int32
		if version >= 3 {
			maxBytes, err = reader.Int32()
			if err != nil {
				return nil, nil, err
			}
		}
		isolationLevel := int8(0)
		if version >= 4 {
			if isolationLevel, err = reader.Int8(); err != nil {
				return nil, nil, err
			}
		}
		sessionID := int32(0)
		sessionEpoch := int32(0)
		if version >= 7 {
			if sessionID, err = reader.Int32(); err != nil {
				return nil, nil, err
			}
			if sessionEpoch, err = reader.Int32(); err != nil {
				return nil, nil, err
			}
		}
		var topicCount int32
		if flexible {
			topicCount, err = compactArrayLenNonNull(reader)
		} else {
			topicCount, err = reader.Int32()
			if topicCount < 0 {
				return nil, nil, fmt.Errorf("fetch topic count invalid %d", topicCount)
			}
		}
		if err != nil {
			return nil, nil, err
		}

		topics := make([]FetchTopicRequest, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			var (
				name    string
				topicID [16]byte
			)
			if version >= 12 {
				topicID, err = reader.UUID()
				if err != nil {
					return nil, nil, err
				}
			} else {
				if flexible {
					name, err = reader.CompactString()
				} else {
					name, err = reader.String()
				}
				if err != nil {
					return nil, nil, err
				}
			}
			var partCount int32
			if flexible {
				partCount, err = compactArrayLenNonNull(reader)
			} else {
				partCount, err = reader.Int32()
				if partCount < 0 {
					return nil, nil, fmt.Errorf("fetch partition count invalid %d", partCount)
				}
			}
			if err != nil {
				return nil, nil, err
			}
			partitions := make([]FetchPartitionRequest, 0, partCount)
			for j := int32(0); j < partCount; j++ {
				partitionID, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				if version >= 9 {
					if _, err := reader.Int32(); err != nil { // leader epoch
						return nil, nil, err
					}
				}
				fetchOffset, err := reader.Int64()
				if err != nil {
					return nil, nil, err
				}
				if version >= 12 {
					if _, err := reader.Int32(); err != nil { // last fetched epoch
						return nil, nil, err
					}
				}
				if version >= 5 {
					if _, err := reader.Int64(); err != nil { // log start offset
						return nil, nil, err
					}
				}
				maxBytes, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				partitions = append(partitions, FetchPartitionRequest{
					Partition:   partitionID,
					FetchOffset: fetchOffset,
					MaxBytes:    maxBytes,
				})
				if flexible {
					if err := reader.SkipTaggedFields(); err != nil {
						return nil, nil, fmt.Errorf("skip fetch partition tags: %w", err)
					}
				}
			}
			topics = append(topics, FetchTopicRequest{
				Name:       name,
				TopicID:    topicID,
				Partitions: partitions,
			})
			if flexible {
				if err := reader.SkipTaggedFields(); err != nil {
					return nil, nil, fmt.Errorf("skip fetch topic tags: %w", err)
				}
			}
		}
		if version >= 7 {
			var forgottenCount int32
			if flexible {
				forgottenCount, err = reader.CompactArrayLen()
			} else {
				forgottenCount, err = reader.Int32()
			}
			if err != nil {
				return nil, nil, fmt.Errorf("read forgotten topics count: %w", err)
			}
			if forgottenCount > 0 {
				for i := int32(0); i < forgottenCount; i++ {
					if version >= 12 {
						if _, err := reader.UUID(); err != nil {
							return nil, nil, fmt.Errorf("read forgotten topic id: %w", err)
						}
					} else {
						if _, err := reader.String(); err != nil {
							return nil, nil, fmt.Errorf("read forgotten topic name: %w", err)
						}
					}
					var partCount int32
					if flexible {
						partCount, err = reader.CompactArrayLen()
					} else {
						partCount, err = reader.Int32()
					}
					if err != nil {
						return nil, nil, fmt.Errorf("read forgotten partitions: %w", err)
					}
					for j := int32(0); j < partCount; j++ {
						if _, err := reader.Int32(); err != nil {
							return nil, nil, fmt.Errorf("read forgotten partition: %w", err)
						}
					}
					if flexible {
						if err := reader.SkipTaggedFields(); err != nil {
							return nil, nil, fmt.Errorf("skip forgotten topic tags: %w", err)
						}
					}
				}
			}
		}
		if version >= 11 {
			if flexible {
				if _, err := reader.CompactNullableString(); err != nil {
					return nil, nil, fmt.Errorf("read rack id: %w", err)
				}
			} else {
				if _, err := reader.NullableString(); err != nil {
					return nil, nil, fmt.Errorf("read rack id: %w", err)
				}
			}
		}
		if flexible {
			if err := reader.SkipTaggedFields(); err != nil {
				return nil, nil, fmt.Errorf("skip fetch request tags: %w", err)
			}
		}
		req = &FetchRequest{
			ReplicaID:      replicaID,
			MaxWaitMs:      maxWaitMs,
			MinBytes:       minBytes,
			MaxBytes:       maxBytes,
			IsolationLevel: isolationLevel,
			SessionID:      sessionID,
			SessionEpoch:   sessionEpoch,
			Topics:         topics,
		}
	case APIKeyFindCoordinator:
		var key string
		if flexible {
			key, err = reader.CompactString()
		} else {
			key, err = reader.String()
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read coordinator key: %w", err)
		}
		var keyType int8
		if header.APIVersion >= 1 {
			if keyType, err = reader.Int8(); err != nil {
				return nil, nil, fmt.Errorf("read coordinator key type: %w", err)
			}
		}
		if flexible {
			if err := reader.SkipTaggedFields(); err != nil {
				return nil, nil, fmt.Errorf("skip coordinator tags: %w", err)
			}
		}
		req = &FindCoordinatorRequest{KeyType: keyType, Key: key}
	case APIKeyJoinGroup:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		sessionTimeout, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		rebalanceTimeout, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		memberID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		protocolType, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		protocolCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		protocols := make([]JoinGroupProtocol, 0, protocolCount)
		for i := int32(0); i < protocolCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			meta, err := reader.Bytes()
			if err != nil {
				return nil, nil, err
			}
			protocols = append(protocols, JoinGroupProtocol{Name: name, Metadata: meta})
		}
		req = &JoinGroupRequest{
			GroupID:            groupID,
			SessionTimeoutMs:   sessionTimeout,
			RebalanceTimeoutMs: rebalanceTimeout,
			MemberID:           memberID,
			ProtocolType:       protocolType,
			Protocols:          protocols,
		}
	case APIKeySyncGroup:
		var groupID string
		var err error
		if flexible {
			groupID, err = reader.CompactString()
		} else {
			groupID, err = reader.String()
		}
		if err != nil {
			return nil, nil, err
		}
		generationID, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		var memberID string
		if flexible {
			memberID, err = reader.CompactString()
		} else {
			memberID, err = reader.String()
		}
		if err != nil {
			return nil, nil, err
		}
		if header.APIVersion >= 3 {
			if flexible {
				if _, err := reader.CompactNullableString(); err != nil {
					return nil, nil, err
				}
			} else {
				if _, err := reader.NullableString(); err != nil {
					return nil, nil, err
				}
			}
		}
		if header.APIVersion >= 5 {
			if flexible {
				if _, err := reader.CompactNullableString(); err != nil {
					return nil, nil, err
				}
				if _, err := reader.CompactNullableString(); err != nil {
					return nil, nil, err
				}
			} else {
				if _, err := reader.NullableString(); err != nil {
					return nil, nil, err
				}
				if _, err := reader.NullableString(); err != nil {
					return nil, nil, err
				}
			}
		}
		var assignCount int32
		if flexible {
			if assignCount, err = compactArrayLenNonNull(reader); err != nil {
				return nil, nil, err
			}
		} else {
			assignCount, err = reader.Int32()
			if err != nil {
				return nil, nil, err
			}
		}
		assignments := make([]SyncGroupAssignment, 0, assignCount)
		for i := int32(0); i < assignCount; i++ {
			var mid string
			if flexible {
				mid, err = reader.CompactString()
			} else {
				mid, err = reader.String()
			}
			if err != nil {
				return nil, nil, err
			}
			var data []byte
			if flexible {
				data, err = reader.CompactBytes()
			} else {
				data, err = reader.Bytes()
			}
			if err != nil {
				return nil, nil, err
			}
			if flexible {
				if err := reader.SkipTaggedFields(); err != nil {
					return nil, nil, fmt.Errorf("skip sync assignment tags: %w", err)
				}
			}
			assignments = append(assignments, SyncGroupAssignment{MemberID: mid, Assignment: data})
		}
		if flexible {
			if err := reader.SkipTaggedFields(); err != nil {
				return nil, nil, fmt.Errorf("skip sync group tags: %w", err)
			}
		}
		req = &SyncGroupRequest{
			GroupID:      groupID,
			GenerationID: generationID,
			MemberID:     memberID,
			Assignments:  assignments,
		}
	case APIKeyHeartbeat:
		var err error
		var groupID string
		if flexible {
			groupID, err = reader.CompactString()
		} else {
			groupID, err = reader.String()
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read heartbeat group id: %w", err)
		}
		generationID, err := reader.Int32()
		if err != nil {
			return nil, nil, fmt.Errorf("read heartbeat generation: %w", err)
		}
		var memberID string
		if flexible {
			memberID, err = reader.CompactString()
		} else {
			memberID, err = reader.String()
		}
		if err != nil {
			return nil, nil, fmt.Errorf("read heartbeat member id: %w", err)
		}
		var instanceID *string
		if header.APIVersion >= 3 {
			if flexible {
				instanceID, err = reader.CompactNullableString()
			} else {
				instanceID, err = reader.NullableString()
			}
			if err != nil {
				return nil, nil, fmt.Errorf("read heartbeat group instance id: %w", err)
			}
		}
		if flexible {
			if err := reader.SkipTaggedFields(); err != nil {
				return nil, nil, fmt.Errorf("skip heartbeat tags: %w", err)
			}
		}
		req = &HeartbeatRequest{
			GroupID:      groupID,
			GenerationID: generationID,
			MemberID:     memberID,
			InstanceID:   instanceID,
		}
	case APIKeyLeaveGroup:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		memberID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		req = &LeaveGroupRequest{
			GroupID:  groupID,
			MemberID: memberID,
		}
	case APIKeyOffsetCommit:
		version := header.APIVersion
		if version != 3 {
			return nil, nil, fmt.Errorf("offset commit version %d not supported", version)
		}
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		generationID, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		memberID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		var retentionMs int64
		if version >= 2 && version <= 4 {
			retentionMs, err = reader.Int64()
			if err != nil {
				return nil, nil, err
			}
		}
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topics := make([]OffsetCommitTopic, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			partitions := make([]OffsetCommitPartition, 0, partCount)
			for j := int32(0); j < partCount; j++ {
				partition, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				offset, err := reader.Int64()
				if err != nil {
					return nil, nil, err
				}
				metaPtr, err := reader.NullableString()
				if err != nil {
					return nil, nil, err
				}
				meta := ""
				if metaPtr != nil {
					meta = *metaPtr
				}
				partitions = append(partitions, OffsetCommitPartition{
					Partition: partition,
					Offset:    offset,
					Metadata:  meta,
				})
			}
			topics = append(topics, OffsetCommitTopic{Name: name, Partitions: partitions})
		}
		req = &OffsetCommitRequest{
			GroupID:      groupID,
			GenerationID: generationID,
			MemberID:     memberID,
			RetentionMs:  retentionMs,
			Topics:       topics,
		}
	case APIKeyOffsetFetch:
		groupID, err := reader.String()
		if err != nil {
			return nil, nil, err
		}
		topicCount, err := reader.Int32()
		if err != nil {
			return nil, nil, err
		}
		topics := make([]OffsetFetchTopic, 0, topicCount)
		for i := int32(0); i < topicCount; i++ {
			name, err := reader.String()
			if err != nil {
				return nil, nil, err
			}
			partCount, err := reader.Int32()
			if err != nil {
				return nil, nil, err
			}
			partitions := make([]OffsetFetchPartition, 0, partCount)
			for j := int32(0); j < partCount; j++ {
				partition, err := reader.Int32()
				if err != nil {
					return nil, nil, err
				}
				partitions = append(partitions, OffsetFetchPartition{Partition: partition})
			}
			topics = append(topics, OffsetFetchTopic{Name: name, Partitions: partitions})
		}
		req = &OffsetFetchRequest{
			GroupID: groupID,
			Topics:  topics,
		}
	default:
		return nil, nil, fmt.Errorf("unsupported api key %d", header.APIKey)
	}

	return header, req, nil
}
