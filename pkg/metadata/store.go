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

package metadata

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	metadatapb "github.com/novatechflow/kafscale/pkg/gen/metadata"
	"github.com/novatechflow/kafscale/pkg/protocol"
)

// Store exposes read-only access to cluster metadata used by Kafka protocol handlers.
type Store interface {
	// Metadata returns brokers, controller ID, and topics. When topics is non-empty,
	// the implementation should filter to that subset and omit missing topics.
	Metadata(ctx context.Context, topics []string) (*ClusterMetadata, error)
	// NextOffset returns the next offset to assign for a topic/partition.
	NextOffset(ctx context.Context, topic string, partition int32) (int64, error)
	// UpdateOffsets records the last persisted offset so future appends continue from there.
	UpdateOffsets(ctx context.Context, topic string, partition int32, lastOffset int64) error
	// CommitConsumerOffset persists a consumer group offset.
	CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error
	// FetchConsumerOffset retrieves the committed offset for a consumer group partition.
	FetchConsumerOffset(ctx context.Context, group, topic string, partition int32) (int64, string, error)
	// PutConsumerGroup persists consumer group metadata.
	PutConsumerGroup(ctx context.Context, group *metadatapb.ConsumerGroup) error
	// FetchConsumerGroup retrieves consumer group metadata.
	FetchConsumerGroup(ctx context.Context, groupID string) (*metadatapb.ConsumerGroup, error)
	// DeleteConsumerGroup removes consumer group metadata.
	DeleteConsumerGroup(ctx context.Context, groupID string) error
	// CreateTopic creates a new topic with the provided specification.
	CreateTopic(ctx context.Context, spec TopicSpec) (*protocol.MetadataTopic, error)
	// DeleteTopic removes a topic and associated offsets.
	DeleteTopic(ctx context.Context, name string) error
}

// TopicSpec describes a topic creation request.
type TopicSpec struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

var (
	// ErrTopicExists indicates the topic is already present.
	ErrTopicExists = errors.New("topic already exists")
	// ErrInvalidTopic indicates the topic specification is invalid.
	ErrInvalidTopic = errors.New("invalid topic configuration")
	// ErrUnknownTopic indicates the topic does not exist.
	ErrUnknownTopic = errors.New("unknown topic")
)

// ClusterMetadata describes the Kafka-visible cluster state.
type ClusterMetadata struct {
	Brokers      []protocol.MetadataBroker
	ControllerID int32
	Topics       []protocol.MetadataTopic
	ClusterID    *string
}

// InMemoryStore is a simple Store backed by in-process state. Useful for early development and tests.
type InMemoryStore struct {
	mu              sync.RWMutex
	state           ClusterMetadata
	offsets         map[string]int64
	consumerOffsets map[string]int64
	consumerMeta    map[string]string
	consumerGroups  map[string]*metadatapb.ConsumerGroup
}

// NewInMemoryStore builds an in-memory metadata store with the provided state.
func NewInMemoryStore(state ClusterMetadata) *InMemoryStore {
	return &InMemoryStore{
		state:           cloneMetadata(state),
		offsets:         make(map[string]int64),
		consumerOffsets: make(map[string]int64),
		consumerMeta:    make(map[string]string),
		consumerGroups:  make(map[string]*metadatapb.ConsumerGroup),
	}
}

// Update swaps the cluster metadata atomically.
func (s *InMemoryStore) Update(state ClusterMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = cloneMetadata(state)
}

// Metadata implements Store.
func (s *InMemoryStore) Metadata(ctx context.Context, topics []string) (*ClusterMetadata, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	state := cloneMetadata(s.state)
	if len(topics) == 0 {
		return &state, nil
	}

	filtered := filterTopics(state.Topics, topics)
	state.Topics = filtered
	return &state, nil
}

func filterTopics(all []protocol.MetadataTopic, requested []string) []protocol.MetadataTopic {
	if len(requested) == 0 {
		return cloneTopics(all)
	}
	index := make(map[string]protocol.MetadataTopic, len(all))
	for _, topic := range all {
		index[topic.Name] = topic
	}
	result := make([]protocol.MetadataTopic, 0, len(requested))
	for _, name := range requested {
		if topic, ok := index[name]; ok {
			result = append(result, topic)
		} else {
			result = append(result, protocol.MetadataTopic{
				ErrorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
				Name:      name,
			})
		}
	}
	return result
}

func cloneMetadata(src ClusterMetadata) ClusterMetadata {
	return ClusterMetadata{
		Brokers:      cloneBrokers(src.Brokers),
		ControllerID: src.ControllerID,
		Topics:       cloneTopics(src.Topics),
		ClusterID:    cloneStringPtr(src.ClusterID),
	}
}

func cloneBrokers(brokers []protocol.MetadataBroker) []protocol.MetadataBroker {
	if len(brokers) == 0 {
		return nil
	}
	out := make([]protocol.MetadataBroker, len(brokers))
	copy(out, brokers)
	return out
}

func cloneTopics(topics []protocol.MetadataTopic) []protocol.MetadataTopic {
	if len(topics) == 0 {
		return nil
	}
	out := make([]protocol.MetadataTopic, len(topics))
	for i, topic := range topics {
		topicID := topic.TopicID
		if topicID == ([16]byte{}) {
			topicID = TopicIDForName(topic.Name)
		}
		out[i] = protocol.MetadataTopic{
			ErrorCode:                 topic.ErrorCode,
			Name:                      topic.Name,
			TopicID:                   topicID,
			IsInternal:                topic.IsInternal,
			Partitions:                clonePartitions(topic.Partitions),
			TopicAuthorizedOperations: topic.TopicAuthorizedOperations,
		}
	}
	return out
}

func clonePartitions(parts []protocol.MetadataPartition) []protocol.MetadataPartition {
	if len(parts) == 0 {
		return nil
	}
	out := make([]protocol.MetadataPartition, len(parts))
	for i, part := range parts {
		out[i] = protocol.MetadataPartition{
			ErrorCode:       part.ErrorCode,
			PartitionIndex:  part.PartitionIndex,
			LeaderID:        part.LeaderID,
			LeaderEpoch:     part.LeaderEpoch,
			ReplicaNodes:    cloneInt32Slice(part.ReplicaNodes),
			ISRNodes:        cloneInt32Slice(part.ISRNodes),
			OfflineReplicas: cloneInt32Slice(part.OfflineReplicas),
		}
	}
	return out
}

func cloneInt32Slice(src []int32) []int32 {
	if len(src) == 0 {
		return nil
	}
	out := make([]int32, len(src))
	copy(out, src)
	return out
}

func cloneStringPtr(s *string) *string {
	if s == nil {
		return nil
	}
	c := *s
	return &c
}

// NextOffset implements Store.NextOffset.
func (s *InMemoryStore) NextOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !topicHasPartition(s.state.Topics, topic, partition) {
		return 0, ErrUnknownTopic
	}
	return s.offsets[partitionKey(topic, partition)], nil
}

// UpdateOffsets implements Store.UpdateOffsets.
func (s *InMemoryStore) UpdateOffsets(ctx context.Context, topic string, partition int32, lastOffset int64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[partitionKey(topic, partition)] = lastOffset + 1
	return nil
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}

func consumerKey(group, topic string, partition int32) string {
	return fmt.Sprintf("%s:%s:%d", group, topic, partition)
}

// CreateTopic implements Store.CreateTopic.
func (s *InMemoryStore) CreateTopic(ctx context.Context, spec TopicSpec) (*protocol.MetadataTopic, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if spec.Name == "" || spec.NumPartitions <= 0 {
		return nil, ErrInvalidTopic
	}
	if spec.ReplicationFactor <= 0 {
		spec.ReplicationFactor = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, topic := range s.state.Topics {
		if topic.Name == spec.Name {
			return nil, ErrTopicExists
		}
	}
	if int(spec.ReplicationFactor) > len(s.state.Brokers) {
		return nil, ErrInvalidTopic
	}
	leaderID := s.defaultLeaderID()
	partitions := make([]protocol.MetadataPartition, spec.NumPartitions)
	for i := range partitions {
		partitions[i] = protocol.MetadataPartition{
			PartitionIndex: int32(i),
			LeaderID:       leaderID,
			ReplicaNodes:   []int32{leaderID},
			ISRNodes:       []int32{leaderID},
		}
	}
	newTopic := protocol.MetadataTopic{
		Name:       spec.Name,
		TopicID:    TopicIDForName(spec.Name),
		IsInternal: false,
		Partitions: partitions,
	}
	s.state.Topics = append(s.state.Topics, newTopic)
	return &newTopic, nil
}

func topicHasPartition(topics []protocol.MetadataTopic, name string, partition int32) bool {
	for _, topic := range topics {
		if topic.Name != name {
			continue
		}
		for _, part := range topic.Partitions {
			if part.PartitionIndex == partition {
				return true
			}
		}
		return false
	}
	return false
}

func (s *InMemoryStore) defaultLeaderID() int32 {
	if len(s.state.Brokers) == 0 {
		return s.state.ControllerID
	}
	return s.state.Brokers[0].NodeID
}

// DeleteTopic implements Store.DeleteTopic.
func (s *InMemoryStore) DeleteTopic(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	index := -1
	for i, topic := range s.state.Topics {
		if topic.Name == name {
			index = i
			break
		}
	}
	if index == -1 {
		return ErrUnknownTopic
	}
	s.state.Topics = append(s.state.Topics[:index], s.state.Topics[index+1:]...)
	for key := range s.offsets {
		if strings.HasPrefix(key, name+":") {
			delete(s.offsets, key)
		}
	}
	return nil
}

// CommitConsumerOffset implements Store.CommitConsumerOffset.
func (s *InMemoryStore) CommitConsumerOffset(ctx context.Context, group, topic string, partition int32, offset int64, metadata string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := consumerKey(group, topic, partition)
	s.consumerOffsets[key] = offset
	s.consumerMeta[key] = metadata
	return nil
}

// FetchConsumerOffset implements Store.FetchConsumerOffset.
func (s *InMemoryStore) FetchConsumerOffset(ctx context.Context, group, topic string, partition int32) (int64, string, error) {
	select {
	case <-ctx.Done():
		return 0, "", ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := consumerKey(group, topic, partition)
	return s.consumerOffsets[key], s.consumerMeta[key], nil
}

// PutConsumerGroup implements Store.PutConsumerGroup.
func (s *InMemoryStore) PutConsumerGroup(ctx context.Context, group *metadatapb.ConsumerGroup) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if group == nil || group.GroupId == "" {
		return errors.New("consumer group id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumerGroups[group.GroupId] = cloneConsumerGroup(group)
	return nil
}

// FetchConsumerGroup implements Store.FetchConsumerGroup.
func (s *InMemoryStore) FetchConsumerGroup(ctx context.Context, groupID string) (*metadatapb.ConsumerGroup, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if group, ok := s.consumerGroups[groupID]; ok {
		return cloneConsumerGroup(group), nil
	}
	return nil, nil
}

// DeleteConsumerGroup implements Store.DeleteConsumerGroup.
func (s *InMemoryStore) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.consumerGroups, groupID)
	return nil
}

func cloneConsumerGroup(group *metadatapb.ConsumerGroup) *metadatapb.ConsumerGroup {
	if group == nil {
		return nil
	}
	out := &metadatapb.ConsumerGroup{
		GroupId:      group.GroupId,
		State:        group.State,
		ProtocolType: group.ProtocolType,
		Protocol:     group.Protocol,
		Leader:       group.Leader,
		GenerationId: group.GenerationId,
		Members:      make(map[string]*metadatapb.GroupMember, len(group.Members)),
	}
	for memberID, member := range group.Members {
		cloned := &metadatapb.GroupMember{
			ClientId:      member.ClientId,
			ClientHost:    member.ClientHost,
			HeartbeatAt:   member.HeartbeatAt,
			Subscriptions: append([]string(nil), member.Subscriptions...),
		}
		if len(member.Assignments) > 0 {
			cloned.Assignments = make([]*metadatapb.Assignment, 0, len(member.Assignments))
			for _, assignment := range member.Assignments {
				cloned.Assignments = append(cloned.Assignments, &metadatapb.Assignment{
					Topic:      assignment.Topic,
					Partitions: append([]int32(nil), assignment.Partitions...),
				})
			}
		}
		out.Members[memberID] = cloned
	}
	return out
}

var (
	// ErrStoreUnavailable is returned when the metadata store cannot be reached.
	ErrStoreUnavailable = errors.New("metadata store unavailable")
)
