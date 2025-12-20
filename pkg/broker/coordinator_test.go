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

package broker

import (
	"testing"
	"time"
)

func TestConsumerGroupTimeoutPersistence(t *testing.T) {
	now := time.Now().UTC()
	state := &groupState{
		protocolName:     "range",
		protocolType:     "consumer",
		generationID:     2,
		leaderID:         "member-1",
		state:            groupStateStable,
		members:          make(map[string]*memberState),
		assignments:      make(map[string][]assignmentTopic),
		rebalanceTimeout: 45 * time.Second,
	}
	state.members["member-1"] = &memberState{
		topics:         []string{"orders"},
		sessionTimeout: 20 * time.Second,
		lastHeartbeat:  now,
		joinGeneration: 2,
	}

	group := buildConsumerGroup("group-1", state)
	if group.RebalanceTimeoutMs != 45000 {
		t.Fatalf("expected rebalance timeout 45000ms got %d", group.RebalanceTimeoutMs)
	}
	member := group.Members["member-1"]
	if member == nil {
		t.Fatalf("expected group member persisted")
	}
	if member.SessionTimeoutMs != 20000 {
		t.Fatalf("expected session timeout 20000ms got %d", member.SessionTimeoutMs)
	}

	restored := restoreGroupState(group)
	if restored.rebalanceTimeout != 45*time.Second {
		t.Fatalf("expected rebalance timeout 45s got %s", restored.rebalanceTimeout)
	}
	restoredMember := restored.members["member-1"]
	if restoredMember == nil {
		t.Fatalf("expected restored member")
	}
	if restoredMember.sessionTimeout != 20*time.Second {
		t.Fatalf("expected session timeout 20s got %s", restoredMember.sessionTimeout)
	}
}
