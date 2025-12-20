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

package operator

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kafscalev1alpha1 "github.com/novatechflow/kafscale/api/v1alpha1"
)

func TestBuildClusterMetadata(t *testing.T) {
	replicas := int32(3)
	cluster := &kafscalev1alpha1.KafscaleCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "prod",
			Namespace: "kafscale",
			UID:       "cluster-uid",
		},
		Spec: kafscalev1alpha1.KafscaleClusterSpec{
			Brokers: kafscalev1alpha1.BrokerSpec{
				Replicas: &replicas,
			},
			S3: kafscalev1alpha1.S3Spec{
				Bucket: "test",
				Region: "us-east-1",
			},
			Etcd: kafscalev1alpha1.EtcdSpec{
				Endpoints: []string{"http://localhost:2379"},
			},
		},
	}

	topics := []kafscalev1alpha1.KafscaleTopic{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "orders"},
			Spec: kafscalev1alpha1.KafscaleTopicSpec{
				ClusterRef: "prod",
				Partitions: 2,
			},
		},
	}

	meta := BuildClusterMetadata(cluster, topics)

	if len(meta.Brokers) != int(replicas) {
		t.Fatalf("expected %d brokers, got %d", replicas, len(meta.Brokers))
	}
	if meta.ClusterID == nil || *meta.ClusterID != "cluster-uid" {
		t.Fatalf("unexpected cluster id: %v", meta.ClusterID)
	}
	if len(meta.Topics) != 1 || meta.Topics[0].Name != "orders" {
		t.Fatalf("expected orders topic, got %+v", meta.Topics)
	}
	if len(meta.Topics[0].Partitions) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(meta.Topics[0].Partitions))
	}
	for _, part := range meta.Topics[0].Partitions {
		if len(part.ReplicaNodes) != int(replicas) {
			t.Fatalf("partition %+v replica mismatch", part)
		}
		if len(part.ISRNodes) != len(part.ReplicaNodes) {
			t.Fatalf("partition %+v ISR mismatch", part)
		}
	}
}
