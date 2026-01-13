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

package sql

import "testing"

func TestParseShowTopics(t *testing.T) {
	q, err := Parse("SHOW TOPICS;")
	if err != nil {
		t.Fatalf("expected show topics, got error: %v", err)
	}
	if q.Type != QueryShowTopics {
		t.Fatalf("expected show topics, got %v", q.Type)
	}
}

func TestParseShowPartitions(t *testing.T) {
	q, err := Parse("SHOW PARTITIONS FROM orders;")
	if err != nil {
		t.Fatalf("expected show partitions, got error: %v", err)
	}
	if q.Type != QueryShowPartitions || q.Topic != "orders" {
		t.Fatalf("unexpected query: %+v", q)
	}
}

func TestParseSelectWithFilters(t *testing.T) {
	q, err := Parse("SELECT _offset FROM orders WHERE _partition = 2 AND _offset >= 10 LIMIT 5;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if q.Type != QuerySelect || q.Topic != "orders" {
		t.Fatalf("unexpected query: %+v", q)
	}
	if q.Partition == nil || *q.Partition != 2 {
		t.Fatalf("expected partition filter, got %+v", q.Partition)
	}
	if q.OffsetMin == nil || *q.OffsetMin != 10 {
		t.Fatalf("expected offset min, got %+v", q.OffsetMin)
	}
	if q.Limit != "5" {
		t.Fatalf("expected limit 5, got %q", q.Limit)
	}
}

func TestParseSelectScanFull(t *testing.T) {
	q, err := Parse("SELECT * FROM orders SCAN FULL LIMIT 10;")
	if err != nil {
		t.Fatalf("expected select, got error: %v", err)
	}
	if !q.ScanFull {
		t.Fatalf("expected scan full flag")
	}
}

func TestParseSelectJoin(t *testing.T) {
	q, err := Parse("SELECT * FROM orders JOIN payments WITHIN 10m LAST 1h;")
	if err != nil {
		t.Fatalf("expected select join, got error: %v", err)
	}
	if q.JoinTopic != "payments" || q.JoinType != "inner" {
		t.Fatalf("unexpected join: %+v", q)
	}
	if q.TimeWindow != "10m" || q.Last != "1h" {
		t.Fatalf("unexpected time bounds: %+v", q)
	}
}

func TestParseInvalid(t *testing.T) {
	if _, err := Parse("INSERT INTO orders VALUES (1);"); err == nil {
		t.Fatalf("expected error for unsupported statement")
	}
}
