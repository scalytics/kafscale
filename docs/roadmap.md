<!--
Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Kafscale Roadmap

This roadmap tracks completed work and open gaps. It is intentionally high level; detailed specs live in `kafscale-spec.md`.

## Milestones (Completed)

- Core protocol parsing and metadata support
- Produce and fetch paths with S3-backed durability
- Consumer group coordination with offset and group persistence
- Kubernetes operator with managed etcd + snapshot backups
- End-to-end tests for broker durability and operator resilience

## Milestones (Open)

### Admin Operations

- etcd topic/partition management

### Ops and Scaling APIs

- DescribeGroups/ListGroups (ops visibility)
- OffsetForLeaderEpoch (consumer recovery)
- DescribeConfigs/AlterConfigs (runtime tuning)
- CreatePartitions (scale without recreation)
- DeleteGroups (group cleanup)

### Observability

- Structured logging
- Grafana dashboard templates
- Expanded Prometheus metrics

### Testing and Hardening

- Performance benchmarks
- Multi-segment restart durability e2e
- Security review (TLS/auth)

## Gap Backlog (Open)

- Implement Milestone 6.5 Ops APIs
- Expand Prometheus metrics to match observability spec
- E2E: multi-segment restart durability
