<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
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

# LFS SDKs: Solution Design

This document defines the design, packaging, builds, and testing for LFS SDKs in Go, Java, JavaScript/TypeScript, and Python. SDKs are **client-side only** and do not introduce new public endpoints.

## Goals
- Wrap plain Kafka clients with LFS-aware helpers for producing and consuming large payloads.
- Provide consistent envelope parsing, checksum validation, and S3 resolution.
- Keep parity across Go, Java, JavaScript/TypeScript, and Python.

## Architecture Overview
SDKs expose two primary concerns:
- **Consumer helpers**: detect LFS envelope and resolve blob content from S3.
- **Producer helpers**: upload blob via LFS proxy HTTP endpoint and return envelope for Kafka produce.

Envelope schema and checksum behavior are shared across languages.

## Repository Layout
```
lfs-client-sdk/java/          # Maven module
lfs-client-sdk/js/            # Node package (TS)
lfs-client-sdk/python/        # PyPI-style package
pkg/lfs/               # Go SDK (existing)
```

## Packaging and Builds

### Go
- Module: `pkg/lfs/` (already implemented).
- Build: standard `go test ./pkg/lfs/...`.
- Docs/examples: `pkg/lfs/doc.go`.

### Java
- Package: `lfs-client-sdk/java` (Maven).
- Group/artifact: `org.kafscale:lfs-sdk`.
- Kafka dependency: `org.kafscale:kafka-clients`.
- S3 dependency: AWS SDK v2 (S3).
- Build/test: `mvn test`.

### JavaScript/TypeScript
- Package: `lfs-client-sdk/js` (npm).
- Name: `@kafscale/lfs-sdk`.
- Kafka: `node-rdkafka`.
- S3: AWS SDK v3.
- Build/test: `npm run build`, `npm test`.

### Python
- Package: `lfs-client-sdk/python` (PyPI).
- Kafka: `confluent-kafka`.
- S3: `boto3`.
- Build/test: `pytest`.

## Testing Strategy
- **Unit tests**: envelope parsing, checksum logic, resolver behavior.
- **Integration tests**: LFS proxy + MinIO for each language SDK.
- **E2E tests**: referenced from the main test suite (not required for SDKs alone).

## API Design (Language-Agnostic)
- `is_lfs_envelope(bytes)`
- `decode_envelope(bytes)`
- `resolve(record)` → returns payload + metadata
- `produce(topic, key, reader)` → calls HTTP `/lfs/produce` and returns envelope

## Notes
- SDKs are intended to be library-safe and optional to adopt.
- E70+ example series will be created in a **later milestone/branch**.
