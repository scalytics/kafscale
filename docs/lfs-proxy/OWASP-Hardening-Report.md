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

# OWASP Hardening Report

This report tracks security inspections for publicly exposed endpoints. Update this file whenever a public endpoint is added or changed.

## Scope
Public endpoints include any externally reachable HTTP, gRPC, or Kafka endpoints exposed via LoadBalancer, Ingress, NodePort, or public DNS.

## Latest Review
- Date: 2026-02-02
- Reviewer: Codex
- Scope: LFS proxy HTTP, LFS proxy Kafka, health/metrics endpoints

## Findings (LFS Proxy)

### HTTP /lfs/produce
- Auth: Optional API key (when configured) — risk if exposed without key.
- Input validation: Topic name validation enforced.
- Integrity: Checksum algorithm configurable (sha256/md5/crc32/none); default sha256.
- Integrity: Checksum mismatch deletes uploaded object; orphan tracked if delete fails.
- Transport: Optional in-process TLS supported; otherwise rely on ingress/TLS termination.
- Size limits: Enforced via max blob size.
- Rate limiting: Not implemented.

### Kafka listener
- Auth: Supports SASL/PLAIN to backend brokers when configured.
- Transport: Supports TLS to backend brokers when configured.

### Health/Metrics
- Health endpoints exposed on separate port; typically internal only.
- Metrics endpoint public exposure should be avoided.

## Action Items
- Enforce auth for HTTP endpoint when public.
- Ensure TLS termination at ingress/load balancer.
- Add rate limiting or WAF if public.
- Avoid exposing health/metrics publicly.

SDKs are client-side only and do not introduce new public endpoints.
