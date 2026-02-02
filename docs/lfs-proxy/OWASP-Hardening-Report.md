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
