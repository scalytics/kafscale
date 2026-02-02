# Security Tasks

This file tracks security hardening work for recent LFS proxy additions and provides a phased plan.

## Phase 0 - Baseline & Scope

- Inventory current exposure: list enabled services, service types, and ingress paths for lfs-proxy.
- Confirm current S3/etcd credential sources in Helm values and demo scripts.
- Record default envs and ports for lfs-proxy in a quick reference section.

Acceptance criteria:
- Documented current defaults for HTTP/metrics/health and service type.
- Confirmed how credentials are injected (values vs Secret) in Helm and demo.

## Phase 1 - High Priority (Default Hardening)

- Require auth for HTTP LFS endpoint by default (disable HTTP or require `apiKey` when enabled).
- Change default Service type to `ClusterIP` for LFS proxy (avoid public exposure).
- Store S3/etcd credentials in Kubernetes Secrets and use `valueFrom` (avoid plaintext Helm values/env).

Acceptance criteria:
- Helm defaults: `lfsProxy.http.enabled=false` or enforce non-empty `apiKey`.
- Helm defaults: `lfsProxy.service.type=ClusterIP`.
- Helm templates support `existingSecret` (or create Secret) for S3 + etcd creds.
- Demo script uses Secret for credentials, not inline values.

## Phase 2 - Medium Priority (Runtime Hardening)

- Add HTTP server timeouts (read, header, write, idle) to mitigate slowloris.
- Validate `X-Kafka-Topic` header (length + allowed charset) before building S3 key.
- Track orphan objects on checksum mismatch and decide cleanup policy (delete or quarantine).

Acceptance criteria:
- HTTP server timeouts configured (sane defaults + env overrides).
- Invalid topic header returns 400 with clear error message.
- Checksum mismatch increments orphan metric and logs orphan key (and optional delete).

## Phase 3 - Low Priority (Data Hygiene)

- Consider redacting `OriginalHeaders` in the envelope or allowlist safe headers.
- Use constant-time compare for API key validation.

Acceptance criteria:
- Envelope header policy documented and enforced (allowlist or redaction).
- API key comparison uses constant-time function.

## Phase 4 - Future Enhancements

- Add TLS/SASL options for Kafka backend connections.
- Support TLS for HTTP endpoint (or enforce ingress termination).

Acceptance criteria:
- Documented TLS/SASL config options and examples.
- Integration test or manual recipe confirming TLS endpoint works.
