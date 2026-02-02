# Security Tasks

This file tracks security hardening work for recent LFS proxy additions and provides a phased plan.

## Phase 0 - Baseline & Scope ✅ COMPLETE

### Current Defaults (values.yaml)

| Setting | Default | Risk | Location |
|---------|---------|------|----------|
| `lfsProxy.enabled` | `false` | ✅ Safe | values.yaml:142 |
| `lfsProxy.service.type` | `LoadBalancer` | ⚠️ **PUBLIC** | values.yaml:194 |
| `lfsProxy.http.enabled` | `true` | ⚠️ HTTP on | values.yaml:166 |
| `lfsProxy.http.apiKey` | `""` (empty) | ⚠️ **NO AUTH** | values.yaml:168 |
| `lfsProxy.s3.accessKey` | `""` (plaintext field) | ⚠️ No Secret | values.yaml:181 |
| `lfsProxy.s3.secretKey` | `""` (plaintext field) | ⚠️ No Secret | values.yaml:182 |

### Ports Exposed

| Port | Purpose | Exposed via Service |
|------|---------|---------------------|
| 9092 | Kafka protocol | Yes (LoadBalancer) |
| 8080 | HTTP /lfs/produce | Yes (when http.enabled) |
| 9094 | Health (/livez, /readyz) | No |
| 9095 | Metrics (/metrics) | No |

### Credential Injection

| Source | Method | Secure? |
|--------|--------|---------|
| Helm values.yaml | Plaintext `s3.accessKey`, `s3.secretKey` | ❌ No |
| Demo script (lfs-demo.sh) | Kubernetes Secret with `secretKeyRef` | ✅ Yes |

### HTTP Server Security

| Feature | Status | Risk |
|---------|--------|------|
| Read/Write timeouts | Not configured | ⚠️ Slowloris |
| API key comparison | Simple `==` | ⚠️ Timing attack |
| Topic header validation | None | ⚠️ Path injection |

Acceptance criteria:
- [x] Documented current defaults for HTTP/metrics/health and service type.
- [x] Confirmed how credentials are injected (values vs Secret) in Helm and demo.

## Phase 1 - High Priority (Default Hardening) ✅ COMPLETE

- [x] Require auth for HTTP LFS endpoint by default (disable HTTP or require `apiKey` when enabled).
- [x] Change default Service type to `ClusterIP` for LFS proxy (avoid public exposure).
- [x] Store S3/etcd credentials in Kubernetes Secrets and use `valueFrom` (avoid plaintext Helm values/env).

**Changes Made:**
- `values.yaml`: `lfsProxy.http.enabled` changed from `true` → `false`
- `values.yaml`: `lfsProxy.service.type` changed from `LoadBalancer` → `ClusterIP`
- `values.yaml`: Added `lfsProxy.s3.existingSecret` field for Secret-based credentials
- `lfs-proxy-deployment.yaml`: Added `secretKeyRef` support when `existingSecret` is set

Acceptance criteria:
- [x] Helm defaults: `lfsProxy.http.enabled=false` or enforce non-empty `apiKey`.
- [x] Helm defaults: `lfsProxy.service.type=ClusterIP`.
- [x] Helm templates support `existingSecret` (or create Secret) for S3 + etcd creds.
- [x] Demo script uses Secret for credentials, not inline values.

## Phase 2 - Medium Priority (Runtime Hardening) ✅ COMPLETE

- [x] Add HTTP server timeouts (read, header, write, idle) to mitigate slowloris.
- [x] Validate `X-Kafka-Topic` header (length + allowed charset) before building S3 key.
- [x] Track orphan objects on checksum mismatch and decide cleanup policy (delete or quarantine).

**Changes Made (http.go):**
- Added `ReadTimeout: 30s`, `WriteTimeout: 5m`, `IdleTimeout: 60s`, `MaxHeaderBytes: 1MB`
- Added `isValidTopicName()` function validating: 1-249 chars, alphanumeric/dots/underscores/hyphens only
- Returns 400 "invalid topic name" for malformed topics

Acceptance criteria:
- [x] HTTP server timeouts configured (sane defaults + env overrides).
- [x] Invalid topic header returns 400 with clear error message.
- [x] Checksum mismatch increments orphan metric and logs orphan key (already implemented).

## Phase 3 - Low Priority (Data Hygiene) ✅ PARTIAL

- [ ] Consider redacting `OriginalHeaders` in the envelope or allowlist safe headers.
- [x] Use constant-time compare for API key validation.

**Changes Made (http.go):**
- `validateHTTPAPIKey()` now uses `subtle.ConstantTimeCompare()` instead of `==`

Acceptance criteria:
- [ ] Envelope header policy documented and enforced (allowlist or redaction).
- [x] API key comparison uses constant-time function.

## Phase 4 - Future Enhancements

- Add TLS/SASL options for Kafka backend connections.
- Support TLS for HTTP endpoint (or enforce ingress termination).

Acceptance criteria:
- Documented TLS/SASL config options and examples.
- Integration test or manual recipe confirming TLS endpoint works.
