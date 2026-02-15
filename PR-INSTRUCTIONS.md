# PR Submission Instructions

## Prerequisites — Local Testing

Before pushing, run these checks locally:

```bash
# 1. Build the LFS proxy
go build ./cmd/lfs-proxy/

# 2. Run unit tests
go test ./pkg/lfs/...
go test ./cmd/lfs-proxy/...
go test ./internal/console/...
go test ./pkg/idoc/...
go test ./pkg/operator/...

# 3. Run e2e tests (requires MinIO + Kafka + etcd)
go test ./test/e2e/... -tags e2e -count=1

# 4. Verify the examples branch has no Go dependency issues
git checkout pr/kafscale-101-examples
# (this branch has no Go code changes beyond examples — should be clean)
git checkout pr/lfs-feature
```

---

## Step 1: Add the upstream remote

```bash
git remote add upstream https://github.com/KafScale/platform.git
git fetch upstream main
```

## Step 2: Rebase both branches onto upstream/main

```bash
git checkout pr/kafscale-101-examples
git rebase upstream/main

git checkout pr/lfs-feature
git rebase upstream/main
```

Resolve any conflicts if upstream/main has diverged from origin/main.

## Step 3: Push branches to your fork

```bash
git push kamir pr/kafscale-101-examples
git push kamir pr/lfs-feature
```

## Step 4: Create the PRs

### PR 1: KafScale 101 Examples

```bash
gh pr create \
  --repo KafScale/platform \
  --head kamir:pr/kafscale-101-examples \
  --base main \
  --title "Add KafScale 101 developer guide and example collection" \
  --body "$(cat <<'EOF'
## Summary

- Add **KafScale 101 developer guide** (8-chapter tutorial covering introduction, quick-start, Spring Boot configuration, Flink/Spark demos, and troubleshooting)
- Add **5 example applications** demonstrating KafScale integration with common frameworks:
  - E10: Java Kafka client demo
  - E20: Spring Boot demo (REST → Kafka producer/consumer with web UI)
  - E30: Apache Flink WordCount demo (Docker, K8s, and standalone modes)
  - E40: Apache Spark WordCount demo
  - E50: JavaScript/Node.js demo (browser UI + LLM agent integration)
- Add **deploy/demo/** Kubernetes manifests (namespace, cluster, topics, MinIO, Spring Boot app, Flink app, Nginx LB, S3 credentials, Kafka client pod)
- Add example improvement task files and quality assessment

## Scope

91 files, 12,876 lines added. Documentation and examples only — no changes to core platform code.

## Test plan

- [ ] Verify dev guide renders correctly on GitHub (markdown, images)
- [ ] Verify E10 Java demo compiles: `cd examples/E10_java-kafka-client-demo && mvn compile`
- [ ] Verify E20 Spring Boot demo compiles: `cd examples/E20_spring-boot-kafscale-demo && mvn compile`
- [ ] Verify E30 Flink demo compiles: `cd examples/E30_flink-kafscale-demo && mvn compile`
- [ ] Verify E40 Spark demo compiles: `cd examples/E40_spark-kafscale-demo && mvn compile`
- [ ] Verify E50 JS demo installs: `cd examples/E50_JS-kafscale-demo && npm install`
- [ ] Verify deploy/demo manifests are valid: `kubectl apply --dry-run=client -f deploy/demo/`
EOF
)"
```

### PR 2: LFS Feature

```bash
gh pr create \
  --repo KafScale/platform \
  --head kamir:pr/lfs-feature \
  --base main \
  --title "Add Large File Support (LFS) proxy and SDK" \
  --body "$(cat <<'EOF'
## Summary

Add Large File Support (LFS) to KafScale, enabling Kafka topics to reference arbitrarily large objects stored in S3-compatible storage. This PR includes:

### Core components
- **LFS proxy** (`cmd/lfs-proxy/`) — a TCP+HTTP proxy that intercepts Kafka produce requests, uploads large payloads to S3, and replaces them with lightweight envelope records containing S3 references
- **LFS Go library** (`pkg/lfs/`) — envelope encoding/decoding, producer/consumer wrappers, S3 resolver, checksum verification
- **IDoc exploder** (`cmd/idoc-explode/`, `pkg/idoc/`) — splits SAP IDoc XML documents into individual records for LFS ingestion

### Client SDKs
- **Java SDK** (`lfs-client-sdk/java/`) — LfsProducer, LfsConsumer, LfsResolver, LfsCodec with full S3 integration
- **Python SDK** (`lfs-client-sdk/python/`) — envelope parsing, HTTP producer, S3 resolver
- **JavaScript SDK** (`lfs-client-sdk/js/`) — Node.js envelope + producer + resolver
- **Browser SDK** (`lfs-client-sdk/js-browser/`) — browser-compatible TypeScript SDK for chunked uploads

### Infrastructure
- Helm chart templates for LFS proxy deployment, service, ingress, metrics, alerting
- Docker Compose setup for local development
- CRD type updates (`api/v1alpha1/`) and operator resource generation (`pkg/operator/lfs_proxy_resources.go`)
- CI workflow updates and stage-release workflow
- OpenAPI 3.0 specification (`api/lfs-proxy/openapi.yaml`)

### Console integration
- LFS consumer, S3 client, and handler extensions for the web console (`internal/console/`)

### Testing
- 12 test files covering e2e proxy, HTTP API, SDK, etcd integration, and Iceberg processor integration
- Unit tests for envelope, producer, consumer, resolver, checksum, record, codec, SASL, TLS, tracker, and HTTP handlers

### Security hardening (included in this PR)
- **Path traversal protection**: `validateObjectKey()` rejects URL-encoded traversal (`%2e%2e`, `%2f`) and canonicalizes with `path.Clean()`
- **CORS restriction**: replaced wildcard `Access-Control-Allow-Origin: *` with configurable origins via `KAFSCALE_LFS_CORS_ORIGINS` (deny-by-default)
- **Upload session cap**: configurable `KAFSCALE_LFS_PROXY_MAX_UPLOAD_SESSIONS` (default 1000) prevents session exhaustion
- **Context cancellation**: `UploadStream()` checks `ctx.Err()` each iteration, aborting stuck uploads promptly
- **Bug fix**: `getClientIP()` corrected `strings.Cut` return type (`bool`, not `error`)
- **Error sanitization**: raw S3/backend errors no longer leak to HTTP clients; details logged server-side only
- **TLS warning**: startup warns when API key is set without TLS

### Documentation
- LFS proxy README, data flow, Helm deployment guide, SDK roadmap, OWASP hardening report, traceability, demos guide, security tasks, broker deep-dive, IDoc architecture, blob transformer proposal

## Scope

216 files changed, 41,742 insertions, 218 deletions.

## Test plan

- [ ] `go build ./cmd/lfs-proxy/` — proxy compiles
- [ ] `go build ./cmd/idoc-explode/` — IDoc exploder compiles
- [ ] `go test ./pkg/lfs/...` — LFS library unit tests pass
- [ ] `go test ./cmd/lfs-proxy/...` — proxy unit tests pass (HTTP handlers, SASL, TLS, tracker)
- [ ] `go test ./internal/console/...` — console LFS handler tests pass
- [ ] `go test ./pkg/idoc/...` — IDoc tests pass
- [ ] `go test ./pkg/operator/...` — operator tests pass (LFS proxy resource generation)
- [ ] `go test ./test/e2e/... -tags e2e` — e2e tests pass (requires MinIO + Kafka + etcd)
- [ ] `cd lfs-client-sdk/java && mvn test` — Java SDK tests pass
- [ ] `cd lfs-client-sdk/python && python -m pytest` — Python SDK tests pass
- [ ] `cd lfs-client-sdk/js && npm test` — JS SDK tests pass
- [ ] `kubectl apply --dry-run=client -f deploy/helm/kafscale/crds/` — CRDs are valid
- [ ] Verify `validateObjectKey()` rejects: `../etc/passwd`, `foo/%2e%2e/bar`, `foo/..%2fbar`
- [ ] Verify CORS returns no `Access-Control-Allow-Origin` header when `KAFSCALE_LFS_CORS_ORIGINS` is unset
- [ ] Verify upload session creation returns 429 when session limit is reached
- [ ] Helm template renders without errors: `helm template deploy/helm/kafscale/`
EOF
)"
```

---

## Quick reference: branch status

| Branch | Base | Files | Status |
|--------|------|-------|--------|
| `pr/kafscale-101-examples` | origin/main | 91 | Local, ready to push |
| `pr/lfs-feature` | origin/main | 216 | Local, ready to push |
| `pr/docuflow-agents` | origin/main | 40 | Local, parked (not submitting yet) |
