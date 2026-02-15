# LFS-Feature-Proposal — Cleanup & PR Preparation Plan

## Priority Order

The phases must run in this order because each unblocks the next.

---

## Phase 1: Clean Artifacts (FIRST — blocks everything)

Remove committed trash and update `.gitignore` so it never comes back.

### 1.1 Remove PID / runtime files (10 files)
```
test/e2e/.pf/broker.pid
test/e2e/.pf/lfs_http.pid
test/e2e/.pf/minio.pid
lfs-client-sdk/.pf/broker.pid
lfs-client-sdk/.pf/lfs_http.pid
lfs-client-sdk/.pf/minio.pid
examples/E70_java-lfs-sdk-demo/.tmp/port-forward-broker.pid
examples/E70_java-lfs-sdk-demo/.tmp/port-forward-lfs-proxy-health.pid
examples/E70_java-lfs-sdk-demo/.tmp/port-forward-lfs-proxy.pid
examples/E70_java-lfs-sdk-demo/.tmp/port-forward-minio.pid
```

### 1.2 Remove Python build artifacts (2 files)
```
lfs-client-sdk/python/dist/kafscale_lfs_sdk-0.1.0-py3-none-any.whl
lfs-client-sdk/python/dist/kafscale_lfs_sdk-0.1.0.tar.gz
```

### 1.3 Remove JS compiled output (10 files)
```
lfs-client-sdk/js/dist/__tests__/envelope.test.d.ts
lfs-client-sdk/js/dist/__tests__/envelope.test.js
lfs-client-sdk/js/dist/envelope.d.ts
lfs-client-sdk/js/dist/envelope.js
lfs-client-sdk/js/dist/index.d.ts
lfs-client-sdk/js/dist/index.js
lfs-client-sdk/js/dist/producer.d.ts
lfs-client-sdk/js/dist/producer.js
lfs-client-sdk/js/dist/resolver.d.ts
lfs-client-sdk/js/dist/resolver.js
```

### 1.4 Remove local config files (3 files)
```
Containers/group.com.docker/settings.json
lfs-client-sdk/java/.testcontainers.properties
lfs-client-sdk/java/src/test/resources/.testcontainers.properties
```

### 1.5 Update .gitignore
Add these patterns:
```
# Runtime artifacts
*.pid
.pf/
.tmp/

# Build artifacts
dist/
*.whl
*.tar.gz

# Local configs
.testcontainers.properties
Containers/
```

**Commit: "chore: remove committed artifacts and update .gitignore"**

---

## Phase 2: Fix Security Issues (SECOND — blocks PR acceptance)

These are real vulnerabilities that reviewers will reject.

### 2.1 CRITICAL: Harden `validateObjectKey()` in `cmd/lfs-proxy/http.go`
Current code only checks for `..` and leading `/`. Fix:
- Reject URL-encoded traversal (`%2e%2e`, `%2f`)
- Clean the path with `path.Clean()` and re-validate
- Verify the cleaned path still starts with the namespace

### 2.2 CRITICAL: Restrict CORS in `corsMiddleware()` in `cmd/lfs-proxy/http.go`
Current: `Access-Control-Allow-Origin: *`
Fix: Make CORS origin configurable via env var `KAFSCALE_LFS_CORS_ORIGINS` (default: deny all)

### 2.3 HIGH: Add upload session limits in `storeUploadSession()` in `cmd/lfs-proxy/http.go`
Current: No cap on session count.
Fix: Add `maxUploadSessions` field, reject new sessions when limit hit.

### 2.4 HIGH: Add `ctx.Done()` checks in `UploadStream()` in `cmd/lfs-proxy/s3.go`
Current: Upload continues after context cancellation.
Fix: Check `ctx.Err()` at the top of each loop iteration; abort multipart upload if cancelled.

### 2.5 HIGH: Fix `getClientIP()` bug in `cmd/lfs-proxy/http.go`
Current: `strings.Cut()` returns `(before, after, found bool)` — code treats the bool as `err`.
Fix: Use the correct return signature.

### 2.6 MEDIUM: Sanitize error messages in `writeHTTPError()` / `handleHTTPProduce()`
Current: Raw S3 error messages leak backend details to clients.
Fix: Map internal errors to generic client-facing messages; log details server-side only.

### 2.7 MEDIUM: Add TLS warning in `cmd/lfs-proxy/main.go`
Current: Silent fallback to plain HTTP when no TLS config.
Fix: Log a WARN when API key is set but TLS is not configured.

### 2.8 LOW: Replace hardcoded creds in `deploy/demo/s3-secret.yaml`
Replace plaintext `minioadmin` values with placeholder comments explaining users must supply their own.

**Commit: "security: harden LFS proxy (path traversal, CORS, session limits, error sanitization)"**

---

## Phase 3: Split the Branch (THIRD — makes PRs reviewable)

Split 63 commits / 375 files into 3 focused PRs.

### PR 1: "Add KafScale 101 developer guide and example collection"
Scope (~120 files):
```
examples/101_kafscale-dev-guide/
examples/E10_java-kafka-client-demo/
examples/E20_spring-boot-kafscale-demo/
examples/E30_flink-kafscale-demo/
examples/E40_spark-kafscale-demo/
examples/E50_JS-kafscale-demo/
examples/HowTo/
examples/Makefile
examples/REVIEW.md
examples/tasks/E10-improvements.md
examples/tasks/E20-improvements.md
examples/tasks/E30-improvements.md
examples/tasks/E40-improvements.md
examples/tasks/E50-improvements.md
examples/tasks/QUALITY-ASSESSMENT.md
examples/tasks/README.md
deploy/demo/ (base manifests, not LFS-specific)
```

### PR 2: "Add LFS (Large File Support) feature" — the main PR
Scope (~210 files):
```
cmd/lfs-proxy/          — LFS proxy server
cmd/idoc-explode/       — IDoc exploder
cmd/console/main.go     — Console with LFS
cmd/e2e-client/main.go  — E2E client updates
pkg/lfs/                — Core LFS library
pkg/idoc/               — IDoc package
pkg/operator/lfs_proxy_resources.go
pkg/operator/cluster_controller.go (LFS additions)
pkg/operator/cluster_controller_test.go
pkg/gen/                — Generated protobuf updates
api/v1alpha1/           — CRD type updates
api/lfs-proxy/          — OpenAPI spec
internal/console/lfs_*  — Console LFS handlers
lfs-client-sdk/         — Java, Python, JS SDKs
deploy/helm/kafscale/   — Helm chart LFS templates
deploy/docker/          — LFS proxy Dockerfile
deploy/docker-compose/  — Docker Compose with LFS
deploy/ms23/            — Kind cluster setup
deploy/synology-s3/     — Synology deployment
docs/lfs-proxy/         — LFS documentation
examples/E60-E72        — LFS demo examples
examples/tasks/LFS/     — LFS planning docs
scripts/*lfs*           — LFS scripts
test/e2e/lfs_*          — LFS E2E tests
test/e2e/ports.go       — Test port allocation
.github/workflows/      — CI updates
go.mod, go.sum          — Dependency updates
Makefile                — LFS targets
.env.example            — Environment template
deploy/helm/kafscale/values.yaml
deploy/helm/kafscale/values-lfs-demo.yaml
deploy/helm/kafscale/crds/
addons/processors/iceberg-processor/ — Iceberg LFS integration
```

### PR 3: "Add documentation agents and publishing workflow" (optional)
Scope (~47 files):
```
examples/agents/        — Publishing agent prompts
examples/claims/        — Claims registry
examples/docuflow/      — Publishing pipeline
examples/review-output/ — Generated drafts
examples/publication-drafts/
AGENTS.md
Makefile-MK
```

### Git workflow for splitting:
From the cleaned LFS-Feature-Proposal branch:
1. Create `pr/kafscale-101-examples` — cherry-pick or `git checkout` only PR1 files from main
2. Create `pr/lfs-feature` — cherry-pick or checkout only PR2 files
3. Create `pr/docuflow-agents` — cherry-pick or checkout only PR3 files
4. Squash each branch into 4-6 logical commits

---

## Execution Summary

| Phase | Effort | Commits | Risk |
|-------|--------|---------|------|
| 1. Clean artifacts | ~15 min | 1 | None |
| 2. Fix security | ~60 min | 1 | Low (targeted fixes) |
| 3. Split branch | ~30 min | N/A (new branches) | Medium (git surgery) |

Total: ~2 hours of focused work.
