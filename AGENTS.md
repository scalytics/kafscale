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

# CODEX AGENT CONTRACT – Go Project

## 1. Stack Context
- Language: Go 1.22+
- API: Kubernetes CRDs (`api/v1alpha1`), no OpenAPI generator in repo today
- Platform: Kubernetes via Helm on kind
- Messaging: Kafka with KafScale LFS
- Testing: Go test + table-driven tests

## 2. Contract-First Rule
- Source of truth is `api/v1alpha1` CRD types and protobufs in `proto/`
- Never invent fields or endpoints
- If API changes are needed, update CRD types/proto first and call out breaking changes

## 2.5 Configuration Rules
- All runtime properties must be overridable by environment variables.
- Avoid hardcoded non-configurable values in LFS-related code.
- Every env var must be listed with defaults in `.env.example`.

## 2.6 Swagger/OpenAPI Rules
- When a new endpoint is added or changed, update `openapi.yaml` first.
- Keep request/response data types consistent across spec and implementation.
- For every endpoint change, add or update tests that exercise request handling.
- Align `openapi.yaml`, endpoint behavior, and tests for each change.

## 2.7 OWASP Hardening Report Rule
- For each publicly available endpoint, perform a security inspection and update `docs/lfs-proxy/OWASP-Hardening-Report.md`.
- Update the report whenever public endpoints change.

## 2.8 Architecture Sync Rule
- For each release, create or update the architecture chart.
- Keep the architecture chart in sync when new code is generated.
- Keep specs, solution proposals, and architecture docs in sync on each change.
- Keep `docs/lfs-proxy/README.md` in sync with the LFS proxy docs set.

## 2.9 Examples Sync Rule
- Review all examples and keep them in sync with specs and docs on each change.

## 3. Go Coding Rules

### 3.1 Structure
- context.Context is first parameter
- no global state
- explicit error wrapping
- interfaces in domain layer
- table-driven tests

### 3.2 Style
- Standard library preferred
- Router: net/http `ServeMux` (no chi/fiber in repo)
- Structured logging: `log/slog` for broker/proxy/MCP; zap for operator/controller-runtime; std `log` in console
- dependency injection via constructors

### 3.3 Error Handling
- Use `%w` wrapping
- Domain errors as types
- No panic for flow control

### 3.4 Example Pattern
```go
func (s *Service) Process(ctx context.Context, req Request) (Result, error) {
    if err := validate(req); err != nil {
        return Result{}, fmt.Errorf("validate: %w", err)
    }

    res, err := s.repo.Save(ctx, req)
    if err != nil {
        return Result{}, fmt.Errorf("repo: %w", err)
    }

    return res, nil
}
```

## 4. Helm & Kind
- Target: kind only
- Config from values.schema.json
- Never hardcode resources
- Always propose kubectl diff
- No cloud services

## 5. Kafka LFS Rules

Messages >1MB must:
- include LFS_BLOB header
- include checksum header
- include content-type
- be idempotent
- allow replay

## 6. Generated Code Boundaries

Codex MAY generate:
- CRD types or protobufs (if explicitly requested)
- DTOs
- Helm templates
- Tests
- Kind configs

Codex MUST NOT generate:
- credentials
- IAM/RBAC
- encryption algorithms
- business decisions

## 7. Workflow
1. Validate CRD/proto
2. Generate Go
3. Implement domain
4. Helm lint
5. Kind deploy

## 8. Review Checklist
- context used correctly
- no globals
- API compatibility
- LFS headers present
- helm schema aligned

## 9. Decision & Alternatives
- When asked for feasibility, provide a success probability (0–100%) and brief rationale.
- Always state the current blocker explicitly.
- If a primary approach is blocked or high-risk, propose at least one viable alternative that achieves the same outcome.

---

## How to Use in Practice

### Start every session with:
> "Follow rules from AGENTS.md"

### Typical prompts
- "Generate server from api/v1alpha1 using agent rules"
- "Create Helm chart respecting AGENTS.md"
- "Write LFS producer per agent contract"

---

## Optional Strict Mode

MODE: STRICT
- Ask before adding dependencies
- Show diff before edits
- Explain deviations

---

## Tailoring Questions

Tell me:
1. Your router (chi / fiber / std?)
2. Logging lib
3. Folder layout
4. oapi-codegen or openapi-generator
