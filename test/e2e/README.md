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

# Kafscale E2E Harness

These tests spin up a full cluster (via [kind](https://kind.sigs.k8s.io)), install the local Helm chart, and hit the console API over a port-forward. They are opt-in because they require Docker/kind/helm on the host and take several minutes.

## Prerequisites

1. Docker daemon (Colima, Docker Desktop, etc.)
2. `kind`, `kubectl`, and `helm` binaries on your `$PATH`
3. Internet access to pull the Bitnami `etcd` chart (the harness installs a single-node etcd for the operator)

## Running

```bash
KAFSCALE_E2E=1 go test -tags=e2e ./test/e2e -v
```

Optional environment variables:

- `KAFSCALE_KIND_CLUSTER`: reuse an existing kind cluster without creating/deleting one.

The harness installs everything into the `kafscale-e2e` namespace and removes it after the test (unless you reused a cluster).
