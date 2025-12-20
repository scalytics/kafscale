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

# Contributing to Kafscale

Thanks for your interest in Kafscale! This project follows Apache 2.0 conventions for licensing and contributions.

## Before You Start

- Use the issue tracker to discuss larger changes.
- Keep changes focused and add tests for non-trivial logic.

## License and Headers

All source and documentation files must include the Apache 2.0 license header used in this repo. Generated files under `pkg/gen/` are exempt and should not be edited manually. JSON files under `docs/grafana/` are excluded because the format does not support comments.

The CI workflow checks for missing headers and fails if new files are added without them.

## Tests and Coverage

Pull requests must include strict test coverage for the changes they introduce. At a minimum:

- Add or extend unit tests for all non-trivial logic.
- Run the relevant e2e suite(s). Broker changes should run:
  - `make test-produce-consume`
  - `make test-consumer-group` (if group behavior is affected)
- Extend e2e coverage when fixing bugs so regressions are caught earlier.

CI will run `go test ./...` and enforce a coverage floor.

## Development Workflow

See `docs/development.md` for build/test commands, environment variables, and local setup.

## Code of Conduct

By participating, you agree to follow `CODE_OF_CONDUCT.md`.
