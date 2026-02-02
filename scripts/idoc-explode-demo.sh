#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUTPUT_DIR=${KAFSCALE_IDOC_OUTPUT_DIR:-"${ROOT_DIR}/.build/idoc-output"}
INPUT_XML=${KAFSCALE_IDOC_INPUT_XML:-"${ROOT_DIR}/examples/tasks/LFS/idoc-sample.xml"}

mkdir -p "${OUTPUT_DIR}"

export KAFSCALE_IDOC_OUTPUT_DIR="${OUTPUT_DIR}"

"${ROOT_DIR}/bin/idoc-explode" -input "${INPUT_XML}"

echo "IDoc explode output written to ${OUTPUT_DIR}"
ls -1 "${OUTPUT_DIR}" | sed 's/^/ - /'
