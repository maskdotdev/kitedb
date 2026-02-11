#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORKFLOW="$ROOT_DIR/.github/workflows/ray-rs.yml"

if [[ ! -f "$WORKFLOW" ]]; then
  echo "failed: workflow not found at $WORKFLOW"
  exit 1
fi

# Guardrail: ring's aarch64 assembly build in maturin needs __ARM_ARCH defined
# in this workflow's cross-compile path.
if ! awk '
  /^  build-python-linux:/ {in_job=1}
  in_job && /^  [^ ]/ && $0 !~ /^  build-python-linux:/ {in_job=0}
  in_job && /CFLAGS_aarch64_unknown_linux_gnu:[[:space:]]*-D__ARM_ARCH=8/ {found=1}
  END {exit found ? 0 : 1}
' "$WORKFLOW"; then
  echo "failed: missing CFLAGS_aarch64_unknown_linux_gnu=-D__ARM_ARCH=8 in build-python-linux job"
  exit 1
fi

echo "pass: linux aarch64 ring guard is configured"
