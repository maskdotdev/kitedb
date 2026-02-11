#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "== Replication perf gate: commit overhead"
"$ROOT_DIR/scripts/replication-bench-gate.sh"

echo
echo "== Replication perf gate: replica catch-up throughput"
"$ROOT_DIR/scripts/replication-catchup-gate.sh"

echo
echo "pass: all replication perf gates satisfied"
